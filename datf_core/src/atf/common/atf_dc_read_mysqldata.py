from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig,initilize_dbutils

def read_mysqldata(tc_datasource_config,spark):
  log_info("Reading from Mysql Table")
  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  
  #Importing dbutils
  dbutils =  initilize_dbutils(spark)
  
  #Reading connection config from json file
  connectionconfig = readconnectionconfig(connectionname)

  #Fetching credentials from key vault
  username =  dbutils.secrets.get(scope="akv-mckesson-scope",  key= connectionconfig['user'])  
  password = dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['password'])
  host = dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['host'])
  port = dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['port'])
  database = dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['database'])
                                 
  connectionurl="jdbc:mysql://"+host+":"+port+"/"+database

  
  query="select * from "+ tc_datasource_config['filename']
  df = (spark.read
                  .format("jdbc")
                  .option("driver","com.mysql.jdbc.Driver")
                  .option("url", connectionurl)
                  .option("user", username)
                  .option("password", password)
                  .option("useSSL", "false") \
                  .option("ssl", "false") \
                  .option("query", query)
                  .load())
  columns = df.columns
  columnlist = list(set(columns) - set(tc_datasource_config['excludecolumns'].split(",")))
  columnlist.sort()
  df_data = df.select(columnlist)
  columnlist_str = ','.join(columnlist)
  query = "SELECT " + columnlist_str + f" FROM {tc_datasource_config['filename']}"
  df_data.printSchema()
  df_data.show()
  log_info("Returning the DataFrame from read_mysqldata Function")
  
  return df_data, query