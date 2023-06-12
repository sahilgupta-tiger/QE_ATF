
from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig

def read_mysqldata(tc_datasource_config,spark):
  log_info("Reading from Mysql Table")
  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  connectionconfig = readconnectionconfig(connectionname)
  connectionurl="jdbc:mysql://"+connectionconfig['host']+":"+connectionconfig['port']+"/"+connectionconfig['database']
  
  query="select * from "+ tc_datasource_config['filename']
  df = (spark.read
                  .format("jdbc")
                  .option("driver","com.mysql.jdbc.Driver")
                  .option("url", connectionurl)
                  .option("user", connectionconfig['user'])
                  .option("password", connectionconfig['password'])
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