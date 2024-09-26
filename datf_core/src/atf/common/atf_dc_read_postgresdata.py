from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig,initilize_dbutils


def read_postgresdata(tc_datasource_config, spark):
  log_info("Reading from PostGRESQL Table")

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
                                
  connectionurl= f"jdbc:postgresql://{host}:{port}/{database}"

  #connurl = f"{connectionconfig['url']}/{tc_datasource_config['filepath']}"
  resourcename = tc_datasource_config['filename']
  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)
  selectallcolqry = f"SELECT * FROM {resourcename} "
  if len(datafilter) > 0:
    selectallcolqry = selectallcolqry + datafilter

  df_postgresdata = (spark.read
                    .format("jdbc")
                    .option("driver", "org.postgresql.Driver") \
                    .option("url", connectionurl) \
                    .option("dbtable", resourcename) \
                    .option("user", username) \
                    .option("password", password) \
                    .load())
                    
  columns = df_postgresdata.columns
  columnlist = list(set(columns) - set(exclude_cols))
  columnlist.sort()

  columnlist_str = ','.join(columnlist)

  df_postgresdata.createOrReplaceTempView("postgresview")
  selectcolqry = "SELECT " + columnlist_str + " FROM postgresview"
  selectcolqry_ret = "SELECT " + columnlist_str + f" FROM {resourcename}"
  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show() 
  log_info("Returning the DataFrame from read_postgresdata Function")
  return df_out, selectcolqry_ret