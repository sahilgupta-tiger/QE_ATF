from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config,get_dbutils
from testconfig import *

def read_adls_parquetdata(tc_datasource_config,spark):
  log_info("Reading parquet Data from ADLS storage")
  resourcename = tc_datasource_config['aliasname']
  connectionname = tc_datasource_config['connectionname']

  #Reading Adls Connection Configuration
  connectionconfig = readconnectionconfig(connectionname)
  
  #Importing dbutils
  dbutils =  get_dbutils(spark)

  #Set Adls Connection Configuration
  set_azure_connection_config(spark,connectionconfig,dbutils)

  # Relative path within the container
  parquet_path = tc_datasource_config['path'] 

  if ('abfs' not in parquet_path) and ('Volume' not in parquet_path):
      log_info("Appending Root path to relative parquet file Path")
      parquet_path = root_path + parquet_path

  log_info(f'ADLS File Path :- {parquet_path}')

  df = spark.read.parquet(parquet_path)
  df.createOrReplaceTempView(tc_datasource_config['aliasname'])
  
  if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
    pass      

  elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = root_path+tc_datasource_config['querypath']
    f = open(querypath,"r")
    query= f.read().splitlines()
    query=' '.join(query)
    log_info(f"Select Table Command statement - \n{query}")

  elif tc_datasource_config['comparetype'] == 'likeobjectcompare':
    excludecolumns = tc_datasource_config['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = tc_datasource_config['filter']
    datafilter = str(datafilter)
    columns = df.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query= "SELECT " + columnlist + " FROM "+tc_datasource_config['aliasname']
    if len(datafilter) >=5:
      query= query + " WHERE " + datafilter
    log_info(f"Select Table Command statement - \n{query}")
    
  df_data = spark.sql(query)
  df_data.printSchema()
  #df_data.show()
  log_info("Returning the DataFrame from read_avrodata Function")
  
  return df_data, query