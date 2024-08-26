from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig
from constants import *

def read_adls_parquetdata(tc_datasource_config,spark):
  log_info("Reading parquet Data from ADLS storage")
  resourcename = tc_datasource_config['aliasname']
  comparetype = tc_datasource_config['testquerygenerationmode']
  connectionname = tc_datasource_config['connectionname']
  connectionconfig = readconnectionconfig(connectionname)
  storage_account = connectionconfig['STORAGE_ACCOUNT_NAME']
  container_name = connectionconfig['CONTAINER_NAME']  # Assuming you have this in your config
  delta_path = tc_datasource_config['path']  # Relative path within the container
  sas_token = connectionconfig['SAS_TOKEN']  # Optional SAS token

  # Set the configuration using SAS Token
  spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
  spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
  spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)
  
  #Reading parquet from ADLS Stoarge Container
  log_info("Reading parquet file from ADLS")
  # datasrc_path = 'file:'+root_path+tc_datasource_config['path']
  # df = spark.read.parquet(datasrc_path)

  datasrc_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{delta_path}"
  print(datasrc_path)
  df = spark.read.parquet(datasrc_path)
  df.createOrReplaceTempView(tc_datasource_config['aliasname'])

  df.display()
  df.printSchema()
  
  if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
    pass      

  elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = root_path+tc_datasource_config['querypath']
    f = open(querypath,"r")
    query= f.read().splitlines()
    query=' '.join(query)
    print(query)

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
    print(query)
    
  df_data = spark.sql(query)
  df_data.printSchema()
  df_data.show()
  log_info("Returning the Source DataFrame and Query")
  
  return df_data, query