from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config,get_dbutils
from testconfig import *

def read_adls_delimiteddata(tc_datasource_config,spark):
  log_info("Reading delimited file from ADLS storage")
  resourcename = tc_datasource_config['aliasname']
  connectionname = tc_datasource_config['connectionname']
  delimiter =tc_datasource_config['delimiter']

  # Reading Adls Connection Configuration
  connectionconfig = readconnectionconfig(connectionname)
  
  #Importing dbutils
  dbutils =  get_dbutils(spark)

  #Set Adls Connection Configuration
  set_azure_connection_config(spark,connectionconfig,dbutils)
  
  delimited_path = tc_datasource_config['path']  # Relative path within the container

  if ('abfs' not in delimited_path) and ('Volume' not in delimited_path):
    log_info("Appending Root path to relative delimited file Path")
    delimited_path = root_path + delimited_path

  log_info(f'ADLS File Path :- {delimited_path}')

  if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
    pass  

  elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = root_path+tc_datasource_config['querypath']
    f = open(querypath,"r")
    query= f.read().splitlines()
    query=' '.join(query)
    df=spark.read.option("delimiter", delimiter).schema(tc_datasource_config['schemastruct']).csv(delimited_path, header = True)
    log_info(f"Select Table Command statement - \n{query}")

  elif tc_datasource_config['comparetype'] == 'likeobjectcompare':
    excludecolumns = tc_datasource_config['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = tc_datasource_config['filter']
    datafilter = str(datafilter)

    df=spark.read.option("delimiter", delimiter).csv(delimited_path, header = True)

    columns = df.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query= "SELECT " + columnlist + " FROM "+tc_datasource_config['aliasname']
    if len(datafilter) >=5:
      query= query + " WHERE " + datafilter
    log_info(f"Select Table Command statement - \n{query}")

  df.createOrReplaceTempView(tc_datasource_config['aliasname'])
  df_data = spark.sql(query)
  log_info("Returning the DataFrame from read_delimiteddata Function")

  return df_data, query
