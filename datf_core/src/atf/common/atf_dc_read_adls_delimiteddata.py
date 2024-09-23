from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config
from constants import *

def read_adls_delimiteddata(tc_datasource_config,spark):
  log_info("Reading delimited file from ADLS storage")
  resourcename = tc_datasource_config['aliasname']
  comparetype = tc_datasource_config['testquerygenerationmode']
  connectionname = tc_datasource_config['connectionname']

  # Reading Adls Connection Configuration
  connectionconfig = readconnectionconfig(connectionname)

  # Set Adls Connection Configuration
  storage_account, container_name = set_azure_connection_config(connectionconfig, spark)
  delimited_path = tc_datasource_config['path']  # Relative path within the container
  print('ADLS File Path :', delimited_path)

  connectiontype =tc_datasource_config['connectiontype']
  resourceformat =tc_datasource_config['format']
  delimiter =tc_datasource_config['delimiter']

  if tc_datasource_config['testquerygenerationmode'] == 'Auto':
    resourcename = tc_datasource_config['name']
    datafilter = tc_datasource_config['filter']
    excludecolumns =tc_datasource_config['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)
    df = spark.read.option("delimiter", delimiter).csv(delimited_path, header = True)
    df.printSchema()
    df.createOrReplaceTempView(resourcename + "_csvview")
    columns = df.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query_csv = "SELECT " + columnlist + " FROM " + resourcename + "_csvview"
    if len(datafilter) >=5:
      query_csv = query_csv + " WHERE " + datafilter
    df_data = spark.sql(query_csv)

  elif tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = root_path+tc_datasource_config['querypath']
    f = open(querypath,"r")
    query= f.read().splitlines()
    query=' '.join(query)
    df=spark.read.option("delimiter", delimiter).schema(tc_datasource_config['schemastruct']).csv(delimited_path, header = True)
    df.printSchema()
    print(tc_datasource_config['aliasname'])
    df.createOrReplaceTempView(tc_datasource_config['aliasname'])
    df_data = spark.sql(query)
  log_info("Returning the DataFrame from read_delimiteddata Function")
  return df_data, query
