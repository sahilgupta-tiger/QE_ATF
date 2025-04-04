from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info


def read_avrodata(tc_datasource_config,spark):
  log_info("Reading the avro File")
  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
   
  if tc_datasource_config['testquerygenerationmode'] == 'Auto':
    resourcename = tc_datasource_config['name']
    datafilter = tc_datasource_config['filter']
    avrofilepath = get_mount_path(tc_datasource_config['path'])
    excludecolumns = tc_datasource_config['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)
    readavroschema =spark.read.format("avro").load(avrofilepath).schema
    readavrodata=spark.read.format("avro").schema(readavroschema).load(avrofilepath)
    df= preproc_unnestfields(readavrodata)
    df.createOrReplaceTempView(resourcename + "_avroview")
    columns = df.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query_avro = "SELECT " + columnlist + " FROM " + resourcename + "_avroview"
    if len(datafilter) >=5:
      query_avro = query_avro + " WHERE " + datafilter
    df_data = spark.sql(query_avro)
  elif tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = tc_datasource_config['querypath']
    with open(querypath, "r") as f:
      query= f.read().splitlines()
    query=' '.join(query)
    print(query)
    df=spark.read.format("avro").load(tc_datasource_config['path'])
    df.printSchema()
    print(tc_datasource_config['aliasname'])
    df.createOrReplaceTempView(tc_datasource_config['aliasname'])
    df_data = spark.sql(query)

  '''col_names = df_avrodata.columns
  for i in col_names:
    df_avrodata=df_avrodata.withColumnRenamed(i,i.lower())
  '''
  log_info("Returning the DataFrame from read_avrodata Function")
  return df_data, query