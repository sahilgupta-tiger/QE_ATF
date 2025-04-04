
from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,debugexit
from testconfig import root_path


def read_parquetdata(tc_datasource_config,spark):
  log_info("Reading parquet file")
  tc_datasrc_path = root_path + tc_datasource_config['path']
  df = spark.read.parquet(tc_datasrc_path)
  df.createOrReplaceTempView(tc_datasource_config['aliasname'])
  
  if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
    pass      

  elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = tc_datasource_config['querypath']
    with open(querypath, "r") as f:
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
  return df_data, query