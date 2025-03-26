from pyspark.sql.functions import *
from pyspark.sql.types import *
import math as m
from atf.common.atf_common_functions import log_info, get_mount_path
from testconfig import *
import re

def read_deltadata(tc_datasource_config, spark):
  log_info("Reading delta Data")
  resourcename = tc_datasource_config['filename']
  comparetype = tc_datasource_config['testquerygenerationmode']

  #tc_datasource_config['targetfilepath']
  log_info(f"Resource Name - {resourcename}")
  alias_name = tc_datasource_config['aliasname']
  log_info(f"Alias Name - {alias_name}")

  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)

  deltatable = tc_datasource_config['path'].replace('/','.')
  deltatable = re.sub(r'^[.]+|[.]+$', '', deltatable)

  log_info(f"Delta Table Path - {deltatable}")
  df = spark.table(deltatable)
  df.createOrReplaceTempView(alias_name)
  
  if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
    pass   
    
  elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
    deltatable = deltatable.replace('/','.')
    querypath = root_path+tc_datasource_config['querypath']
    f = open(querypath,"r")
    query_delta= f.read().splitlines()
    query_delta=' '.join(query_delta)
    querydelta = query_delta.replace(alias_name,deltatable)
    log_info(f"Select Table Command statement - \n{querydelta}")

  elif tc_datasource_config['comparetype'] == 'likeobjectcompare':
    if tc_datasource_config['testquerygenerationmode'] == 'Auto':
      log_info('Inside likeobjectcompare code')
      columns = df.columns
      columnlist = list(set(columns) - set(exclude_cols))
      columnlist.sort()
      columnlist = ','.join(columnlist)

      query_delta = "SELECT " + columnlist + " FROM "+ alias_name
      querydelta = query_delta.replace(alias_name,deltatable)

      if len(datafilter) >=5:
        querydelta= querydelta + " WHERE " + datafilter

    if tc_datasource_config['testquerygenerationmode'] == 'Manual':
      querypath = tc_datasource_config['querypath']
      f = open(querypath, "r+")
      selectmanualqry = f.read().splitlines()
      selectmanualqry = ' '.join(selectmanualqry)
      selectmanualqry = str(selectmanualqry)
      print(selectmanualqry)
      querydelta = selectmanualqry
      f.close()


    log_info(f"Select Table Command statement - \n{querydelta}")
    df_deltadata = spark.sql(querydelta)
  
  df_deltadata.printSchema()
  df_deltadata.show()
  log_info("Returning the Delta DataFrame")

  return df_deltadata, querydelta