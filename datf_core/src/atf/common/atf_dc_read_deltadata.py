# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math as m
from atf.common.atf_common_functions import log_info

# COMMAND ----------

# MAGIC %run ./atf_common_functions

# COMMAND ----------

# DBTITLE 1,Function to read delta data
def read_deltadata(dict_configdf, comparetype):
  log_info("Reading delta Data")
  connectionname = dict_configdf['connectionname']
  connectiontype = dict_configdf['connectiontype']
  resourceformat = dict_configdf['format']
  
  if comparetype == 'Auto':
    resourcename = dict_configdf['name']
    datafilter = dict_configdf['filter']
    deltapath = get_mount_path(dict_configdf['path'])
    excludecolumns = dict_configdf['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)
    descquery = 'DESCRIBE delta.`' + deltapath + '`;'
    col_df =spark.sql(descquery)
    col_df = col_df.filter((col("col_name") != "") & (col("col_name") != "# Partitioning") & (~col("col_name").contains("Part ")) & (col("col_name") != "Not partitioned"))
    columns = list(col_df.select('col_name').toPandas()['col_name'])
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query_delta = "SELECT " + columnlist +  " FROM delta.`" + deltapath + "`"
    if len(datafilter) >=5:
      query_delta = query_delta + " WHERE " + datafilter
    df_deltadata = spark.sql(query_delta)
    
  elif comparetype == 'Manual':
    querypath = get_mount_path(dict_configdf['querypath'])
    query_delta = spark.read.text(querypath).collect()[0][0]
    df_deltadata = spark.sql(query_delta)
    
  log_info("Returning the DataFrame")
  return df_deltadata, query_delta