# Databricks notebook source
# DBTITLE 1, Import Required Libraries
from atf.common.atf_common_functions import log_info, readconnectionconfig

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# COMMAND ----------

# DBTITLE 1,Load Common Functions notebook
# MAGIC %run ./atf_common_functions

# COMMAND ----------

# DBTITLE 1,Function to read Big Query data
def read_snowflakedata(tc_datasource_config, spark):
  log_info("Reading from Snowflake Warehouse")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  connectionconfig = readconnectionconfig(connectionname)
  resourcename = tc_datasource_config['filename']

  if tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = tc_datasource_config['querypath']
    f = open(querypath, "r+")
    selectmanualqry = f.read().splitlines()
    selectmanualqry = ' '.join(selectmanualqry)
    selectmanualqry = str(selectmanualqry)
    print(selectmanualqry)
    selectcolqry_ret = selectmanualqry
    f.close()

    df_out = spark.sql(selectmanualqry)

  elif tc_datasource_config['testquerygenerationmode'] == 'Auto':
    datafilter = tc_datasource_config['filter']
    excludecolumns = tc_datasource_config['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)
    selectallcolqry = f"SELECT * FROM {resourcename} "
    if len(datafilter) > 0:
      selectallcolqry = selectallcolqry + datafilter

    df_snowflakedata = spark.table(resourcename)

    columns = df_snowflakedata.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()

    columnlist = ','.join(columnlist)

    df_snowflakedata.create_or_replace_temp_view("snowflakeview")
    selectcolqry = "SELECT " + columnlist + " FROM snowflakeview" + datafilter
    selectcolqry_ret = "SELECT " + columnlist + f" FROM {resourcename} {datafilter}"
    df_out = spark.sql(selectcolqry)
    del [df_snowflakedata]

  df_out.printSchema()
  df_out.show()
  df_ret = df_out.cache_result()
  del [df_out]
  log_info("Returning the DataFrame from read_snowflakedata Function")
  return df_ret, selectcolqry_ret