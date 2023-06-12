# Databricks notebook source
# DBTITLE 1, Import Required Libraries
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig
import os

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

  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)
  selectallcolqry = f"SELECT * FROM {resourcename} "
  if len(datafilter) > 0:
    selectallcolqry = selectallcolqry + datafilter

  df_snowflakedata = (spark.read
                    .format("jdbc")
                    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver")
                    .option("url", connectionconfig['url'])
                    .option("user", connectionconfig['user'])
                    .option("privateKey", connectionconfig['privateKey'])
                    .option("db", connectionconfig['database'])
                    .option("warehouse", connectionconfig['warehouse'])
                    .option("schema", connectionconfig['schema'])
                    .option("query", selectallcolqry)
                    .load())
  
  columns = df_snowflakedata.columns
  columnlist = list(set(columns) - set(exclude_cols))
  columnlist.sort()
  
  columnlist = ','.join(columnlist)

  df_snowflakedata.createOrReplaceTempView("snowflakeview")
  selectcolqry = "SELECT " + columnlist + " FROM snowflakeview"
  selectcolqry_ret = "SELECT " + columnlist + f" FROM {resourcename}"
  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show()
  log_info("Returning the DataFrame from read_snowflakedata Function")
  return df_out, selectcolqry_ret