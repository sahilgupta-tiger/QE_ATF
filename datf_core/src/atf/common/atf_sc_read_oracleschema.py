# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./atf_common_functions

# COMMAND ----------

def read_oracleschema(dict_connection, comparetype):

  connectionname = dict_connection['connectionname']
  tablename = dict_connection['tablename']
  schemaname = dict_connection['schemaname']
  layer = ''
  connectionconfig = get_connection_config(connectionname)
  
  log_info(f"Connecting to Oracle with url - {connectionconfig['CONNURL']}")
  
  oraclequery = "select COLUMN_NAME as columnname, DATA_TYPE as datatype from ALL_TAB_COLUMNS where TABLE_NAME= '" + tablename + "' and owner ='" + schemaname +"'"
  
  df_oracleschema = (spark.read
                   .format("jdbc")
                   .option("url", connectionconfig['CONNURL'])
                   .option("user", connectionconfig['CONNUSR'])
                   .option("password", connectionconfig['CONNPWD'])
                   .option("query", oraclequery)
                   .option("oracle.jdbc.timezoneAsRegion", "false")
                   .option("driver", "oracle.jdbc.driver.OracleDriver")
                   .load())
  
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'likeobjectcompare':
    layer == ''
  
  df_oracleschema = (df_oracleschema
                   .withColumn('columnname',lower(col('columnname')))
                   .withColumn('datatype',lower(col('datatype'))))
  
  return df_oracleschema