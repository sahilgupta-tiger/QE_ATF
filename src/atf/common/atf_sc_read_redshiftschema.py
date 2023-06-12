# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./atf_common_functions

# COMMAND ----------

def read_redshiftschema(dict_connection, comparetype):
  connectionname = dict_connection['connectionname']
  tablename = dict_connection['tablename']
  schemaname = dict_connection['schemaname']
  layer = ''
  connectionconfig = get_connection_config(connectionname)
  
  log_info(f"Connecting to Redshift with url - {connectionconfig['CONNURL']}")
  
  redshiftquery = "SELECT column_name as columnname, data_type as datatype FROM SVV_COLUMNS where table_schema = '" + schemaname + "' and table_name = '" + tablename + "'"

  df_redshiftschema = (spark.read
                     .format("com.databricks.spark.redshift")
                     .option("url", connectionconfig['CONNURL'])
                     .option("user", connectionconfig['CONNUSR'])
                     .option("password", connectionconfig['CONNPWD'])
                     .option("query", redshiftquery)
                     .option("aws_iam_role", connectionconfig['CONNIAMROLE'])
                     .option("tempdir", connectionconfig['CONNTEMPDIR'])
                     .load())
                     
  df_redshiftschema = (df_redshiftschema
                       .withColumn('columnname',lower(col('columnname')))
                       .withColumn('datatype',lower(col('datatype'))))

  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
  
  return df_redshiftschema

# COMMAND ----------

