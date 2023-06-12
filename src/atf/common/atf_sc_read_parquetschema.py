# Databricks notebook source
from pyspark.sql.functions import lit

# COMMAND ----------

def read_parquetschema(dict_connection, comparetype):
  connectionname = dict_connection['connectionname']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  parquet_path = get_mount_path(s3bucket + '/' + dict_connection['filepath'])
  layer = ''
  log_info(f"Reading the parquet file located at {parquet_path}")
  
  df_parquetddata = (spark.read.format("parquet").load(parquet_path).limit(1))
  df_parquetddata.createOrReplaceTempView("parquetview")
  df_describe = (spark.sql("DESCRIBE parquetview;"))
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
  
  df_parquetschema = df_describe['col_name','data_type']
  df_parquetschema = (df_parquetschema
                      .withColumnRenamed('col_name','columnname')
                      .withColumnRenamed('data_type','datatype'))
  
  return df_parquetschema