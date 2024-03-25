# Databricks notebook source
def read_jsonschema(dict_connection, comparetype):
  connectionname = dict_connection['connectionname']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  json_path = get_mount_path(s3bucket + '/' + dict_connection['filepath'])
  layer = ''
  log_info(f"Reading the json file located at {json_path}")
  df_jsondnesteddata=(spark.read.option("multiline","true").json(json_path).limit(1))
  df_jsonunnesteddata = preproc_unnestfields(df_jsondnesteddata)
  
  df_jsonunnesteddata.createOrReplaceTempView("jsonview")
  df_describe = (spark.sql("DESCRIBE jsonview;"))
  
  df_jsonschema = df_describe['col_name','data_type']
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
    
  df_jsonschema = (df_jsonschema
                   .withColumnRenamed('col_name','columnname')
                   .withColumnRenamed('data_type','datatype'))
  
  return df_jsonschema