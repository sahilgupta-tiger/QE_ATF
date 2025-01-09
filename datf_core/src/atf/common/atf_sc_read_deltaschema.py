

def read_deltaschema(dict_connection, comparetype):
  connectionname = dict_connection['connectionname']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  delta_path = get_mount_path(s3bucket + '/' + dict_connection['filepath'])
  tablename = dict_connection['tablename']
  layer = ''
  log_info(f"Reading the delta table located at {delta_path}")
  sql_desc_query = "DESCRIBE delta.`" + delta_path + "`;"
  desc_df = spark.sql(sql_desc_query)
  df_deltaschema = desc_df['col_name','data_type']
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
    
  df_deltaschema = (df_deltaschema
                    .withColumnRenamed('col_name','columnname')
                    .withColumnRenamed('data_type','datatype'))
  
  df_deltaschema = df_deltaschema.filter((col("columnname") != "") & (col("columnname") != "# Partitioning") & (~col("columnname").contains("Part ")))

  
  return df_deltaschema



