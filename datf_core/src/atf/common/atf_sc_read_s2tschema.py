

def read_S2Tschema(dict_connection,comparetype):
  connectionname = dict_connection['connectionname']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  S2TFilePath = '/dbfs' + get_mount_path(s3bucket + '/' + dict_connection['S2Tpath'])
  layer = str(dict_connection['layer'])
  S2T = LoadS2T(S2TFilePath)
  log_info(f"Reading the S2T located at {S2TFilePath}")
    

  df_S2Tschema = spark.createDataFrame([], StructType([]))
  if layer == 'source':
    df_S2Tschema = (S2T.sourceschema_df.select("columnname","datatype","length","scale"))
  if layer == 'stage':
    df_S2Tschema = (S2T.stageschema_df.select("columnname","datatype","length","scale"))
  elif layer == 'target':
    df_S2Tschema = (S2T.targetschema_df.select("columnname","datatype","length","scale"))
  else:
    df_S2Tschema = None
    
  df_S2Tschema = df_S2Tschema.withColumn("length", regexp_replace('length', '\..*$', ''))
  df_S2Tschema = df_S2Tschema.withColumn("scale", regexp_replace('scale', '\..*$', ''))
  
  df_S2Tschema = (df_S2Tschema
          .select("columnname", "datatype",
            concat(col("datatype"), lit("("), df_S2Tschema.length, lit(","), df_S2Tschema.scale, lit(")"))
            .alias("new_datatype")))

  df_S2Tschema = (df_S2Tschema.withColumn("datatype", when(col("new_datatype").isNull(), col("datatype")).otherwise(col("new_datatype"))))
  df_S2Tschema = df_S2Tschema.drop("new_datatype")
  df_S2Tschema = (df_S2Tschema
                  .withColumn('columnname',lower(col('columnname')))
                  .withColumn('datatype',lower(col('datatype'))))
    
  return df_S2Tschema
