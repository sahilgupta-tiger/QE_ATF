from atf.common.atf_common_functions import get_connection_config, get_mount_path, log_info


def read_delimitedschema(dict_connection, comparetype):
  connectionname = dict_connection['connectionname']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  delimited_path = get_mount_path(s3bucket + '/' + dict_connection['filepath'])
  delimiter = dict_connection['delimiter']
  layer = ''
  log_info(f"Reading the delimited file located at {delimited_path}")
  df_delimiteddata = (spark.read.format("csv").option("header",True).option("delimiter",delimiter).load(delimited_path).limit(1))
  
  df_delimiteddata.createOrReplaceTempView("delimitedview")
  df_describe = (spark.sql("DESCRIBE delimitedview;"))
  df_delimitedschema = df_describe['col_name','data_type']
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
  
  df_delimitedschema = (df_delimitedschema
                        .withColumnRenamed('col_name','columnname')
                        .withColumnRenamed('data_type','datatype'))
  
  return df_delimitedschema


