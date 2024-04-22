
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *


def read_avroschema(dict_connection, comparetype):
  connectionname = dict_connection['connectionname']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  avro_path = get_mount_path(s3bucket + '/' + dict_connection['filepath'])
  layer = ''
  log_info(f"Reading the avro file located at {avro_path}")
  df_avronesteddata = (spark.read.format("avro").load(avro_path).limit(1))
  df_avrounnesteddata = preproc_unnestfields(df_avronesteddata)
  
  df_avrounnesteddata.createOrReplaceTempView("avroview")
  df_describe = (spark.sql("DESCRIBE avroview;"))
  df_avroschema = df_describe['col_name','data_type']
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
    
  df_avroschema = (df_avroschema
                   .withColumnRenamed('col_name','columnname')
                   .withColumnRenamed('data_type','datatype'))
  
  return df_avroschema