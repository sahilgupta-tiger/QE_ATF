from pyspark.sql.functions import lit
from atf.common.atf_common_functions import (get_connection_config, get_mount_path,log_info, preproc_unnestfields)



def read_parquetschema(dict_connection, comparetype,spark):
  connectionname = dict_connection['connectionname'] 
  # s3bucket = get_connection_config(connectionname)['BUCKETNAME'] #Please uncomment the line if it is commented
  #parquet_path = get_mount_path(s3bucket + '/' + dict_connection['filepath']) #Please uncomment the line if it is commented
  layer = ''
  parquet_path = 'file:/Workspace/Shared/QE_ATF_Enhanced_03/datf_core/test/data/source/patients_source_parquet'  #Please comment the above line or delete
  log_info(f"Reading the parquet file located at {parquet_path}") 
  
  df_parquetddata = (spark.read.format("parquet").option("inferSchema","true").load(parquet_path).limit(1))
  df_parquetddata.createOrReplaceTempView("parquetview")
  query = "DESCRIBE parquetview"
  df_describe = (spark.sql("DESCRIBE parquetview;"))
  df_describe.show()
  #The below loop has been commented out as it is not required for schema validation
  '''if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ""'''

  
  df_parquetschema = df_describe['col_name','data_type']
  df_parquetschema = (df_parquetschema
                      .withColumnRenamed('col_name','columnname')
                      .withColumnRenamed('data_type','datatype'))
  
  return df_parquetschema,query