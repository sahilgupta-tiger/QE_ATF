from atf.common.atf_common_functions import log_info
from atf.common.atf_sc_read_parquetschema import read_parquetschema
from atf.common.atf_sc_read_delimitedschema import read_delimitedschema
from atf.common.atf_sc_read_avroschema import read_avroschema
from atf.common.atf_sc_read_jsonschema import read_jsonschema
from atf.common.atf_sc_read_bigqueryschema import read_bigqueryschema
from atf.common.atf_sc_read_redshiftschema import read_redshiftschema
from atf.common.atf_sc_read_oracleschema import read_oracleschema
from atf.common.atf_sc_read_mysqlschema import read_mysqlschema
from atf.common.atf_sc_read_deltaschema import read_deltaschema
from atf.common.atf_sc_read_s2tschema import read_S2Tschema
from pyspark.sql.types import *
from pyspark.sql.functions import *



def read_schema(dict_connection,comparetype,spark): 
  df_schema = spark.createDataFrame([], StructType([]))
  connectiontype = dict_connection['connectiontype'].strip().lower()
  resourceformat = dict_connection['format'].strip().lower()
    
  if(connectiontype == 'aws-s3' and resourceformat == 'xlsx'):
    df_S2Tschema,query = read_S2Tschema(dict_connection,comparetype,spark)
    df_schema = df_S2Tschema
    
  elif(connectiontype == 'aws-s3' and resourceformat == 'parquet'):
    df_parquetschema,query = read_parquetschema(dict_connection, comparetype,spark)
    df_schema = df_parquetschema
  
  elif(connectiontype == 'aws-s3' and resourceformat == 'avro'):
    df_avroschema,query = read_avroschema(dict_connection, comparetype,spark)
    df_schema = df_avroschema
  
  elif(connectiontype == 'aws-s3' and resourceformat == 'delimited'):
    df_delimitedschema,query = read_delimitedschema(dict_connection, comparetype,spark)
    df_schema = df_delimitedschema
  
  elif(connectiontype == 'aws-s3' and resourceformat == 'json'):
    df_jsonschema,query = read_jsonschema(dict_connection, comparetype,spark)
    df_schema = df_jsonschema
  
  elif(connectiontype == 'aws-s3' and resourceformat == 'delta'):
    df_deltaschema,query = read_deltaschema(dict_connection, comparetype,spark)
    df_schema = df_deltaschema
    
  elif(connectiontype == 'oracle' and resourceformat == 'table'):
    df_oracleschema,query = read_oracleschema(dict_connection, comparetype,spark)
    df_schema = df_oracleschema

  elif(connectiontype == 'mysql' and resourceformat == 'table'):
    df_mysqlschema,query = read_mysqlschema(dict_connection, comparetype,spark)
    df_schema = df_mysqlschema
    
  elif(connectiontype == 'redshift' and resourceformat == 'table'):
    df_redshiftschema,query = read_redshiftschema(dict_connection, comparetype,spark)
    df_schema = df_redshiftschema

  elif(connectiontype == 'bigquery' and resourceformat == 'table'):
    df_bigqueryschema,query = read_bigqueryschema(dict_connection, comparetype,spark)
    df_schema = df_bigqueryschema

  else:
    pass

  return df_schema, query