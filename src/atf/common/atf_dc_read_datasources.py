from atf.common.atf_common_functions import log_info
from atf.common.atf_dc_read_parquetdata import read_parquetdata
from atf.common.atf_dc_read_delimiteddata import read_delimiteddata
from atf.common.atf_dc_read_avrodata import read_avrodata
from atf.common.atf_dc_read_json import read_jsondata
from atf.common.atf_dc_read_mysqldata import read_mysqldata
from atf.common.atf_dc_read_redshiftdata import read_redshiftdata
from atf.common.atf_dc_read_oracledata import read_oracledata
from atf.common.atf_dc_read_bigquerydata import read_bigquerydata
from atf.common.atf_dc_read_deltadata import read_deltadata
from atf.common.atf_dc_read_snowflakedata import read_snowflakedata
from atf.common.atf_dc_read_postgresdata import read_postgresdata

def read_data(tc_datasource_config,spark):
  log_info("Inside read_data function")
  connectiontype = tc_datasource_config['connectiontype'].lower().split()[0]
  resourceformat = tc_datasource_config['format']


  if connectiontype == 'aws-s3' and resourceformat == 'delta':
    df_deltadata, query = read_deltadata(tc_datasource_config,spark)
    df = df_deltadata
    
  elif connectiontype == 'aws-s3' and resourceformat == 'parquet':
    df_parquetdata, query = read_parquetdata(tc_datasource_config,spark)
    df = df_parquetdata
    
  elif connectiontype == 'aws-s3' and resourceformat == 'delimited':
    df_csvdata, query = read_delimiteddata(tc_datasource_config,spark)
    df = df_csvdata    
    
  elif connectiontype == 'aws-s3' and resourceformat == 'avro':
    df_avrodata, query = read_avrodata(tc_datasource_config,spark)
    df = df_avrodata
    
  elif connectiontype == 'aws-s3' and resourceformat == 'json':
    df_jsondata, query = read_jsondata(tc_datasource_config,spark)
    df = df_jsondata
        
  elif connectiontype == 'oracle' and resourceformat == 'table':
    df, query = read_oracledata(tc_datasource_config,spark)
    
  elif connectiontype == 'redshift' and resourceformat == 'table':
    df, query = read_redshiftdata(tc_datasource_config,spark)
  
  elif connectiontype == 'mysql' and resourceformat == 'table':
    df, query = read_mysqldata(tc_datasource_config,spark)

  elif connectiontype == 'bigquery' and resourceformat == 'table':
      df, query = read_bigquerydata(tc_datasource_config,spark)

  elif connectiontype == 'snowflake' and resourceformat == 'table':
      df, query = read_snowflakedata(tc_datasource_config,spark)

  elif connectiontype == 'postgresql' and resourceformat == 'table':
      df, query = read_postgresdata(tc_datasource_config,spark)

  else:
    df = None
    query = ''
  
  return df, query