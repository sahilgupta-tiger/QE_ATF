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
from atf.common.atf_dc_read_adls_delta import read_adls_deltadata
from atf.common.atf_dc_read_adls_parquetdata import read_adls_parquetdata
from atf.common.atf_dc_read_adls_delimiteddata import read_adls_delimiteddata
from atf.common.atf_dc_read_adls_jsondata import read_adls_jsondata
from atf.common.atf_dc_read_adls_avrodata import read_adls_avrodata


def read_data(tc_datasource_config,spark):
  log_info("Inside read_data function")
  connectiontype = tc_datasource_config['connectiontype'].lower().split()[0]
  resourceformat = tc_datasource_config['format']

  df = None
  query = ''

  # Call the read function based on connectiontype
  if connectiontype == 'adls':
    if resourceformat == 'delta':
      df, query = read_adls_deltadata(tc_datasource_config, spark)
    elif resourceformat == 'parquet':
      df, query = read_adls_parquetdata(tc_datasource_config, spark)
    elif resourceformat == 'delimited':
      df, query = read_adls_delimiteddata(tc_datasource_config, spark)
    elif resourceformat == 'avro':
      df, query = read_adls_avrodata(tc_datasource_config, spark)
    elif resourceformat == 'json':
      df, query = read_adls_jsondata(tc_datasource_config, spark)

  elif connectiontype in ['aws-s3','databricks']:
    if resourceformat == 'delta':
      df, query = read_deltadata(tc_datasource_config, spark)
    elif resourceformat == 'parquet':
      df, query = read_parquetdata(tc_datasource_config, spark)
    elif resourceformat == 'delimited':
      df, query = read_delimiteddata(tc_datasource_config, spark)
    elif resourceformat == 'avro':
      df, query = read_avrodata(tc_datasource_config, spark)
    elif resourceformat == 'json':
      df, query = read_jsondata(tc_datasource_config, spark)

  elif connectiontype == 'oracle' and resourceformat == 'table':
    df, query = read_oracledata(tc_datasource_config, spark)

  elif connectiontype == 'redshift' and resourceformat == 'table':
    df, query = read_redshiftdata(tc_datasource_config, spark)

  elif connectiontype == 'mysql' and resourceformat == 'table':
    df, query = read_mysqldata(tc_datasource_config, spark)

  elif connectiontype == 'bigquery' and resourceformat == 'table':
    df, query = read_bigquerydata(tc_datasource_config, spark)

  elif connectiontype == 'snowflake' and resourceformat == 'table':
    df, query = read_snowflakedata(tc_datasource_config, spark)

  elif connectiontype == 'postgres' and resourceformat == 'table':
    df, query = read_postgresdata(tc_datasource_config, spark)

  return df, query