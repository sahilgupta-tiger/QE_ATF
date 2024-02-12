from atf.common.atf_common_functions import log_info
from atf.common.atf_dc_read_snowflakedata import read_snowflakedata

def read_data(tc_datasource_config,spark):
  log_info("Inside read_data function")
  connectiontype = tc_datasource_config['connectiontype'].lower().split()[0]
  resourceformat = tc_datasource_config['format']


  if connectiontype == 'snowflake' and resourceformat == 'table':
      df, query = read_snowflakedata(tc_datasource_config,spark)

  else:
    df = None
    query = ''
  
  return df, query