from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info, readconnectionconfig
from testconfig import decryptcredential

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


def read_snowflakedata(tc_datasource_config, spark):
  log_info("Reading from Snowflake Warehouse")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  connectionconfig = readconnectionconfig(connectionname)
  resourcename = tc_datasource_config['filename']

  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)
  selectallcolqry = f"SELECT * FROM {resourcename} "
  if len(datafilter) > 0:
    selectallcolqry = selectallcolqry + datafilter

  sfOptions = {
      "sfURL": connectionconfig['url'],
      "sfUser": connectionconfig['user'],
      "sfPassword": decryptcredential(connectionconfig['password']),
      "sfDatabase": connectionconfig['database'],
      "sfSchema": connectionconfig['schema'],
      "sfWarehouse": connectionconfig['warehouse'],
      "autopushdown": "on"
  }

  df_snowflakedata = (spark.read.format(SNOWFLAKE_SOURCE_NAME)
                    .option("sfURL", connectionconfig['url'])
                    .option("sfUser", connectionconfig['user'])
                    .option("sfPassword", decryptcredential(connectionconfig['password']))
                    .option("sfDatabase", connectionconfig['database'])
                    .option("sfSchema", connectionconfig['schema'])
                    .option("sfWarehouse", connectionconfig['warehouse'])
                    .option("autopushdown", "on")
                    .option("query", selectallcolqry)
                    .load())
  
  columns = df_snowflakedata.columns
  columnlist = list(set(columns) - set(exclude_cols))
  columnlist.sort()
  
  columnlist = ','.join(columnlist)

  df_snowflakedata.createOrReplaceTempView("snowflakeview")
  selectcolqry = "SELECT " + columnlist + " FROM snowflakeview"
  selectcolqry_ret = "SELECT " + columnlist + f" FROM {resourcename}"
  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show()
  log_info("Returning the DataFrame from read_snowflakedata Function")
  return df_out, selectcolqry_ret