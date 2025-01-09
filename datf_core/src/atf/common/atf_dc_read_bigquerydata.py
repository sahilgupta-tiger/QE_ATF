
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info, readconnectionconfig
import os


def read_bigquerydata(tc_datasource_config, spark):
  log_info("Reading from Big Query Table")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  resourcename = tc_datasource_config['filename']
  connectionconfig = readconnectionconfig(connectionname)
  bqprojecttable = resourcename.split(".")

  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)
  selectallcolqry = f"SELECT * FROM {resourcename} "
  if len(datafilter) > 0:
    selectallcolqry = selectallcolqry + datafilter

  # Authenticating to BigQuery GCP project using Base64 string of the Service Account JSON key
  bqcredentials = connectionconfig['encodedServiceAccount']

  df_bigquerydata = (spark.read
                    .format("bigquery")
                    .option("parentProject", bqprojecttable[0])
                    .option("credentials", bqcredentials)
                    .option("project", bqprojecttable[0])
                    .option("dataset", bqprojecttable[1])
                    .option("table", f"{bqprojecttable[1]}.{bqprojecttable[2]}")
                    .load())

  columns = df_bigquerydata.columns
  columnlist = list(set(columns) - set(exclude_cols))
  columnlist.sort()
  
  columnlist = ','.join(columnlist)

  df_bigquerydata.createOrReplaceTempView("bigqueryview")
  selectcolqry = "SELECT " + columnlist + " FROM bigqueryview"
  selectcolqry_ret = "SELECT " + columnlist + f" FROM {resourcename}"
  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show()
  log_info("Returning the DataFrame from read_biquerydata Function")
  return df_out, selectcolqry_ret
