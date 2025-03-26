from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig
from testconfig import decryptcredential
import os


def read_postgresdata(tc_datasource_config, spark):
  log_info("Reading from PostGRESQL Table")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  connectionconfig = readconnectionconfig(connectionname)
  #connurl = f"{connectionconfig['url']}/{tc_datasource_config['filepath']}"
  resourcename = tc_datasource_config['filename']

  if tc_datasource_config['testquerygenerationmode'] == 'Manual':
      querypath = tc_datasource_config['querypath']
      f = open(querypath, "r+")
      selectmanualqry = f.read().splitlines()
      selectmanualqry = ' '.join(selectmanualqry)
      selectmanualqry = str(selectmanualqry)
      print(selectmanualqry)
      selectcolqry_ret = selectmanualqry
      f.close()
      df_out = (spark.read
                         .format("jdbc")
                         .option("driver", "org.postgresql.Driver")
                         .option("url", connectionconfig['url'])
                         .option("query", selectcolqry_ret)
                         .option("user", connectionconfig['user'])
                         .option("password", decryptcredential(connectionconfig['password']))
                         .load())

  elif tc_datasource_config['testquerygenerationmode'] == 'Auto':
      datafilter = tc_datasource_config['filter']
      excludecolumns = tc_datasource_config['excludecolumns']
      excludecolumns = str(excludecolumns)
      exclude_cols = excludecolumns.split(',')
      datafilter = str(datafilter)
      selectallcolqry = f"SELECT * FROM {resourcename} "
      if len(datafilter) > 0:
        selectallcolqry = selectallcolqry + datafilter

      df_postgresdata = (spark.read
                        .format("jdbc")
                        .option("driver", "org.postgresql.Driver")
                        .option("url", connectionconfig['url'])
                        .option("dbtable", resourcename)
                        .option("user", connectionconfig['user'])
                        .option("password", decryptcredential(connectionconfig['password']))
                        .load())
                    
      columns = df_postgresdata.columns
      columnlist = list(set(columns) - set(exclude_cols))
      columnlist.sort()

      columnlist_str = ','.join(columnlist)

      df_postgresdata.createOrReplaceTempView("postgresview")
      selectcolqry = "SELECT " + columnlist_str + " FROM postgresview"
      selectcolqry_ret = "SELECT " + columnlist_str + f" FROM {resourcename}"
      df_out = spark.sql(selectcolqry)

  df_out.printSchema()
  df_out.show()
  log_info("Returning the DataFrame from read_postgresdata Function")
  return df_out, selectcolqry_ret