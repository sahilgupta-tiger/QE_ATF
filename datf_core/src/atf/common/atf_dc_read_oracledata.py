# Databricks notebook source
# DBTITLE 1, Import Required Libraries
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info, readconnectionconfig
from src.constants import decryptcredential
import os
# COMMAND ----------

# DBTITLE 1,Load Common Functions notebook
# MAGIC %run ./atf_common_functions

# COMMAND ----------

# DBTITLE 1,Function to read Oracle data
def read_oracledata(tc_datasource_config, spark):
    log_info("Reading from Oracle Table")

    connectionname = tc_datasource_config['connectionname']
    connectiontype = tc_datasource_config['connectiontype']
    resourceformat = tc_datasource_config['format']
    connectionconfig = readconnectionconfig(connectionname)
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

        df_out = (spark.read.format("jdbc")
                        .option("driver", "oracle.jdbc.driver.OracleDriver")
                        .option("url", connectionconfig['url'])
                        .option("user", connectionconfig['user'])
                        .option("password", decryptcredential(connectionconfig['password']))
                        .option("query", selectmanualqry)
                        .option("oracle.jdbc.timezoneAsRegion", "false")
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

        df_oracledata = (spark.read
                        .format("jdbc")
                        .option("driver", "oracle.jdbc.driver.OracleDriver")
                        .option("url", connectionconfig['url'])
                        .option("user", connectionconfig['user'])
                        .option("password", connectionconfig['password'])
                        .option("query", selectallcolqry)
                        .option("oracle.jdbc.timezoneAsRegion", "false")
                        .load())

        columns = df_oracledata.columns
        columnlist = list(set(columns) - set(exclude_cols))
        columnlist.sort()
        columnlist_str = ','.join(columnlist)

        df_oracledata.createOrReplaceTempView("oracleview")
        selectcolqry = "SELECT " + columnlist_str + " FROM oracleview"
        selectcolqry_ret = "SELECT " + columnlist_str + f" FROM {resourcename}"
        df_out = spark.sql(selectcolqry)

    df_out.printSchema()
    df_out.show()
    log_info("Returning the DataFrame from read_oracledata Function")
    return df_out, selectcolqry_ret
