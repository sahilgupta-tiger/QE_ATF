# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig

# COMMAND ----------

# DBTITLE 1,Load Common Functions notebook
# MAGIC %run ./atf_common_functions

# COMMAND ----------

# DBTITLE 1,Function to read Redshift data
def read_redshiftdata(dict_configdf, comparetype):
  
  log_info("Reading from Redshift Table")
  connectionname = dict_configdf['connectionname']
  connectiontype = dict_configdf['connectiontype']
  resourceformat = dict_configdf['format']
  connectionconfig = get_connection_config(connectionname)
  
  if comparetype == 'Auto':
    resourcename = dict_configdf['name']
    datafilter = dict_configdf['filter']
    excludecolumns = dict_configdf['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)
    selectallcolqry = f"SELECT * FROM {resourcename}"
    if len(datafilter) > 0:
      selectallcolqry = selectallcolqry +  datafilter
    df_redshiftdata = (spark.read
                       .format("com.databricks.spark.redshift")
                       .option("url", connectionconfig['CONNURL'])
                       .option("user", connectionconfig['CONNUSR'])
                       .option("password", connectionconfig['CONNPWD'])
                       .option("query", selectallcolqry)
                       .option("aws_iam_role", connectionconfig['CONNIAMROLE'])
                       .option("tempdir", connectionconfig['CONNTEMPDIR'])
                       .load())
    columns = df_redshiftdata.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    df_out = df_redshiftdata.select(columnlist)
    columnlist = ','.join(columnlist)
    df_redshiftdata.createOrReplaceTempView("redshiftview")
    selectcolqry = "SELECT " + columnlist + " FROM redshiftview"
    selectcolqry_ret = "SELECT " + columnlist + f" FROM {resourcename}"
   
  elif comparetype == 'S2Tcompare':
    pass
  
  col_names = df_out.columns
#   if resourcename == 'stage.add_country_oracle':
#     for i in col_names:
#       df_out=df_out.withColumnRenamed(i,i.upper())
  
  log_info("Returning the DataFrame from read_redshiftdata Function")
  return df_out, selectcolqry_ret