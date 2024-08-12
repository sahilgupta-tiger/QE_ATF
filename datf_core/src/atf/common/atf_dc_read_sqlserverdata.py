from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig


def read_sqlserverdata(tc_datasource_config, spark):
  log_info("Reading from SQLSERVER Table")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  connectionconfig = readconnectionconfig(connectionname)
  
  #connurl = f"{connectionconfig['url']}/{tc_datasource_config['filepath']}"
  resourcename = tc_datasource_config['filename']
  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)
  selectallcolqry = f"SELECT * FROM {resourcename} "
  if len(datafilter) > 0:
    selectallcolqry = selectallcolqry + datafilter
  
  df_sqlserverdata = (spark.read
                    .format("jdbc")
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                    .option("url", connectionconfig['url']) \
                    .option("table", resourcename) \
                    .option("user", connectionconfig['user']) \
                    .option("password", connectionconfig['password']) \
                    .load())
                    
  columns = df_sqlserverdata.columns
  columnlist = list(set(columns) - set(exclude_cols))
  columnlist.sort()
  
  columnlist_str = ','.join(columnlist)

  df_sqlserverdata.createOrReplaceTempView("sqlserverview")
  selectcolqry = "SELECT " + columnlist_str + " FROM sqlserverview"
  selectcolqry_ret = "SELECT " + columnlist_str + f" FROM {resourcename}"
  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show() 
  log_info("Returning the DataFrame from read_sqlserverdata Function")
  return df_out, selectcolqry_ret