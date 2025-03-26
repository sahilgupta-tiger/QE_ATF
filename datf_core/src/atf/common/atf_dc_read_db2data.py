
from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig
from testconfig import decryptcredential


def read_db2data(tc_datasource_config,spark):
  log_info("Reading from DB2 Table")
  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  connectionconfig = readconnectionconfig(connectionname)
  connectionurl="jdbc:db2://"+connectionconfig['host']+":"+connectionconfig['port']+"/"+connectionconfig['database']
  if tc_datasource_config['testquerygenerationmode'] == 'Auto':
      query="select * from "+ tc_datasource_config['filename']
      df = (spark.read
                      .format("jdbc")
                      .option("driver","com.ibm.db2.jcc.DB2Driver")
                      .option("url", connectionurl)
                      .option("user", connectionconfig['user'])
                      .option("password", decryptcredential(connectionconfig['password']))
                      .option("useSSL", "false")
                      .option("ssl", "false")
                      .option("query", query)
                      .load())
      columns = df.columns
      columnlist = list(set(columns) - set(tc_datasource_config['excludecolumns'].split(",")))
      columnlist.sort()
      df_data = df.select(columnlist)
      columnlist_str = ','.join(columnlist)
      selectcolqry_ret = "SELECT " + columnlist_str + f" FROM {tc_datasource_config['filename']}"
  elif tc_datasource_config['testquerygenerationmode'] == 'Manual':
      querypath = tc_datasource_config['querypath']
      f = open(querypath, "r+")
      selectmanualqry = f.read().splitlines()
      selectmanualqry = ' '.join(selectmanualqry)
      selectmanualqry = str(selectmanualqry)
      print(selectmanualqry)
      selectcolqry_ret = selectmanualqry
      f.close()
      df_data = (spark.read
            .format("jdbc")
            .option("driver", "com.ibm.db2.jcc.DB2Driver")
            .option("url", connectionurl)
            .option("user", connectionconfig['user'])
            .option("password", decryptcredential(connectionconfig['password']))
            .option("useSSL", "false")
            .option("ssl", "false")
            .option("query", selectcolqry_ret)
            .load())

  df_data.printSchema()
  df_data.show()
  log_info("Returning the DataFrame from read_db2data Function")
  
  return df_data, selectcolqry_ret