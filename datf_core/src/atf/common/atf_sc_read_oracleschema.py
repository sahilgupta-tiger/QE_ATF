from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import (get_connection_config, get_mount_path,log_info, preproc_unnestfields)


def read_oracleschema(dict_connection, comparetype,spark):

  connectionname = dict_connection['connectionname']
  #Added the below three lines on 07/04/2025
  resourcename = dict_connection['filename']
  tablename = resourcename.split(".")[1]
  schemaname = resourcename.split(".")[0]
   #Commented the below two lines on 07/04/2025
  #tablename = dict_connection['tablename']
  #schemaname = dict_connection['schemaname']
  layer = ''
  connectionconfig = get_connection_config(connectionname)
  
  log_info(f"Connecting to Oracle with url - {connectionconfig['CONNURL']}")
  
   #Commented the below line on 07/04/2025
  #oraclequery = "select COLUMN_NAME as columnname, DATA_TYPE as datatype from ALL_TAB_COLUMNS where TABLE_NAME= '" + tablename + "' and owner ='" + schemaname +"'"
  
  #Added the below line on 07/04/2025
  query = "SELECT col.TABLE_NAME AS tablename, col.COLUMN_NAME AS columnname, col.COLUMN_ID AS columnindex, col.DATA TA_TYPE AS datatype,col.NULLABLE AS nullable,CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS primarykey, col.DATA_LENGTH AS length,col.DATA_PRECISION AS precision,col.DATA_SCALE AS scale FROM ALL_TAB_COLUMNS col LEFT JOIN (SELECT acc.OWNER,acc.TABLE_NAME,acc.COLUMN_NAME FROM ALL_CONS_COLUMNS acc JOIN ALL_CONSTRAINTS ac ON acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME AND acc.OWNER = ac.OWNER WHERE ac.CONSTRAINT_TYPE = 'P') pk ON col.OWNER = pk.OWNER AND col.TABLE_NAME = pk.TABLE_NAME AND col.COLUMN_NAME = pk.COLUMN_NAME WHERE col.TABLE_NAME = '" + tablename + "' AND col.OWNER = +'" + schemaname + "' ORDER BY col.COLUMN_ID"

  log_info (f"Meta data query used is - {oraclequery}")

  df_oracleschema = (spark.read
                   .format("jdbc")
                   .option("url", connectionconfig['CONNURL'])
                   .option("user", connectionconfig['CONNUSR'])
                   .option("password", connectionconfig['CONNPWD'])
                   .option("query", oraclequery)
                   .option("oracle.jdbc.timezoneAsRegion", "false")
                   .option("driver", "oracle.jdbc.driver.OracleDriver")
                   .load())
  
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'likeobjectcompare':
    layer == ''
  #Commented the below three lines on 07/04/2025
  #df_oracleschema = (df_oracleschema
                  # .withColumn('columnname',lower(col('columnname')))
                  # .withColumn('datatype',lower(col('datatype'))))
  
  return df_oracleschema,query