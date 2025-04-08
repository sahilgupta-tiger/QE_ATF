
from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,readconnectionconfig

def read_mysqlschema(tc_datasource_config,comparetype,spark):
  log_info("Reading from Mysql Table")
  #Commented the below 6 lines on 07/04/2025
  #connectionname = tc_datasource_config['connectionname']
  #connectiontype = tc_datasource_config['connectiontype']
  #resourceformat = tc_datasource_config['format']
  #connectionconfig = readconnectionconfig(connectionname)
  #connectionurl="jdbc:mysql://"+connectionconfig['host']+":"+connectionconfig['port']+"/"+connectionconfig['database']
  #query="select * from "+ tc_datasource_config['filename']
  
  #Added the below five  lines on 07/04/2025
  connectionname = dict_connection['connectionname']
  resourcename = dict_connection['filename']
  tablename = resourcename.split(".")[1]
  schemaname = resourcename.split(".")[0]
  layer = ''

  query = "SELECT cols.TABLE_NAME AS tablename,cols.COLUMN_NAME AS columnname,cols.ORDINAL_POSITION AS columnindex,cols.COLUMN_TYPE AS datatype,CASE WHEN cols.IS_NULLABLE = 'YES' THEN 'YES' ELSE 'NO' END AS nullable,CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS primarykey,cols.CHARACTER_MAXIMUM_LENGTH AS length,cols.NUMERIC_PRECISION AS precision,cols.NUMERIC_SCALE AS scale FROM INFORMATION_SCHEMA.COLUMNS cols LEFT JOIN (SELECT kcu.TABLE_SCHEMA,kcu.TABLE_NAME,kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY')kcu ON cols.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND cols.TABLE_NAME = kcu.TABLE_NAME AND cols.COLUMN_NAME = kcu.COLUMN_NAME WHERE cols.TABLE_NAME =  '" + tablename + "' AND cols.TABLE_SCHEMA = '" + schemaname + "' ORDER BY cols.ORDINAL_POSITION" 
  
  df_data = (spark.read
                  .format("jdbc")
                  .option("driver","com.mysql.jdbc.Driver")
                  .option("url", connectionurl)
                  .option("user", connectionconfig['user'])
                  .option("password", connectionconfig['password'])
                  .option("useSSL", "false") \
                  .option("ssl", "false") \
                  .option("query", query)
                  .load())
  
  #Added the below four  lines on 07/04/2025
  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'likeobjectcompare':
    layer == ''

  #Commented the below 8 lines on 07/04/2025
  #columns = df.columns
  #columnlist = list(set(columns) - set(tc_datasource_config['excludecolumns'].split(",")))
  #columnlist.sort()
  #df_data = df.select(columnlist)
  #columnlist_str = ','.join(columnlist)
  #query = "SELECT " + columnlist_str + f" FROM {tc_datasource_config['filename']}"
  #df_data.printSchema()
  #df_data.show()
  log_info("Returning the DataFrame from read_mysqldata Function")
  
  return df_data,query