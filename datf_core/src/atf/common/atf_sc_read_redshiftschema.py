from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import (get_connection_config, get_mount_path,log_info, preproc_unnestfields)


def read_redshiftschema(dict_connection, comparetype,spark):
  connectionname = dict_connection['connectionname']
  
  #Added the below three lines on 07/04/2025
  resourcename = dict_connection['filename']
  tablename = resourcename.split(".")[1]
  schemaname = resourcename.split(".")[0]
  
  #tablename = dict_connection['tablename'] #Commented the below line on 07/04/2025
  #schemaname = dict_connection['schemaname'] #Commented the below line on 07/04/2025
  layer = ''
  connectionconfig = get_connection_config(connectionname)
  
  log_info(f"Connecting to Redshift with url - {connectionconfig['CONNURL']}")
  
  #Commented the below line on 07/04/2025
  #redshiftquery = "SELECT column_name as columnname, data_type as datatype FROM SVV_COLUMNS where table_schema = '" + schemaname + "' and table_name = '" + tablename + "'"
  
  #Added the below line on 07/04/2025
  query = "SELECT c.attrelid::regclass::text AS tablename,a.attname AS columnname,a.attnum AS columnindex,format_type(a.atttypid, a.atttypmod) AS datatype,CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS nullable,CASE WHEN pk.attname IS NOT NULL THEN 'YES' ELSE 'NO' END AS primarykey,a.attlen AS length,a.atttypmod AS precision, NULL AS scale FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid LEFT JOIN (SELECT i.indrelid,a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indisprimary) pk ON pk.indrelid = c.oid AND pk.attname = a.attname WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relname = '" + tablename + "' AND n.nspname = '" + schemaname + "' ORDER BY a.attnum"

  df_redshiftschema = (spark.read
                     .format("com.databricks.spark.redshift")
                     .option("url", connectionconfig['CONNURL'])
                     .option("user", connectionconfig['CONNUSR'])
                     .option("password", connectionconfig['CONNPWD'])
                     .option("query", redshiftquery)
                     .option("aws_iam_role", connectionconfig['CONNIAMROLE'])
                     .option("tempdir", connectionconfig['CONNTEMPDIR'])
                     .load())
                     
  df_redshiftschema = (df_redshiftschema
                       .withColumn('columnname',lower(col('columnname')))
                       .withColumn('datatype',lower(col('datatype'))))

  if comparetype == 's2tcompare':
    layer = dict_connection['layer']
  elif comparetype == 'objectcompare':
    layer == ''
  
  return df_redshiftschema,query

# COMMAND ----------

