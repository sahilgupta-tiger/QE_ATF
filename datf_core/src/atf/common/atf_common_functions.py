import pandas as pd
import json
import os
from datetime import datetime
from re import search
from testconfig import *
from pyspark.sql import functions as F
from pyspark.sql.functions import md5,slice, concat_ws, trim,lit, collect_list, map_from_arrays
from functools import reduce


def log_info(msg):
  print(f'INFO [{str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip()}]: {msg}')


def log_error(msg):
  print(f'ERROR [{str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip()}]: {msg}')


def read_protocol_file(filepath):
  df_protocol = pd.read_excel(filepath, engine='openpyxl',sheet_name=protocol_tab_name,keep_default_na=False, header=None)
  df_testcases = pd.read_excel(filepath, engine='openpyxl',sheet_name=exec_sheet_name) #,keep_default_na=False
  df_testcases=df_testcases[df_testcases['Sno.']!='']
  #df_testcases=df_testcases.iloc[:,0:4]
  #df_testcases= df_testcases.dropna()
  dict_protocol = dict(df_protocol.values)
  #context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  #workspacename = context['tags']['browserHostName']
  dict_protocol['test_run_environment'] = ""
  return dict_protocol, df_testcases


def read_test_case(filepath):
  filepath = filepath.strip()
  df = pd.read_excel(filepath, engine='openpyxl', header=None, keep_default_na=False, usecols="A,B")
  mapping = dict(df.values)
  return mapping


def get_mount_path(src_path):
  src_path = src_path.replace("s3://", "s3a://")
  #mountinfo = dbutils.fs.mounts()
  mnt_path =[src_path.replace(i[1],i[0]) for i in mountinfo if search(i[1], src_path)]
  return str(mnt_path[0])


def get_mount_src_path(mnt_path):
 # mountinfo = dbutils.fs.mounts()
  #mnt_src_path =[mnt_path.replace(i[0],i[1]) for i in dbutils.fs.mounts()  if search(i[0], mnt_path)]
  #mnt_src_path = str(mnt_src_path[0]).replace("/dbfs","")
  #mnt_src_path.replace("s3a://", "s3://")
  return mnt_path


def check_combination_allowed(combinations_path, dict_combination):
  
  log_info("Inside check_combination_allowed function in atf_common_functions notebook")
  
  flag_allowed = ''
  combinations_path = get_mount_path(combinations_path)
  sourceconnectiontype = dict_combination['sourceconnectiontype'].strip().lower()
  targetconnectiontype = dict_combination['targetconnectiontype'].strip().lower()
  sourceformat = dict_combination['sourceformat'].strip().lower()
  targetformat = dict_combination['targetformat'].strip().lower()
  
  df_combinations = spark.read.option("delimiter", ',').csv(combinations_path, header = True)
  df_schemacompare = df_combinations.filter(col("testtype")=='schemacompare')
  pdf_schemacompare = df_schemacompare.toPandas()
  
  for index, row in pdf_schemacompare.iterrows():
    if (row['sourceconnectiontype'].strip().lower() == sourceconnectiontype and row['targetconnectiontype'].strip().lower() == targetconnectiontype and row['sourceformat'].strip().lower() == sourceformat and row['targetformat'].strip().lower() == targetformat):
      flag_allowed = 'Y'
      break
    else:
      flag_allowed = 'N'
  
  return flag_allowed


def get_connection_config(connectionname):
  #connobj = dbutils.secrets.get(scope = "connections", key = connectionname)
  #configdct = json.loads(connobj)
  configdct={}
  return connectionname



def generate_autoscript_path(path):
  path = path.split("testresults")
  auto_script_path = path[0] + "queries/auto"
  if(os.path.exists(auto_script_path) == False):
    dbutils.fs.mkdirs(get_mount_src_path(auto_script_path))
  return auto_script_path


def preproc_unnestfields(microBatchDF):
  log_info("Inside preproc_unnestfields function in atf_common_functions notebook")
  
  #Compute Complex Fields (Lists and Structs) in Schema   
  complex_fields = dict([(field.name, field.dataType)
                           for field in microBatchDF.schema.fields
                           if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
  while len(complex_fields)!=0:
    col_name = list(complex_fields.keys())[0]
  
    #If StructType then convert all sub element to columns.
    #i.e. flatten structs
    if (type(complex_fields[col_name]) == StructType):
        expanded = [col(col_name+'.'+k).alias((col_name+'_'+k).lower()) for k in [ n.name for n in complex_fields[col_name]]]
        microBatchDF = microBatchDF.select("*", *expanded).drop(col_name)
  
    #If ArrayType then add the Array Elements as Rows using the explode function
    #i.e. explode Arrays
    elif (type(complex_fields[col_name]) == ArrayType):    
       microBatchDF = microBatchDF.withColumn(col_name,explode_outer(col_name))
  
    #Recompute remaining Complex Fields in Schema       
    complex_fields = dict([(field.name, field.dataType)
                           for field in microBatchDF.schema.fields
                           if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
  
  log_info("Exiting preproc_unnestfields function in atf_common_functions notebook")
  return microBatchDF

def debugexit():
  print("Debug Exit Reached")
  exit()

def readconnectionconfig(connectionname):
  with open(f"{root_path}test/connections/"+connectionname+".json", 'r+') as conn_file:
    connection_config=json.load(conn_file)
  print(connection_config)
  return connection_config

def set_azure_connection_config(spark, connectionconfig, dbutils):
  log_info('Setting Azure ADLS spark connection configuration to Databricks.')

  storage_account = dbutils.secrets.get(scope="akv-mckesson-scope", key=connectionconfig['STORAGE_ACCOUNT_NAME'])
  sas_token = dbutils.secrets.get(scope="akv-mckesson-scope", key=connectionconfig['SAS_TOKEN'])

  # Set the configuration using SAS Token
  spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
  spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
  spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)

  log_info('Azure ADLS connection configuration completed.')


def get_dbutils(spark):
  dbutils = None
  if spark.conf.get("spark.databricks.service.client.enabled") == "true":
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
  else:
    import IPython
    dbutils = IPython.get_ipython().user_ns["dbutils"]
  return dbutils


def read_json_file(file_path):
  log_info(f"Reading JSON file from {file_path}")
  with open(file_path, 'r') as file:
    data = json.load(file)
  return data


# Function to create a json file in desired path
def create_json_file(json_data, file_path):
  with open(file_path, 'w') as json_file:
    json.dump(json_data, json_file, indent=4)


def apply_md5_hash(df):
  df_hashed = df.withColumn("hashval", md5(concat_ws("|", *df.columns)))
  df_final_hashed = df_hashed.select("hashval")

  return df_final_hashed


def verify_nulls(df):
  columns_with_nulls = [(c, df.select(c).filter(F.col(c).isNull()).count()) for c in df.columns if df.select(c).filter(F.col(c).isNull()).count() > 0]
  print("Columns with null values:", columns_with_nulls)
  return columns_with_nulls


def verify_duplicates(df):
  duplicate_columns = []
  for column in df.columns:
    count_distinct = df.select(column).distinct().count()
    count_total = df.select(column).count()
    if count_distinct < count_total:
      duplicate_columns.append(column)
  return duplicate_columns


def list_duplicate_values(df):

  duplicate_dfs = []

  for col_name in df.columns:
    duplicate_values = (
      df.groupBy(col_name).count()
      .filter("count > 1")  # Only keep duplicate values
      .select(
        lit(col_name).alias("Column Name"),
        concat_ws("|", slice(collect_list(F.col(col_name).cast("string")), 1, 50)).alias("Duplicate Values")
      )
    )
    duplicate_dfs.append(duplicate_values)


  if duplicate_dfs:
    final_df = reduce(lambda df1, df2: df1.union(df2), duplicate_dfs)
    final_df.cache()  # Cache the result for faster reuse
    return final_df
  else:
    return None


def verify_dup_pk(df, joincolumns, limit):
  print(f"Join columns - {joincolumns}")
  print(f"type of {type(joincolumns)}")
  duplicate_values = df.groupBy(joincolumns).count().filter("count > 1")
  if duplicate_values.count() > 0:
    duplicate_values = duplicate_values.limit(limit)
  else:
    duplicate_values = None

  return duplicate_values


def check_empty_values(df):
  columns_with_empty = [(c, df.select(c).filter(trim(F.col(c)) == "").count()) for c in df.columns if df.select(c).filter(trim(F.col(c)) == "").count() > 0]
  print("Columns with empty values:", columns_with_empty)
  return columns_with_empty


def check_entire_row_is_null(df):
  # Create a condition to check if all columns in a row are null
  condition = reduce(lambda acc, col_name: acc & F.col(col_name).isNull(), df.columns[1:], F.col(df.columns[0]).isNull())
  # Filter the DataFrame based on the condition
  null_rows_df = df.filter(condition)
  return null_rows_df