
import datacompy
import pandas as pd
import json
import os
from datetime import datetime
from re import search


def log_info(msg):
  print(f'INFO [{str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip()}]: {msg}')


def log_error(msg):
  print(f'ERROR [{str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip()}]: {msg}')


def read_protocol_file(filepath):
  df_protocol = pd.read_excel(filepath, engine='openpyxl',sheet_name='protocol',keep_default_na=False, header=None) 
  df_testcases = pd.read_excel(filepath, engine='openpyxl',sheet_name='protocoltestcasedetails') #,keep_default_na=False
  df_testcases=df_testcases[df_testcases['Sno.']!='']
  df_testcases=df_testcases.iloc[:,0:4]
  df_testcases= df_testcases.dropna()
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
  connection_config=json.load(open("test\\connections\\"+connectionname+".json"))
  print(connection_config)
  return connection_config