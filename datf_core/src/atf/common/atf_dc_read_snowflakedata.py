from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info, readconnectionconfig,initilize_dbutils
from constants import *
import re

def read_snowflakedata(tc_datasource_config, spark):
  log_info("Reading from Snowflake Warehouse")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  
  #Importing dbutils
  dbutils =  initilize_dbutils(spark)

  #Reading connection config from json file
  connectionconfig = readconnectionconfig(connectionname)
  
  #Fetching credentials from key vault
  username =  connectionconfig['user'] #dbutils.secrets.get(scope="akv-mckesson-scope",  key= connectionconfig['user'])  
  password = connectionconfig['password'] #dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['password'])
  # host = dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['host'])
  # port = dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['port'])
  database = connectionconfig['database'] #dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['database'])
  schema = connectionconfig['schema'] #dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['schema'])
  warehouse = connectionconfig['warehouse'] #dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['warehouse'])
  role = connectionconfig['role']
                                
  connectionurl= connectionconfig['url'] #dbutils.secrets.get(scope="akv-mckesson-scope", key= connectionconfig['url'])

  uncleaned_path = tc_datasource_config['path']
  log_info(f'Before cleaning up the resource name - {uncleaned_path}')
  resource_name = tc_datasource_config['path'].replace('/','.')
  #resourcename = resource_name.replace('/','.').lstrip('.').rstrip('.').lstrip('/').rstrip('/')

  
  resourcename = re.sub(r'^[.]+|[.]+$', '', resource_name)
  log_info(f'After cleaning up the resource name - {resourcename}')

  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)

  log_info(f"Resource Name - {resourcename}")

  alias_name = tc_datasource_config['aliasname']
  log_info(f"Alias Name - {alias_name}")

  #read data from snoflake table
  df_snowflakedata = (spark.read.format(connectiontype)
                    .option("sfURL", connectionurl)
                    .option("sfUser", username)
                    .option("sfPassword", password)
                    .option("sfDatabase", database)
                    .option("sfSchema", schema)
                    .option("sfWarehouse", warehouse)
                    .option("dbtable", resourcename) \
                    .option("role",role)
                    .load())

  if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
    pass  

  elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = root_path+tc_datasource_config['querypath']
    f = open(querypath,"r")
    query= f.read().splitlines()
    query=' '.join(query)
    log_info(f"Select Table Command statement - \n{query}")
    df_snowflakedata.createOrReplaceTempView(alias_name)
    selectcolqry = query
    selectcolqry_ret = query.replace(alias_name,resourcename)

  elif tc_datasource_config['comparetype'] == 'likeobjectcompare':

    selectallcolqry = f"SELECT * FROM {resourcename} "
    if len(datafilter) > 0:
      selectallcolqry = selectallcolqry  + " WHERE " + datafilter

    log_info(f'Exclude columns - {excludecolumns}')
    columns = df_snowflakedata.columns
    log_info(f"{list(set(columns) - set(exclude_cols))}")
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    
    column_list = ','.join(columnlist)
    log_info(f'Column list after excluding exclude columns - {column_list}')

    df_snowflakedata.createOrReplaceTempView(alias_name)
    selectcolqry = "SELECT " + column_list + " FROM " + alias_name
    selectcolqry_ret = "SELECT " + column_list + f" FROM {resourcename}"

  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show()
  log_info("Returning the DataFrame from read_snowflakedata Function")
  return df_out, selectcolqry_ret