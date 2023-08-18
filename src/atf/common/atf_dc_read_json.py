from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import *
from atf.common.atf_common_functions import log_info, get_mount_path


def read_jsondata(tc_datasource_config,spark):
  log_info("Reading json File")
  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  
  if tc_datasource_config['testquerygenerationmode'] == 'Auto':
    resourcename = tc_datasource_config['name']
    datafilter = tc_datasource_config['filter']
    jsonfilepath = get_mount_path(tc_datasource_config['path'])
    excludecolumns = tc_datasource_config['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)
    employeeDF=(spark.read
    .option("multiline","true")
    .json(jsonfilepath))
    employeeDF= preproc_unnestfields(employeeDF)
    employeeDF.createOrReplaceTempView(resourcename + "_jsonview")
    columns = employeeDF.columns
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query_json = "SELECT " + columnlist + " FROM " + resourcename + "_jsonview"
    if len(datafilter) >=5:
      query_json = query_json + " WHERE " + datafilter
    df_data = spark.sql(query_json)

  elif tc_datasource_config['testquerygenerationmode'] == 'Manual':
    querypath = tc_datasource_config['querypath']
    f = open(querypath,"r")
    query= f.read().splitlines()
    query=' '.join(query)
    print(query)
    df = spark.read.option("multiline","false").json(tc_datasource_config['path'])
    df.printSchema()
    print(tc_datasource_config['aliasname'])
    df.createOrReplaceTempView(tc_datasource_config['aliasname'])
    df_data = spark.sql(query)

  log_info("Returning the DataFrame from read_jsondata Function")
  return df_data, query