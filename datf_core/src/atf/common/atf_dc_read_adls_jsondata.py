from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import *
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config,initilize_dbutils
from constants import *

def read_adls_jsondata(tc_datasource_config, spark):
    log_info("Reading json File")
    connectionname = tc_datasource_config['connectionname']
    connectiontype = tc_datasource_config['connectiontype']
    resourceformat = tc_datasource_config['format']

    # Reading Adls Connection Configuration
    connectionconfig = readconnectionconfig(connectionname)
    
    #Importing dbutils
    dbutils =  initilize_dbutils(spark)

    # Set Adls Connection Configuration
    storage_account = connectionconfig['STORAGE_ACCOUNT_NAME']
    sas_token = connectionconfig['SAS_TOKEN']
    
    #Set Adls Connection Configuration
    set_azure_connection_config(spark,connectionconfig,dbutils)
    
    jsonfilepath = tc_datasource_config['path']  # Relative path within the container

    if ('abfs' not in jsonfilepath) and ('Volume' not in jsonfilepath):
        log_info("Appending Root path to relative json file Path")
        jsonfilepath = root_path + jsonfilepath

    log_info(f'ADLS File Path :-  {jsonfilepath}')

    if tc_datasource_config['testquerygenerationmode'] == 'Auto':
        resourcename = tc_datasource_config['name']
        datafilter = tc_datasource_config['filter']
        # jsonfilepath = tc_datasource_config['path']
        excludecolumns = tc_datasource_config['excludecolumns']
        excludecolumns = str(excludecolumns)
        exclude_cols = excludecolumns.split(',')
        datafilter = str(datafilter)
        employeeDF = (spark.read
                      .option("multiline", "true")
                      .json(jsonfilepath))
        employeeDF = preproc_unnestfields(employeeDF)
        employeeDF.createOrReplaceTempView(resourcename + "_jsonview")
        columns = employeeDF.columns
        columnlist = list(set(columns) - set(exclude_cols))
        columnlist.sort()
        columnlist = ','.join(columnlist)
        query_json = "SELECT " + columnlist + " FROM " + resourcename + "_jsonview"
        if len(datafilter) >= 5:
            query_json = query_json + " WHERE " + datafilter
        log_info(f"Select Table Command statement - \n{query_json}")
        df_data = spark.sql(query_json)

    elif tc_datasource_config['testquerygenerationmode'] == 'Manual':
        querypath = root_path + tc_datasource_config['querypath']
        f = open(querypath, "r")
        query = f.read().splitlines()
        query = ' '.join(query)
        log_info(f"Select Table Command statement - \n{query}")
        # json_path = root_path + tc_datasource_config['path']
        df = spark.read.option("multiline", "false").json(jsonfilepath)
        df.printSchema()
        log_info(tc_datasource_config['aliasname'])
        df.createOrReplaceTempView(tc_datasource_config['aliasname'])
        df_data = spark.sql(query)

    log_info("Returning the DataFrame from read_jsondata Function")
    return df_data, query