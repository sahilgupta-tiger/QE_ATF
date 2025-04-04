from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import *
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config,get_dbutils
from testconfig import *

def read_adls_jsondata(tc_datasource_config, spark):
    log_info("Reading json File")
    connectionname = tc_datasource_config['connectionname']
    resourceformat = tc_datasource_config['format']

    # Reading Adls Connection Configuration
    connectionconfig = readconnectionconfig(connectionname)
    
    #Importing dbutils
    dbutils =  get_dbutils(spark)

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
    df = spark.read.option("multiline", "false").json(jsonfilepath)
    df.createOrReplaceTempView(tc_datasource_config['aliasname'])
    
    if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
        pass  

    elif  tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
        querypath = root_path + tc_datasource_config['querypath']
        with open(querypath, "r") as f:
            query = f.read().splitlines()
        query = ' '.join(query)
        log_info(f"Select Table Command statement - \n{query}")
        df.createOrReplaceTempView(tc_datasource_config['aliasname'])

    elif tc_datasource_config['comparetype'] == 'likeobjectcompare':
        excludecolumns = tc_datasource_config['excludecolumns']
        excludecolumns = str(excludecolumns)
        exclude_cols = excludecolumns.split(',')
        datafilter = tc_datasource_config['filter']
        datafilter = str(datafilter)
        columns = df.columns
        columnlist = list(set(columns) - set(exclude_cols))
        columnlist.sort()
        columnlist = ','.join(columnlist)
        query= "SELECT " + columnlist + " FROM "+tc_datasource_config['aliasname']
        if len(datafilter) >=5:
            query= query + " WHERE " + datafilter
        log_info(f"Select Table Command statement - \n{query}")
    
    df_data = spark.sql(query)
    log_info("Returning the DataFrame from read_jsondata Function")
    
    return df_data, query