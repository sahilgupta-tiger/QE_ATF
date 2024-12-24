from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config,get_dbutils
from testconfig import *


def read_adls_avrodata(tc_datasource_config, spark):
    log_info("Reading the avro File")
    connectionname = tc_datasource_config['connectionname']
    resourceformat = tc_datasource_config['format']

    # Reading Adls Connection Configuration
    connectionconfig = readconnectionconfig(connectionname)
    
    #Importing dbutils
    dbutils =  get_dbutils(spark)
    
    #Set Adls Connection Configuration
    set_azure_connection_config(spark,connectionconfig,dbutils)
    
    avrofilepath = tc_datasource_config['path']  # Relative path within the container
    

    if ('abfs' not in avrofilepath) and ('Volume' not in avrofilepath):
        log_info("Appending Root path to relative avro file Path")
        avrofilepath = root_path + avrofilepath
    
    log_info(f'ADLS File Path :- {avrofilepath}')

    readschemadf = spark.read.format("avro").load(avrofilepath).schema
    df= spark.read.format("avro").schema(readschemadf).load(avrofilepath)
    df.createOrReplaceTempView(tc_datasource_config['aliasname'])

    if tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Auto':
        pass  
    
    elif tc_datasource_config['comparetype'] == 's2tcompare' and tc_datasource_config['testquerygenerationmode'] == 'Manual':
        querypath = root_path + tc_datasource_config['querypath']
        f = open(querypath, "r")
        query = f.read().splitlines()
        query = ' '.join(query)
        log_info(f"Select Table Command statement - \n{query}")

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
    df_data.printSchema()
    #df_data.show() 
    log_info("Returning the DataFrame from read_avrodata Function")

    return df_data, query