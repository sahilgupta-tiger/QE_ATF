from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info,debugexit,readconnectionconfig,set_azure_connection_config
from constants import *

def read_adls_avrodata(tc_datasource_config, spark):
    log_info("Reading the avro File")
    connectionname = tc_datasource_config['connectionname']
    connectiontype = tc_datasource_config['connectiontype']
    resourceformat = tc_datasource_config['format']

    # Reading Adls Connection Configuration
    connectionconfig = readconnectionconfig(connectionname)

    # Set Adls Connection Configuration
    storage_account, container_name = set_azure_connection_config(connectionconfig, spark)
    avrofilepath = tc_datasource_config['path']  # Relative path within the container
    print('ADLS File Path :', avrofilepath)

    if tc_datasource_config['testquerygenerationmode'] == 'Auto':
        resourcename = tc_datasource_config['name']
        datafilter = tc_datasource_config['filter']
        # avrofilepath = tc_datasource_config['path']
        excludecolumns = tc_datasource_config['excludecolumns']
        excludecolumns = str(excludecolumns)
        exclude_cols = excludecolumns.split(',')
        datafilter = str(datafilter)
        readavroschema = spark.read.format("avro").load(avrofilepath).schema
        readavrodata = spark.read.format("avro").schema(readavroschema).load(avrofilepath)
        df = preproc_unnestfields(readavrodata)
        df.createOrReplaceTempView(resourcename + "_avroview")
        columns = df.columns
        columnlist = list(set(columns) - set(exclude_cols))
        columnlist.sort()
        columnlist = ','.join(columnlist)
        query_avro = "SELECT " + columnlist + " FROM " + resourcename + "_avroview"
        if len(datafilter) >= 5:
            query_avro = query_avro + " WHERE " + datafilter
        df_data = spark.sql(query_avro)
    elif tc_datasource_config['testquerygenerationmode'] == 'Manual':
        querypath = root_path + tc_datasource_config['querypath']
        f = open(querypath, "r")
        query = f.read().splitlines()
        query = ' '.join(query)
        print(query)
        df = spark.read.format("avro").load(avrofilepath)
        df.printSchema()
        print(tc_datasource_config['aliasname'])
        df.createOrReplaceTempView(tc_datasource_config['aliasname'])
        df_data = spark.sql(query)

    '''col_names = df_avrodata.columns
    for i in col_names:
      df_avrodata=df_avrodata.withColumnRenamed(i,i.lower())
    '''
    log_info("Returning the DataFrame from read_avrodata Function")
    return df_data, query