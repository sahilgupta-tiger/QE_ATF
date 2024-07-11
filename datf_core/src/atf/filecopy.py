from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from atf.common.atf_common_functions import read_protocol_file, log_error, log_info, read_test_case, get_connection_config, get_mount_src_path
from atf.common.atf_dc_read_datasources import read_data
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_cls_loads2t import LoadS2T
from atf.common.atf_cls_s2tautosqlgenerator import S2TAutoLoadScripts
from atf.common.atf_pdf_constants import *
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,DateType
import os
import datacompy
import sys

def createsparksession():
    spark = SparkSession.builder.master("local[1]") \
        .appName('s2ttester') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print(spark.sparkContext)
    print("Spark App Name : " + spark.sparkContext.appName)
    return spark

def getSchemaDefinitionSource(schemadf):
    schemaStruct = StructType()
    #print("I am at the point")
    #schemadf.printSchema()
    schemadfCollect = schemadf.collect()
    #print(schemadfCollect)
    for row in schemadfCollect:
      #print(row['sourcecolumnname'] + "," +row['sourcecolumndatatype'])
      if row['sourcecolumndatatype'] == "string" :
        schemaStruct.add(StructField(row['sourcecolumnname'],StringType(),True))
      elif row['sourcecolumndatatype'] == "datetime" :
        schemaStruct.add(StructField(row['sourcecolumnname'],DateType(),True))
      elif row['sourcecolumndatatype'] == "double" :
        schemaStruct.add(StructField(row['sourcecolumnname'],DoubleType(),True))
      elif row['sourcecolumndatatype'] == "integer" :
        schemaStruct.add(StructField(row['sourcecolumnname'],IntegerType(),True))
    #print(schemaStruct)
    return schemaStruct
    
if __name__ == "__main__":
    spark = createsparksession()
    s2t = LoadS2T("/app/s2ttestingtool/test/s2t/source_csv_target_csv_s2t_3.xlsx",spark)
    schemaStruct= getSchemaDefinitionSource(s2t.srcschema_df)
    datadf=spark.read.format("csv").option('delimiter',",").option('header','true').schema(schemaStruct).load('/app/s2ttestingtool/test/data/source/patients_source.csv')
    print("csv Schema")
    datadf.printSchema()
    print(datadf.count())
    datadf.write.mode("overwrite").parquet("/app/s2ttestingtool/test/data/source/patients_source_parquet")
    datadf.write.mode("overwrite").parquet("/app/s2ttestingtool/test/data/stage/patients_target_parquet_match")
    datadf.write.mode("overwrite").json("/app/s2ttestingtool/test/data/source/patients_source_json")
    datadf.write.mode("overwrite").json("/app/s2ttestingtool/test/data/stage/patients_target_json_match")
    print("parquet Schema")
    parquetdf=spark.read.format("parquet").load("/app/s2ttestingtool/test/data/source/patients_source_parquet")
    parquetdf.printSchema()
    print(parquetdf.count())
    print("json Schema")
    jsondf=spark.read.format("json").schema(schemaStruct).option('multiline','true').load("/app/s2ttestingtool/test/data/source/patients_source_json")
    jsondf.printSchema()
    print(jsondf.count())
    filterdf=datadf.where("Id not in ('1d604da9-9a81-4ba9-80c2-de3375d59b40','034e9e3b-2def-4559-bb2a-7850888ae060')")
    print(filterdf.count())
    filterdf.write.mode("overwrite").parquet("/app/s2ttestingtool/test/data/stage/patients_target_parquet_mismatch")
    filterdf.write.mode("overwrite").json("/app/s2ttestingtool/test/data/stage/patients_target_json_mismatch")
