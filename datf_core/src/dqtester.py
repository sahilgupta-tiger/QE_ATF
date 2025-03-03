
import os
import pandas as pd
import json
from datetime import datetime,timezone
from datf_core.src.atf.common.atf_great_expectation_dq import *
import json
import os
import traceback
from datetime import datetime
import pandas as pd
from datacompy.spark.legacy import LegacySparkCompare
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datf_core.src.atf.common.atf_cls_pdfformatting import generatePDF
from datf_core.src.atf.common.atf_pdf_constants import *
from datf_core.src.atf.common.atf_common_functions_dq import read_protocol_file, log_error, log_info, read_test_case, get_connection_config, get_mount_src_path
from datf_core.src.testconfig import *
import ast
#import databricks.koalas as ks

# Use pandas profiling to generate a report
#from pandas_profiling import ProfileReport
#from pandas_profiling import ProfilingReport


def createsparksession():

    if protocol_engine == "databricks":
        spark = SparkSession.getActiveSession()
        if spark is not None:
            print("!!! Databricks Spark Session Acquired !!!")
    else:
        conf_dict = json.loads(spark_conf_JSON)
        myconf = SparkConf().setMaster("local[*]").setAppName('s2ttester')
        for key, val in conf_dict.items():
            myconf.set(key, val)

        spark = SparkSession.builder.config(conf=myconf).getOrCreate()
        spark.sparkContext.setLogLevel('WARN')

        configs = spark.sparkContext.getConf().getAll()
        for item in configs:
            print(item)

    print(spark)
    return spark

def startdqtest(work_path,df,testsuite,json_file_path,stype,createdate):
    spark = createsparksession()
    spark_df = spark.createDataFrame(df)
    print(type(spark_df))
    log_info("DQ test execution has been started")
    #log_info(f"JSON Data --> {json_data}")
    #spark_df.show()
    try:
        pdfobj = generatePDF(work_path)
        pdfobj_summary = generatePDF(work_path)
        testsuite = testsuite
        resultfolder =  dq_result_path+testsuite
        if not os.path.exists(resultfolder):
            os.mkdir(resultfolder)
            log_info(f"{resultfolder} is created")
        else:
            log_info(f"{resultfolder} is already exist")
        utctimezone = timezone.utc
        timenow = datetime.now(utctimezone)
        created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))

        detailresultpath = resultfolder + "/data_quality_analysis_"+stype+"_detail_report_"+created_time+".pdf"
        summaryresultpath = resultfolder + "/data_quality_analysis_"+stype+"_summary_report_"+created_time+".pdf"
        pdfobj.write_text("Data Quality Analysis Detailed Report", 'report header')
        pdfobj.write_text(testsuite, 'subheading')
        rows = read_json_file(json_file_path)

        print(rows)
        tc_file_name = f"{testsuite}_{createdate}.json"
        tc_file_path = os.path.join(dq_testconfig_path, tc_file_name)
        tc_data = read_json_file(tc_file_path)
        print(f"tc file data is {tc_data}")
        #rows = json_data

        for item in rows:
            #print("inside for loop")
            if item["DQ Check"] in ["Regexp", "DistinctSet", "ColumnOrder","Regexplist"]:
                #print("inside if loop")
                value = item["Value"]
                try:
                    item["Value"] = json.loads(item["Value"])
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON value: {item['Value']}")

        log_info(f"JSON Data after decode --> {rows}")
        batch = ge_test_initalization(spark_df)
        pdfobj,pdfobj_summary = ge_test_execution(pdfobj,pdfobj_summary, testsuite, batch,rows,spark,createdate,tc_data)
        #pdfobj = ge_test_execution(pdfobj, pdfobj_summary, testsuite, batch, rows, spark)
        DQValidation_endtime = datetime.now(utctimezone)
        pdfobj.pdf.output(detailresultpath, 'F')
        log_info(
                    f"Data Quality Analysis is completed. The detailed report is created at location:{detailresultpath}")
        pdfobj_summary.pdf.output(summaryresultpath, 'F')
        log_info(
                    f"The summary report is created at location:{summaryresultpath}")
        log_info("DQ test execution has been completed")
        return detailresultpath,summaryresultpath
        #return detailresultpath
    except Exception as e2:
        log_error(f"Protocol Execution ERRORED: {str(e2)}")
        log_info("DQ test execution has been completed with error")

def read_json_file(file_path):
    log_info(f"Reading JSON file from {file_path}")
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


