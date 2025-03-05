import os.path
import json
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datf_core.src.atf.common.atf_cls_great_expectation_dq import *
from datf_core.src.atf.common.atf_cls_pdfformatting import generatePDF
from datf_core.src.testconfig import *


def log_info(msg):
    print(f'INFO [{str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip()}]: {msg}')


def log_error(msg):
    print(f'ERROR [{str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip()}]: {msg}')


def read_json_file(file_path):
    log_info(f"Reading JSON file from {file_path}")
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


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


def startdqtest(df, testsuite, json_file_path, stype, createdate):

    spark = createsparksession()
    spark_df = spark.createDataFrame(df)
    log_info("DQ test execution has been started")

    timenow = datetime.now(utctimezone)
    created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))

    try:
        pdfobj = generatePDF()
        pdfobj_summary = generatePDF()
        current_result_folder = f"run_dq_{created_time}"
        resultfolder =  os.path.join(dq_result_path, testsuite, current_result_folder)

        if not os.path.exists(resultfolder):
            os.mkdir(resultfolder)
            log_info(f"{resultfolder} is created")
        else:
            log_info(f"{resultfolder} is already exist")

        detailresultpath = resultfolder + "/dq_analysis_"+stype+"_detail_report_"+created_time+".pdf"
        summaryresultpath = resultfolder + "/dq_analysis_"+stype+"_summary_report_"+created_time+".pdf"

        pdfobj.write_text("Data Quality Analysis Detailed Report", 'report header')
        pdfobj.write_text(testsuite, 'subheading')
        rows = read_json_file(json_file_path)

        tc_file_path = os.path.join(dq_testconfig_path)
        tc_data = read_json_file(tc_file_path)
        print(f"Current testcase file data is: {tc_data}")

        for item in rows:
            if item["DQ Check"] in ["Regexp", "DistinctSet", "ColumnOrder","Regexplist"]:
                value = item["Value"]
                try:
                    item["Value"] = json.loads(item["Value"])
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON value: {item['Value']}")

        log_info(f"JSON Data after decode --> {rows}")
        batch = ge_test_initalization(spark_df)
        pdfobj,pdfobj_summary = ge_test_execution(pdfobj, pdfobj_summary, testsuite, batch, rows, spark, createdate, tc_data)
        pdfobj.pdf.output(detailresultpath, 'F')
        log_info(f"Data Quality Analysis is completed. The detailed report is created at location: {detailresultpath}")
        pdfobj_summary.pdf.output(summaryresultpath, 'F')
        log_info(f"The summary report is created at location: {summaryresultpath}")
        log_info("!!! DQ test execution has been completed !!!")
        return detailresultpath, summaryresultpath

    except Exception as e2:
        log_error(f"Protocol Execution ERRORED: {str(e2)}")
        log_info("DQ test execution has been completed with error")





