
import os
import pandas as pd
import json
from datetime import datetime,timezone
from atf.common.atf_great_expectation_dq import *
import json
import os
import traceback
from datetime import datetime
import pandas as pd
from datacompy.spark.legacy import LegacySparkCompare
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_pdf_constants import *
from atf.common.atf_common_functions import read_protocol_file, log_error, log_info, read_test_case, get_connection_config, get_mount_src_path
from testconfig import *
#import databricks.koalas as ks

# Use pandas profiling to generate a report
#from pandas_profiling import ProfileReport
#from pandas_profiling import ProfilingReport


def createsparksession():

    if protocol_engine == "databricks":
        spark = SparkSession.getActiveSession()
        if spark is not None:
            log_info("!!! Databricks Spark Session Acquired !!!")
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

class dqtester:

    def __init__(self, spark):
        self.var = ""
        self.spark = spark

    def startdqtest(self, work_path, df,testsuite,json_file_path):
        log_info("DQ test execution has been started")
        print(f"work path: {work_path}")
        pdfobj = generatePDF(work_path)
        pdfobj_summary = generatePDF(work_path)
        self.testsuite = testsuite
        resultfolder =  work_path + "/test/results/data_quality/"+testsuite
        if not os.path.exists(resultfolder):
            os.mkdir(resultfolder)
            log_info(f"{resultfolder} is created")
        else: 
            log_info(f"{resultfolder} is already exist")
        utctimezone = timezone.utc
        timenow = datetime.now(utctimezone)
        created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))
        detailresultpath = resultfolder + "/data_quality_analysis_detail_report_"+created_time+".pdf"
        summaryresultpath = resultfolder + "/data_quality_analysis_summary_report_"+created_time+".pdf"
        pdfobj.write_text("Data Quality Analysis Detailed Report", 'report header')
        pdfobj.write_text(testsuite, 'subheading')
        rows = read_json_file(json_file_path)
        print(rows)

        for item in rows:
            if item["DQ Check"] in ["Regexp", "DistinctSet", "ColumnOrder"]:
                try:
                    item["Value"] = json.loads(item["Value"])
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON value: {item['Value']}")

        print(rows)
        batch = ge_test_initalization(dfname)
        pdfobj,pdfobj_summary = ge_test_execution(pdfobj,pdfobj_summary, testsuite, batch,rows,spark)
        DQValidation_endtime = datetime.now(utctimezone)  
        pdfobj.pdf.output(detailresultpath, 'F')
        log_info(
                    f"Data Quality Analysis is completed. The detailed report is created at location:{detailresultpath}") 
        pdfobj_summary.pdf.output(summaryresultpath, 'F')  
        log_info(
                    f"The summary report is created at location:{summaryresultpath}")          

def read_json_file(file_path):
    log_info(f"Reading JSON file from {file_path}")
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# Example usage
#file_path = '/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/dataquality/test_data.json'


# Print the records







if __name__ == "__main__":
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    #os.environ["POLARS_ALLOW_FORKING_THREAD"]="1"
    spark = createsparksession()
    #testcasesrunlist = ['all']
    work_path = sys.argv[1]
    dfname = sys.argv[2]
    print(dfname)
    testsuite = sys.argv[3]
    json_file_path = sys.argv[4]

    log_info(f"work_path: {work_path}")
    log_info(f"testsuite: {testsuite}")
    log_info(f"json_file_path: {json_file_path}")
    #create_db(work_path)
    testerobj = dqtester(spark)
    testerobj.startdqtest(work_path, df, testsuite,json_file_path)
   
