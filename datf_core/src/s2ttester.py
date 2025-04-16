import json
import os
import traceback
from datetime import datetime
import datacompy
import pandas as pd
from datacompy.spark.legacy import LegacySparkCompare
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from atf.common.atf_cls_loads2t import LoadS2T
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_cls_results_chart import generate_results_charts
from atf.common.atf_cls_s2tautosqlgenerator import S2TAutoLoadScripts
from atf.common.atf_common_functions import read_protocol_file, log_error, log_info, read_test_case, \
    get_connection_config, get_mount_src_path,apply_md5_hash,verify_nulls,verify_duplicates,check_empty_values, check_entire_row_is_null, list_duplicate_values,verify_dup_pk,read_s2t,get_actual_schema

from atf.common.atf_dc_read_datasources import read_data
from atf.common.atf_pdf_constants import *
from atf.common.atf_sc_read_datasources import read_schema #Added the line on 07/04/2025
from testconfig import *


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

        log_info("Spark Session Configuration items are listed below -")
        configs = spark.sparkContext.getConf().getAll()
        log_info('Spark Version :' + spark.version)
        log_info('SparkContext Version :' + spark.sparkContext.version)
        for item in configs:
            log_info(item)

    log_info(spark)
    return spark


class S2TTester:

    def __init__(self, spark):
        self.var = ""
        self.spark = spark
    
    def starttestexecute(self, protocol_file_path, testcasetype, testcasesrunlist):
        log_info(f"Protocol Execution Started")
        try:
            log_info(
                f"Reading the Protocol file details from {protocol_file_path}")
            dict_protocol, df_testcases = read_protocol_file(
                protocol_file_path)
            log_info("Protocol read completed :- ")
            log_info(dict_protocol)

            results_path = str(root_path+dict_protocol['protocol_results_path'])
            #Creating directory for results folder
            if not os.path.exists(results_path):
                log_info(f"Creating directory - {results_path}")
                os.mkdir(results_path)
            else:
                log_info(f"The directory - `{results_path}` already exists.")

            timenow = datetime.now(utctimezone)
            created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))

            cloud_results_folder = results_path + \
                str(dict_protocol['protocol_name']) + \
                '/run_'+testcasetype+"_"+created_time+'/'
            log_info(f"Protocol Result folder Path: {cloud_results_folder}")
            os.mkdir(cloud_results_folder)
            testcase_cloud_results_folder = cloud_results_folder + '/run_testcase_summary_' + created_time+'/'
            os.mkdir(testcase_cloud_results_folder)

            protocol_output_path = cloud_results_folder
            testcase_output_path = testcase_cloud_results_folder
            combined_testcase_output_path = protocol_output_path + "/run_tc_combined_" + \
                str(dict_protocol['protocol_name']) + \
                "_" + created_time + ".pdf"

            df_protocol_summary, protocol_run_details, protocol_run_params = self.execute_protocol(
                dict_protocol, df_testcases, testcase_output_path, combined_testcase_output_path, testcasetype, testcasesrunlist)

            summary_output_path = self.generate_protocol_summary_report(
                df_protocol_summary, protocol_run_details, protocol_run_params, protocol_output_path, created_time, testcasetype)
            # generate HTML report ** new function **
            generate_results_charts(df_protocol_summary, protocol_run_details, protocol_run_params, created_time, testcasetype, cloud_results_folder, combined_testcase_output_path, summary_output_path)
            log_info("Protocol Execution Completed")

        except Exception as e2:
            log_error(f"Protocol Execution ERRORED: {str(e2)}")

    def execute_protocol(self, dict_protocol, df_testcases, output_path, combined_testcase_output_path, testcasetype,testcasesrunlist):

        log_info("Protocol Testcases Execution Started")
        protocol_starttime = datetime.now(utctimezone)
        # auto_script_path = generate_autoscript_path(combined_testcase_output_path)
        auto_script_path = ""
        if testcasetype == "count":
            df_protocol_summary = pd.DataFrame(columns=[
                                               'Testcase Name', 'No. of Rows in Source', 'No. of Rows in Target', 'Test Result', 'Reason', 'Runtime'])

        elif testcasetype == "duplicate":
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No. of Rows in Source', 'No. of Distinct Rows in Source',
                                    'No. of Rows in Target', 'No. of Distinct Rows in Target', 'Test Result', 'Reason', 'Runtime'])

        elif testcasetype == "null":
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No of column has null in source','No of column has null in target',
                                    'No of columns has null count match','No of columns has null count mismatch', 'Test Result', 'Reason', 'Runtime'])

        elif testcasetype == "content" or testcasetype == 'schema': #Included the testcase type Schema condition on 07/05
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No. of Rows in Source', 'No. of Rows in Target',
                                    'No. of Rows matched', 'No. of Rows mismatched', 'Test Result', 'Reason', 'Runtime'])
        
        elif testcasetype == "fingerprint":
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No. of KPIs in Source',
                                    'No. of KPIs in Target', 'No. of KPIs matched',
                                    'No. of KPIs mismatched', 'Test Result', 'Reason', 'Runtime'])

        pdfobj_combined_testcase = generatePDF()

        testcases_run_list = []

        if testcasesrunlist[0] != 'all':
            log_info("Test Case(s) part of current execution are:")
            print(testcasesrunlist)
        lst_run_testcases = testcasesrunlist
        
        for index, row in df_testcases.iterrows():
            test_case_name = row['test_case_name']
            log_info(f"{test_case_name}: Testcase Picked up for Execution")
            row = row.fillna('')
            row = row.apply(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x)
            testcase_details = {}
            tcnbr = str(int(row['Sno.']))
            execute_flag = row['execute']
            #test_case_file_path = root_path+row['test_case_file_path']

            try:
                if (lst_run_testcases[0] == 'all' and (execute_flag == 'Y' or execute_flag == True)):
                    testcases_run_list.append(test_case_name)
                    pass
                elif (test_case_name.lower() in lst_run_testcases):
                    testcases_run_list.append(test_case_name)
                    pass
                else:
                    log_info(
                        f"The Test Case No.{tcnbr} is not in run_test_cases param, skipping its execution")
                    continue

                if (len(testcases_run_list) > 1):
                    pdfobj_combined_testcase.pdf.add_page()

                log_info(
                    f"Reading Test Case Config for:  {test_case_name}")
                testcase_details = dict(row[2:])
                #testcase_details = read_test_case(test_case_file_path)

                testcase_starttime = datetime.now(utctimezone)

                log_info(f"Reading Source and Target Data based on TestCase Configuration:  {test_case_name}")

                compare_input = self.execute_testcase(test_case_name, testcase_details, auto_script_path, testcasetype)

                log_info(f"Comparing Source and Target Data based on TestCase Configuration Started:  {test_case_name}")
                dict_compareoutput = self.compare_data(compare_input, testcasetype)

                testcase_endtime = datetime.now(utctimezone)
                testcase_exectime = testcase_endtime - testcase_starttime
                testcase_exectime = str(testcase_exectime).split('.')[0]
                log_info(f"Comparing Source and Target Data based on TestCase Configuration Completed for {test_case_name}")
                log_info(f"Execution of Test Case {test_case_name} completed in {testcase_exectime}")

                source_df = compare_input['sourcedf']
                target_df = compare_input['targetdf']
                source_null_counts = []
                target_null_counts = []

                log_info(
                    f"{row['test_case_name']}: Test Results PDF Generation for Test Case Started for {test_case_name}")
                testcase_starttime = testcase_starttime.astimezone(
                    utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
                testcase_endtime = testcase_endtime.astimezone(
                    utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
                # testcase_exectime = testcase_exectime.strftime("%H:%M:%S")

                dict_runsummary = {'Application Name': dict_protocol['protocol_application_name'], 'Protocol Name': dict_protocol['protocol_name'],
                                   "Protocol File Path": protocol_file_path, 'Testcase Name': test_case_name,
                                   'Testcase Type': testcasetype,
                                   'Test Environment': dict_protocol['protocol_run_environment'], 'Start Time': testcase_starttime,
                                   'End Time': testcase_endtime, 'Run Time': testcase_exectime, 'Test Result': dict_compareoutput['test_result'],
                                   'Reason': dict_compareoutput['result_desc']}
                # ADD if condition for testcasetype
                join_cols = ",".join(compare_input['joincolumns'])
                dict_config = {'Compare Type': testcase_details['comparetype'], 'testquerygenerationmode': testcase_details['testquerygenerationmode'], 'Testcase Type': testcasetype, 'Source Connection Name': testcase_details['sourceconnectionname'],
                                'Source Connection Type': testcase_details['sourceconnectiontype'], 'Source Connection Value': testcase_details['sourceconnectionname'], 
                                'Source Format': testcase_details['sourcefileformat'], 'Source Schema': '', 'Source Name': testcase_details['sourcefilename'],
                                  'Source Path': testcase_details['sourcefilepath'], 'Source Exclude Columns': testcase_details['sourceexcludecolumnlist'], 
                                  'Source Filter': testcase_details['sourcefilter'], 'Target Connection Name': testcase_details['targetconnectionname'], 
                                  'Target Connection Type': testcase_details['targetconnectiontype'], 'Target Connection Value': testcase_details['targetconnectionname'], 
                                  'Target Format': testcase_details['targetfileformat'], 'Target Schema': '', 'Target Name': testcase_details['targetfilename'], 
                                  'Target Path': testcase_details['targetfilepath'], 'Target Exclude Columns': testcase_details['targetexcludecolumnlist'], 
                                  'Target Filter': testcase_details['targetfilter'], 'S2T Path': testcase_details['s2tpath'], 'Primary Keys': join_cols}
                dict_config_temp = dict_config.copy()
                testcasetype = compare_input['testcasetype']
                col_match_summary = dict_compareoutput['col_match_summary']
                comparison_type = testcase_details['comparetype']
                dict_testresults = dict_compareoutput['dict_results']
                testcasereportheader = ""
                pdfobj = generatePDF()
                pdfobj.write_text(testcasereportheader, 'report header')
                pdfobj.write_text(
                    dict_runsummary['Testcase Name'], 'subheading')
                pdfobj_combined_testcase.write_text(
                    dict_runsummary['Testcase Name'], 'report header')
                results_path = output_path + '/' + test_case_name + '_' + \
                    dict_compareoutput['test_result'].lower() + '.pdf'
                #testcase summary pdf creation
                pdfobj = self.generate_testcase_summary_report(dict_runsummary, dict_config, results_path, compare_input, dict_compareoutput, testcasetype, comparison_type, pdfobj)
                pdfobj.pdf.output(results_path, 'F')
                log_info(
                    f"{row['test_case_name']}: Testcase Results PDF for {test_case_name} testcase is created at location:{results_path}")
                log_info(
                    f"{row['test_case_name']}: TestCase Results PDF Generation for Test Case Completed:{test_case_name}")
                dict_config = dict_config_temp

                log_info(
                    f"{row['test_case_name']}: TestCase Results of  {test_case_name} are Appended to the Summary")
                pdfobj_combined_testcase = self.generate_testcase_summary_report(
                    dict_runsummary, dict_config, results_path, compare_input, dict_compareoutput, testcasetype, comparison_type, pdfobj_combined_testcase)
                
                if testcasetype == "count":
                    df_protocol_summary.loc[index] = [test_case_name, str(dict_testresults['No. of rows in Source']), str(
                        dict_testresults['No. of rows in Target']), dict_compareoutput['test_result'], dict_compareoutput['result_desc'], str(testcase_exectime)]

                elif testcasetype == "duplicate":
                    df_protocol_summary.loc[index] = [test_case_name, str(dict_testresults['No. of rows in Source']), str(dict_testresults['No. of distinct rows in Source']),
                                    str(dict_testresults['No. of rows in Target']), str(dict_testresults['No. of distinct rows in Target']), dict_compareoutput['test_result'],
                                                      dict_compareoutput['result_desc'], str(testcase_exectime)]

                elif testcasetype == "null":
                    df_protocol_summary.loc[index] = [test_case_name,str(dict_testresults['No of column has null in source']),str(dict_testresults['No of column has null in target']),
                                    str(dict_testresults['No of columns has null count match']),str(dict_testresults['No of columns has null count mismatch']),dict_compareoutput['test_result'],
                                                      dict_compareoutput['result_desc'], str(testcase_exectime)]

                elif testcasetype == "content" or testcasetype == 'schema': #Included schema condtion on 07/04/2025
                    df_protocol_summary.loc[index] = [test_case_name, str(dict_testresults['No. of rows in Source']), str(dict_testresults['No. of rows in Target']),
                                    str(dict_testresults['No. of matched rows']), str(dict_testresults['No. of mismatched rows']), dict_compareoutput['test_result'],
                                                      dict_compareoutput['result_desc'], str(testcase_exectime)]
                
                elif testcasetype == "fingerprint":
                    df_protocol_summary.loc[index] = [test_case_name, str(dict_testresults['No. of KPIs in Source']), str(dict_testresults['No. of KPIs in Target']),
                                    str(dict_testresults['No. of KPIs matched']), str(dict_testresults['No. of KPIs mismatched']), dict_compareoutput['test_result'],
                                                      dict_compareoutput['result_desc'], str(testcase_exectime)]
                    
                log_info(
                    f"{row['test_case_name']}: Testcase Execution Completed for {row['test_case_name']}")
            except Exception as e1:
                log_error(f"{row['test_case_name']}: Testcase Execution ERRORED:{row['test_case_name']} - ERRORMSG:{str(e1)}")
                log_error(traceback.format_exc())
                if testcasetype == "count":
                    df_protocol_summary.loc[index] = [
                        test_case_name, '', '', 'Failed', 'Execution Error', '']
                if (testcasetype == "content" or testcasetype == "duplicate" or testcasetype == "fingerprint"):
                    df_protocol_summary.loc[index] = [
                        test_case_name, '', '', '', '', 'Failed', 'Execution Error', '']

        pdfobj_combined_testcase.pdf.output(
            combined_testcase_output_path, 'F')  # generate combined pdf
        log_info("Combined Test Case PDF Generated")
        protocol_endtime = datetime.now(utctimezone)
        protocol_exectime = protocol_endtime - protocol_starttime
        protocol_exectime = str(protocol_exectime).split('.')[0]
        log_info(
            f"Protocol {dict_protocol['protocol_name']} executed in {protocol_exectime}")
        protocol_starttime = protocol_starttime.astimezone(
            utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
        protocol_endtime = protocol_endtime.astimezone(
            utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")

        totaltestcases = 0
        testcasespassed = 0
        testcasesfailed = 0
        if (len(df_protocol_summary) != 0):
            totaltestcases = len(df_protocol_summary)
            testcasespassed = len(
                df_protocol_summary[df_protocol_summary["Test Result"] == "Passed"])
            testcasesfailed = len(
                df_protocol_summary[df_protocol_summary["Test Result"] == "Failed"])
            df_protocol_summary = df_protocol_summary.sort_values('Reason')
            df_protocol_summary = spark.createDataFrame(df_protocol_summary)
        else:
            log_info("No testcase executed.")
            df_protocol_summary = None

        protocol_run_details = {'Application Name': dict_protocol['protocol_application_name'], 'Test Protocol Name': dict_protocol['protocol_name'], 'Test Protocol Version': dict_protocol['protocol_version'], 'Test Environment': dict_protocol['protocol_run_environment'],
                                'Test Protocol Start Time': protocol_starttime, 'Test Protocol End Time': protocol_endtime, 'Total Protocol Run Time': protocol_exectime, 'Total No of Test Cases': totaltestcases, 'No of Test Cases Passed': testcasespassed, 'No of Test Cases Failed': testcasesfailed}

        if (lst_run_testcases[0] == ''):
            testcases_run_list = "All testcases from protocol executed"
        else:
            testcases_run_list = ",".join(testcases_run_list)
        protocol_run_params = {"Protocol File Path": protocol_file_path,
                               "Testcases Executed": testcases_run_list, "Testcase Type": testcasetype}

        return df_protocol_summary, protocol_run_details, protocol_run_params
    

    def execute_testcase(self, test_case_name, tc_config, auto_script_path, testcasetype):
        print(tc_config)
        s2tmappingsheet = tc_config['s2tmappingsheet']
        
        source_file_details_dict = None
        if (tc_config['samplelimit'] is None):
            limit = 10
        else:
            limit = tc_config['samplelimit']

        log_info(f"Sample limit is {limit}")
        
        join_cols = tc_config['primarykey'].split(',')
        #join_cols = [word.upper() for word in join_cols]
        join_cols_source = []
        join_cols_target = []
        map_cols = []

        #tc_config['testquerygenerationmode'] = 'Auto' #Please comment it

        if tc_config['s2tpath'] != "":
            log_info(f"Reading the S2T for Source Details located at {tc_config['s2tpath']}")

        if tc_config['comparetype'] == 's2tcompare':
            s2tobj = LoadS2T(tc_config['s2tpath'], self.spark)

        if (tc_config['comparetype'] == 's2tcompare' and tc_config['testquerygenerationmode'] == 'Manual'):
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path':  tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': root_path + tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],
                           'schemastruct': s2tobj.getSchemaStruct("source"),'comparetype':tc_config['comparetype'],'filename':tc_config['sourcefilename']}
            if testcasetype == 'schema':
                log_info(f"PLEASE USE QUERY MODE AS 'MANUAL' FOR FORMAT : {tc_config['sourcefileformat']} IN SOURCE ")
            else:
                source_df, source_query = read_data(tc_source_config,self.spark)


            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path':  tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': root_path + tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],
                           'schemastruct': s2tobj.getSchemaStruct("target"),'comparetype':tc_config['comparetype'],'filename':tc_config['targetfilename']} 
            if testcasetype == 'schema':
                log_info(f"PLEASE USE QUERY MODE AS 'MANUAL' FOR FORMAT : {tc_config['targetfileformat']} IN TARGET")
            else:
                target_df, target_query = read_data(tc_target_config,self.spark)

        elif (tc_config['comparetype'] == 's2tcompare' and tc_config['testquerygenerationmode'] == 'Auto'):
            # s2tconnectionval = get_connection_config(testcase_details['s2tconnectionname'])['BUCKETNAME']
            # s2tfilepath = s2tconnectionval + testcase_details['s2tpath']
            log_info(f"Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'],'connectionname': tc_config['sourceconnectionname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath'], 'format': tc_config['sourcefileformat'], 'name': tc_config['sourcefilename'],
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'],
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'],
                           'test_case_name': test_case_name, 'autoscripttype': 'source', 'autoscriptpath': auto_script_path,'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilename']}
            autoldscrobj = S2TAutoLoadScripts(s2tobj, tc_source_config, self.spark)
            scriptpath, source_df, source_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)
            log_info(f"Reading the Source Data s2t {source_file_details_dict}")
            source_conn_name = source_file_details_dict["connectionname"]
            join_cols_source = source_file_details_dict["join_columns"]
            source_query = open(scriptpath).read().split('\n')
            #Added the if loop completely on 07/04/2025
            if testcasetype == 'schema':
                source_s2t_schema_df = s2tobj.getSchema("source")
                sourceflag, source_s2t_schema_df = ('Y', source_s2t_schema_df.drop("comments")) if source_s2t_schema_df.count() > 0 else ('N', source_s2t_schema_df)
                log_info("Printing s2t source schema")
                source_s2t_schema_df.show()
                source_actual_schema_df, source_schema_query = read_schema(source_file_details_dict,tc_config['comparetype'],self.spark)
                source_actual_schema_df = get_actual_schema(source_file_details_dict,source_actual_schema_df)

            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['sourcealiasname'],'connectionname': tc_config['targetconnectionname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath'], 'format': tc_config['targetfileformat'], 'name': tc_config['targetfilename'],
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'],
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'],
                           'test_case_name': test_case_name, 'autoscripttype': 'target', 'autoscriptpath': auto_script_path,'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilename']}
            autoldscrobj = S2TAutoLoadScripts(s2tobj, tc_target_config, self.spark)
            scriptpath, target_df, target_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)
            log_info(f"Reading the target Data s2t {target_file_details_dict}")
            target_conn_name = target_file_details_dict["connectionname"]
            join_cols_target = target_file_details_dict["join_columns"]
            target_query = open(scriptpath).read().split('\n')
            #Added if loop completely on 07/04/2025
            if testcasetype == 'schema':
                target_s2t_schema_df = s2tobj.getSchema("target")
                targetflag, target_s2t_schema_df = ('Y', target_s2t_schema_df.drop("comments")) if target_s2t_schema_df.count() > 0 else ('N', target_s2t_schema_df)
                log_info("Printing s2t target schema")
                target_s2t_schema_df.show()
                log_info(f"row count of source -- {source_s2t_schema_df.count()}")
                log_info(f"row count of target -- {target_s2t_schema_df.count()}")
                target_actual_schema_df, target_schema_query = read_schema(target_file_details_dict,tc_config['comparetype'],self.spark)
                #target_actual_schema_df = target_actual_schema_df.withColumn(lit("target"),"tabletype").select("tabletype","tablename","columnname","columnindex","datatype","nullable","primarykey","length","precision","scale")
                target_actual_schema_df = get_actual_schema(target_file_details_dict,target_actual_schema_df)

        elif tc_config['comparetype'] == 'likeobjectcompare' and tc_config['testquerygenerationmode'] == 'Manual':
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': root_path + tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilename']}
            source_df, source_query = read_data(tc_source_config,self.spark)
           
            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': root_path + tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilename']}    
            target_df, target_query = read_data(tc_target_config,self.spark)


        #Added the elif statement and loop completely on 07/04/2025 
        
        #Added the code to handle Auto query mode on 07/04/2025
        elif tc_config['comparetype'] == 'likeobjectcompare' and tc_config['testquerygenerationmode'] == 'Auto':
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': root_path + tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilepath']+"."+tc_config['sourcefilename']}
            if testcasetype != 'schema':
                source_df, source_query = read_data(tc_source_config,self.spark)
            else:
                log_info("In Schem condition 1")
                #tc_config['s2tpath'] = 'test/s2t/s2t_1_parquet_parquet_mismatch.xlsx' #Please comment it
                source_s2t_schema_df, target_s2t_schema_df,sourceflag,targetflag = read_s2t(tc_config['s2tpath'],self.spark)
                source_actual_schema_df, source_schema_query = read_schema(tc_source_config,tc_config['comparetype'],self.spark)
                source_actual_schema_df = get_actual_schema(tc_source_config,source_actual_schema_df)
                
                    

            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': root_path + tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilepath']+"."+tc_config['targetfilename']} 
            if testcasetype != 'schema':   
                target_df, target_query = read_data(tc_target_config,self.spark)
            else:
                log_info("In Schem condition 2")
                target_actual_schema_df, target_schema_query = read_schema(tc_target_config,tc_config['comparetype'],self.spark)
                target_actual_schema_df = get_actual_schema(tc_target_config,target_actual_schema_df)

        if testcasetype == 'schema' and tc_config['testquerygenerationmode'] == 'Auto':
            log_info("In New Condition")
            if sourceflag == 'N' and targetflag !='N':
                target_df = target_actual_schema_df
                source_df = target_s2t_schema_df
            elif sourceflag != 'N' and targetflag =='N':
                source_df = source_s2t_schema_df
                target_df = source_actual_schema_df
            else:
                source_df = source_s2t_schema_df.union(target_s2t_schema_df)
                target_df = source_actual_schema_df.union(target_actual_schema_df)
            source_df.show()
            target_df.show()  
            source_query = "select * from s2tmappingsheet.schema where tabletype in ('source','target')"
            target_query = source_schema_query + " union " + target_schema_query

        if (source_file_details_dict is not None):
            file_details_dict = {"sourcefile": source_file_details_dict["file_path"], "targetfile": target_file_details_dict["file_path"], "sourceconnectionname": source_conn_name,
                                 "targetconnectionname": target_conn_name, "sourceconnectiontype": source_file_details_dict['connectiontype'], "targetconnectiontype": target_file_details_dict['connectiontype']}
        else:
            file_details_dict = {"sourcefile": None, "targetfile": None,
                                 "sourceconnectionname": "", "targetconnectionname": ""}

        '''
        # splitting the columns from source/target queries and writing mismatched in a tuple-list
        if (tc_config['comparetype'] == 'likeobjectcompare' and tc_config['testquerygenerationmode'] == 'Manual'):
            find_src_cols = source_query.split(" ")
            find_tgt_cols = target_query.split(" ")
            temp_src_cols = find_src_cols[1].split(",")
            temp_tgt_cols = find_tgt_cols[1].split(",")
  
            map_cols = [(x, y) for x in temp_src_cols for y in temp_tgt_cols if x != y and x.lower() == y.lower()]
            log_info(f"Case-sensitive mismatched columns in both queries are: {map_cols}")
        '''

        # compareInput Output values
        compare_input = {'sourcedf': source_df, 
                         'targetdf': target_df, 
                         'sourcequery': source_query, 
                         'targetquery': target_query,
                         'colmapping': map_cols, 
                         'joincolumns': join_cols, 
                         'testcasetype': testcasetype, 
                         'limit': limit, 
                         "filedetails": file_details_dict,
                         "sourceformat": tc_config['sourcefileformat'],
                         "targetformat": tc_config['targetfileformat'],
                         "testmode":tc_config['testquerygenerationmode']}
        # print(compare_input)
        return compare_input

    
    def compare_data(self, compare_input, testcasetype):
        log_info(f"Data Compare Started for TestingType - {testcasetype} ")
        sourcedf = compare_input['sourcedf']
        targetdf = compare_input['targetdf']
        joincolumns = compare_input['joincolumns']
        colmapping = compare_input['colmapping']
        limit = compare_input['limit']
        testmode = compare_input['testmode']
        sourceformat = compare_input['sourceformat']
        targetformat = compare_input['targetformat']
        dict_match_summary = {}
        dict_match_details = {}
        # Initial spark memory processing STARTS HERE...
        print("Counting Source Rows now...")
        rowcount_source = sourcedf.count()
        print("Counting Target Rows now...")
        rowcount_target = targetdf.count()

        if (testcasetype == 'content'):
            
            print("Comparing Contents of Source and Target now...(this may take a while)...")
            log_info(f"Joining with column names: {joincolumns}")
            comparison_obj = LegacySparkCompare(self.spark, sourcedf, targetdf, \
                                                    join_columns=joincolumns, column_mapping=colmapping, \
                                                    cache_intermediates=True)
            #comparison_obj.report()
            distinct_rowcount_source = sourcedf.select(joincolumns).distinct().count()
            distinct_rowcount_target = targetdf.select(joincolumns).distinct().count()
            duplicate_rowcount_source = rowcount_source - distinct_rowcount_source
            duplicate_rowcount_target = rowcount_target - distinct_rowcount_target
            rows_both_all = comparison_obj.rows_both_all
            rows_mismatch = comparison_obj.rows_both_mismatch
            rows_only_source = comparison_obj.rows_only_base
            rows_only_target = comparison_obj.rows_only_compare
            rowcount_common = comparison_obj.common_row_count
            rowcount_only_source = rows_only_source.count()
            rowcount_only_target = rows_only_target.count()
            rowcount_mismatch = rows_mismatch.count()
            rowcount_match = rowcount_common - rowcount_mismatch
            col_only_source = comparison_obj.columns_only_base
            colcount_source = len(col_only_source)
            col_only_target = comparison_obj.columns_only_compare
            colcount_target = len(col_only_target)
            columns_match = comparison_obj.columns_in_both
            colcount_match = len(columns_match)
            columns_compared = comparison_obj.columns_compared
            colcount_compared = len(columns_compared)

            rowcount_total_mismatch = rowcount_only_target + \
                rowcount_only_source  # + rowcount_mismatch

            if (rowcount_mismatch > 0 or rowcount_only_target > 0 or rowcount_only_source > 0):
                log_info("Test Case Failed as Content Mismatched")
                result_desc = "Content mismatched"
                test_result = "Failed"
            else:
                log_info("Test Case Passed as Content Matched")
                result_desc = "Content matched"
                test_result = "Passed"

            dict_results = {'Test Result': test_result,
                            'No. of matched columns': f"{colcount_match:,}",
                            'No. of columns compared': f"{colcount_compared:,}",
                            'No. of cols in Source but not in Target': f"{colcount_source:,}",
                            'No. of cols in Target but not in Source': f"{colcount_target:,}",
                            'No. of rows in Source': f"{rowcount_source:,}",
                            'No. of distinct rows in Source': f"{distinct_rowcount_source:,}",
                            'No. of duplicate rows in Source': f"{duplicate_rowcount_source:,}",
                            'No. of rows in Target': f"{rowcount_target:,}",
                            'No. of distinct rows in Target': f"{distinct_rowcount_target:,}",
                            'No. of duplicate rows in Target': f"{duplicate_rowcount_target:,}",
                            'No. of matched rows': f"{rowcount_match:,}",
                            'No. of mismatched rows': f"{rowcount_mismatch:,}",
                            'No. of rows in Source but not in Target': f"{rowcount_only_source:,}",
                            'No. of rows in Target but not in Source': f"{rowcount_only_target:,}"
                            }


            dict_no_of_rows = {'No. of rows in Source': rowcount_source,
                               'No. of rows in Target': rowcount_target}

            if rows_only_source.limit(1).count()==0 and rows_only_target.limit(1).count()==0:
                pass
            if rows_mismatch.limit(1).count()==0:
                rows_mismatch = None
                sample_mismatch = None
            else:
                sample_mismatch = rows_mismatch.select(
                    joincolumns).limit(limit)
                sample_mismatch, concat_list = self.concat_keys(
                    sample_mismatch, joincolumns)
                sample_mismatch = sample_mismatch.select(
                    concat(*concat_list).alias("Key Columns"))
            if rows_only_source.limit(1).count()==0:
                rows_only_source = None
                sample_source_only = None
            else:
                sample_source_only = rows_only_source.select(
                    joincolumns).limit(limit)
                sample_source_only, concat_list = self.concat_keys(
                    sample_source_only, joincolumns)
                sample_source_only = sample_source_only.select(
                    concat(*concat_list).alias("Key Columns"))
            if rows_only_target.limit(1).count()==0:
                rows_only_target = None
                sample_target_only = None
            else:
                sample_target_only = rows_only_target.select(
                    joincolumns).limit(limit)
                sample_target_only, concat_list = self.concat_keys(
                    sample_target_only, joincolumns)
                sample_target_only = sample_target_only.select(
                    concat(*concat_list).alias("Key Columns"))

            if rows_both_all == None:
                df_match_summary = None

            else:
                df = rows_both_all
                #col_list = df.columns
                if len(colmapping) == 0:
                    column_mapping = {
                        i: i for i in sourcedf.columns if i not in joincolumns}
                    collist = [
                        i for i in sourcedf.columns if i not in joincolumns]
                else:
                    column_mapping = dict(colmapping)
                    collist = [
                        i for i in sourcedf.columns if i not in joincolumns]

                for column in collist:

                    base_col = column + "_base"
                    compare_col = column + "_compare"
                    match_col = column + "_match"
                    sel_col_list = []
                    sel_col_list = joincolumns.copy()
                    sel_col_list.append(base_col)
                    sel_col_list.append(compare_col)
                    key_cols = joincolumns.copy()

                    filter_false = match_col + " == False"
                    filter_true = match_col + " == True"

                    mismatch = df.select(match_col).filter(
                        filter_false).count()
                    if (mismatch == 0):
                        continue

                    match = df.select(match_col).filter(filter_true).count()
                    dict_match_summary[column] = [match, mismatch]

                    df_details = df.select(sel_col_list).filter(filter_false).withColumnRenamed(
                        base_col, "Source value").withColumnRenamed(compare_col, "Target value").distinct().limit(limit)

                    df_details, concat_list = self.concat_keys(
                        df_details, key_cols)
                    df_details = df_details.select(
                        concat(*concat_list).alias("Key Columns"), "Source value", "Target value")

                    dict_match_details[column] = df_details

                list_match_summary = []
                for k, v in dict_match_summary.items():
                    list_match_summary.append(
                        [k, column_mapping[k], rowcount_source, rowcount_target, rowcount_match, v[0], v[1]])

                df_match_summary = pd.DataFrame(list_match_summary, columns=[
                                                "Source Column Name", "Target Column Name", "Rows in Source", "Rows in Target", "Rows with Common Keys", "Rows Matched", "Rows Mismatch"])

                if (len(df_match_summary) == 0):
                    df_match_summary = None
                else:
                    df_match_summary = spark.createDataFrame(df_match_summary)
            duppkdict_results = rownulldict_results = rowdupdict_results = empdict_results = rowdict_results = coldict_results = nulldict_results = hashdict_results = dupdict_results = None

        #Added the complete elif loop for schema validation
        if (testcasetype == 'schema'):
            if testmode == 'manual' and (sourceformat !='table' or targetformat!='table'):
                log_info("Schema comparison can not be done for file format with MANUAL mode. PLEASE USE AUTO MODE")
            joincolumns = ['tabletype','columnname']
            log_info(f"Joining with column names: {joincolumns}")
            print("Comparing schema of Source and Target now...(this may take a while)...")
            
            comparison_obj = LegacySparkCompare(self.spark, sourcedf, targetdf, \
                                                    join_columns=joincolumns, column_mapping=colmapping, \
                                                    cache_intermediates=True)
            #comparison_obj.report()
            distinct_rowcount_source = sourcedf.select(joincolumns).distinct().count()
            distinct_rowcount_target = targetdf.select(joincolumns).distinct().count()
            duplicate_rowcount_source = rowcount_source - distinct_rowcount_source
            duplicate_rowcount_target = rowcount_target - distinct_rowcount_target
            rows_both_all = comparison_obj.rows_both_all
            rows_mismatch = comparison_obj.rows_both_mismatch
            rows_only_source = comparison_obj.rows_only_base
            rows_only_target = comparison_obj.rows_only_compare
            rowcount_common = comparison_obj.common_row_count
            rowcount_only_source = rows_only_source.count()
            rowcount_only_target = rows_only_target.count()
            rowcount_mismatch = rows_mismatch.count()
            rowcount_match = rowcount_common - rowcount_mismatch
            col_only_source = comparison_obj.columns_only_base
            colcount_source = len(col_only_source)
            col_only_target = comparison_obj.columns_only_compare
            colcount_target = len(col_only_target)
            columns_match = comparison_obj.columns_in_both
            colcount_match = len(columns_match)
            columns_compared = comparison_obj.columns_compared
            colcount_compared = len(columns_compared)

            rowcount_total_mismatch = rowcount_only_target + \
                rowcount_only_source  # + rowcount_mismatch

            if (rowcount_mismatch > 0 or rowcount_only_target > 0 or rowcount_only_source > 0):
                log_info("Test Case Failed as Content Mismatched")
                result_desc = "Content mismatched"
                test_result = "Failed"
            else:
                log_info("Test Case Passed as Content Matched")
                result_desc = "Content matched"
                test_result = "Passed"

            dict_results = {'Test Result': test_result,
                            'No. of matched columns': f"{colcount_match:,}",
                            'No. of columns compared': f"{colcount_compared:,}",
                            'No. of cols in Source but not in Target': f"{colcount_source:,}",
                            'No. of cols in Target but not in Source': f"{colcount_target:,}",
                            'No. of rows in Source': f"{rowcount_source:,}",
                            'No. of distinct rows in Source': f"{distinct_rowcount_source:,}",
                            'No. of duplicate rows in Source': f"{duplicate_rowcount_source:,}",
                            'No. of rows in Target': f"{rowcount_target:,}",
                            'No. of distinct rows in Target': f"{distinct_rowcount_target:,}",
                            'No. of duplicate rows in Target': f"{duplicate_rowcount_target:,}",
                            'No. of matched rows': f"{rowcount_match:,}",
                            'No. of mismatched rows': f"{rowcount_mismatch:,}",
                            'No. of rows in Source but not in Target': f"{rowcount_only_source:,}",
                            'No. of rows in Target but not in Source': f"{rowcount_only_target:,}"
                            }


            dict_no_of_rows = {'No. of rows in Source': rowcount_source,
                               'No. of rows in Target': rowcount_target}

            if rows_only_source.limit(1).count()==0 and rows_only_target.limit(1).count()==0:
                pass
            if rows_mismatch.limit(1).count()==0:
                rows_mismatch = None
                sample_mismatch = None
            else:
                sample_mismatch = rows_mismatch.select(
                    joincolumns).limit(limit)
                sample_mismatch, concat_list = self.concat_keys(
                    sample_mismatch, joincolumns)
                sample_mismatch = sample_mismatch.select(
                    concat(*concat_list).alias("Key Columns"))
            if rows_only_source.limit(1).count()==0:
                rows_only_source = None
                sample_source_only = None
            else:
                sample_source_only = rows_only_source.select(
                    joincolumns).limit(limit)
                sample_source_only, concat_list = self.concat_keys(
                    sample_source_only, joincolumns)
                sample_source_only = sample_source_only.select(
                    concat(*concat_list).alias("Key Columns"))
            if rows_only_target.limit(1).count()==0:
                rows_only_target = None
                sample_target_only = None
            else:
                sample_target_only = rows_only_target.select(
                    joincolumns).limit(limit)
                sample_target_only, concat_list = self.concat_keys(
                    sample_target_only, joincolumns)
                sample_target_only = sample_target_only.select(
                    concat(*concat_list).alias("Key Columns"))

            if rows_both_all == None:
                df_match_summary = None

            else:
                df = rows_both_all
                #col_list = df.columns
                if len(colmapping) == 0:
                    column_mapping = {
                        i: i for i in sourcedf.columns if i not in joincolumns}
                    collist = [
                        i for i in sourcedf.columns if i not in joincolumns]
                else:
                    column_mapping = dict(colmapping)
                    collist = [
                        i for i in sourcedf.columns if i not in joincolumns]

                for column in collist:

                    base_col = column + "_base"
                    compare_col = column + "_compare"
                    match_col = column + "_match"
                    sel_col_list = []
                    sel_col_list = joincolumns.copy()
                    sel_col_list.append(base_col)
                    sel_col_list.append(compare_col)
                    key_cols = joincolumns.copy()

                    filter_false = match_col + " == False"
                    filter_true = match_col + " == True"

                    mismatch = df.select(match_col).filter(
                        filter_false).count()
                    if (mismatch == 0):
                        continue

                    match = df.select(match_col).filter(filter_true).count()
                    dict_match_summary[column] = [match, mismatch]

                    df_details = df.select(sel_col_list).filter(filter_false).withColumnRenamed(
                        base_col, "Source value").withColumnRenamed(compare_col, "Target value").distinct().limit(limit)

                    df_details, concat_list = self.concat_keys(
                        df_details, key_cols)
                    df_details = df_details.select(
                        concat(*concat_list).alias("Key Columns"), "Source value", "Target value")

                    dict_match_details[column] = df_details

                list_match_summary = []
                for k, v in dict_match_summary.items():
                    list_match_summary.append(
                        [k, column_mapping[k], rowcount_source, rowcount_target, rowcount_match, v[0], v[1]])

                df_match_summary = pd.DataFrame(list_match_summary, columns=[
                                                "Source Column Name", "Target Column Name", "Rows in Source", "Rows in Target", "Rows with Common Keys", "Rows Matched", "Rows Mismatch"])

                if (len(df_match_summary) == 0):
                    df_match_summary = None
                else:
                    df_match_summary = spark.createDataFrame(df_match_summary)
            duppkdict_results = rownulldict_results = rowdupdict_results = empdict_results = rowdict_results = coldict_results = nulldict_results = hashdict_results = dupdict_results = None

        elif (testcasetype == "duplicate"):
            distinct_rowcount_source = sourcedf.select(
                joincolumns).distinct().count()
            distinct_rowcount_target = targetdf.select(
                joincolumns).distinct().count()
            duplicate_rowcount_source = rowcount_source - distinct_rowcount_source
            duplicate_rowcount_target = rowcount_target - distinct_rowcount_target
            if (distinct_rowcount_source == rowcount_source and rowcount_target == distinct_rowcount_target):
                test_result = "Passed"
                result_desc = "No Duplicates"
                log_info("Test Case Passed - no duplicates")
            else:
                test_result = "Failed"
                result_desc = "Duplicates"
                log_info("Test Case Failed - duplicates found")
            dict_results = {
                'Test Result': test_result, 'No. of rows in Source': f"{rowcount_source:,}", 'No. of distinct rows in Source': f"{distinct_rowcount_source:,}", 'No. of duplicate rows in Source': f"{duplicate_rowcount_source:,}", 'No. of rows in Target': f"{rowcount_target:,}", 'No. of distinct rows in Target': f"{distinct_rowcount_target:,}", 'No. of duplicate rows in Target': f"{duplicate_rowcount_target:,}"
            }

            rows_both_all = rows_mismatch = rows_only_source = rows_only_target = sample_mismatch = sample_source_only = sample_target_only = df_match_summary = dict_no_of_rows = dict_match_details = None
            duppkdict_results = rownulldict_results = rowdupdict_results = empdict_results = rowdict_results = coldict_results = nulldict_results = hashdict_results = dupdict_results = None
        elif (testcasetype == "count"):
            if (rowcount_source == rowcount_target):
                test_result = "Passed"
                result_desc = "Count matched"
                log_info("Test Case Passed - Count matched")
            else:
                test_result = "Failed"
                result_desc = "Count mismatched"
                log_info("Test Case Failed - Count mismatched")
            dict_results = {
                'Test Result': test_result, 'No. of rows in Source': f"{rowcount_source:,}", 'No. of rows in Target': f"{rowcount_target:,}"
            }
            rows_both_all = rows_mismatch = rows_only_source = rows_only_target = sample_mismatch = sample_source_only = sample_target_only = df_match_summary = dict_no_of_rows = dict_match_details = None
            duppkdict_results = rownulldict_results = rowdupdict_results = empdict_results = rowdict_results = coldict_results = nulldict_results = hashdict_results = dupdict_results = None
        elif (testcasetype == 'null'):
            totalsrcnullcols = 0
            totaltgtnullcols = 0
            source_null_counts = []
            target_null_counts = []
            sflag=0
            for col_name in sourcedf.columns:
                src_null_count = sourcedf.filter(col(col_name).isNull()).count()
                source_null_counts.append((col_name, src_null_count))
                if src_null_count > 0:
                    sflag = 1
                    totalsrcnullcols = totalsrcnullcols +1
            src_null_counts_df = spark.createDataFrame(source_null_counts, ["Column", "Count"])
            src_null_counts_df = src_null_counts_df.filter(col("Count") > 0)
            tflag = 0
            for col_name in targetdf.columns:
                tgt_null_count = targetdf.filter(col(col_name).isNull()).count()
                target_null_counts.append((col_name, tgt_null_count))
                if tgt_null_count > 0:
                    tflag = 1
                    totaltgtnullcols = totaltgtnullcols +1
            tgt_null_counts_df = spark.createDataFrame(target_null_counts, ["Column", "Count"])
            tgt_null_counts_df = tgt_null_counts_df.filter(col("Count") > 0)

            nullflag = 0
            null_col_counts = []
            columns_match_null_count = 0
            columns_mis_null_count = 0
            for srccolpos, src_col_name in enumerate(sourcedf.columns):
                for tgtcolpos, tgt_col_name in enumerate(targetdf.columns):
                    if srccolpos == tgtcolpos:
                        src_null_col_count = sourcedf.filter(col(src_col_name).isNull()).count()
                        tgt_null_col_count = targetdf.filter(col(tgt_col_name).isNull()).count()
                        if src_null_col_count!=0 or tgt_null_col_count != 0:
                            if src_null_col_count != tgt_null_col_count:
                                nullflag=1
                                null_col_counts.append((src_col_name,src_null_col_count,tgt_col_name, tgt_null_col_count))
                                columns_mis_null_count = columns_mis_null_count + 1
                            else:
                                columns_match_null_count = columns_match_null_count + 1


            if len(null_col_counts) == 0:
                null_col_counts_df = None
            else:
                null_col_counts_df = spark.createDataFrame(null_col_counts, ["Source Column Name", "Src Null Count", "Target Column Name", "Tgt Null Count"])

            if nullflag == 0:
                test_result = "Passed"
                result_desc = "Nulls are matching for each column"
                log_info("Test Case Passed - Null count is matching between source and target for each column")
            else:
                test_result = "Failed"
                result_desc = "Nulls are not matching for each column"
                log_info("Test Case Failed - Null count is not matching between source and target for each column")
            dict_results = {
                'Test Result': test_result, 'No of column has null in source':f"{totalsrcnullcols:,}",'No of column has null in target':f"{totaltgtnullcols:,}", 'No of columns has null count match':f"{columns_match_null_count:,}", 'No of columns has null count mismatch': f"{columns_mis_null_count:,}"}
            sample_mismatch = null_col_counts_df
            sample_source_only = src_null_counts_df
            sample_target_only = tgt_null_counts_df

            rows_both_all = rows_mismatch  = rows_only_source =rows_only_target = df_match_summary = dict_no_of_rows = dict_match_details = None
            duppkdict_results = rownulldict_results = rowdupdict_results=empdict_results = rowdict_results = coldict_results = nulldict_results = hashdict_results = dupdict_results = None

        elif (testcasetype == "fingerprint"):
            special_srcdf = sourcedf
            special_tgtdf = targetdf
            duppkstatus = rownullcountstatus = rowdupcountstatus = rowcountstatus = colcountstatus = nullcountstatus = dupcountstatus = hashcountstatus = empcountstatus = ''
            duppkmsg = rownullcountmsg = rowdupcountmsg = rowcountmsg = colcountmsg = nullcountmsg = dupcountmsg = hashcountmsg = empcountmsg = ''
            fingerprint_flag = True
            total_kpi = 8
            reason = ''
            failed_kpi = 0
            print("Comparing Fingerprints of Source and Target now...")
            #fingerprintcomp_obj = datacompy.LegacySparkCompare(self.spark, special_srcdf, special_tgtdf,
                                                    #column_mapping=colmapping, join_columns=joincolumns)


            #KPI - 1 - Row Count
            timenow = datetime.now(utctimezone)
            print(f"KPI Starts {timenow}")

            sourcerowcount = special_srcdf.count()
            targetrowcount = special_tgtdf.count()
            print(f"source row count is {sourcerowcount}")
            print(f"target row count is {targetrowcount}")
            if sourcerowcount != targetrowcount:
                rowcountstatus = 'Failed'
                fingerprint_flag = False
                failed_kpi += 1
                rowcountmsg = "Source and Target row count is not matching"
                reason = reason + f"source row count: {sourcerowcount}. target row count: {targetrowcount}"
            else:
                rowcountstatus = 'Passed'
                rowcountmsg = "Source and Target row count is matching"
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")

            # KPI - 2 - Column Count
            timenow = datetime.now(utctimezone)
            print(f"KPI Column Count Starts {timenow}")
            sourcecols = special_srcdf.columns
            sourcecolcount = len(sourcecols)
            print(f"source col count is {sourcecolcount}")
            targetcols = special_tgtdf.columns
            targetcolcount = len(targetcols)
            print(f"target col count is {targetcolcount}")
            if targetcolcount != sourcecolcount:
                fingerprint_flag = False
                colcountstatus = 'Failed'
                colcountmsg = "Source column count and target column count is not matching"
                failed_kpi += 1
                reason = reason + f"source column count: {sourcecolcount}. target column count: {targetcolcount}. "
            else:
                colcountstatus = 'Passed'
                colcountmsg = "Source column count and target column count is matching"
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")


            # KPI - 3 - Column Level Null Count
            timenow = datetime.now(utctimezone)
            print(f"KPI C L Null count Starts {timenow}")
            sourcenullcolumnlist = verify_nulls(special_srcdf)
            targetnullcolumnlist = verify_nulls(special_tgtdf)

            if len(sourcenullcolumnlist) >  0 or len(targetnullcolumnlist) > 0:
                nullcountstatus = 'Failed'
                nullcountmsg = 'Source or/and Target has nulls in some columns'
                fingerprint_flag = False
                failed_kpi += 1
                if len(sourcenullcolumnlist) >  0 and len(targetnullcolumnlist) == 0:
                    reason = reason + "Few column have nulls in src. "
                elif len(targetnullcolumnlist) >  0 and len(sourcenullcolumnlist) == 0:
                    reason = reason + "Few column have nulls in tgt. "
                elif len(sourcenullcolumnlist) >  0 and len(targetnullcolumnlist) >  0:
                    reason = reason + "Few columns has nulls in src & tgt. "

            else:
                nullcountstatus = 'Passed'
                nullcountmsg = 'Source or/and Target has no nulls in any columns'
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")


            #KPI - 4 - Row Level Null Check
            timenow = datetime.now(utctimezone)
            print(f"KPI R L null count Starts {timenow}")
            sourceentirerownulldf = check_entire_row_is_null(special_srcdf)
            targetentirerownulldf = check_entire_row_is_null(special_tgtdf)
            sourceentirerownullcount = sourceentirerownulldf.count()
            targetentirerownullcount = targetentirerownulldf.count()
            if sourceentirerownullcount != 0 or targetentirerownullcount != 0:
                fingerprint_flag = False
                failed_kpi += 1
                rownullcountstatus = 'Failed'
                rownullcountmsg = 'One or more rows are null'
            else:
                rownullcountstatus = 'Passed'
                rownullcountmsg = 'No rows are null'
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")

            # KPI - 5 - Column Level Empty Check
            timenow = datetime.now(utctimezone)
            print(f"KPI C L EMpty Starts {timenow}")
            sourceempcolumnlist = check_empty_values(special_srcdf)
            targetempcolumnlist = check_empty_values(special_tgtdf)

            if len(sourceempcolumnlist) > 0 or len(targetempcolumnlist) > 0:
                empcountstatus = 'Failed'
                empcountmsg = 'Source or/and Target has empty values in some columns'
                fingerprint_flag = False
                failed_kpi += 1
                if len(sourceempcolumnlist) > 0 and len(targetempcolumnlist) == 0:
                    reason = reason + "columns has empty in source. "
                elif len(targetempcolumnlist) > 0 and len(sourceempcolumnlist) == 0:
                    reason = reason + "columns has empty in target. "
                elif  len(sourceempcolumnlist) > 0 and  len(targetempcolumnlist) > 0:
                    reason = reason + "columns has empty in src & tgt. "
            else:
                empcountstatus = 'Passed'
                empcountmsg = 'Source or/and Target has no empty values in any columns'
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")

            '''#KPI - 6 - Column Level Duplicate Check
            timenow = datetime.now(utctimezone)
            print(f"KPI C L Dup check Starts {timenow}")
            sourcedupcolumnlist = verify_duplicates(special_srcdf)
            targetdupcolumnlist = verify_duplicates(special_tgtdf)
            sourcedupdf = list_duplicate_values(special_srcdf)
            targetdupdf = list_duplicate_values(special_tgtdf)
            dup_diff_df = sourcedupdf.exceptAll(targetdupdf).union(targetdupdf.exceptAll(sourcedupdf))

            if dup_diff_df.count() == 0:
                duplicate_source_target = "Matched"
            else:
                duplicate_source_target = "Not Matched"
            if len(sourcedupcolumnlist) > 0 or len(targetdupcolumnlist) > 0:
                dupcountstatus = 'Failed'
                dupcountmsg = 'Source or/and Target has duplicate values in some columns'
                fingerprint_flag = False
                failed_kpi += 1
                if len(sourcedupcolumnlist) > 0 and len(targetdupcolumnlist) == 0:
                    reason = reason + "columns has duplicates in source. "
                elif len(targetdupcolumnlist) > 0 and len(sourcedupcolumnlist) == 0:
                    reason = reason + "columns has duplicates in target. "
                elif  len(sourcedupcolumnlist) > 0 and  len(targetdupcolumnlist) > 0:
                    reason = reason + "columns has duplicates in src & tgt. "
            else:
                dupcountstatus = 'Passed'
                dupcountmsg = 'Source or/and Target has no duplicate values in any columns'
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")
'''
            #KPI - 9 - CheckSum Data CHeck
            timenow = datetime.now(utctimezone)
            print(f"KPI CHeck Sum Starts {timenow}")
            sourcedf_hashed = apply_md5_hash(special_srcdf)
            targetdf_hashed = apply_md5_hash(special_tgtdf)
            sourcedf_hashed_count = sourcedf_hashed.count()
            targetdf_hashed_count = targetdf_hashed.count()
            source_df_diff = sourcedf_hashed.subtract(targetdf_hashed)
            source_df_diff = source_df_diff.limit(limit)
            sourcebutnottarget_count = source_df_diff.count()
            if sourcebutnottarget_count == 0:
                source_df_diff = None
            target_df_diff = targetdf_hashed.subtract(sourcedf_hashed)
            target_df_diff = target_df_diff.limit(limit)
            targetbutnotsource_count = target_df_diff.count()
            if targetbutnotsource_count == 0:
                target_df_diff = None

            if targetbutnotsource_count !=0 or sourcebutnottarget_count !=0:
                fingerprint_flag = False
                failed_kpi +=1
                hashcountstatus = 'Failed'
                hashcountmsg = 'Data is not matching between source and target'
                #reason = reason + f"No of records in target not in source: {targetbutnotsource_count} No of records in source not in target: {sourcebutnottarget_count}. "
            else:
                hashcountstatus = 'Passed'
                hashcountmsg = 'Data is matching between source and target'
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")


            #KPI - 7 - Row Level Duplicate Check
            timenow = datetime.now(utctimezone)
            print(f"KPI R L Dup check Starts {timenow}")
            sourcedf_hashed_distinct_count = sourcedf_hashed.distinct().count()
            sourcedf_hashed_distinct_count = targetdf_hashed.distinct().count()
            sourcedf_hashed_dup_count = sourcedf_hashed_count - sourcedf_hashed_distinct_count
            targetdf_hashed_dup_count = targetdf_hashed_count - sourcedf_hashed_distinct_count
            if sourcedf_hashed_dup_count!=0 or targetdf_hashed_dup_count != 0:
                fingerprint_flag = False
                failed_kpi += 1
                rowdupcountstatus = 'Failed'
                rowdupcountmsg = 'One or more rows are duplicated'
            else:
                rowdupcountstatus = 'Passed'
                rowdupcountmsg = 'No rows are duplicated'
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")

            # KPI 8 - Duplicate values
            timenow = datetime.now(utctimezone)
            print(f"KPI Dup PK Starts {timenow}")

            sourceduppkdf = verify_dup_pk(special_srcdf, joincolumns,limit)
            targetduppkdf = verify_dup_pk(special_tgtdf, joincolumns,limit)
            distinct_rowcount_source = sourcedf.select(joincolumns).distinct().count()
            distinct_rowcount_target = targetdf.select(joincolumns).distinct().count()
            duplicate_rowcount_source = sourcerowcount - distinct_rowcount_source
            duplicate_rowcount_target = targetrowcount - distinct_rowcount_target
            if (distinct_rowcount_source == sourcerowcount and targetrowcount == distinct_rowcount_target):
                duppkstatus = "Passed"
                duppkmsg = "No Duplicates"
                log_info("Test Case Passed - no duplicates")
            else:
                fingerprint_flag = False
                failed_kpi += 1
                duppkstatus = "Failed"
                duppkmsg = "Duplicates"
                log_info("Test Case Failed - duplicates found")
            timenow = datetime.now(utctimezone)
            print(f"KPI Ends {timenow}")

            passed_kpi = total_kpi - failed_kpi

            if fingerprint_flag == True:
                test_result = "Passed"
                result_desc = "Fingerprint matched"
                log_info("Test Case Passed - KPIs matched")
            else:
                test_result = "Failed"
                result_desc = "One or more fingerprint mismatched. Please check detail report for more info"
                log_info("Test Case Failed - KPIs mismatched")
            '''if targetbutnotsource_count!=0:
                sample_target_only = target_df_diff
            elif sourcebutnottarget_count!=0:
                sample_source_only = source_df_diff
            elif targetbutnotsource_count ==0 and sourcebutnottarget_count ==0:
                sample_target_only = sample_source_only = None'''
            sample_target_only = target_df_diff
            sample_source_only = source_df_diff
            '''Commenting this line for duplicate column level check
            if sourcedupdf.count() == 0:
                rows_only_source = None
            else:
                rows_only_source = sourcedupdf


            if targetdupdf.count() == 0:
                rows_only_target = None
            else:
                rows_only_target = targetdupdf
'''
            rows_only_source = rows_only_target = None
            if sourceduppkdf is None:
                sample_mismatch = None
            elif sourceduppkdf.count() > 0 and sourceduppkdf is not None:
                sample_mismatch = sourceduppkdf

            if targetduppkdf is None:
                rows_mismatch = None
            elif targetduppkdf.count() > 0 and targetduppkdf is not None:
                rows_mismatch = targetduppkdf

            rows_both_all = df_match_summary = dict_no_of_rows = dict_match_details = None
            dict_results = {
                'Test Result': test_result, 'No. of KPIs in Source': f"{total_kpi:,}",
                'No. of KPIs in Target': f"{total_kpi:,}",
                'No. of KPIs matched': f"{passed_kpi:,}",
                'No. of KPIs mismatched': f"{failed_kpi:,}"}

            rowdict_results = {'Source Row Count': sourcerowcount, 'Target Row Count': targetrowcount, 'KPI Status':rowcountstatus ,
            'Reason': rowcountmsg}
            coldict_results = {'Source Column Count': sourcecolcount, 'Target Column Count': targetcolcount, 'KPI Status':colcountstatus ,
            'Reason': colcountmsg}
            nulldict_results = {'List of Columns has null in source': sourcenullcolumnlist, 'List of Columns has null in target': targetnullcolumnlist, 'KPI Status':nullcountstatus ,
            'Reason': nullcountmsg}
            #dupdict_results = {'List of Columns has duplicates in source': sourcedupcolumnlist, 'List of Columns has duplicates in target': targetdupcolumnlist, 'KPI Status':dupcountstatus ,
            #'Reason': dupcountmsg,'Dup values match source & target':duplicate_source_target}
            dupdict_results = None
            empdict_results = {'List of Columns has empty in source': sourceempcolumnlist, 'List of Columns has empty in target': targetempcolumnlist, 'KPI Status':empcountstatus ,
            'Reason': empcountmsg }
            rowdupdict_results = {'No of duplicate rows in source': sourcedf_hashed_dup_count, 'No of duplicate rows in target': targetdf_hashed_dup_count, 'KPI Status':rowdupcountstatus ,
            'Reason': rowdupcountmsg}
            hashdict_results = { 'KPI Status':hashcountstatus ,'Reason': hashcountmsg}
            rownulldict_results = {'No of null rows in source': sourceentirerownullcount, 'No of null rows in target': targetentirerownullcount, 'KPI Status':rownullcountstatus ,
            'Reason': rownullcountmsg}
            duppkdict_results = {
                'Test Result': duppkstatus, 'No. of rows in Source': f"{sourcerowcount:,}",
                'No. of distinct rows in Source': f"{distinct_rowcount_source:,}",
                'No. of duplicate rows in Source': f"{duplicate_rowcount_source:,}",
                'No. of rows in Target': f"{targetrowcount:,}",
                'No. of distinct rows in Target': f"{distinct_rowcount_target:,}",
                'No. of duplicate rows in Target': f"{duplicate_rowcount_target:,}"
            }

            '''fingerprintcomp_obj.report()

            rows_both_all = fingerprintcomp_obj.rows_both_all
            rows_mismatch = fingerprintcomp_obj.rows_both_mismatch
            rows_only_source = fingerprintcomp_obj.rows_only_base
            rows_only_target = fingerprintcomp_obj.rows_only_compare
            rowcount_common = fingerprintcomp_obj.common_row_count
            rowcount_only_source = rows_only_source.count()
            rowcount_only_target = rows_only_target.count()
            rowcount_mismatch = rows_mismatch.count()
            rowcount_match = rowcount_common - rowcount_mismatch
            if (rowcount_mismatch == 0 or rowcount_only_target == 0 or rowcount_only_source == 0):
                test_result = "Passed"
                result_desc = "Fingerprint matched"
                log_info("Test Case Passed - KPIs matched")
            else:
                test_result = "Failed"
                result_desc = "Fingerprint mismatched"
                log_info("Test Case Failed - KPIs mismatched")

            if rows_both_all == None:
                df_match_summary = None

            else:
                df = rows_both_all
                #col_list = df.columns
                if len(colmapping) == 0:
                    column_mapping = {
                        i: i for i in sourcedf.columns if i not in joincolumns}
                    collist = [
                        i for i in sourcedf.columns if i not in joincolumns]
                else:
                    column_mapping = dict(colmapping)
                    collist = [
                        i for i in sourcedf.columns if i not in joincolumns]

                    for column in collist:

                        base_col = column + "_base"
                        compare_col = column + "_compare"
                        match_col = column + "_match"
                        sel_col_list = []
                        sel_col_list = joincolumns.copy()
                        sel_col_list.append(base_col)
                        sel_col_list.append(compare_col)
                        key_cols = joincolumns.copy()

                        filter_false = match_col + " == False"
                        filter_true = match_col + " == True"

                        mismatch = df.select(match_col).filter(
                            filter_false).count()
                        if (mismatch == 0):
                            continue

                        match = df.select(match_col).filter(filter_true).count()
                        dict_match_summary[column] = [match, mismatch]

                        df_details = df.select(sel_col_list).filter(filter_false).withColumnRenamed(
                            base_col, "Source value").withColumnRenamed(compare_col, "Target value").distinct().limit(limit)

                        df_details, concat_list = self.concat_keys(
                            df_details, key_cols)
                        df_details = df_details.select(
                            concat(*concat_list).alias("Key Columns"), "Source value", "Target value")

                        dict_match_details[column] = df_details

                list_match_summary = []
                for k, v in dict_match_summary.items():
                    list_match_summary.append(
                        [k, column_mapping[k], rowcount_source, rowcount_target, rowcount_match, v[0], v[1]])

                df_match_summary = pd.DataFrame(list_match_summary, columns=[
                                                "Source Column Name", "Target Column Name", "Rows in Source", "Rows in Target", "Rows with Common Keys", "Rows Matched", "Rows Mismatch"])

            if (len(df_match_summary) == 0):
                df_match_summary = None
            else:
                df_match_summary = spark.createDataFrame(df_match_summary)

            dict_results = {
                'Test Result': test_result, 'No. of KPIs in Source': f"{rowcount_source:,}", 'No. of KPIs in Target': f"{rowcount_target:,}"
            }
            rows_both_all = rows_mismatch = rows_only_source = rows_only_target = sample_mismatch = sample_source_only = sample_target_only = df_match_summary = dict_no_of_rows = dict_match_details = None
'''
        dict_compareoutput = {'rows_both_all': rows_both_all, 'rows_mismatch': rows_mismatch, 'rows_only_source': rows_only_source, 'rows_only_target': rows_only_target, 'test_result': test_result, 'sample_mismatch': sample_mismatch,
                              'sample_source': sample_source_only, 'sample_target': sample_target_only, 'dict_results': dict_results, 'col_match_summary': df_match_summary, 'row_count': dict_no_of_rows, 'col_match_details': dict_match_details, 'result_desc': result_desc,
                              'rowdict_results':rowdict_results, 'coldict_results':coldict_results,'nulldict_results':nulldict_results, 'dupdict_results':dupdict_results,
                              'hashdict_results':hashdict_results, 'empdict_results':empdict_results,'rowdupdict_results':rowdupdict_results, 'rownulldict_results':rownulldict_results,'duppkdict_results':duppkdict_results}
        print(f"dict_compateoutput --- {dict_compareoutput}")
        log_info(f"Data Compare Completed for TestingType - {testcasetype} ")
        return dict_compareoutput


    def generate_testcase_summary_report(self, dict_runsummary, dict_config, results_path, compare_input, dict_compareoutput, testcasetype, comparison_type, pdfobj):
        if (compare_input['filedetails']["sourcefile"] is not None):
            file_details = compare_input['filedetails']
            source_file_path = file_details["sourcefile"]
            dict_config['Source Format'], dict_config['Source Path'] = source_file_path.split(
                ".", 1)
            dict_config['Source Path'] = get_mount_src_path(
                dict_config['Source Path'].replace("`", ""))
            target_file_path = file_details["targetfile"]
            dict_config['Target Format'], dict_config['Target Path'] = target_file_path.split(
                ".", 1)
            dict_config['Target Path'] = get_mount_src_path(
                dict_config['Target Path'].replace("`", ""))
        elif compare_input['sourcequery'] != '' or compare_input['targetquery'] != '':
            source_file_path = compare_input['sourcequery'].split(
                "FROM")[-1].split(" ")[1]
            target_file_path = compare_input['targetquery'].split(
                "FROM")[-1].split(" ")[1]
        else:
            source_file_path = dict_config['Source Name']
            target_file_path = dict_config['Target Name']

        if (testcasetype == 'count'):
            srcquery = compare_input['sourcequery']
            srcquery = "SELECT Count(1) FROM "+source_file_path
            tgtquery = compare_input['targetquery']
            tgtquery = "SELECT Count(1) FROM "+target_file_path
            query_details = {'Source Query': srcquery,
                             'Target Query': tgtquery}
        elif (testcasetype == 'duplicate'):
            srcquery = compare_input['sourcequery']
            join_cols = dict_config['Primary Keys']
            srcquery = "SELECT Count(DISTINCT " + \
                join_cols + ") FROM "+source_file_path
            tgtquery = compare_input['targetquery']
            tgtquery = "SELECT Count(DISTINCT " + \
                join_cols + ") FROM "+target_file_path
            query_details = {'Source Query': srcquery,
                             'Target Query': tgtquery}
        else:
            query_details = {
                'Source Query': compare_input['sourcequery'], 'Target Query': compare_input['targetquery']}

        sample_source_only = dict_compareoutput['sample_source']
        sample_target_only = dict_compareoutput['sample_target']
        sample_mismatch = dict_compareoutput['sample_mismatch']
        col_match_summary = dict_compareoutput['col_match_summary']
        col_match_details = dict_compareoutput['col_match_details']
        rows_only_source = dict_compareoutput['rows_only_source']
        rows_only_target = dict_compareoutput['rows_only_target']
        sample_mismatch = dict_compareoutput['sample_mismatch']
        rows_mismatch = dict_compareoutput['rows_mismatch']

        row_count = dict_compareoutput['row_count']
        if (dict_config['Compare Type'] == 's2tcompare'  and  dict_config['testquerygenerationmode']  =="Auto"):
            src_conn = get_connection_config(
                compare_input['filedetails']['sourceconnectionname'])
            tgt_conn = get_connection_config(
                compare_input['filedetails']['targetconnectionname'])
            dict_config['Source Connection Type'] = compare_input['filedetails']['sourceconnectiontype']
            dict_config['Target Connection Type'] = compare_input['filedetails']['targetconnectiontype']
            dict_config['Source Connection Name'] = compare_input['filedetails']['sourceconnectionname']
            dict_config['Target Connection Name'] = compare_input['filedetails']['sourceconnectionname']
            '''
            if (src_conn['CONNTYPE'].lower() == 'aws-s3'):
                dict_config['Source Connection Value'] = src_conn['BUCKETNAME']
            else:
                dict_config['Source Connection Value'] = src_conn['CONNURL']
            if (tgt_conn['CONNTYPE'].lower() == 'aws-s3'):
                dict_config['Target Connection Value'] = tgt_conn['BUCKETNAME']
            else:
                dict_config['Target Connection Value'] = tgt_conn['CONNURL']
            dict_config['Source Path'] = dict_config['Source Path'].replace(
                dict_config['Source Connection Value'], "")
            dict_config['Target Path'] = dict_config['Target Path'].replace(
                dict_config['Target Connection Value'], "")
            '''
        else:
            '''
            if (dict_config['Source Connection Type'].lower() == 'aws-s3'):
                dict_config['Source Connection Value'] = get_connection_config(
                    dict_config['Source Connection Name'])['BUCKETNAME']
            else:
                dict_config['Source Connection Value'] = get_connection_config(
                    dict_config['Source Connection Name'])['CONNURL']
            if (dict_config['Target Connection Type'].lower() == 'aws-s3'):
                dict_config['Target Connection Value'] = get_connection_config(
                    dict_config['Target Connection Name'])['BUCKETNAME']
            else:
                dict_config['Target Connection Value'] = get_connection_config(
                    dict_config['Target Connection Name'])['CONNURL']
            '''
        dict_testresults = dict_compareoutput['dict_results']
        compare_type = dict_config['Testcase Type'] + " comparison"

        dict_runsummary = {k: v for k, v in dict_runsummary.items() if v != ''}
        dict_config = {k: v for k, v in dict_config.items() if v != ''}
        

        pdfobj.write_text(compare_type, 'subheading')
        pdfobj.write_text(rundetails_subheading, 'section heading')
        pdfobj.create_table_summary(dict_runsummary)
        pdfobj.write_text(configdetails_subheading, 'section heading')
        pdfobj.create_table_summary(dict_config)
        testcasetype_subheading = "3. " + testcasetype.capitalize() + " Summary"
        pdfobj.write_text(testcasetype_subheading, 'section heading')
        pdfobj.create_table_summary(dict_testresults)  # ,'L')
        pdfobj.write_text('4. SQL Queries', 'section heading')
        pdfobj.write_text('4.1 Source Query', 'section heading')

        if (testcasetype == "content" and dict_config['Compare Type'] == 's2tcompare' and  dict_config['testquerygenerationmode']  =="Auto"):
            for i in query_details['Source Query']:
                pdfobj.display_sql_query(i)
        else:
            pdfobj.display_sql_query(query_details['Source Query'])
        pdfobj.write_text('4.2 Target Query', 'section heading')

        if (testcasetype == "content" and dict_config['Compare Type'] == 's2tcompare' and  dict_config['testquerygenerationmode']  =="Auto"):
            for i in query_details['Target Query']:
                pdfobj.display_sql_query(i)
        else:
            pdfobj.display_sql_query(query_details['Target Query'])

        if testcasetype == 'count' or testcasetype == "duplicate":
            pass
        elif testcasetype == "fingerprint":
            duppkdict_results = dict_compareoutput["duppkdict_results"]
            rowdict_results = dict_compareoutput["rowdict_results"]
            coldict_results = dict_compareoutput["coldict_results"]
            hashdict_results = dict_compareoutput["hashdict_results"]
            nulldict_results = dict_compareoutput["nulldict_results"]
            #dupdict_results = dict_compareoutput["dupdict_results"]
            rownulldict_results = dict_compareoutput["rownulldict_results"]
            empdict_results = dict_compareoutput["empdict_results"]
            rowdupdict_results = dict_compareoutput["rowdupdict_results"]
            pdfobj.write_text("5. Fingerprint Comparison Report ", 'section heading')
            '''if col_match_summary != None:
                pdfobj.create_table_summary(row_count)
            pdfobj.create_table_details(col_match_summary, 'mismatch_summary')'''
            pdfobj.write_text("5.1. KPI (1) - Row Count ", 'section heading')
            pdfobj.create_table_summary(rowdict_results)
            pdfobj.write_text("5.2. KPI (2) - Column Count ", 'section heading')
            pdfobj.create_table_summary(coldict_results)
            pdfobj.write_text("5.3. KPI (3) - Column Level Null Check ", 'section heading')
            pdfobj.create_table_summary(nulldict_results)
            pdfobj.write_text("5.4. KPI (4) - Row Level Null Check ", 'section heading')
            pdfobj.create_table_summary(rownulldict_results)
            pdfobj.write_text("5.5. KPI (5) - Column Level empty Check ", 'section heading')
            pdfobj.create_table_summary(empdict_results)
            '''pdfobj.write_text("5.6. KPI (6) - Column Level Duplicate Check ", 'section heading')
            pdfobj.create_table_summary(dupdict_results)
            pdfobj.write_text("5.6.1 Source Duplicate - Column Level ", 'section heading')
            pdfobj.create_table_details(rows_only_source, 'mismatch_details')
            pdfobj.write_text("5.6.2 Target Duplicate - Column Level ", 'section heading')
            pdfobj.create_table_details(rows_only_target, 'mismatch_details')
'''
            pdfobj.write_text("5.6. KPI (6) - Row Level Duplicate Check ", 'section heading')
            pdfobj.create_table_summary(rowdupdict_results)
            pdfobj.write_text("5.7. KPI (7) Primary Key Duplicate Check ", 'section heading')
            pdfobj.create_table_summary(duppkdict_results)
            pdfobj.write_text("5.7.1 Primary Key Duplicate Check - Source", 'section heading')

            pdfobj.create_table_details(sample_mismatch, 'mismatch_details')
            pdfobj.write_text("5.7.2 Primary Key Duplicate Check - Target", 'section heading')

            pdfobj.create_table_details(rows_mismatch, 'mismatch_details')
            pdfobj.write_text("5.8. KPI (8) - CheckSum Row Level", 'section heading')
            pdfobj.create_table_summary(hashdict_results)
            pdfobj.write_text(
                '5.8.1 Addition Rows in Source', 'section heading')
            pdfobj.create_table_details(sample_source_only, 'mismatch_details')
            pdfobj.write_text(
                '5.8.2 Addition Rows in Target', 'section heading')
            pdfobj.create_table_details(sample_target_only, 'mismatch_details')




        elif testcasetype == 'null':
            mismatch_heading = "5. Columns having nulls at source and target"
            pdfobj.write_text(mismatch_heading, 'section heading')
            pdfobj.write_text(
                '5.1 Columns having nulls in source', 'section heading')
            pdfobj.create_table_details(sample_source_only, 'mismatch_details')
            pdfobj.write_text(
                '5.2 Columns having nulls in target', 'section heading')
            pdfobj.create_table_details(sample_target_only, 'mismatch_details')
            pdfobj.write_text(
                '5.3 Columns having null count mismatch between source and target', 'section heading')
            pdfobj.create_table_details(sample_mismatch, 'mismatch_summary')

        elif (testcasetype == 'content' or testcasetype == 'schema'):
            mismatch_heading = "5. Sample Mismatches " + \
                str(compare_input['limit']) + " rows"
            pdfobj.write_text(mismatch_heading, 'section heading')
            pdfobj.write_text(
                '5.1 Keys in source but not in target', 'section heading')
            pdfobj.create_table_details(sample_source_only, 'mismatch')
            pdfobj.write_text(
                '5.2 Keys in target but not in source', 'section heading')
            pdfobj.create_table_details(sample_target_only, 'mismatch')
            pdfobj.write_text(
                '5.3 Keys having one or more unequal column values', 'section heading')
            pdfobj.create_table_details(sample_mismatch, 'mismatch')
            pdfobj.write_text(
                '6. Columnwise Mismatch Summary', 'section heading')
            if col_match_summary != None:
                pdfobj.create_table_summary(row_count)
            pdfobj.create_table_details(col_match_summary, 'mismatch_summary')
            pdfobj.write_text(
                '7. Columnwise Mismatch Details', 'section heading')
            sno = 1
            if len(col_match_details) != 0:
                for key, value in col_match_details.items():
                    header = "7." + str(sno) + " " + str(key)
                    pdfobj.write_text(header, 'section heading')
                    pdfobj.create_table_details(value, 'mismatch_details')
                    sno = sno + 1
            else:
                df_7 = None
                pdfobj.create_table_details(df_7)
        return pdfobj


    def generate_protocol_summary_report(self, df_protocol_summary, protocol_run_details, protocol_run_params, output_path, created_time, testcasetype):
        pdfobj_protocol = generatePDF()
        comparison_type = testcasetype + " comparison"
        pdfobj_protocol.write_text(protocolreportheader, 'report header')
        pdfobj_protocol.write_text(protocol_run_details['Test Protocol Name'], 'subheading')
        pdfobj_protocol.write_text(comparison_type, 'subheading')
        pdfobj_protocol.write_text(protocolrunparams, 'section heading')
        pdfobj_protocol.create_table_summary(protocol_run_params)
        pdfobj_protocol.write_text(protocoltestcaseheader, 'section heading')
        pdfobj_protocol.create_table_summary(protocol_run_details)
        pdfobj_protocol.write_text(testresultheader, 'section heading')
        table_type = 'protocol'
        if (testcasetype == "count"):
            table_type = 'protocol_count'
        sno = 1
        test_results_list = ['Failed', 'Passed']
        for test_result in test_results_list:
            testheader = "3." + str(sno) + ". " + test_result + " Testcases"
            pdfobj_protocol.write_text(testheader, 'section heading')
            if (df_protocol_summary is not None):
                df_protocol_summary_temp = df_protocol_summary.select(
                    '*').filter(col('Test Result') == test_result)
                if (df_protocol_summary_temp.count() == 0):
                    df_protocol_summary_temp = None
            else:
                df_protocol_summary_temp = None
            pdfobj_protocol.create_table_details(
                df_protocol_summary_temp, table_type)
            sno = sno + 1

        protocol_output_path = output_path + "/run_" + \
            protocol_run_details['Test Protocol Name'] + \
            "_" + created_time+".pdf"
        pdfobj_protocol.pdf.output(protocol_output_path, 'F')
        log_info("Protocol Summary PDF Generated")
        return protocol_output_path


    def concat_keys(self, df, key_cols_list):
            keycols_name_temp = [i+'_temp' for i in key_cols_list]
            keycols_val_temp = [j+'=' if i == 0 else ', ' +
                                j+'= ' for i, j in enumerate(key_cols_list)]
            for i in range(len(keycols_name_temp)):
                df = df.withColumn(keycols_name_temp[i], lit(keycols_val_temp[i]))

            concat_key_cols = []
            for i, j in zip(keycols_name_temp, key_cols_list):
                concat_key_cols.append(i)
                concat_key_cols.append(j)
            return df, concat_key_cols


if __name__ == "__main__":
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    spark = createsparksession()
    testcasesrunlist = ['all']
    protocol_file_path = sys.argv[1]
    testtype = sys.argv[2]

    log_info(f"Protocol Config path: {protocol_file_path}")
    log_info(f"TestType: {testtype}")
    log_info(f"TestCasesRunList: {testcasesrunlist}")

    testerobj = S2TTester(spark)
    testerobj.starttestexecute(protocol_file_path, testtype, testcasesrunlist)
   
