from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from atf.common.atf_common_functions import read_protocol_file, log_error, log_info, read_test_case, get_connection_config, get_mount_src_path,debugexit,update_dict_empty_fields
from atf.common.atf_dc_read_datasources import read_data
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_cls_results_chart import generate_results_charts
from atf.common.atf_cls_loads2t import LoadS2T
from atf.common.atf_cls_s2tautosqlgenerator import S2TAutoLoadScripts
from atf.common.atf_pdf_constants import *
import os
import datacompy
from datacompy.spark.legacy import LegacySparkCompare
import sys
import traceback
from constants import *


class S2TTester:

    def __init__(self, spark):
        self.var = ""
        self.spark = spark
    
    def starttestexecute(self, protocol_file_path, testcasetype, testcasesrunlist):
        log_info(f"Protocol Execution Started")
        try:
            log_info(f"Reading the Protocol file details from :- {protocol_file_path}")
            dict_protocol, df_testcases = read_protocol_file(protocol_file_path)
            log_info("Protocol read completed :- ")
            log_info(dict_protocol)
            # "test_case_type", "count", ["count","duplicate","content"]
            # s3_path = get_connection_config(s3_conn_name)
            # s3_path = s3_path['BUCKETNAME']
            # s3_path = ""
            # + '/' + str(testcasetype) + '/'
            # results_path = str(s3_path) + \
            #   str(dict_protocol['protocol_results_path'])

            results_path = str(root_path+dict_protocol['protocol_results_path'])
            #Creating directory for results folder
            if not os.path.exists(results_path):
                log_info(f"Creating directory - {results_path}")
                os.mkdir(results_path)
            else:
                log_info(f"The directory - `{results_path}` already exists.")
                
            timenow = datetime.now(utctimezone)
            created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))
            # cloud_results_folder = results_path + str(dict_protocol['protocol_name']) + \
            # '/run_' + str(dict_protocol['protocol_name']) + "_" + created_time+'/'

            #Creating directory for results folder with directory name as protocol_name
            protocol_result_directory = results_path + dict_protocol['protocol_name'] + '/'
            if not os.path.exists(protocol_result_directory):
                log_info(f"Creating directory - {protocol_result_directory}")
                os.mkdir(protocol_result_directory)
            else:
                log_info(f"The directory - `{protocol_result_directory}` already exists.")
                
            cloud_results_folder = results_path + \
                str(dict_protocol['protocol_name']) + \
                '/run_'+testcasetype+"_"+created_time+'/'
            log_info(f"Protocol Result folder Path :- {cloud_results_folder}")
            os.mkdir(cloud_results_folder)
            testcase_cloud_results_folder = cloud_results_folder + '/run_' + testcasetype + '_testcase_summary_' + created_time+'/'
            os.mkdir(testcase_cloud_results_folder)
            # protocol_output_path = "/dbfs" + get_mount_path(cloud_results_folder)
            # testcase_output_path = "/dbfs" + get_mount_path(testcase_cloud_results_folder)
            protocol_output_path = cloud_results_folder
            testcase_output_path = testcase_cloud_results_folder
            combined_testcase_output_path = protocol_output_path + "/run_tc_" + testcasetype + "_combined_" + \
                str(dict_protocol['protocol_name']) + \
                "_" + created_time + ".pdf"

            df_protocol_summary, protocol_run_details, protocol_run_params = self.execute_protocol(
                dict_protocol, df_testcases, testcase_output_path, combined_testcase_output_path, testcasetype, testcasesrunlist)

            summary_output_path = self.generate_protocol_summary_report(
                df_protocol_summary, protocol_run_details, protocol_run_params, protocol_output_path, created_time, testcasetype)
            # generate HTML report ** new function **
            #generate_results_charts(df_protocol_summary, protocol_run_details, protocol_run_params, created_time, testcasetype, cloud_results_folder, combined_testcase_output_path, summary_output_path)
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

        elif testcasetype == "content" or testcasetype == "count and content":
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No. of Rows in Source', 'No. of Rows in Target',
                                               'No. of Rows matched', 'No. of Rows mismatched', 'Test Result', 'Reason', 'Runtime'])
        
        elif testcasetype == "fingerprint":
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No. of KPIs in Source',
                                                        'No. of KPIs in Target', 'No. of KPIs matched',
                                                        'No. of KPIs mismatched', 'Test Result', 'Reason', 'Runtime'])

        pdfobj_combined_testcase = generatePDF()

        testcases_run_list = []

        if testcasesrunlist[0] != 'all':
            log_info(f"Test Case(s) part of current execution are :- {testcasesrunlist}")
            #print(testcasesrunlist)
        lst_run_testcases = testcasesrunlist

        for index, row in df_testcases.iterrows():
            log_info('=====================================================================================')
            #New Change
            row = row.fillna('')
            row = row.apply(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x)

            log_info(f"Reading details of test case :- {row['testcasename']}")
            testcase_details = {}
            test_case_name = row['testcasename']
            tcnbr = str(int(row['Sno.']))
            execute_flag = row['execute']
            
            #test_case_file_path = root_path+row['test_case_file_path']
            #s3_conn_name = dict_protocol['protocol_connection']
            # s3_path = get_connection_config(s3_conn_name)
            # s3_path = s3_path['BUCKETNAME']
            # test_case_mnt_src_path = s3_path + row['test_case_file_path']
            # test_case_file_path = '/dbfs' + get_mount_path(s3_path) + row['test_case_file_path']

            try:
                if (lst_run_testcases[0] == 'all' and execute_flag == 'Y'):
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

                log_info(f"Reading Test Case Config for :- {test_case_name}")
                
                #testcase_details = read_test_case(row,df_testcases)

                testcase_details = dict(row[2:])             
                # rel_source_path = testcase_details['sourcepath']
                # testcase_details['sourcepath'] = s3_path + testcase_details['sourcepath']
                # testcase_details['sourcepath'] = testcase_details['sourcepath']
                # rel_target_path = testcase_details['targetpath']
                # testcase_details['targetpath'] = s3_path + testcase_details['targetpath']
                # testcase_details['targetpath'] = testcase_details['targetpath']
                testcase_starttime = datetime.now(utctimezone)
                
                #Reading Source and Target Data
                log_info(f"Reading Source and Target Data based on TestCase Configuration :- {test_case_name}")
                log_info('Executing execute_testcase function')
                compare_input = self.execute_testcase(testcase_details, dict_protocol,auto_script_path, testcasetype)

                #Comparing Source and Target Data
                log_info(f"Comparing Source and Target Data based on TestCase Configuration Started :- {test_case_name}")
                dict_compareoutput = self.compare_data(compare_input, testcasetype)

                testcase_endtime = datetime.now(utctimezone)
                testcase_exectime = testcase_endtime - testcase_starttime
                testcase_exectime = str(testcase_exectime).split('.')[0]
                log_info(f"Comparing Source and Target Data based on TestCase Configuration Completed for :- {test_case_name}")
                # log_info(f"Execution of Test Case {test_case_name} completed in {testcase_exectime}")

                log_info(f"Test Results PDF Generation for Test Case Started for :- {test_case_name}")
                testcase_starttime = testcase_starttime.astimezone(
                    utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
                testcase_endtime = testcase_endtime.astimezone(
                    utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
                # testcase_exectime = testcase_exectime.strftime("%H:%M:%S")

                dict_runsummary = {'Application Name': dict_protocol['protocol_application_name'], 'Protocol Name': dict_protocol['protocol_name'],
                                   "Protocol File Path": protocol_file_path, 'Testcase Name': testcase_details['testcasename'],
                                   "Testcase Type": testcasetype,
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

                #Testcase summary pdf creation
                pdfobj = self.generate_testcase_summary_report(dict_runsummary, dict_config, results_path, compare_input, dict_compareoutput, testcasetype, comparison_type, pdfobj)
                pdfobj.pdf.output(results_path, 'F')
                
                log_info(f"Testcase Results PDF for - {test_case_name} testcase is created at location :- {results_path}")
                log_info(f"TestCase Results PDF Generation for Test Case Completed :- {test_case_name}")
                dict_config = dict_config_temp

                log_info(f"TestCase Results of  - {test_case_name} are Appended to the Summary")
                pdfobj_combined_testcase = self.generate_testcase_summary_report(
                    dict_runsummary, dict_config, results_path, compare_input, dict_compareoutput, testcasetype, comparison_type, pdfobj_combined_testcase)
                
                if testcasetype == "count":
                    df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of rows in Source']), str(
                        dict_testresults['No. of rows in Target']), dict_compareoutput['test_result'], dict_compareoutput['result_desc'], str(testcase_exectime)]

                elif testcasetype == "duplicate":
                    df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of rows in Source']), str(dict_testresults['No. of distinct rows in Source']), str(
                        dict_testresults['No. of rows in Target']), str(dict_testresults['No. of distinct rows in Target']), dict_compareoutput['test_result'], dict_compareoutput['result_desc'], str(testcase_exectime)]

                elif testcasetype == "content" or testcasetype == "count and content":
                    df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of rows in Source']), str(dict_testresults['No. of rows in Target']), str(
                        dict_testresults['No. of matched rows']), str(dict_testresults['No. of mismatched rows']), dict_compareoutput['test_result'], dict_compareoutput['result_desc'], str(testcase_exectime)]
                
                elif testcasetype == "fingerprint":
                    df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of KPIs in Source']), str(dict_testresults['No. of KPIs in Target']), str(
                        dict_testresults['No. of KPIs matched']), str(dict_testresults['No. of KPIs mismatched']), dict_compareoutput['test_result'], dict_compareoutput['result_desc'], str(testcase_exectime)]
                    
                log_info(f"Testcase Execution Completed for :- {test_case_name}")
            except Exception as e1:
                log_error(f"Testcase Execution ERRORED :- {test_case_name} - ERRORMSG:{str(e1)}")
                log_error(traceback.format_exc())
                if testcasetype == "count":
                    df_protocol_summary.loc[index] = [
                        test_case_name, '', '', 'Failed', 'Execution Error', '']
                if (testcasetype == "content" or testcasetype == "duplicate" or testcasetype == "fingerprint"):
                    df_protocol_summary.loc[index] = [
                        test_case_name, '', '', '', '', 'Failed', 'Execution Error', '']
    
        #Creating Combined pdf for all testcases
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
    

    def execute_testcase(self, tc_config, dict_protocol,auto_script_path, testcasetype):
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

        if tc_config['s2tpath'] != "":
            log_info(f"Reading the S2T for Source Details located at - {root_path+tc_config['s2tpath']}")

        if tc_config['comparetype'] == 's2tcompare':
            loads2t_path = root_path+tc_config['s2tpath']
            log_info('Initializing class LoadS2T')
            s2tobj = LoadS2T(loads2t_path, spark)
        
        if (tc_config['comparetype'] == 's2tcompare' and tc_config['testquerygenerationmode'] == 'Manual'):
            
            #Reading the Source and Target Data
            log_info('Reading Source and Target Data for Compare Type - s2tcompare and using Manual Query Generation Mode.')

            #Fetching common fields from protocol sheet if protocoltestcasedetails sheet fields are empty
            tc_config=update_dict_empty_fields(tc_config,dict_protocol)
            
            #Reading Source Data
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],
                           'schemastruct': s2tobj.getSchemaStruct("source"),'comparetype':tc_config['comparetype'],'filename':tc_config['sourcefilename']}
            source_df, source_query = read_data(tc_source_config,spark)
            log_info("Reading the Source Data completed.")

            #Reading Target Data
            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],
                           'schemastruct': s2tobj.getSchemaStruct("target"),'comparetype':tc_config['comparetype'],'filename':tc_config['targetfilename']}    
            target_df, target_query = read_data(tc_target_config,spark)
            log_info("Reading the Target Data completed.")

        elif (tc_config['comparetype'] == 's2tcompare' and tc_config['testquerygenerationmode'] == 'Auto'):

            #Reading the Source and Target Data
            log_info('Reading Source and Target Data for Compare Type - s2tcompare and using Manual Query Generation Mode.')

            #Reading Source Data
            log_info(f"Reading the Source Data")
            tc_source_config = {'connectionname': tc_config['sourceconnectionname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath'], 'format': tc_config['sourcefileformat'], 'name': tc_config['sourcefilename'],
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'],
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'],
                           'testcasename': tc_config['testcasename'], 'autoscripttype': 'source', 'autoscriptpath': auto_script_path,'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilename']}
            
            log_info('Initializing class S2TAutoLoadScripts')
            autoldscrobj = S2TAutoLoadScripts(s2tobj, tc_source_config, spark)
            
            scriptpath, source_df, source_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)
            source_conn_name = source_file_details_dict["connectionname"]
            join_cols_source = source_file_details_dict["join_columns"]
            source_query = open(scriptpath).read().split('\n')
            log_info("Reading the Source Data completed.")

            #Reading Target Data
            log_info(f"Reading the Target Data")
            tc_target_config = {'connectionname': tc_config['targetconnectionname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath'], 'format': tc_config['targetfileformat'], 'name': tc_config['targetfilename'],
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'],
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'],
                           'testcasename': tc_config['testcasename'], 'autoscripttype': 'target', 'autoscriptpath': auto_script_path,'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilename']}
            
            autoldscrobj = S2TAutoLoadScripts(s2tobj, tc_target_config, spark)
            scriptpath, target_df, target_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)
            target_conn_name = target_file_details_dict["connectionname"]
            join_cols_target = target_file_details_dict["join_columns"]
            target_query = open(scriptpath).read().split('\n')
            log_info("Reading the Target Data completed.")

        elif tc_config['comparetype'] == 'likeobjectcompare':

            #Reading the Source and Target Data
            log_info('Reading Source and Target Data for Compare Type - s2tcompare and using Manual Query Generation Mode.')

            #Fetching common fields from protocol sheet if protocoltestcasedetails sheet fields are empty
            tc_config=update_dict_empty_fields(tc_config,dict_protocol)
            
            #Reading Source Data
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilename']}
            source_df, source_query = read_data(tc_source_config,spark)
            log_info("Reading the Source Data completed.")

            #Reading Target Data
            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilename']}    
            target_df, target_query = read_data(tc_target_config,spark)
            log_info("Reading the Target Data completed.")
            

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
                         "filedetails": file_details_dict}
        # print(compare_input)
        return compare_input

    
    def compare_data(self, compare_input, testcasetype):
        log_info(f"Data Compare Started for TestingType - {testcasetype} ")
        sourcedf = compare_input['sourcedf']
        targetdf = compare_input['targetdf']
        joincolumns = compare_input['joincolumns']
        log_info(f"Joining with column names: {joincolumns}")
        colmapping = compare_input['colmapping']
        limit = compare_input['limit']
        dict_match_summary = {}
        dict_match_details = {}
        # Initial spark memory processing STARTS HERE...
        log_info("Counting Source Rows now...")
        rowcount_source = sourcedf.count()
        log_info("Counting Target Rows now...")
        rowcount_target = targetdf.count()

        if (testcasetype == 'content'):
            
            log_info("Comparing Contents of Source and Target now...(this may take a while)...")
            comparison_obj = LegacySparkCompare(spark, sourcedf, targetdf,  \
                                                    column_mapping=colmapping, \
                                                    join_columns=joincolumns, \
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

            if rows_only_source.limit(1).count() ==0 and rows_only_target.limit(1).count() ==0:
                rows_both_all = None
            if rows_mismatch.limit(1).count() ==0:
                rows_mismatch = None
                sample_mismatch = None
            else:
                sample_mismatch = rows_mismatch.select(
                    joincolumns).limit(limit)
                sample_mismatch, concat_list = self.concat_keys(
                    sample_mismatch, joincolumns)
                sample_mismatch = sample_mismatch.select(
                    concat(*concat_list).alias("Key Columns"))
            if rows_only_source.limit(1).count() ==0:
                rows_only_source = None
                sample_source_only = None
            else:
                sample_source_only = rows_only_source.select(
                    joincolumns).limit(limit)
                sample_source_only, concat_list = self.concat_keys(
                    sample_source_only, joincolumns)
                sample_source_only = sample_source_only.select(
                    concat(*concat_list).alias("Key Columns"))
            if rows_only_target.limit(1).count() ==0:
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

        elif (testcasetype == "duplicate"):
            distinct_rowcount_source = sourcedf.select(
                joincolumns).distinct().count()
            distinct_rowcount_target = targetdf.select(
                joincolumns).distinct().count()
            duplicate_rowcount_source = rowcount_source - distinct_rowcount_source
            duplicate_rowcount_target = rowcount_target - distinct_rowcount_target
            if (distinct_rowcount_source == rowcount_target and rowcount_target == distinct_rowcount_target):
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

        elif (testcasetype == "fingerprint"):
            special_srcdf = sourcedf
            special_tgtdf = targetdf

            log_info("Comparing Fingerprints of Source and Target now...")
            fingerprintcomp_obj = datacompy.SparkCompare(spark, special_srcdf, special_tgtdf,
                                                    column_mapping=colmapping, join_columns=joincolumns)
            fingerprintcomp_obj.report()

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

        dict_compareoutput = {'rows_both_all': rows_both_all, 'rows_mismatch': rows_mismatch, 'rows_only_source': rows_only_source, 'rows_only_target': rows_only_target, 'test_result': test_result, 'sample_mismatch': sample_mismatch,
                              'sample_source': sample_source_only, 'sample_target': sample_target_only, 'dict_results': dict_results, 'col_match_summary': df_match_summary, 'row_count': dict_no_of_rows, 'col_match_details': dict_match_details, 'result_desc': result_desc}
        
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
            pdfobj.write_text("5. Fingerprint Comparison Report ", 'section heading')
            if col_match_summary != None:
                pdfobj.create_table_summary(row_count)
            pdfobj.create_table_details(col_match_summary, 'mismatch_summary')

        elif (testcasetype == 'content' or testcasetype == 'count and content'):
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

        protocol_output_path = output_path + "/run_" + testcasetype + '_' + \
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
    spark = SparkSession.getActiveSession()
    if spark is not None:
        log_info("!!! Databricks Spark Session Acquired !!!")
    log_info(spark)
    testcasesrunlist = []
    protocol_file_path = sys.argv[1]
    protocol_file_path = root_path + protocol_file_path
    testtype = sys.argv[2]
    temporaryrunlist=sys.argv[3].rstrip()
    if "," in sys.argv[3]:
        testcasesrunlist = temporaryrunlist.split(",")
    else:
        testcasesrunlist.append(temporaryrunlist)
    log_info(f"Protocol Config path :{protocol_file_path}")
    log_info(f"TestType: {testtype}")
    log_info(f"TestCasesRunList: {testcasesrunlist}")
    testerobj = S2TTester(spark)
    testerobj.starttestexecute(protocol_file_path, testtype, testcasesrunlist)
   
