import json
import traceback
from datetime import datetime
import pandas as pd
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from atf.common.atf_cls_loads2t import LoadS2T
from atf.common.atf_cls_s2tautosqlgenerator import S2TAutoLoadScripts
from atf.common.atf_common_functions import read_protocol_file, log_error, log_info, create_json_file
from atf.common.atf_dc_read_datasources import read_data
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


class ConnectionS2T:

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
            else:
                log_info(f"The directory - `{results_path}` already exists.")

            timenow = datetime.now(utctimezone)
            created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))

            cloud_results_folder = results_path + \
                str(dict_protocol['protocol_name']) + \
                '/run_'+testcasetype+"_"+created_time+'/'
            log_info(f"Protocol Result folder Path: {cloud_results_folder}")

            testcase_cloud_results_folder = cloud_results_folder + '/run_testcase_summary_' + created_time+'/'


            protocol_output_path = cloud_results_folder
            testcase_output_path = testcase_cloud_results_folder
            combined_testcase_output_path = protocol_output_path + "/run_tc_combined_" + \
                str(dict_protocol['protocol_name']) + \
                "_" + created_time + ".pdf"

            df_protocol_summary, protocol_run_details, protocol_run_params = self.execute_protocol(
                dict_protocol, df_testcases, testcase_output_path, combined_testcase_output_path, testcasetype, testcasesrunlist)

            log_info("Protocol Execution Completed")

        except Exception as e2:
            log_error(f"Protocol Execution ERRORED: {str(e2)}")

    def execute_protocol(self, dict_protocol, df_testcases, output_path, combined_testcase_output_path, testcasetype, testcasesrunlist):

        log_info("Protocol Testcases Execution Started")
        protocol_starttime = datetime.now(utctimezone)
        # auto_script_path = generate_autoscript_path(combined_testcase_output_path)
        auto_script_path = ""
        if testcasetype == "count":
            df_protocol_summary = pd.DataFrame(columns=[
                                               'Testcase Name', 'No. of Rows in Source', 'No. of Rows in Target', 'Test Result', 'Reason', 'Runtime'])

        else:
            df_protocol_summary = pd.DataFrame(columns=['Testcase Name', 'No. of Rows in Source', 'No. of Distinct Rows in Source',
                                    'No. of Rows in Target', 'No. of Distinct Rows in Target', 'Test Result', 'Reason', 'Runtime'])

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


                log_info(
                    f"Reading Test Case Config for:  {test_case_name}")
                testcase_details = dict(row[2:])
                #testcase_details = read_test_case(test_case_file_path)

                testcase_starttime = datetime.now(utctimezone)

                log_info(f"Reading Source and Target Data based on TestCase Configuration:  {test_case_name}")

                compare_input = self.execute_testcase(test_case_name, testcase_details, auto_script_path, testcasetype)

                log_info(f"Saving Source and Target Data based on TestCase Configuration Started:  {test_case_name}")
                dict_compareoutput = self.compare_data(compare_input)

                testcase_endtime = datetime.now(utctimezone)
                testcase_exectime = testcase_endtime - testcase_starttime
                testcase_exectime = str(testcase_exectime).split('.')[0]
                log_info(f"Comparing Source and Target Data based on TestCase Configuration Completed for {test_case_name}")
                log_info(f"Execution of Test Case {test_case_name} completed in {testcase_exectime}")

                source_df = compare_input['sourcedf']
                target_df = compare_input['targetdf']
                source_null_counts = []
                target_null_counts = []

                testcase_starttime = testcase_starttime.astimezone(
                    utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
                testcase_endtime = testcase_endtime.astimezone(
                    utctimezone).strftime("%d-%b-%Y %H:%M:%S %Z")
                # testcase_exectime = testcase_exectime.strftime("%H:%M:%S")

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
                comparison_type = testcase_details['comparetype']

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
            log_info("Done.")
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
            limit = 5
            dplimit = 100
        else:
            limit = tc_config['samplelimit']
            dplimit = tc_config['dataprofilelimit']

        log_info(f"Sample limit for mismatches is {limit}")
        log_info(f"Sample limit for data profiling is {dplimit}")
        
        join_cols = tc_config['primarykey'].split(',')
        #join_cols = [word.upper() for word in join_cols]
        join_cols_source = []
        join_cols_target = []
        map_cols = []

        if tc_config['s2tpath'] != "":
            log_info(f"Reading the S2T for Source Details located at {tc_config['s2tpath']}")

        if tc_config['comparetype'] == 's2tcompare':
            s2tobj = LoadS2T(tc_config['s2tpath'], self.spark)

        if (tc_config['comparetype'] == 's2tcompare' and tc_config['testquerygenerationmode'] == 'Manual'):
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],
                           'schemastruct': s2tobj.getSchemaStruct("source"),'comparetype':tc_config['comparetype'],'filename':tc_config['sourcefilename']}
            source_df, source_query = read_data(tc_source_config,self.spark)

            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],
                           'schemastruct': s2tobj.getSchemaStruct("target"),'comparetype':tc_config['comparetype'],'filename':tc_config['targetfilename']}    
            target_df, target_query = read_data(tc_target_config,self.spark)

        elif (tc_config['comparetype'] == 's2tcompare' and tc_config['testquerygenerationmode'] == 'Auto'):

            log_info(f"Reading the Source Data")
            tc_source_config = {'connectionname': tc_config['sourceconnectionname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath'], 'format': tc_config['sourcefileformat'], 'name': tc_config['sourcefilename'],
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'],
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'],
                           'test_case_name': test_case_name, 'autoscripttype': 'source', 'autoscriptpath': auto_script_path,'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilename']}
            autoldscrobj = S2TAutoLoadScripts(s2tobj, tc_source_config, self.spark)
            scriptpath, source_df, source_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)
            source_conn_name = source_file_details_dict["connectionname"]
            join_cols_source = source_file_details_dict["join_columns"]
            source_query = open(scriptpath).read().split('\n')

            log_info(f"Reading the Target Data")
            tc_target_config = {'connectionname': tc_config['targetconnectionname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath'], 'format': tc_config['targetfileformat'], 'name': tc_config['targetfilename'],
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'],
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'],
                           'test_case_name': test_case_name, 'autoscripttype': 'target', 'autoscriptpath': auto_script_path,'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilename']}
            autoldscrobj = S2TAutoLoadScripts(s2tobj, tc_target_config, self.spark)
            scriptpath, target_df, target_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)
            target_conn_name = target_file_details_dict["connectionname"]
            join_cols_target = target_file_details_dict["join_columns"]
            target_query = open(scriptpath).read().split('\n')

        elif tc_config['comparetype'] == 'likeobjectcompare':
            log_info("Reading the Source Data")
            tc_source_config = {'aliasname': tc_config['sourcealiasname'], 'connectiontype': tc_config['sourceconnectiontype'],
                           'path': tc_config['sourcefilepath']+"/"+tc_config['sourcefilename'], 'format': tc_config['sourcefileformat'], 
                           'connectionname': tc_config['sourceconnectionname'], 
                           'excludecolumns': tc_config['sourceexcludecolumnlist'], 'filter': tc_config['sourcefilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['sourcefiledelimiter'], 
                           'querypath': tc_config['sourcequerysqlpath']+"/"+tc_config['sourcequerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['sourcefilename']}
            source_df, source_query = read_data(tc_source_config,self.spark)
           
            log_info(f"Reading the Target Data")
            tc_target_config = {'aliasname': tc_config['targetaliasname'], 'connectiontype': tc_config['targetconnectiontype'],
                           'path': tc_config['targetfilepath']+"/"+tc_config['targetfilename'], 'format': tc_config['targetfileformat'], 
                           'connectionname': tc_config['targetconnectionname'], 
                           'excludecolumns': tc_config['targetexcludecolumnlist'], 'filter': tc_config['targetfilter'], 
                           'testquerygenerationmode': tc_config['testquerygenerationmode'], 'delimiter': tc_config['targetfiledelimiter'], 
                           'querypath': tc_config['targetquerysqlpath']+"/"+tc_config['targetquerysqlfilename'],'comparetype':tc_config['comparetype'],
                           'filename':tc_config['targetfilename']}    
            target_df, target_query = read_data(tc_target_config,self.spark)

        testcase_config_list.append(tc_source_config)
        testcase_config_list.append(tc_target_config)
        log_info(f"Test case config created by DATF is: {testcase_config_list}")
        create_json_file(testcase_config_list, dq_testconfig_path)
        log_info(f"Test case config saved at location: {dq_testconfig_path}")

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
                         'dataprofilelimit': dplimit,
                         "filedetails": file_details_dict}
        # print(compare_input)
        return compare_input

    
    def compare_data(self, compare_input):
        log_info(f"Dataframe and Query Save Started for Source / Target ")
        sourcedf = compare_input['sourcedf']
        targetdf = compare_input['targetdf']
        joincolumns = compare_input['joincolumns']
        log_info(f"Joining with column names: {joincolumns}")
        colmapping = compare_input['colmapping']
        limit = compare_input['limit']
        dplimit = compare_input['dataprofilelimit']
        # Spark memory processing STARTS HERE...
        sourcedf = sourcedf.limit(dplimit)
        src_df_excel = sourcedf.pandas_api()
        src_df_excel.to_excel(src_column_path)
        targetdf = targetdf.limit(dplimit)
        tgt_df_excel = targetdf.pandas_api()
        tgt_df_excel.to_excel(tgt_column_path)
        log_info(f"Dataframe Saved as Excel for Source and Target in location - {column_data_path} ")
        src_query = ""
        tgt_query = ""
        src_qry_list = compare_input['sourcequery']
        print(src_qry_list)
        tgt_qry_list = compare_input['targetquery']
        print(tgt_qry_list)

        if isinstance(src_qry_list, list):
            for s in src_qry_list:
                if s.startswith('spark.sql'):
                    src_query = s[11:-3].strip() #Extracting the query
        if isinstance(tgt_qry_list, list):
            for t in tgt_qry_list:
                if t.startswith('spark.sql'):
                    tgt_query = t[11:-3].strip() #Extracting the query

        if isinstance(src_qry_list, list) and isinstance(tgt_qry_list, list):
            json_data = {"sourcequery": src_query, "targetquery": tgt_query}
        elif isinstance(src_qry_list, str) and isinstance(tgt_qry_list, list):
            json_data = {"sourcequery": src_qry_list, "targetquery": tgt_query}
        elif isinstance(src_qry_list, list) and isinstance(tgt_qry_list, str):
            json_data = {"sourcequery": src_query, "targetquery": tgt_qry_list}
        else:
            json_data = {"sourcequery": src_qry_list, "targetquery": tgt_qry_list}

        create_json_file(json_data, gen_queries_path)
        log_info(f"Queries Saved as JSON for Source and Target at: {gen_queries_path} ")


if __name__ == "__main__":
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    testcase_config_list = []
    spark = createsparksession()

    protocol_file_path = sys.argv[1]
    testcasesrunlist = sys.argv[2]
    testtype = "count"

    log_info(f"Protocol Config path: {protocol_file_path}")
    log_info(f"TestCasesRunList: {testcasesrunlist}")

    testerobj = ConnectionS2T(spark)
    testerobj.starttestexecute(protocol_file_path, testtype, testcasesrunlist)
   
