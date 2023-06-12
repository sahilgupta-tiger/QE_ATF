# Databricks notebook source
# DBTITLE 1,Notebook library imports
import sys
import json
import datacompy
import pandas as pd
from fpdf import FPDF
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_pdf_constants import *
from atf.common.atf_common_functions import *
from atf.common.atf_sc_read_datasources import read_schema
from s2ttester import spark

# COMMAND ----------

# MAGIC %run ./includes

# COMMAND ----------

# DBTITLE 1,Notebook parameters
dbutils.widgets.text("protocol_config_file_path","s3://abt-bdaa-cleansed-us-east-1-adot-d/suresh/atf/SchemaCompare/Protocol/Protocolconfig_oracleredshift_schemacompare.xlsx")
protocol_file_path_s3 = dbutils.widgets.get("protocol_config_file_path")

dbutils.widgets.text("testcases_torun","")
string_testcases_torun = dbutils.widgets.get("testcases_torun")
string_testcases_torun = string_testcases_torun.replace(' ','')
lst_run_testcases = string_testcases_torun.split(',')
list_testcases_torun = [i.strip().lower() for i in lst_run_testcases]
if '' in list_testcases_torun:
  list_testcases_torun.remove('')

# COMMAND ----------

def execute_testcase(testcase_details,testcaseno):
  dict_source = dict_target = {}
  source_df = target_df = spark.createDataFrame([],StructType([]))
  comparetype = testcase_details['comparesubtype']
  log_info(f'Started Executing Testcase {testcaseno}')
  
  if comparetype =='likeobjectcompare':
    map_cols=[]
    join_cols = ['columnname']
    dict_source = {'connectionname':testcase_details['sourceconnectionname'], 'connectiontype':testcase_details['sourceconnectiontype'],
                    'filepath':testcase_details['sourcepath'], 'filename':testcase_details['sourcename'], 
                   'format':testcase_details['sourceformat'], 'delimiter':testcase_details['sourcedelimiter'],
                   'tablename':testcase_details['sourcetablename'], 'schemaname':testcase_details['sourcedatabaseschemaname'], 'comparetype':testcase_details['comparesubtype']}
    
    dict_target = {'connectionname':testcase_details['targetconnectionname'], 'connectiontype':testcase_details['targetconnectiontype'],
                    'filepath':testcase_details['targetpath'], 'filename':testcase_details['targetname'], 
                   'format':testcase_details['targetformat'], 'delimiter':testcase_details['targetdelimiter'],
                   'tablename':testcase_details['targettablename'], 'schemaname':testcase_details['targetdatabaseschemaname'], 'comparetype':testcase_details['comparesubtype']}
    
    source_df = read_schema(dict_source,comparetype)
    target_df = read_schema(dict_target,comparetype)
    
  elif comparetype =='s2tcompare':
    map_cols=[]
    join_cols = ['columnname']
    dict_source = {'connectionname':testcase_details['s2tconnectionname'], 'connectiontype':testcase_details['s2tconnectiontype'],
                    'filepath':testcase_details['sourcepath'], 'format':'xlsx', 'S2Tpath':testcase_details['s2tpath'],
                   'comparetype':testcase_details['comparesubtype'], 'layer':testcase_details['s2tlayer']}
    
    dict_target = {'connectionname':testcase_details['targetconnectionname'], 'connectiontype':testcase_details['targetconnectiontype'],
                    'filepath':testcase_details['targetpath'], 'format':testcase_details['targetformat'], 
                   'name':testcase_details['targetname'],'delimiter':testcase_details['targetdelimiter'], 'tablename':testcase_details['targettablename'], 'schemaname':testcase_details['targetdatabaseschemaname'], 'layer':testcase_details['targetlayer'], 'comparetype':testcase_details['comparesubtype']}
    
    
    source_df = read_schema(dict_source,comparetype)
    target_df = read_schema(dict_target,comparetype)
  dict_compareinput = {'sourcedf':source_df, 'targetdf':target_df, 'colmapping':map_cols, 'joincolumns':join_cols, 'comparetype':comparetype}
  
  log_info(f'Completed Executing Testcase {testcaseno}')

  
  return dict_compareinput 

# COMMAND ----------

def compare_schema(dict_compareinput): 
  
  log_info("Schema Compare Started")
  
  sourcedf = dict_compareinput['sourcedf']
  targetdf = dict_compareinput['targetdf']
  joincolumns = dict_compareinput['joincolumns']  
  colmapping = dict_compareinput['colmapping']
  
  comparison_obj  = datacompy.SparkCompare(spark, sourcedf, targetdf, join_columns = joincolumns)
  
  columns_both_all = comparison_obj.rows_both_all
  columns_match = (columns_both_all.filter(columns_both_all.datatype_match=='true').drop('datatype_match'))
  columns_mismatch = comparison_obj.rows_both_mismatch.drop('datatype_match')
  columns_only_source = comparison_obj.rows_only_base
  columns_only_target = comparison_obj.rows_only_compare
  
  columncount_source = sourcedf.count()
  columncount_target = targetdf.count()
  columncount_only_source = columns_only_source.count()
  columncount_only_target = columns_only_target.count()
  columncount_common = comparison_obj.common_row_count
  columncount_mismatch = columns_mismatch.count()
  columncount_match = columns_match.count()
  
  
  columns_mismatch = columns_mismatch.withColumn('Target Column Name', col('columnname'))
  columns_mismatch = columns_mismatch.select(col("columnname").alias("Source Column name"),
                                             col("datatype_base").alias("Source Datatype"),
                                             col('Target Column Name'),
                                             col("datatype_compare").alias("Target Datatype"))
                                                        
  columns_match = columns_match.withColumn('Target Column Name', col('columnname'))  
  columns_match = columns_match.select(col("columnname").alias("Source Column name"),
                                       col("datatype_base").alias("Source Datatype"),
                                       col('Target Column Name'),
                                       col("datatype_compare").alias("Target Datatype"))
    
  
  columns_only_source = columns_only_source.select(col("columnname").alias("Column Name"),
                                                   col("datatype").alias("Source Datatype"))

                                                                                 
  columns_only_target = columns_only_target.select(col("columnname").alias("Column Name"),
                                                   col("datatype").alias("Target Datatype"))
  
  if (columncount_only_source==0 and columncount_only_target==0 and columncount_mismatch==0):
    test_result = "Passed"        
  else:
    test_result = "Failed"
    
  if columns_match.rdd.isEmpty():
    columns_match = None
  if columns_mismatch.rdd.isEmpty():
    columns_mismatch = None
  if columns_only_source.rdd.isEmpty():
    columns_only_source = None
  if columns_only_target.rdd.isEmpty():
    columns_only_target = None
    
  dict_results={
    'No. of columns in Source':columncount_source,'No. of columns in Target':columncount_target, 
    'Total no. of matched columns':columncount_match,'No. of columns in Source but not in Target':columncount_only_source, 
    'No. of columns in Target but not in Source':columncount_only_target,'No. of columns with datatype mismatch':columncount_mismatch,
    'Test Result':test_result}


  dict_compareoutput = {'cols_only_source':columns_only_source,'cols_only_target':columns_only_target,
                        'cols_match':columns_match,'cols_mismatch':columns_mismatch,'dict_results':dict_results, 
                        'test_result':test_result}
  log_info("Schema Compare Completed")
  
  return dict_compareoutput

# COMMAND ----------

def generate_testcase_summary_report(pdfobj_testcase_combined, dict_runsummary, dict_configdetails, dict_compareoutput, protocol_starttime, results_path):
    
  log_info("Started Writing Testcase Summary Report")
  
  dict_results = dict_compareoutput['dict_results']
  cols_only_source = dict_compareoutput['cols_only_source']
  cols_only_target = dict_compareoutput['cols_only_target']
  cols_match = dict_compareoutput['cols_match']
  cols_mismatch = dict_compareoutput['cols_mismatch']
  test_result = dict_compareoutput['test_result']
  results_path = results_path + '_' + test_result.lower() + '_' + protocol_starttime + '.pdf'
  
  pdfobj_testcase = generatePDF()
  pdfobj_testcase.write_text(testcasereportheader, 'report header')
  pdfobj_testcase.write_text(dict_runsummary['Testcase Name'], 'subheading')
  pdfobj_testcase_combined.write_text(dict_runsummary['Testcase Name'], 'subheading')
  testcaseheader = 'Testcase '+ str(dict_runsummary['Testcase No.'])+' Summary'
  pdfobj_testcase.write_text(testcaseheader, 'testcase header')
  pdfobj_testcase_combined.write_text(testcaseheader, 'testcase header')
  pdfobj_testcase.write_text(rundetails_subheading, 'section heading')
  pdfobj_testcase_combined.write_text(rundetails_subheading, 'section heading')
  pdfobj_testcase.create_table_summary(dict_runsummary)
  pdfobj_testcase_combined.create_table_summary(dict_runsummary)
  pdfobj_testcase.write_text(configdetails_subheading, 'section heading')
  pdfobj_testcase_combined.write_text(configdetails_subheading, 'section heading')
  pdfobj_testcase.create_table_summary(dict_configdetails)
  pdfobj_testcase_combined.create_table_summary(dict_configdetails)
  pdfobj_testcase.write_text(testresults_subheading, 'section heading')
  pdfobj_testcase_combined.write_text(testresults_subheading, 'section heading')
  pdfobj_testcase.create_table_summary(dict_results,'L')
  pdfobj_testcase_combined.create_table_summary(dict_results,'L')
  pdfobj_testcase.write_text(mismatch_heading, 'section heading')
  pdfobj_testcase_combined.write_text(mismatch_heading, 'section heading')
  pdfobj_testcase.write_text('4.1 Columns with Datatype mismatch', 'section heading')
  pdfobj_testcase_combined.write_text('4.1 Columns with Datatype mismatch', 'section heading')
  col_width_list = [15,50,30,50,30]
  pdfobj_testcase.create_table_schemacomp(cols_mismatch, col_width_list, 'mismatch')
  pdfobj_testcase_combined.create_table_schemacomp(cols_mismatch, col_width_list, 'mismatch')
  pdfobj_testcase.write_text("4.2 Columns in Source but not in Target", 'section heading')
  pdfobj_testcase_combined.write_text("4.2 Columns in Source but not in Target", 'section heading')
  col_width_list = [15,80,30]
  pdfobj_testcase.create_table_schemacomp(cols_only_source, col_width_list, 'source_only')
  pdfobj_testcase_combined.create_table_schemacomp(cols_only_source, col_width_list, 'source_only')
  pdfobj_testcase.write_text("4.3 Columns in Target but not in Sarget", 'section heading')
  pdfobj_testcase_combined.write_text("4.3 Columns in Target but not in Sarget", 'section heading')
  col_width_list = [15,80,30]
  pdfobj_testcase.create_table_schemacomp(cols_only_target, col_width_list, 'target_only')
  pdfobj_testcase_combined.create_table_schemacomp(cols_only_target, col_width_list, 'target_only')
  pdfobj_testcase.write_text("5 Column Match details", 'section heading')
  pdfobj_testcase_combined.write_text("5 Column Match details", 'section heading')
  pdfobj_testcase.write_text("5.1 Columns with Datatype match", 'section heading')
  pdfobj_testcase_combined.write_text("5.1 Columns with Datatype match", 'section heading')
  col_width_list = [15,50,30,50,30]
  pdfobj_testcase.create_table_schemacomp(cols_match, col_width_list, 'match')
  pdfobj_testcase_combined.create_table_schemacomp(cols_match, col_width_list, 'match')
  pdfobj_testcase_combined.pdf.add_page()
  pdfobj_testcase.pdf.output(results_path,'F')
  
  log_info("Completed Writing Testcase Summary Report")
  
  return pdfobj_testcase_combined

# COMMAND ----------

def generate_protocol_summary_report(dict_input_params, dict_protocol_rundetails, df_protocolsummary, protocol_outputfolder):
  
  log_info("Started Writing Protocol Summary Report")
  
  protocol_outputpath = protocol_outputfolder + '/' + 'run_' +dict_protocol_rundetails['Protocol Name'] + '_' + dict_protocol_rundetails['Protocol Start Time'] + '.pdf'
  
  pdfobj_protocol = generatePDF()
  list_testcases =  dict_input_params['testcases to run']
  if not list_testcases:
    dict_input_params['testcases to run'] = 'All testcases in the protocol file are executed'
  pdfobj_protocol.write_text(protocolreportheader, 'report header')
  pdfobj_protocol.write_text(dict_protocol_rundetails['Protocol Name'], 'subheading')
  pdfobj_protocol.write_text('1 Input parameters', 'section heading')
  pdfobj_protocol.create_table_summary(dict_input_params)
  pdfobj_protocol.write_text('2 Protocol Run Summary', 'section heading')
  pdfobj_protocol.create_table_summary(dict_protocol_rundetails)
  pdfobj_protocol.write_text('3 Test Results Summary', 'section heading')
  
  sno = 1
  test_results_list = list(df_protocolsummary.select('Test Result').distinct().toPandas()['Test Result'])
  test_results_list.sort()
  for test_result in test_results_list:
    testheader = "3." + str(sno) + ". " + test_result + " Testcases"
    pdfobj_protocol.write_text(testheader, 'section heading')
    df_protocolsummary_temp = df_protocolsummary.select('*').filter(col('Test Result') == test_result)
    col_width_list = [8,40,16,16,16,16,16,16,13,20,15]
    pdfobj_protocol.create_table_schemacomp(df_protocolsummary_temp, col_width_list, 'protocol')
    sno = sno + 1
    
  log_info("Completed Writing Protocol Summary Report")
    
  pdfobj_protocol.pdf.output(protocol_outputpath,'F')

# COMMAND ----------

def execute_protocol(dict_protocol, df_testcases, testcase_output_path):
  
  log_info(f"Started Protocol Execution for {dict_protocol['protocol_name']}")
  
  context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  workspacename = context['tags']['browserHostName']

  connectionname = dict_protocol['protocol_connection']
  s3bucket = get_connection_config(connectionname)['BUCKETNAME']
  combinations_path = s3bucket + '/' + dict_protocol['compare_combinations_path']
  
  protocol_starttime = datetime.now()
  
  df_protocol_summary = pd.DataFrame(columns = 
                                     ['Testcase Name','No. of columns in Source', 'No. of Columns in Target',
                                      'No. of Columns in Source but not in Target','No. of Columns in Target but not in Source',
                                      'No. of Columns matched','No. of Columns mismatched','Test Result', 'Reason','Runtime'])
  
  test_case_no = 0
  pdfobj_testcase_combined = generatePDF()
  pdfobj_testcase_combined.write_text(testcasereportheader, 'report header')
  for index, row in df_testcases.iterrows():
    
    test_case_no = test_case_no + 1
    testcase_starttime = datetime.now()
    testcase_details = {}
    
    test_case_name = row['test_case_name']
    try:
      if (not list_testcases_torun or (list_testcases_torun and test_case_name in list_testcases_torun)):
        execute_flag = 'Y'
        pass
      else:
        execute_flag = 'N'
        continue
      
      test_case_file_path =  '/dbfs' + get_mount_path(s3bucket + '/' + row['test_case_file_path'])
      results_path = testcase_output_path + '/' + test_case_name 
      
      testcase_details = read_test_case(test_case_file_path)
      dict_combination = {}
      
      if testcase_details['comparesubtype'] == 's2tcompare':
        dict_combination = {'sourceconnectiontype':testcase_details['s2tconnectiontype'], 
                          'targetconnectiontype':testcase_details['targetconnectiontype'], 
                          'sourceformat':'xlsx', 
                          'targetformat':testcase_details['targetformat']}
      elif testcase_details['comparesubtype'] == 'objectcompare':
        dict_combination = {'sourceconnectiontype':testcase_details['sourceconnectiontype'], 
                          'targetconnectiontype':testcase_details['targetconnectiontype'], 
                          'sourceformat':testcase_details['sourceformat'], 
                          'targetformat':testcase_details['targetformat']}
        
      src_dbconn_url = tgt_dbconn_url = src_s3_bucket = tgt_s3_bucket = ''
      
      if testcase_details['comparesubtype'] == 's2tcompare':
        if testcase_details['s2tconnectiontype'].strip().lower() in ['aws-s3']:
          src_s3_bucket = get_connection_config(testcase_details['s2tconnectionname'])['BUCKETNAME']
      
      elif testcase_details['comparesubtype'] == 'objectcompare':
        if testcase_details['sourceconnectiontype'].strip().lower() in ['redshift','oracle']:
          src_dbconn_url = get_connection_config(testcase_details['sourceconnectionname'])['CONNURL']
        elif testcase_details['sourceconnectiontype'].strip().lower() in ['aws-s3']:
          src_s3_bucket = get_connection_config(testcase_details['sourceconnectionname'])['BUCKETNAME']
      
        
      if testcase_details['targetconnectiontype'].strip().lower() in ['redshift','oracle']:
        tgt_dbconn_url = get_connection_config(testcase_details['targetconnectionname'])['CONNURL']
      elif testcase_details['targetconnectiontype'].strip().lower() in ['aws-s3']:
          tgt_s3_bucket = get_connection_config(testcase_details['targetconnectionname'])['BUCKETNAME']
      
      flag_allowed = check_combination_allowed(combinations_path, dict_combination)
      
      if flag_allowed == 'Y':
        log_info("Schemacompare for the Testcase is Supported")
        dict_compareinput = execute_testcase(testcase_details,test_case_no)
        dict_compareoutput = compare_schema(dict_compareinput)
      
        testcase_endtime = datetime.now()
        testcase_exectime = testcase_endtime - testcase_starttime
        testcase_exectime= str(testcase_exectime).split('.')[0]
        testcase_starttime = str(datetime.astimezone(testcase_starttime).strftime("%d-%b-%Y_%H:%M:%S_%Z"))
        testcase_endtime = str(datetime.astimezone(testcase_endtime).strftime("%d-%b-%Y_%H:%M:%S_%Z"))
        
        dict_runsummary = {'Testcase No.': test_case_no, 'Testcase Name':test_case_name, 
                           'Test Environment': workspacename, 
                           'Testcase description':testcase_details['testcasedescription'],
                           'Application Name':dict_protocol['protocol_application_name'], 
                           'Protocol Name':dict_protocol['protocol_name'],
                           'Start Time':testcase_starttime,'End Time':testcase_endtime,
                           'Run Time':testcase_exectime,'Test Result': dict_compareoutput['test_result']}
        
          
        dict_configdetails = {'Compare Type':testcase_details['comparesubtype'],
                            'S2T path':testcase_details['s2tpath'],
                            'Source Connection Type':testcase_details['sourceconnectiontype'],
                            'Source Connection Name':testcase_details['sourceconnectionname'], 
                            'Source format':testcase_details['sourceformat'], 
                            'Source S3 bucket':src_s3_bucket,
                            'Source filename':testcase_details['sourcename'], 
                            'Source filepath':testcase_details['sourcepath'],
                            'Source database schemaname':testcase_details['sourcedatabaseschemaname'],
                            'Source tablename':testcase_details['sourcetablename'], 
                            'Source table DB Connection URL':src_dbconn_url,
                            'Target Connection Type':testcase_details['targetconnectiontype'],
                            'Target Connection name':testcase_details['targetconnectionname'], 
                            'Target S3 bucket':src_s3_bucket,
                            'Target format':testcase_details['targetformat'], 
                            'Target filename':testcase_details['targetname'], 
                            'Target filepath':testcase_details['targetpath'], 
                            'Target database schemaname':testcase_details['targetdatabaseschemaname'],
                            'Target tablename':testcase_details['targettablename'], 
                            'Target table DB Connection URL':tgt_dbconn_url}
      
        dict_results = dict_compareoutput['dict_results']
        
        #Delete keys with empty values for printing PDF
        empty_keys = [k for k,v in dict_runsummary.items() if not v]
        for k in empty_keys:
          del dict_runsummary[k]
        empty_keys = [k for k,v in dict_configdetails.items() if not v]
        for k in empty_keys:
          del dict_configdetails[k]
          
        pdfobj_testcase_combined = generate_testcase_summary_report(pdfobj_testcase_combined, dict_runsummary, dict_configdetails, dict_compareoutput, str(protocol_starttime), results_path) 
        
        if dict_results['Test Result'] == 'Passed':
          reason = 'Schema matched'
        elif dict_results['Test Result'] == 'Failed':
          reason = 'Schema mismatched'
          
        df_protocol_summary.loc[index] = [test_case_name,str(dict_results['No. of columns in Source']),
                                          str(dict_results['No. of columns in Target']),
                                          str(dict_results['No. of columns in Source but not in Target']),
                                          str(dict_results['No. of columns in Target but not in Source']),
                                          str(dict_results['Total no. of matched columns']),
                                          str(dict_results['No. of columns with datatype mismatch']),
                                          str(dict_results['Test Result']), reason, str(testcase_exectime)]
      
        
      elif flag_allowed == 'N':
        log_error("Schemacompare for the testcase is not supported")
        reason = 'Comparison not supported'
        df_protocol_summary.loc[index] = [test_case_name,'','','','','','','Failed', reason, '']
      
  
    except Exception as e1:
      log_error(f'Testcase failed with error message - {str(e1)}')
      test_case_name = row['test_case_name']
      reason = 'Execution Error'
      df_protocol_summary.loc[index] = [test_case_name,'','','','','','','Failed',reason,'']
      continue
  
  df_protocolsummary = spark.createDataFrame(df_protocol_summary)
                                      
  totaltestcases= df_protocolsummary.count()
  testcasespassed = df_protocolsummary.filter(col("Test Result") == "Passed").count()
  testcasesfailed = totaltestcases - testcasespassed
                                      
  protocol_endtime = datetime.now()
  protocol_exectime = protocol_endtime - protocol_starttime
  protocol_exectime= str(protocol_exectime).split('.')[0]
  protocol_starttime = str(datetime.astimezone(protocol_starttime).strftime("%d-%b-%Y_%H:%M:%S_%Z"))
  protocol_endtime = str(datetime.astimezone(protocol_endtime).strftime("%d-%b-%Y_%H:%M:%S_%Z"))
  
  dict_protocol_rundetails = {'Protocol Name':dict_protocol['protocol_name'],
                              'Application Name':dict_protocol['protocol_application_name'],
                              'Run Environment':workspacename,
                              'Protocol Start Time':protocol_starttime, 
                              'Protocol End Time':protocol_endtime,
                              'Protocol Run Time':protocol_exectime,
                              'Total No of Test Cases': totaltestcases,
                              'No of Test Cases Passed':testcasespassed,
                              'No of Test Cases Failed':testcasesfailed}
  
  pdfobj_testcase_combined.pdf.output(testcase_output_path+'/'+'testcase_summary_report_combined_'+str(protocol_starttime)+'.pdf','F')
  
  log_info("Completed Protocol Execution")
  
                                      
  return dict_protocol_rundetails, df_protocolsummary

# COMMAND ----------

if __name__ == "__main__":
  
  try: 
    protocol_file_path =  '/dbfs' + get_mount_path(protocol_file_path_s3)
    protocol_start_time = datetime.now()
    dict_protocol, df_testcases = read_protocol_file(protocol_file_path)
    connectionname = dict_protocol['protocol_connection']
    s3bucket = get_connection_config(connectionname)['BUCKETNAME']
    dict_input_params = {'Protocol filepath':protocol_file_path, 'testcases to run':list_testcases_torun}
    created_time = str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z"))
    folder_s3 = str(dict_protocol['protocol_results_path']) + str(dict_protocol['protocol_name']) + '/run_' + str(dict_protocol['protocol_name']) + "_" + created_time
    testcase_folder_s3 = get_mount_path(s3bucket + '/' + folder_s3 + '/TestcaseSummary') 
    dbutils.fs.mkdirs(testcase_folder_s3)
    protocol_output_path = "/dbfs" + get_mount_path(s3bucket + '/' + folder_s3)
    testcase_output_path = "/dbfs" + testcase_folder_s3
    dict_protocol_rundetails, df_protocolsummary = execute_protocol(dict_protocol, df_testcases, testcase_output_path)
    generate_protocol_summary_report(dict_input_params, dict_protocol_rundetails, df_protocolsummary, protocol_output_path)
    
  except Exception as e2:
    log_error(f'Protocol failed with error message - {str(e2)}')

# COMMAND ----------

display(df_protocolsummary)