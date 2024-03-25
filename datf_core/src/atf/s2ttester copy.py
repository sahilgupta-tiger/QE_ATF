from pyspark.sql.functions import *
import datacompy
import pandas as pd
from fpdf import FPDF
from datetime import datetime


dbutils.widgets.text("protocol_config_file_path","s3://abt-bdaa-cleansed-us-east-1-adot-d/suresh/atf/SchemaCompare/Protocol/Protocolconfig_AlinityS_V2.xlsx")
protocol_file_path = dbutils.widgets.get("protocol_config_file_path")

dbutils.widgets.dropdown("test_case_type", "count", ["count","duplicate check","content"])
testcasetype = dbutils.widgets.get("test_case_type")

dbutils.widgets.text("run_test_cases","")
str_run_testcases = dbutils.widgets.get("run_test_cases").split(',')
lst_run_testcases = [i.strip().lower() for i in str_run_testcases]


def execute_testcase(testcase_details, auto_script_path):
  comparetype = testcase_details['comparesubtype']
  sourcequerymode = testcase_details['sourcequerymode']
  targetquerymode = testcase_details['targetquerymode']
  s2tmappingsheet = testcase_details['s2tmappingsheet']
  sourceformat = testcase_details['sourceformat']
  targetformat = testcase_details['targetformat']
  
  log_info("Inside execute_testcase Function")

  source_file_details_dict = None
  if(testcase_details['samplelimit'] is None):
    limit = 10
  else:
    limit = testcase_details['samplelimit']

  join_cols = testcase_details['primarykey'].split(',')
  join_cols_source = []
  join_cols_target = []
  map_cols = []
    
  if (sourcequerymode == 'Manual' or comparetype =='likeobjectcompare'):
    
      
    dict_source = {'connectionname':testcase_details['sourceconnectionname'], 'connectiontype':testcase_details['sourceconnectiontype'],
                      'path':testcase_details['sourcepath'], 'format':testcase_details['sourceformat'], 'name':testcase_details['sourcename'], 'excludecolumns':testcase_details['sourceexcludecolumnlist'],'filter':testcase_details['sourcefilter'], 'comparetype':testcase_details['comparesubtype'],'delimiter':testcase_details['sourcedelimiter'],'querypath':testcase_details['sourcequerypath'] }
    log_info("Calling read_data from Source")
    source_df, source_query = read_data(dict_source,sourcequerymode)
      
  elif comparetype =='s2tcompare': 
    s2tconnectionval = get_connection_config(testcase_details['s2tconnectionname'])['BUCKETNAME']
    
    s2tfilepath = s2tconnectionval + testcase_details['s2tpath']
    log_info(f"Reading the S2T located at {s2tfilepath}")
    
    s2tfilepath = '/dbfs' + get_mount_path(s2tconnectionval + testcase_details['s2tpath'])
    
    s2t  =LoadS2T(s2tfilepath)
    log_info("Source S2T object created")
    
    dict_source = {'connectionname':testcase_details['sourceconnectionname'], 'connectiontype':testcase_details['sourceconnectiontype'],
                      'path':testcase_details['sourcepath'], 'format':testcase_details['sourceformat'], 'name':testcase_details['sourcename'], 'excludecolumns':testcase_details['sourceexcludecolumnlist'],'filter':testcase_details['sourcefilter'], 'comparetype':testcase_details['comparesubtype'],'delimiter':testcase_details['sourcedelimiter'], 'testcasename': testcase_details['testcasename'], 'autoscripttype': 'source','autoscriptpath':auto_script_path }   
    
    autoldscrobj =S2TAutoLoadScripts(s2t,dict_source)

    scriptpath,source_df,source_file_details_dict =autoldscrobj.getSelectTableCmd(s2tmappingsheet)
  

    source_conn_name = source_file_details_dict["connectionname"]
    join_cols = source_file_details_dict["join_columns"]
  
    source_query = open(scriptpath).read().split('\n')

      
  if targetquerymode == 'Manual' or comparetype =='likeobjectcompare':
      
    dict_target = {'connectionname':testcase_details['targetconnectionname'], 'connectiontype':testcase_details['targetconnectiontype'],
                      'path':testcase_details['targetpath'], 'format':testcase_details['targetformat'], 'name':testcase_details['targetname'], 'excludecolumns':testcase_details['targetexcludecolumnlist'],'filter':testcase_details['targetfilter'], 'comparetype':testcase_details['comparesubtype'],'delimiter':testcase_details['targetdelimiter'],'querypath':testcase_details['targetquerypath']}
    log_info("Calling read_data from Target")
    target_df, target_query = read_data(dict_target,targetquerymode)
  
      
  elif comparetype =='s2tcompare': 
    s2tconnectionval = get_connection_config(testcase_details['s2tconnectionname'])['BUCKETNAME']
    s2tfilepath = s2tconnectionval + testcase_details['s2tpath']
    
    s2tfilepath = '/dbfs' + get_mount_path(s2tconnectionval + testcase_details['s2tpath'])
    
    s2t  =LoadS2T(s2tfilepath)
    log_info("Target S2T object created")
    dict_target = {'connectionname':testcase_details['targetconnectionname'], 'connectiontype':testcase_details['targetconnectiontype'],
                      'path':testcase_details['targetpath'], 'format':testcase_details['targetformat'], 'name':testcase_details['targetname'], 'excludecolumns':testcase_details['targetexcludecolumnlist'],'filter':testcase_details['targetfilter'], 'comparetype':testcase_details['comparesubtype'],'delimiter':testcase_details['targetdelimiter'], 'testcasename': testcase_details['testcasename'], 'autoscripttype': 'target', 'autoscriptpath':auto_script_path }   
    
  
    autoldscrobj =S2TAutoLoadScripts(s2t,dict_target)
    scriptpath,target_df,target_file_details_dict = autoldscrobj.getSelectTableCmd(s2tmappingsheet)

    target_conn_name = target_file_details_dict["connectionname"]
    join_cols = target_file_details_dict["join_columns"]
    

    target_query = open(scriptpath).read().split('\n')
 
    
  if(source_file_details_dict is not None):    
    file_details_dict = {"sourcefile":source_file_details_dict["file_path"],"targetfile":target_file_details_dict["file_path"], "sourceconnectionname":source_conn_name, "targetconnectionname":target_conn_name, "sourceconnectiontype":source_file_details_dict['connectiontype'],"targetconnectiontype":target_file_details_dict['connectiontype']}
  else:
    file_details_dict = {"sourcefile":None,"targetfile":None,"sourceconnectionname":"","targetconnectionname":""}
  compare_input = {'sourcedf':source_df, 'targetdf':target_df,'sourcequery':source_query, 'targetquery':target_query, 'colmapping':map_cols, 'joincolumns':join_cols, 'testcasetype':testcasetype, 'limit' : limit, "filedetails":file_details_dict}

  log_info("Exiting execute_testcase Function")

  return compare_input 

# COMMAND ----------

# DBTITLE 1,Function to concatenate all primary keys
def concat_keys(df,key_cols_list):
  keycols_name_temp = [i+'_temp' for i in key_cols_list] 
  keycols_val_temp = [j+'=' if i==0 else ', '+j+'= ' for i,j in enumerate(key_cols_list)]
  for i in range(len(keycols_name_temp)):  
    df = df.withColumn(keycols_name_temp[i],lit(keycols_val_temp[i]))  
      
  concat_key_cols =[]  
  for i,j in zip(keycols_name_temp,key_cols_list):  
    concat_key_cols.append(i)  
    concat_key_cols.append(j) 
  return df, concat_key_cols

# COMMAND ----------

# DBTITLE 1,Data Comparison between source and target
def compare_data(compare_input, testcasetype): 
  log_info("Inside compare_data Function")
  sourcedf = compare_input['sourcedf']
  targetdf = compare_input['targetdf']
  joincolumns = compare_input['joincolumns']  
  colmapping = compare_input['colmapping']

  
  limit = compare_input['limit']

  rowcount_source = sourcedf.count()
  rowcount_target = targetdf.count()
  
  if (testcasetype == 'content'):
    
    comparison_obj  = datacompy.SparkCompare(spark, sourcedf, targetdf,  column_mapping = colmapping, join_columns = joincolumns)
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


    rowcount_total_mismatch = rowcount_only_target + rowcount_only_source #+ rowcount_mismatch
    
    if(rowcount_mismatch > 0):
        log_info("Test Case Failed as Content Mismatched")
        result_desc = "Content mismatched"
        test_result = "Failed"
    else:
        log_info("Test Case Passed as Content Matched")
        result_desc = "Content matched"
        test_result = "Passed"
    
    dict_results={'Test Result': test_result,
                  'No. of rows in Source':f"{rowcount_source:,}", 'No. of distinct rows in Source':f"{distinct_rowcount_source:,}", 
                  'No. of duplicate rows in Source':f"{duplicate_rowcount_source:,}",
                  'No. of rows in Target':f"{rowcount_target:,}", 'No. of distinct rows in Target':f"{distinct_rowcount_target:,}", 
                  'No. of duplicate rows in Target':f"{duplicate_rowcount_target:,}",
                  'No. of matched rows': f"{rowcount_match:,}", 
                  'No. of mismatched rows':f"{rowcount_mismatch:,}", 
                  'No. of rows in Source but not in Target':f"{rowcount_only_source:,}",
                  'No. of rows in Target but not in Source':f"{rowcount_only_target:,}"
                 }

    dict_no_of_rows = {'No. of rows in Source':rowcount_source, 'No. of rows in Target':rowcount_target}

    dict_match_summary={}
    dict_match_details={} 
    if rows_both_all.rdd.isEmpty():
      rows_both_all = None
    if rows_mismatch.rdd.isEmpty():
      rows_mismatch = None
      sample_mismatch = None
    else:
      sample_mismatch = rows_mismatch.select(joincolumns).limit(limit)
      sample_mismatch, concat_list = concat_keys(sample_mismatch,joincolumns)
      sample_mismatch  = sample_mismatch.select(concat(*concat_list).alias("Key Columns"))
    if rows_only_source.rdd.isEmpty():
      rows_only_source = None
      sample_source_only = None
    else:
      sample_source_only = rows_only_source.select(joincolumns).limit(limit)
      sample_source_only, concat_list = concat_keys(sample_source_only,joincolumns)
      sample_source_only  = sample_source_only.select(concat(*concat_list).alias("Key Columns"))
    if rows_only_target.rdd.isEmpty():
      rows_only_target = None
      sample_target_only = None
    else:
      sample_target_only = rows_only_target.select(joincolumns).limit(limit)
      sample_target_only, concat_list = concat_keys(sample_target_only,joincolumns)
      sample_target_only  = sample_target_only.select(concat(*concat_list).alias("Key Columns"))

    if rows_both_all == None:
      df_match_summary= None

    else:
      df = rows_both_all
      col_list = df.columns
      if len(colmapping) == 0:
        column_mapping= {i:i for i in sourcedf.columns if i not in joincolumns}
        collist = [i for i in sourcedf.columns if i not in joincolumns]
      else:
        column_mapping = dict(colmapping) 
        collist = [i for i in sourcedf.columns if i not in joincolumns]

      for column in collist:

        base_col= column + "_base"
        compare_col =column + "_compare"
        match_col =column +"_match"
        sel_col_list =[]
        sel_col_list = joincolumns.copy()         
        sel_col_list.append(base_col)
        sel_col_list.append(compare_col)
        key_cols = joincolumns.copy()



        filter_false = match_col+ " == False"
        filter_true = match_col+ " == True"

        mismatch=df.select(match_col).filter(filter_false).count()
        if(mismatch == 0):
          continue


        match=df.select(match_col).filter(filter_true).count()
        dict_match_summary[column]= [match,mismatch]      

        df_details= df.select(sel_col_list).filter(filter_false).withColumnRenamed(base_col, "Source value").withColumnRenamed(compare_col, "Target value").distinct().limit(limit)

        df_details, concat_list = concat_keys(df_details,key_cols)
        df_details = df_details.select(concat(*concat_list).alias("Key Columns"),"Source value","Target value") 

        dict_match_details[column] = df_details


      list_match_summary =[]
      for k,v in dict_match_summary.items():
        list_match_summary.append([k,column_mapping[k],rowcount_source,rowcount_target,rowcount_match, v[0],v[1]])

      df_match_summary = pd.DataFrame(list_match_summary, columns = ["Source Column Name","Target Column Name","Rows in Source","Rows in Target","Rows with Common Keys","Rows Matched","Rows Mismatch"])

      if(len(df_match_summary) == 0):
        df_match_summary = None
      else:
        df_match_summary = spark.createDataFrame(df_match_summary) 
      

  elif(testcasetype == "duplicate check"):
      distinct_rowcount_source = sourcedf.select(joincolumns).distinct().count()
      distinct_rowcount_target = targetdf.select(joincolumns).distinct().count()
      duplicate_rowcount_source = rowcount_source - distinct_rowcount_source
      duplicate_rowcount_target = rowcount_target - distinct_rowcount_target
      if(distinct_rowcount_source == rowcount_target and rowcount_target == distinct_rowcount_target):
        test_result = "Passed"
        result_desc = "No Duplicates"
        log_info("Test Case Passed - no duplicates")
      else:
        test_result = "Failed"
        result_desc = "Duplicates"
        log_info("Test Case Failed - duplicates found")
      dict_results={
                  'Test Result': test_result, 'No. of rows in Source':f"{rowcount_source:,}", 'No. of distinct rows in Source':f"{distinct_rowcount_source:,}", 'No. of duplicate rows in Source':f"{duplicate_rowcount_source:,}", 'No. of rows in Target':f"{rowcount_target:,}", 'No. of distinct rows in Target':f"{distinct_rowcount_target:,}", 'No. of duplicate rows in Target':f"{duplicate_rowcount_target:,}"
      } 
      rows_both_all =rows_mismatch=rows_only_source=rows_only_target=sample_mismatch=sample_source_only=sample_target_only=df_match_summary=dict_no_of_rows=dict_match_details=None
      
  elif(testcasetype == "count"):
    if(rowcount_source == rowcount_target):
      test_result = "Passed"
      result_desc = "Count matched"
      log_info("Test Case Passed - Count matched")
    else:
      test_result = "Failed"
      result_desc = "Count mismatched"
      log_info("Test Case Failed - Count mismatched")
    dict_results={
                  'Test Result': test_result,'No. of rows in Source':f"{rowcount_source:,}", 'No. of rows in Target':f"{rowcount_target:,}"
    }      
    rows_both_all =rows_mismatch=rows_only_source=rows_only_target=sample_mismatch=sample_source_only=sample_target_only=df_match_summary=dict_no_of_rows=dict_match_details=None
    
  dict_compareoutput =   {'rows_both_all':rows_both_all, 'rows_mismatch':rows_mismatch, 'rows_only_source':rows_only_source, 'rows_only_target':rows_only_target, 'test_result':test_result,'sample_mismatch':sample_mismatch, 'sample_source':sample_source_only, 'sample_target':sample_target_only, 'dict_results':dict_results, 'col_match_summary': df_match_summary, 'row_count': dict_no_of_rows,'col_match_details': dict_match_details, 'result_desc': result_desc}
  log_info("Exiting compare_data Function")
  return dict_compareoutput   

# COMMAND ----------

# DBTITLE 1,Generate Testcase Summary Report
def generate_testcase_summary_report(dict_runsummary, dict_config, results_path,compare_input,dict_compareoutput, testcasetype, comparison_subtype, pdfobj):
  log_info("Inside generate_testcase_summary_report Function")
  if(compare_input['filedetails']["sourcefile"] is not None):
    file_details = compare_input['filedetails']
    source_file_path = file_details["sourcefile"]
    dict_config['Source Format'], dict_config['Source Path'] = source_file_path.split(".",1)
    dict_config['Source Path'] = get_mount_src_path(dict_config['Source Path'].replace("`",""))
    target_file_path = file_details["targetfile"]
    dict_config['Target Format'], dict_config['Target Path'] = target_file_path.split(".",1)
    dict_config['Target Path'] = get_mount_src_path(dict_config['Target Path'].replace("`",""))
  else:
    source_file_path = compare_input['sourcequery'].split("FROM")[-1].split(" ")[1]
    target_file_path = compare_input['targetquery'].split("FROM")[-1].split(" ")[1]
  if(testcasetype == 'count'):
    srcquery = compare_input['sourcequery']
    srcquery = "SELECT Count(1) FROM "+source_file_path
    tgtquery = compare_input['targetquery']
    tgtquery = "SELECT Count(1) FROM "+target_file_path
    query_details = {'Source Query': srcquery, 'Target Query':tgtquery}
  elif(testcasetype == 'duplicate check'):
    srcquery = compare_input['sourcequery']
    join_cols = dict_config['Primary Keys']
    srcquery = "SELECT Count(DISTINCT " + join_cols + ") FROM "+source_file_path
    tgtquery = compare_input['targetquery']
    tgtquery = "SELECT Count(DISTINCT " + join_cols + ") FROM "+target_file_path
    query_details = {'Source Query': srcquery, 'Target Query':tgtquery}
  else:
    query_details = {'Source Query': compare_input['sourcequery'], 'Target Query':compare_input['targetquery']}

  sample_source_only = dict_compareoutput['sample_source']
  sample_target_only = dict_compareoutput['sample_target']
  sample_mismatch = dict_compareoutput['sample_mismatch']
  col_match_summary = dict_compareoutput['col_match_summary']
  col_match_details = dict_compareoutput['col_match_details']
  row_count = dict_compareoutput['row_count']
  if(comparison_subtype == 's2tcompare'):
    src_conn = get_connection_config(compare_input['filedetails']['sourceconnectionname'])
    tgt_conn = get_connection_config(compare_input['filedetails']['targetconnectionname'])
    dict_config['Source Connection Type'] = compare_input['filedetails']['sourceconnectiontype']
    dict_config['Target Connection Type'] = compare_input['filedetails']['targetconnectiontype'] 
    dict_config['Source Connection Name'] = compare_input['filedetails']['sourceconnectionname']
    dict_config['Target Connection Name'] = compare_input['filedetails']['sourceconnectionname'] 
    
    if(src_conn['CONNTYPE'].lower() == 'aws-s3' ):
      dict_config['Source Connection Value'] = src_conn['BUCKETNAME']      
    else:
      dict_config['Source Connection Value'] = src_conn['CONNURL']
    if(tgt_conn['CONNTYPE'].lower() == 'aws-s3'  ):
      dict_config['Target Connection Value'] = tgt_conn['BUCKETNAME']      
    else:
      dict_config['Target Connection Value'] = tgt_conn['CONNURL']
    dict_config['Source Path'] = dict_config['Source Path'].replace(dict_config['Source Connection Value'],"")
    dict_config['Target Path'] = dict_config['Target Path'].replace(dict_config['Target Connection Value'],"")
        
  else:
    if(dict_config['Source Connection Type'].lower() == 'aws-s3'):
      dict_config['Source Connection Value'] = get_connection_config(dict_config['Source Connection Name'])['BUCKETNAME']     
    else:
      dict_config['Source Connection Value'] = get_connection_config(dict_config['Source Connection Name'])['CONNURL']
    if(dict_config['Target Connection Type'].lower() == 'aws-s3'):
      dict_config['Target Connection Value'] = get_connection_config(dict_config['Target Connection Name'])['BUCKETNAME']     
    else:
      dict_config['Target Connection Value'] = get_connection_config(dict_config['Target Connection Name'])['CONNURL']
      
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
  pdfobj.create_table_summary(dict_testresults) #,'L')
  pdfobj.write_text('4. SQL Queries', 'section heading')
  pdfobj.write_text('4.1 Source Query', 'section heading')
  if(testcasetype == "content" and comparison_subtype == 's2tcompare'):
    for i in query_details['Source Query']:
      pdfobj.display_sql_query(i)
  else:
    pdfobj.display_sql_query(query_details['Source Query'])
  pdfobj.write_text('4.2 Target Query', 'section heading')
  if(testcasetype == "content" and comparison_subtype == 's2tcompare'):
    for i in query_details['Target Query']:
      pdfobj.display_sql_query(i)
  else:
    pdfobj.display_sql_query(query_details['Target Query'])
  if testcasetype == 'count' or testcasetype == "duplicate check":
    pass
  
  elif (testcasetype == 'content' or testcasetype == 'count and content'): # or testcasetype == "duplicate check"):
    mismatch_heading = "5. Sample Mismatches " + str(compare_input['limit']) + " rows"
    pdfobj.write_text(mismatch_heading, 'section heading')
    pdfobj.write_text('5.1 Keys in source but not in target', 'section heading')
    pdfobj.create_table_details(sample_source_only,'mismatch')
    pdfobj.write_text('5.2 Keys in target but not in source', 'section heading')
    pdfobj.create_table_details(sample_target_only,'mismatch')
    pdfobj.write_text('5.3 Keys having one or more unequal column values', 'section heading')
    pdfobj.create_table_details(sample_mismatch,'mismatch')
    pdfobj.write_text('6. Columnwise Mismatch Summary', 'section heading')
    if col_match_summary != None:
      pdfobj.create_table_summary(row_count)
    pdfobj.create_table_details(col_match_summary,'mismatch_summary') 
    pdfobj.write_text('7. Columnwise Mismatch Details', 'section heading')
    sno = 1
    if len(col_match_details) != 0:
      for key, value in col_match_details.items():
        header = "7." + str(sno) + " " + str(key)
        pdfobj.write_text(header, 'section heading')
        pdfobj.create_table_details(value,'mismatch_details')
        sno = sno + 1
    else:
      df_7 = None
      pdfobj.create_table_details(df_7)
  
  log_info("Exiting generate_testcase_summary_report Function")
  return pdfobj

# COMMAND ----------

# DBTITLE 1,Execute Protocol
def execute_protocol(dict_protocol, df_testcases, output_path, combined_testcase_output_path, testcasetype):
  
  log_info("Inside execute_protocol Function")
  protocol_starttime = datetime.now()
  auto_script_path = generate_autoscript_path(combined_testcase_output_path)
  if testcasetype == "count":
    df_protocol_summary = pd.DataFrame(columns = ['Testcase Name','No. of Rows in Source', 'No. of Rows in Target','Test Result','Reason','Runtime'])
    
  elif testcasetype == "duplicate check":
    df_protocol_summary = pd.DataFrame(columns = ['Testcase Name','No. of Rows in Source', 'No. of Distinct Rows in Source', 'No. of Rows in Target', 'No. of Distinct Rows in Target','Test Result','Reason','Runtime'])
    
  elif (testcasetype == "content" or testcasetype == "count and content"):
    df_protocol_summary = pd.DataFrame(columns = ['Testcase Name','No. of Rows in Source', 'No. of Rows in Target','No. of Rows matched','No. of Rows mismatched','Test Result','Reason','Runtime'])
  
  pdfobj_combined_testcase = generatePDF() 
  testcases_run_list = []
  #df_testcases.shape()
  #df_testcases
  for index, row in df_testcases.iterrows():
    testcase_details = {} 
    test_case_name = row['test_case_name']
    tcnbr = str(int(row['Sno.']))
    s3_conn_name = dict_protocol['protocol_connection']
    s3_path = get_connection_config(s3_conn_name)
    s3_path = s3_path['BUCKETNAME']
    test_case_mnt_src_path = s3_path + row['test_case_file_path']
    test_case_file_path = '/dbfs' + get_mount_path(s3_path) + row['test_case_file_path']
    
    try:
      if (lst_run_testcases[0]=='' or (test_case_name.lower() in lst_run_testcases)):
        execute_flag = 'Y'
        testcases_run_list.append(test_case_name)
        pass
      else:    
        log_info(f"The Test Case No.{tcnbr} is not in run_test_cases param, skipping its execution")
        execute_flag = 'N'
        continue
      if(len(testcases_run_list) > 1):
        pdfobj_combined_testcase.pdf.add_page()
      
      log_info("Reading Test Case Config")
      log_info(f"Test Case Name is {test_case_name}")
      log_info(f"Path is {test_case_mnt_src_path}")
      testcase_details = read_test_case(test_case_file_path)
      #rel_source_path = testcase_details['sourcepath'] 
      testcase_details['sourcepath'] = s3_path + testcase_details['sourcepath']
      #rel_target_path = testcase_details['targetpath']
      testcase_details['targetpath'] = s3_path + testcase_details['targetpath']
      testcase_starttime = datetime.now()
      log_info("Preparing to Execute Test Case")
      #log_info('[{}]: Test Case Config Path {}'.format(str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z")).lstrip().rstrip(), test_case_mnt_src_path))
      compare_input = execute_testcase(testcase_details, auto_script_path)
      log_info("Calling compare_data Function")
      dict_compareoutput = compare_data(compare_input, testcasetype)
      testcase_endtime = datetime.now()
      testcase_exectime = testcase_endtime - testcase_starttime
      testcase_exectime= str(testcase_exectime).split('.')[0]
      log_info(f"Execution of Test Case {test_case_name} completed in {testcase_exectime}")
      testcase_starttime = datetime.astimezone(testcase_starttime).strftime("%d-%b-%Y %H:%M:%S %Z")
      testcase_endtime = datetime.astimezone(testcase_endtime).strftime("%d-%b-%Y %H:%M:%S %Z")
      #testcase_exectime = testcase_exectime.strftime("%H:%M:%S")

      
      dict_runsummary = {'Application Name':dict_protocol['protocol_application_name'],'Protocol Name':dict_protocol['protocol_name'],"Protocol File Path":protocol_file_path, 'Testcase Name':testcase_details['testcasename'], "Testcase Type":testcasetype,  'Test Environment':dict_protocol['protocol_run_environment'], 'Start Time':testcase_starttime,'End Time':testcase_endtime,'Run Time':testcase_exectime,'Test Result':dict_compareoutput['test_result'], 'Reason':dict_compareoutput['result_desc']}
      # ADD if condition for testcasetype
      join_cols = ",".join(compare_input['joincolumns'])
      dict_config = {'Compare Type':testcase_details['comparetype'], 'Testcase Type':testcasetype, 'Source Connection Name':testcase_details['sourceconnectionname'], 'Source Connection Type':testcase_details['sourceconnectiontype'], 'Source Connection Value':testcase_details['sourceconnectionname'], 'Source Format':testcase_details['sourceformat'], 'Source Schema':'', 'Source Name':testcase_details['sourcename'], 'Source Path':testcase_details['sourcepath'], 'Source Exclude Columns':testcase_details['sourceexcludecolumnlist'], 'Source Filter': testcase_details['sourcefilter'], 'Target Connection Name':testcase_details['targetconnectionname'],'Target Connection Type':testcase_details['targetconnectiontype'], 'Target Connection Value':testcase_details['targetconnectionname'], 'Target Format':testcase_details['targetformat'], 'Target Schema':'', 'Target Name':testcase_details['targetname'], 'Target Path':testcase_details['targetpath'], 'Target Exclude Columns':testcase_details['targetexcludecolumnlist'], 'Target Filter':testcase_details['targetfilter'], 'S2T Path':testcase_details['s2tpath'], 'Primary Keys': join_cols}
      dict_config_temp = dict_config.copy()
      
      testcasetype= compare_input['testcasetype']
      col_match_summary = dict_compareoutput['col_match_summary']
      comparison_subtype = testcase_details['comparesubtype']
      dict_testresults = dict_compareoutput['dict_results']
      pdfobj = generatePDF()
      pdfobj.write_text(testcasereportheader, 'report header')
      pdfobj.write_text(dict_runsummary['Testcase Name'], 'subheading')
      pdfobj_combined_testcase.write_text(dict_runsummary['Testcase Name'], 'report header')
      results_path = output_path + '/' + test_case_name + '_' + dict_compareoutput['test_result'].lower() + '.pdf' 
      pdfobj = generate_testcase_summary_report(dict_runsummary, dict_config,results_path,compare_input, dict_compareoutput, testcasetype, comparison_subtype, pdfobj)
      pdfobj.pdf.output(results_path,'F')
      log_info(f"PDF Generated for Test Case {dict_runsummary['Testcase Name']}")
      dict_config = dict_config_temp
      
      pdfobj_combined_testcase = generate_testcase_summary_report(dict_runsummary, dict_config,results_path,compare_input, dict_compareoutput, testcasetype, comparison_subtype, pdfobj_combined_testcase)
      if testcasetype == "count":
        df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of rows in Source']), str(dict_testresults['No. of rows in Target']),dict_compareoutput['test_result'], dict_compareoutput['result_desc'],str(testcase_exectime)]
        
      elif testcasetype == "duplicate check":

        df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of rows in Source']),str(dict_testresults['No. of distinct rows in Source']), str(dict_testresults['No. of rows in Target']), str(dict_testresults['No. of distinct rows in Target']), dict_compareoutput['test_result'],dict_compareoutput['result_desc'],str(testcase_exectime)]
        
      elif (testcasetype == "content" or testcasetype == "count and content"):
        df_protocol_summary.loc[index] = [testcase_details['testcasename'], str(dict_testresults['No. of rows in Source']), str(dict_testresults['No. of rows in Target']),str(dict_testresults['No. of matched rows']),str(dict_testresults['No. of mismatched rows']),dict_compareoutput['test_result'], dict_compareoutput['result_desc'],str(testcase_exectime)]
      log_info(f"Testcase {test_case_name} executed with no errors")
        

    except Exception as e1:
      log_error(f"Testcase {test_case_name} failed with error message: {str(e1)}")
      if testcasetype == "count":
        df_protocol_summary.loc[index] = [test_case_name,'','','Failed','Execution Error','']
      if (testcasetype == "content" or testcasetype == "count and content"):
        df_protocol_summary.loc[index] = [test_case_name, '','','','','Failed','Execution Error','']
    
  pdfobj_combined_testcase.pdf.output(combined_testcase_output_path,'F') # generate combined pdf
  log_info("Combined Test Case PDF Generated")
  protocol_endtime = datetime.now()
  protocol_exectime = protocol_endtime - protocol_starttime
  protocol_exectime = str(protocol_exectime).split('.')[0]
  log_info(f"Protocol {dict_protocol['protocol_name']} executed in {protocol_exectime}")
  protocol_starttime = datetime.astimezone(protocol_starttime).strftime("%d-%b-%Y %H:%M:%S %Z")
  protocol_endtime = datetime.astimezone(protocol_endtime).strftime("%d-%b-%Y %H:%M:%S %Z")
  
  totaltestcases = 0
  testcasespassed = 0
  testcasesfailed = 0
  if(len(df_protocol_summary) != 0):
    totaltestcases= len(df_protocol_summary) 
    testcasespassed = len(df_protocol_summary[df_protocol_summary["Test Result"] == "Passed"])
    testcasesfailed = len(df_protocol_summary[df_protocol_summary["Test Result"] == "Failed"])
    df_protocol_summary = df_protocol_summary.sort_values('Reason')
    df_protocol_summary = spark.createDataFrame(df_protocol_summary)
  else:
    log_info("No testcase executed.")
    df_protocol_summary = None
  
  protocol_run_details =  {'Application Name':dict_protocol['protocol_application_name'],'Test Protocol Name':dict_protocol['protocol_name'], 'Test Protocol Version':dict_protocol['protocol_version'],'Test Environment':dict_protocol['protocol_run_environment'], 'Test Protocol Start Time':protocol_starttime,'Test Protocol End Time':protocol_endtime,'Total Protocol Run Time':protocol_exectime,'Total No of Test Cases': totaltestcases, 'No of Test Cases Passed':testcasespassed, 'No of Test Cases Failed':testcasesfailed } 
  
  if (lst_run_testcases[0]==''):
    testcases_run_list = "All testcases from protocol executed"
  else:
    testcases_run_list = ",".join(testcases_run_list)
  protocol_run_params = {"Protocol File Path":protocol_file_path,"Testcases Executed":testcases_run_list, "Testcase Type":testcasetype}  
  
  log_info("Exiting execute_protocol Function")
  return df_protocol_summary,protocol_run_details,protocol_run_params

# COMMAND ----------

# DBTITLE 1,Generate Protocol Summary Report
def generate_protocol_summary_report(df_protocol_summary, protocol_run_details, protocol_run_params, output_path, created_time):
  
  pdfobj_protocol = generatePDF()
  comparison_type = testcasetype + " comparison"
  pdfobj_protocol.write_text(protocolreportheader, 'report header')
  pdfobj_protocol.write_text(protocol_run_details['Test Protocol Name'], 'subheading')
  pdfobj_protocol.write_text(comparison_type , 'subheading')
  pdfobj_protocol.write_text(protocolrunparams, 'section heading')
  pdfobj_protocol.create_table_summary(protocol_run_params)
  pdfobj_protocol.write_text(protocoltestcaseheader, 'section heading')
  pdfobj_protocol.create_table_summary(protocol_run_details)
  pdfobj_protocol.write_text(testresultheader, 'section heading')
  table_type = 'protocol'
  if(testcasetype == "count"):
    table_type = 'protocol_count'
  sno = 1
  test_results_list = ['Failed','Passed']
  for test_result in test_results_list:
    testheader = "3." + str(sno) + ". " + test_result + " Testcases"
    pdfobj_protocol.write_text(testheader, 'section heading')
    if(df_protocol_summary is not None):
      df_protocol_summary_temp = df_protocol_summary.select('*').filter(col('Test Result') == test_result)
      if(df_protocol_summary_temp.count() == 0):
        df_protocol_summary_temp = None
    else:
      df_protocol_summary_temp = None

    pdfobj_protocol.create_table_details(df_protocol_summary_temp, table_type)
    sno = sno + 1
 
  protocol_output_path = output_path + "/run_" + protocol_run_details['Test Protocol Name'] + "_" + created_time+".pdf"
  pdfobj_protocol.pdf.output(protocol_output_path, 'F')
  log_info("Protocol Summary PDF Generated")

# COMMAND ----------

# DBTITLE 1,Main Function
if __name__ == "__main__":
  log_info("Starting the Main Function")
  
  try:
    log_info(f"Reading the Protocol file from {protocol_file_path}")
    protocol_file_path =  '/dbfs' + get_mount_path(protocol_file_path)

    
    dict_protocol, df_testcases = read_protocol_file(protocol_file_path)
    dict_protocol['testcasetype'] = testcasetype
    s3_conn_name = dict_protocol['protocol_connection']
    s3_path = get_connection_config(s3_conn_name)
    s3_path = s3_path['BUCKETNAME']
    results_path = str(s3_path) + str(dict_protocol['protocol_results_path']) # + '/' + str(testcasetype) + '/'
    created_time = str("".join(testcasetype.split(" "))) + "_" + str(datetime.astimezone(datetime.now()).strftime("%d-%b-%Y_%H:%M:%S_%Z"))
    folder_s3 = results_path + str(dict_protocol['protocol_name']) + '/' + str(testcasetype) + '/run_' + str(dict_protocol['protocol_name']) + "_" + created_time
    dbutils.fs.mkdirs(folder_s3)
    testcase_folder_s3 = folder_s3 + '/run_testcase_summary_' + created_time
    dbutils.fs.mkdirs(testcase_folder_s3)
    protocol_output_path = "/dbfs" + get_mount_path(folder_s3)
    testcase_output_path = "/dbfs" + get_mount_path(testcase_folder_s3)
    combined_testcase_output_path = protocol_output_path + "/run_tc_combined_"+ str(dict_protocol['protocol_name']) + "_" + created_time +".pdf"
    
    df_protocol_summary, protocol_run_details, protocol_run_params = execute_protocol(dict_protocol, df_testcases, testcase_output_path, combined_testcase_output_path, testcasetype) 
   
    generate_protocol_summary_report(df_protocol_summary, protocol_run_details, protocol_run_params, protocol_output_path, created_time)
    
    log_info(f"Execution of Protocol: {dict_protocol['protocol_name']} Completed")
    
  except Exception as e2:
    log_error(f"Protocol failed with error message: {str(e2)}")