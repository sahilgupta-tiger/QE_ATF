import sqlite3
import subprocess
from os import listdir
from os.path import isfile, join
import pandas as pd
from datf_core.src.testconfig import *
import json
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI
import sweetviz as sv
from datetime import datetime


conn_exe = sqlite3.connect(f"{root_path}/utils/{exec_db_name}.db", check_same_thread=False)

openai_json = json.load(open(f"{root_path}/test/connections/{genai_conn_json}.json"))
os.environ["AZURE_OPENAI_API_KEY"] = decryptcredential(openai_json['apikey'])
os.environ["AZURE_OPENAI_ENDPOINT"] = openai_json['endpoint']
openai_api_version = openai_json['apiversion']
azure_deployment = openai_json['deployment']


def get_queries_from_ai(prompt):

    model = AzureChatOpenAI(
        openai_api_version=openai_api_version,
        azure_deployment=azure_deployment,
    )
    message = HumanMessage(
        content=prompt
    )
    output_value=model([message])
    return output_value.content

# Function to read all the test cases within the protocols
def read_test_protocol():

    onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
    for loop in onlyfiles:
        if loop.find("template") != -1:
            onlyfiles.remove(loop)
    return onlyfiles

# Function to read the test case name within the chosen protocol
def read_test_cases(protocol_name):
    excel_sheet_path = f"{tc_path}/{protocol_name}"
    protocol_df = pd.read_excel(excel_sheet_path, sheet_name=exec_sheet_name)
    list_test_cases = protocol_df["test_case_name"].tolist()
    return list_test_cases

# Function to create the tables for each protocol file in execution DB
def create_execution_db():

    list_of_files = read_test_protocol()
    for loop in list_of_files:
        excel_sheet_path = f"{tc_path}/{loop}"
        writedb_df = pd.read_excel(excel_sheet_path, sheet_name=exec_sheet_name)
        writedb_df['execute'].replace({'Y': True, 'N': False}, inplace=True)
        writedb_df.to_sql(con=conn_exe, name=loop, if_exists='replace', index=False)

    # Create DataFrame
    protocoldetails_df = pd.read_excel(f"{tc_path}/{list_of_files[0]}", sheet_name=protocol_tab_name)
    protocoldetails_df.to_sql(con=conn_exe, name=protocol_tab_name, if_exists='replace', index=False)
    conn_exe.commit()

# Function to fetch column names from connected DB
def get_column_names(connection, table_name):
    query = f"SELECT * FROM {table_name} WHERE 1=0"
    df = pd.read_sql(query, connection)
    return df.columns.tolist()

# Function to filter only selected test cases with execute Yes
def get_selected_testcases(selected_df):

    filtered_df = selected_df[selected_df['execute'] == True]
    tcnames_list = filtered_df['test_case_name'].to_list()

    if not tcnames_list:
        tc_names = 'all'
    else:
        tc_names = ','.join(tcnames_list)
    return tc_names

# Function to create the Excel sheet for execution
def write_protocol_to_excel(protocol_name):
    first_df = pd.read_sql_query(f"SELECT * FROM '{protocol_tab_name}'", conn_exe)
    updated_df = pd.read_sql_query(f"SELECT * FROM '{protocol_name}'", conn_exe)
    # Write DataFrames to separate sheets in one Excel file
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        first_df.to_excel(writer, sheet_name=protocol_tab_name, index=False)
        updated_df.to_excel(writer, sheet_name=exec_sheet_name, index=False)

    print(f"Excel file '{output_file_path}' with multiple sheets created successfully.")

# Functions to save uploaded file to desired path
def save_uploadedfile(uploadedfile, filepath):
    with open(os.path.join(filepath, uploadedfile.name), "wb") as f:
        f.write(uploadedfile.getbuffer())
    success_text = f"Saved File: {uploadedfile.name} to '{filepath}' in framework!"
    return success_text

# Function to buid the report for data profiling based on dataframe
def create_data_profile_report(input_df, type_str):
    # Analyze the Pandas DataFrame
    report = sv.analyze([input_df, type_str])
    # Get the current date time from UTC timezone
    timenow = datetime.now(utctimezone)
    created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))
    # Generate the report as an HTML file
    profile_report_path = f"{profile_output_path}/data_profile_report_{created_time}.html"
    report.show_html(profile_report_path)

    return profile_report_path

# Functional to upload files from UI to appropriate location in framework
def file_upload_all(uploaded_file, file_type, convention):

    if uploaded_file is not None:
        name_present = False
        testc_path = f"{root_path}/test/{file_type}"
        onlyfiles = [f for f in listdir(testc_path) if isfile(join(testc_path, f))]
        for loop in onlyfiles:
            if uploaded_file.name == loop:
                name_present = True
                break

        if name_present:
            return "issue1"
        elif not uploaded_file.name.startswith(convention):
            return "issue2"
        else:
            success_message = save_uploadedfile(uploaded_file, testc_path)
            return success_message

# Function to save the edited DF into DB
def save_df_into_db(modified_df, selected_protocol):
    modified_df.to_sql(con=conn_exe, name=selected_protocol, if_exists="replace", index=False)
    conn_exe.commit()

# Function to test the Source and Target Connection and load the pandas dataframes
def test_connectivity_from_testcase(chosen_protocol, chosen_testcase):
    chosen_protocol_path = f"{tc_path}/{chosen_protocol}"
    subprocess.run(f"sh {root_path}scripts/conncheck.sh {chosen_protocol_path} {chosen_testcase}",shell=True)
    src_col_df = pd.read_excel(src_column_path)
    tgt_col_df = pd.read_excel(tgt_column_path)
    return src_col_df, tgt_col_df

