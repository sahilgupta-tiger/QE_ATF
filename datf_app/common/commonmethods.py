import sqlite3
from os import listdir
from os.path import isfile, join
import pandas as pd
from datf_core.src.testconfig import *
import json
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI

tc_path = f"{root_path}/test/testprotocol"
output_file_path = f"{root_path}/test/testprotocol/{exec_table_name}_template.xlsx"
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

# Function to fetch column names
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
