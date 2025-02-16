import sqlite3
import subprocess
from os import listdir
from os.path import isfile, join
import pandas as pd
from pandasql import sqldf
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
    input_df = input_df.drop(input_df.columns[0],axis=1)
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
    sub_out = subprocess.run(f"sh {root_path}scripts/conncheck.sh {chosen_protocol_path} {chosen_testcase}",
                   shell=True)
    print(sub_out)
    src_col_df = pd.read_excel(src_column_path)
    tgt_col_df = pd.read_excel(tgt_column_path)
    return src_col_df, tgt_col_df

# Function to provide prompt engineering for LLM to respond accordingly
def build_sql_generation_prompt(initial_prompt, list_of_columns, table_name):
    final_prompt = f"Generate a sqlite3 based SQL query using table name '{table_name}' and requirement as: {initial_prompt}"
    if len(list_of_columns) > 1:
        final_prompt += f". And use these Columns names as reference: {', '.join(list_of_columns)}"
    elif len(list_of_columns) == 1:
        final_prompt += f" And use this Column name as reference: {list_of_columns[0]}"
    final_prompt += ". And Strictly only provide the SQL query as the output response."
    return final_prompt

# Function to run the generated sql query on dataframe
def running_sql_query_on_df(input_df, temp_tbl_name, generated_query):
    generated_query = generated_query.replace(f"FROM {temp_tbl_name}", "FROM input_df")
    generated_query = generated_query.replace(";", "")
    generated_query += " LIMIT 5;"
    output_df = sqldf(generated_query, locals())
    return output_df

# Function to read SQL Bulk files from the framework
def read_sqlbulk_files():
    onlyfiles = [f for f in listdir(sqlbulk_path) if isfile(join(sqlbulk_path, f))]
    for loop in onlyfiles:
        if loop.find(".html") != -1 or loop.find("reportcheck-") != -1:
            onlyfiles.remove(loop)
    return onlyfiles

# Function to read the bulk sql generator excel and generate queries
def generate_bulk_sql_queries(selected_bulk_file):

    df_to_print = pd.DataFrame(columns=['prompt','sql_query','results'])
    read_sqlbulk_file = f"{sqlbulk_path}/{selected_bulk_file}"
    input_bulk_df = pd.read_excel(read_sqlbulk_file)

    for index, row in input_bulk_df.iterrows():
        user_protocol = row['ProtocolFileName']
        user_testcasename = row['TestCaseName']
        user_dropdown = row['Source/Target']
        user_prompt = row['QueryPrompts']
        user_columns = row['ListofColumns']

        list_user_columns = []
        user_columns = user_columns.replace(" ","")
        if "," in user_columns:
            list_user_columns = user_columns.split(",")
        else:
            list_user_columns.append(user_columns)

        user_protocol = user_protocol.strip()
        user_testcasename = user_testcasename.strip()

        source_df, target_df = test_connectivity_from_testcase(user_protocol, user_testcasename)
        temp_table_name = ''
        loaded_df = None
        if user_dropdown == "source":
            loaded_df = source_df.copy()
            temp_table_name = "source_table"
        elif user_dropdown == "target":
            loaded_df = target_df.copy()
            temp_table_name = "target_table"

        final_user_prompt = build_sql_generation_prompt(user_prompt, list_user_columns, temp_table_name)
        get_ai_response = get_queries_from_ai(final_user_prompt)
        final_user_query = get_ai_response.strip()
        final_user_df = running_sql_query_on_df(loaded_df, temp_table_name, final_user_query)
        final_user_results = repr(final_user_df.to_dict())
        final_user_prompt = final_user_prompt.replace("sqlite3 based ","")

        new_row = pd.DataFrame({"prompt": [final_user_prompt],
                                "sql_query": [final_user_query],
                                "results": [final_user_results]
                                })
        df_to_print = pd.concat([df_to_print, new_row], ignore_index=True)
        remove_list = [new_row, final_user_df, loaded_df, source_df, target_df]
        del remove_list

    print(df_to_print)
    html_output = query_validation_report(df_to_print.copy())
    return html_output

# Function to convert string representation back into dictionary and list
def repr_eval_list(my_dict_str):
    my_list = []
    my_dict = eval(my_dict_str)
    for key in my_dict:
        my_list.append(f"{key}: {list(my_dict[key].values())}")
    return my_list

# Function to load the output results into an html report
def query_validation_report(tables_df):
    # Manipulate and process data as needed
    # To Initialize the HTML content with the header
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Tiger ETL Tool Report</title>
        <style>
            /* Add CSS styles here */
            table {
                border-collapse: collapse;
                width: 100%;
            }
            th, td {
                padding: 8px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #f2f2f2;
            }
            /* Adjust the width and enable wrapping for the results column */
            .results {
            width: 30%;
            word-wrap: break-word;
            }
        </style>
    </head>
    <body>
        <h1>Tiger SQL Generator Tool Report</h1>
    """

    # Add Run Summary and Run Date
    run_summary = "Report Run Summary"
    run_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    function_name = "QueryValidationAndReport"
    function_value = "Validating the query generated from AI against DB and retrieving only the first few rows"

    html_content += f"<h2>{run_summary}</h2>"
    html_content += f"<p><strong>Run Date:</strong> {run_date}</p>"
    html_content += f"<p><strong>Function Name:</strong> {function_name}</p>"
    html_content += f"<p><strong>Function Value:</strong> {function_value}</p>"
    html_content += f"<h2>Results</h2>"

    if tables_df is not None:
        # Create the table header
        html_content += "<table>"
        html_content += "<tr><th>No.</th><th>Prompt</th><th>SQL Query</th><th>Results</th></tr>"

        # Counter for numbering prompts
        prompt_counter = 1

        # Iterate over each key-value pair in the df
        for i, r in tables_df.iterrows():
            # Add row for each key-value pair
            html_content += "<tr>"
            html_content += f"<td>{prompt_counter}</td>"
            html_content += f"<td>{r['prompt']}</td>"
            html_content += f"<td>{r['sql_query']}</td>"
            if r['results'] == "":
                html_content += "<td>No Results</td>"
            else:
                html_content += "<td>"
                html_content += "  <ul>"
                list_of_values = repr_eval_list(r["results"])
                for q in range(len(list_of_values)):
                    html_content += f"    <li>{list_of_values[q]}</li>"
                html_content += "  </ul>"
                html_content += "</td>"

            html_content += "</tr>"
            # Increment prompt counter
            prompt_counter += 1

        # Close the table and HTML content
        html_content += "</table>"
    # Finish the tags
    html_content += """
    </body>
    </html>
    """

    if tables_df is None:
        html_content = None
    else:
        # Step 4: Save HTML
        report_file = f"{bulkresults_path}/bulkresults-{run_date}.html"
        with open(report_file, 'w') as f:
            f.write(html_content)

    return html_content