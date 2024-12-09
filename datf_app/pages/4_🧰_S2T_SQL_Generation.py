import streamlit as st
import pandas as pd
from os import listdir
from os.path import isfile, join
import json
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI
from datf_core.src.testconfig import *


openai_json = json.load(open(f"{root_path}/test/connections/azure_open_ai_connection.json"))
os.environ["AZURE_OPENAI_API_KEY"] = decryptcredential(openai_json['apikey'])
os.environ["AZURE_OPENAI_ENDPOINT"] = openai_json['endpoint']
openai_api_version = openai_json['apiversion']
azure_deployment = openai_json['deployment']
conn_str = f"DRIVER={{SQL Server}};SERVER={'server'};DATABASE={'database'};UID={'username'};PWD={'password'}"
conn = """pyodbc.connect(conn_str)"""


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


def read_test_configs():
    tc_path = f"{root_path}/test/testcases"
    onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
    for loop in onlyfiles:
        if loop.find("template") != -1:
            onlyfiles.remove(loop)

    option = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", option)


# Function to fetch column names
def get_column_names(connection, table_name):
    query = f"SELECT * FROM {table_name} WHERE 1=0"
    df = pd.read_sql(query, connection)
    return df.columns.tolist()


def s2t_sql_generation():

    st.set_page_config(
        page_title="S2T Generator"
    )
    st.title("Source to Target SQL Generator")
    read_test_configs()
    if st.button("Test Connection with Source & Target"):
        pass
    with (st.expander("Click to Generate Source SQL Query")):
        src_table = ""
        source_columns = get_column_names(conn, src_table)
        source_column_selection = st.multiselect("Select Source Columns", source_columns)
        prompt = st.text_area("Enter your prompt for SQL generation")
        if st.button("Generate Source SQL"):
            final_prompt = f"Generate a SQL query with following requirements-{prompt}"
            final_prompt += f"\nAnd use these Columns names as reference: {', '.join(source_column_selection)}"
            response = get_queries_from_ai(final_prompt)
            sql_query = response.strip()
            st.code(sql_query, language='sql')
            # Execute the SQL on the source or target connection
            if st.button("Run SQL on Source"):
                sql_query += " LIMIT 5"
                source_result = pd.read_sql(sql_query, conn)
                st.write("Query Results from Source:")
                st.dataframe(source_result)

    with (st.expander("Click to Generate Target SQL Query")):
        target_columns = get_column_names(conn, src_table)
        target_column_selection = st.multiselect("Select Target Columns", target_columns)
        tgt_prompt = st.text_area("Enter your prompt for SQL generation")
        if st.button("Generate Target SQL"):
            final_tgt_prompt = f"Generate a SQL query with following requirements-{tgt_prompt}"
            final_tgt_prompt += f"\nAnd use these Columns names as reference: {', '.join(target_column_selection)}"
            tgt_response = get_queries_from_ai(final_tgt_prompt)
            tgt_sql_query = tgt_response.strip()
            st.code(tgt_sql_query, language='sql')
            tgt_sql_query += " LIMIT 5"
            # Execute the SQL on the source or target connection
            if st.button("Run SQL on Source"):
                target_result = pd.read_sql(sql_query, conn)
                st.write("Query Results from Target:")
                st.dataframe(target_result)


if __name__ == "__main__":
    s2t_sql_generation()
