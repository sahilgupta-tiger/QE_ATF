from subprocess import Popen
import time
import streamlit.components.v1 as components
import streamlit as st
from tabulate import tabulate
import sqlite3
import pandas as pd
from atf.common.atf_common_functions import log_info
from constants import *

stdout = "Logs will be generated only when execution is completed."
stderr = "Any errors would be called out in logs."
core_path = 'datf_core'
full_path = f'D:/My_Workspaces/GitHub/DATF_Other/Pyspark/QE_ATF/{core_path}'
conn = sqlite3.connect(f'{core_path}/utils/{exec_db_name}.db')

protocol_file_path = f"{core_path}/test/testprotocol/testprotocol.xlsx"
df = pd.read_excel(protocol_file_path, sheet_name=exec_sheet_name)
df['execute'].replace({'Y': True, 'N': False}, inplace=True)


def load_homepage():
    st.title('DATF Execution Portal')
    # Choose the testing type
    test_type = st.radio(
        "Choose the testing type for execution:", ["Count", "Duplicate", "Content"],
        captions=["Row counts from src & tgt.", "Duplicate and Null checks.", "Reconciliation checks."])

    if test_type is not None:
        st.write(f"You selected: {test_type} as your testing type.")
    else:
        st.write("Please select a Testing Type from the list above.")

    # Load the Test Cases as an interactive table
    edited_df = st.data_editor(df,
                   column_order=('Sno.','test_case_name','execute'),
                   column_config={
                       "execute": st.column_config.CheckboxColumn(
                           "Execute?",
                           help="Select the test cases for execution.",
                           default=False,
                       )
                   },
                   hide_index=True, use_container_width = True)
    print(tabulate(edited_df, headers='keys', tablefmt='psql'))
    # Save the edited table into the DB for execution
    edited_df.to_sql(exec_table_name, conn, if_exists='replace', index=False)
    conn.commit()
    st.divider()
    start_execution(test_type)
    report_generation()


def get_selected_testcases():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {exec_table_name}")
    tcnames_list = []
    for row in cur:
        if row[3]:
            tcnames_list.append(row[1])
    tc_names = ','.join(tcnames_list)
    cur.close()
    return tc_names


def start_execution(test_type):
    if st.button("Start Execution"):
        st.write("Chosen Test Cases for execution are:")
        execution_cmd = test_type.lower() + " " + get_selected_testcases()
        st.write(execution_cmd)
        with st.spinner('Execution Started. Please wait...'):
            global stdout, stderr
            p = Popen(f"scripts/run_datf.bat {execution_cmd}", cwd=f"{full_path}")
            stdout, stderr = p.communicate()
        st.success("Completed. Click below to check results...")
        st.divider()


def report_generation():
    if st.button("Generate Report"):
        tab1, tab2, tab3 = st.tabs(["Summary", "Trends", "Console Logs"])

        with tab1:
            html_file = open(f"{core_path}/utils/reports/datfreport.html", 'r', encoding='utf-8')
            source_code = html_file.read()
            components.html(source_code, height=500, width=850, scrolling=True)

        with tab2:
            html_file = open(f"{core_path}/utils/reports/datf_trends_report.html", 'r', encoding='utf-8')
            source_code = html_file.read()
            components.html(source_code, height=500, width=850, scrolling=True)

        with tab3:
            st.write(stdout)
            st.write(stderr)


if __name__ == "__main__":
    log_info(f"Protocol Config path :{protocol_file_path}")
    load_homepage()
    conn.close()


