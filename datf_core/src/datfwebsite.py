import streamlit as st
from tabulate import tabulate
import sqlite3
import pandas as pd
from atf.common.atf_common_functions import log_info
from constants import *

core_path = 'datf_core'
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
    st.divider()
    if st.button("Start Execution"):
        st.write("Chosen Test Cases for execution are:")
        st.write(get_selected_testcases())


def get_selected_testcases():
    cur = conn.cursor()
    tcnames_list = []
    for row in cur:
        if row['execute']:
            tcnames_list.append(row['test_case_name'])
    tc_names = ','.join(tcnames_list)
    return tc_names


def generate_html_content():
    html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Tiger DATF Execution</title>
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
            <h1>Tiger DATF Execution</h1>
        """


if __name__ == "__main__":
    log_info(f"Protocol Config path :{protocol_file_path}")
    load_homepage()


