import sqlite3
from os import listdir
from os.path import isfile, join
import pandas as pd
import streamlit as st
from datf_core.src.testconfig import *

conn = sqlite3.connect(f"{root_path}/utils/{exec_db_name}.db")


def edit_test_cases():

    st.set_page_config(
        page_title="Edit Test Configs"
    )
    st.title("Edit Existing Test Configurations")

    selected_protocol = read_test_protocol()
    if selected_protocol is not None:
        protocol_file_path = f"{root_path}/test/testprotocol/{selected_protocol}"
        df = pd.read_excel(protocol_file_path, sheet_name=exec_sheet_name)
        df['execute'].replace({'Y': True, 'N': False}, inplace=True)

        # Load the Test Cases as an interactive table
        edited_df = st.data_editor(df, key='Sno.',
                           column_config={
                               "execute": st.column_config.CheckboxColumn(
                                   "Execute?",
                                   help="Select the test cases for execution.",
                                   default=False,
                               )
                           },
                           hide_index=True, use_container_width=True)

        edited_df.to_sql(con=conn, name=exec_table_name ,if_exists="replace")

    st.divider()
    st.markdown("**👈 Select the required page from the sidebar** to continue!")


def read_test_protocol():
    tc_path = f"{root_path}test/testprotocol"
    onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
    for loop in onlyfiles:
        if loop.find("template") != -1:
            onlyfiles.remove(loop)

    option = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", option)
    return option


if __name__ == "__main__":
    edit_test_cases()
