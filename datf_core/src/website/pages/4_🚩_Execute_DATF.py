from datf_core.src.website.setpaths import *
import base64
import subprocess
import streamlit.components.v1 as components
import streamlit as st
import sqlite3
import pandas as pd
from datf_core.src.testconfig import *


def load_homepage():
    st.set_page_config(
        page_title="DATF Execution",
        page_icon="📌"
    )
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
    edited_df = st.data_editor(df, key='Sno.',
                   column_order=('Sno.', 'test_case_name', 'execute'),
                   column_config={
                       "execute": st.column_config.CheckboxColumn(
                           "Execute?",
                           help="Select the test cases for execution.",
                           default=False,
                       )
                   },
                   hide_index=True, use_container_width=True)

    # Save the edited table into the DB for execution
    edited_df.to_sql(exec_table_name, conn, if_exists='replace', index=False)
    conn.commit()

    st.divider()
    # Start Execution Button
    start_execution(test_type)
    st.divider()
    # Report Generation Button
    report_generation()


def get_selected_testcases():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {exec_table_name}")
    tcnames_list = []

    for row in cur:
        if row[3]:
            tcnames_list.append(row[1])

    if not tcnames_list:
        tc_names = 'all'
    else:
        tc_names = ','.join(tcnames_list)

    cur.close()
    return tc_names


def start_execution(test_type):

    test_case_list = get_selected_testcases()
    st.write("Chosen Test Cases for execution are: " + test_case_list)
    execution_cmd = test_type.lower() + " " + test_case_list
    print(execution_cmd)

    if st.button("Start Execution"):
        with st.spinner('Execution In-Progress. Please wait...(this may take a while)'):
            subprocess.run(f"{docker_bat_file} {execution_cmd}",
                           cwd=f"{full_path}/{core_path}/scripts",
                            shell=True)
        st.success("Completed. Click below to check results...")


def display_pdf(file):
    # Opening file from file path
    with open(file, "rb") as f:
        base64_pdf = base64.b64encode(f.read()).decode('utf-8')

    # Embedding PDF in HTML
    pdf_display = f"""<iframe
        class="pdfobject"
        width=800
        height=600
        type="application/pdf"
        title="Execution Report"
        src="data:application/pdf;base64,{base64_pdf}"
        style="overflow: auto;">"""

    # Displaying File
    with st.container():
        st.markdown(pdf_display, unsafe_allow_html=True)


def report_generation():

    if st.button("Generate Report"):
        tab1, tab2, tab3 = st.tabs(["Summary", "Trends", "Detailed PDF"])

        with tab1:
            html_file = open(f"{core_path}/utils/reports/datfreport.html", 'r', encoding='utf-8')
            source_code = html_file.read()
            components.html(source_code, height=500, width=850, scrolling=True)

        with tab2:
            html_file = open(f"{core_path}/utils/reports/datf_trends_report.html", 'r', encoding='utf-8')
            source_code = html_file.read()
            components.html(source_code, height=800, width=850, scrolling=True)

        with tab3:
            display_pdf(f"{core_path}/utils/reports/datf_combined.pdf")


if __name__ == "__main__":
    use_protocol = False
    conn = sqlite3.connect(f'{core_path}/utils/{exec_db_name}.db')

    if use_protocol:
        protocol_file_path = f"{core_path}/test/testprotocol/testprotocol.xlsx"
        df = pd.read_excel(protocol_file_path, sheet_name=exec_sheet_name)
        df['execute'].replace({'Y': True, 'N': False}, inplace=True)
    else:
        df = pd.read_sql(f'SELECT * FROM {exec_table_name}', conn)

    load_homepage()
    conn.close()
