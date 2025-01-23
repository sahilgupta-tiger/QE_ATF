import base64
import subprocess
import streamlit as st
import streamlit.components.v1 as components
from datf_app.common.commonmethods import *


def load_homepage():

    st.set_page_config(
        page_title="DATF Execution",
        page_icon="📌"
    )
    st.title('DATF Execution Portal')
    # Choose the testing type
    test_type = st.radio(
        "Choose the testing type for execution:", ["Count", "Null", "Duplicate", "Content"],
        captions=["Row counts from src & tgt.", "Check for Nulls in each columns.",
                  "Duplicate checks via P-key.", "Complete Reconciliation checks."])

    if test_type is not None:
        st.write(f"You selected: {test_type} as your testing type.")
    else:
        st.write("Please select a Testing Type from the list above.")
    st.divider()

    onlyfiles = read_test_protocol()
    selected_protocol = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", selected_protocol)

    if selected_protocol is not None:
        df = pd.read_sql_query(f"SELECT * FROM '{selected_protocol}'", conn_exe)

        # Load the Test Cases as an interactive table
        st.dataframe(df, key='Sno.', on_select='ignore',
                    column_order=('Sno.', 'test_case_name', 'execute'),
                   column_config={
                       "execute": st.column_config.CheckboxColumn(
                           "Execute?",
                           help="Selected test cases will execute."
                       )
                   },
                   hide_index=True, use_container_width=True)

        st.text('**In order to change the Execution order, '
                    "Please select \"Edit Test Configs\" from sidebar to update!**")
        # Start Execution Button
        st.divider()
        start_execution(test_type, selected_protocol)

    # Report Generation Button
    st.divider()
    report_generation("Generate Report")


def start_execution(test_type, selected_protocol):

    write_protocol_to_excel(selected_protocol)
    protocol_file_path = output_file_path
    execution_cmd = protocol_file_path + " " + test_type.lower()
    print(execution_cmd)

    if st.button("Start Execution"):
        with st.spinner('Execution In-Progress. Please wait...(this may take a while)'):
            subprocess.run(f"sh {root_path}scripts/testingstart.sh {execution_cmd}",shell=True)

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


def report_generation(button_text):

    if st.button(button_text):
        tab1, tab2, tab3 = st.tabs(["Summary", "Trends", "Detailed PDF"])

        with tab1:
            html_file = open(f"{root_path}/utils/reports/datfreport.html", 'r', encoding='utf-8')
            source_code = html_file.read()
            components.html(source_code, height=500, width=850, scrolling=True)

        with tab2:
            html_file = open(f"{root_path}/utils/reports/datf_trends_report.html", 'r', encoding='utf-8')
            source_code = html_file.read()
            components.html(source_code, height=800, width=850, scrolling=True)

        with tab3:
            display_pdf(f"{root_path}/utils/reports/datf_combined.pdf")


if __name__ == "__main__":
    load_homepage()

