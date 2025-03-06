import streamlit as st
from datf_app.common.commonmethods import *
import re
from datf_core.src.dqtester import *
from builtins import max
import base64


@st.cache_data
def get_data(protocol, testcase):
    return test_connectivity_from_testcase(protocol, testcase)

@st.cache_resource
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


def create_row(all_fields_filled,row_id,src_col_list,ttype,max_row_id):

    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 0.5])
    reg_format  = True

    with col1:
        option1 = st.selectbox(f"Select DQ check",
                               ["Length", "Null", "NotBeNull", "Unique", "DistinctSet",
                                "ColumnCount", "Regexplist", "ColumnOrder", "Regexp", "Sum"],
                               index=0, key=f"col2_{row_id}_{ttype}")

    if option1 != "ColumnCount" and option1 != "ColumnOrder":
        with col2:
            option2 = st.selectbox(f"Select Column", src_col_list, index=0, key=f"col1_{row_id}_{ttype}")
    else:
        with col2:
            option2 = st.selectbox(f"Select Column", "Table", index=0, key=f"col1_{row_id}_{ttype}",
                                   disabled=True)

    with col3:
        if option1 == "Length":
            option3 = st.selectbox(f"Select check", ["Equal", "Between"], index=0,
                                   key=f"col3_{row_id}_{ttype}")
        elif option1 == "Sum":
            option3 = st.selectbox(f"Select check", ["Between"], index=0, key=f"col3_{row_id}_{ttype}")
        else:
            option3 = st.selectbox(f"Select check", "Not Required", index=0,
                                   key=f"col3_{row_id}_{ttype}", disabled=True)

    with col4:
        if option1 == "Length" and option3 == "Equal":
            option4 = st.text_input(f"Type Len Equal ToBe", key=f"col4_{row_id}_{ttype}",
                                    help="Example - Value should be like '6'")

        elif option1 == "Length" and option3 == "Between":
            option4 = st.text_input(f"Type Len Between", key=f"col4_{row_id}_{ttype}",
                                    help="Example - Value should be like '5-10'")

        elif option1 == "UniqueCount" and option3 == "Between":
            option4 = st.text_input(f"Type Count Between", key=f"col4_{row_id}_{ttype}",
                                    help="Example - Value should be like '5-10'")
        elif option1 in ["DistinctSet", "Regexplist", "ColumnOrder"]:
            option4 = st.text_input(f"Enter value for {option1}", key=f"col4_{row_id}_{ttype}",
                                    help="Example - ['value1', 'value2'] for DistinctSet or 'regex_pattern' for Regexplist or ColumnOrder")

        elif option1 == "ColumnCount":
            option4 = st.text_input(f"Enter expected column count", key=f"col4_{row_id}_{ttype}",
                                    help="Example - Value should be like '5'")

        elif option1 == "Regexp":
            option4 = st.text_input(f"Enter regex pattern", key=f"col4_{row_id}_{ttype}",
                                    help="Example - Value should be like 'a.*'")

        elif option1 == "Sum" and option3 == "Between":
            option4 = st.text_input(f"Type Sum Between", key=f"col4_{row_id}_{ttype}",
                                    help="Example - Value should be like '5.5-10.5'")


        else:
            option4 = st.text_input(f"Enter value", key=f"col4_{row_id}_{ttype}", disabled=True)
            all_fields_filled = True

        if len(option4) == 0 and (option1 != "Unique" and option1 != "Null" and option1 != "NotBeNull"):
            all_fields_filled = False

        if len(option4)>0 and (option1 != "Unique" and option1 != "Null" and option1 !=  "NotBeNull"):
            if option1 == "Length" and option3 == "Equal":
                if not option4.isdigit():
                    all_fields_filled = False
                    st.error(f"Row {row_id}: Please enter a valid integer ex 10,20,30")
                else:
                    all_fields_filled = True

            if option1 == "Length" and option3 == "Between":
                if not re.match(r'^\d+-\d+$', option4):
                    all_fields_filled = False
                    st.error(
                        f"Row {row_id}: Please enter a range in the format 'integer-integer' ex: 10-20.")
                elif re.match(r'^\d+-\d+$', option4):
                    start, end = map(int, option4.split('-'))
                    if start >= end:
                        all_fields_filled = False
                        st.error(
                            f"Row {row_id}: The first integer must be smaller than the second integer.")
                    elif start<end:
                        all_fields_filled = True
                else:
                    all_fields_filled = True
            if option1 == "Sum" and option3 == "Between":
                if not re.match(r'^\d+(\.\d+)?-\d+(\.\d+)?$', option4):
                    all_fields_filled = False
                    st.error(
                        f"Row {row_id}: Please enter a range in the format 'float-float' or 'integer-integer'. ex: 299.22-900.33")
                elif re.match(r'^\d+(\.\d+)?-\d+(\.\d+)?$', option4):
                    start, end = map(float, option4.split('-'))
                    if start >= end:
                        all_fields_filled = False
                        st.error(
                            f"Row {row_id}: The first number must be smaller than the second number.")
                    elif start<end:
                        all_fields_filled = True
                else:
                    all_fields_filled = True
            if option1 == "UniqueCount" and option3 == "Between":
                if not re.match(r'^\d+-\d+$', option4):
                    all_fields_filled = False
                    st.error(
                        f"Row {row_id}: Please enter a range in the format 'integer-integer'.")
                elif re.match(r'^\d+-\d+$', option4):
                    start, end = map(int, option4.split('-'))
                    if start >= end:
                        all_fields_filled = False
                        st.error(
                            f"Row {row_id}: The first integer must be smaller than the second integer.")
                else:
                    all_fields_filled = True
            if option1 == "Regexp":
                try:
                    re.compile(option4)
                    all_fields_filled = True
                except re.error:
                    all_fields_filled = False
                    st.error(f"Row {row_id}: Please enter a valid regular expression.")
            if option1 == "DistinctSet" or option1 == "ColumnOrder":
                try:
                    eval_option4 = eval(option4)
                    if not isinstance(eval_option4, list):
                        all_fields_filled = False
                        raise ValueError
                    else:
                        all_fields_filled = True
                except:
                    st.error(
                        f"Row {row_id}: Please enter a valid list format. Example - ['value1', 'value2']")
            if option1 == "ColumnCount":
                if not option4.isdigit():
                    all_fields_filled = False
                    st.error(f"Row {row_id}: Please enter a valid integer")
                else:
                    all_fields_filled = True
            if option1 == "Regexplist":
                try:
                    eval_option4 = eval(option4)
                    if not isinstance(eval_option4, list):
                        all_fields_filled = False
                        reg_format = False
                        raise ValueError
                    else:
                        all_fields_filled = True
                        reg_format = True
                except Exception as e:
                    st.error(
                        f"Row {row_id}: Please enter a valid list format. Example - ['value1', 'value2']")

                if reg_format:
                    count = 0
                    log_info(f"Reg Exp pattern is : {option4}")
                    valid_patterns = []
                    invalid_patterns = []
                    try:

                        pattern_list = ast.literal_eval(option4)
                    except ValueError as e:
                        all_fields_filled = False
                        print(f"Error converting string to list: {e}")
                        pattern_list = []
                    for i,pattern in enumerate(pattern_list):
                        try:
                            re.compile(pattern)
                            valid_patterns.append(pattern)
                            count = count + 1
                        except re.error:
                            all_fields_filled = False
                            invalid_patterns.append(pattern)
                            st.error(f"Row {row_id}: Invalid Patterns {invalid_patterns}. Please enter a valid regular expression. ")
                    if len(pattern_list) == count:
                        all_fields_filled = True


    with col5:
        if row_id == max_row_id and row_id > 1:
            if st.button("🗑️", key=f"remove_{row_id}_{ttype}", help="Remove this row"):
                if ttype == "source":
                    st.session_state.source_row_ids.remove(row_id)
                    '''for i,rw_dt in enumerate(st.session_state.source_row_data):
                        if rw_dt["row_id"] == row_id:
                            st.session_state.source_row_data.pop(i)
                            log_info(f"After removal {st.session_state.source_row_data}")'''
                elif ttype == "target":
                    st.session_state.target_row_ids.remove(row_id)
                st.rerun()

    row_data = {
        "DQ Check": option1,
        "Column": option2,
        "Check Type": option3,
        "Value": option4
    }
    print(all_fields_filled)
    return row_data, all_fields_filled



def data_quality_checks():
    timenow = datetime.now(utctimezone)
    createdate = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y"))

    st.set_page_config(
        page_title="Apply Data Quality Checks"
    )
    st.title("Source & Target Data Quality Validations")

    onlyfiles = read_test_protocol()
    selected_protocol = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", selected_protocol)

    if selected_protocol is not None:
        onlytestcases = read_test_cases(selected_protocol)
        selected_testcase = st.selectbox(
            "Choose one from Test Case below...",
            onlytestcases, index=None, placeholder="type to search",
        )
        st.write("You selected: ", selected_testcase)

        if selected_testcase is not None:

            with st.spinner('Reading source/target details, creating dataframe'):
                source_df, target_df = get_data(selected_protocol, selected_testcase)

            if not source_df.empty and not target_df.empty:
                st.success("Dataframe has been created. Proceed below for data quality analyser...")
            else:
                st.error("Unable to connect either Source or Target. Please check test Configs and retry.")

            st.divider()
            st.markdown("""
                <div style="text-align: center;">
                    <h1>⚙️ Data Quality Analyzer</h1>
                </div>
            """, unsafe_allow_html=True)
            st.markdown("---")

            selected_option = st.radio("Please select DQ checks to be performed at source or target or both level:",
                                       ["Source", "Target", "Both"])
            if selected_option == "Both":
                tab1, tab2 = st.tabs(["Source DQ", "Target DQ"])

                with tab1:
                    if source_df.empty:
                        st.error("Source Data is empty please check connection & retry.")
                    else:
                        src_col_list = source_df.columns[1:]
                        st.markdown("""
                            <style>
                                .streamlit-expanderHeader {
                                    white-space: normal;
                                }
                                .stSelectbox div[data-baseweb="select"] {
                                    width: auto;  # Automatically adjust width based on content
                                    min-width: 300px;
                                }
                                .stSelectbox div[data-baseweb="select"] > div {
                                    overflow: visible;  # Ensure the values are completely visible
                                }
                                .stSelectbox {
                                    margin-right: 10px;  # Add equal gap between dropdowns
                                }
                                .stSelectbox div[data-baseweb="select"] ul {
                                    overflow: visible;  # Ensure the dropdown options are completely visible
                                }
                                .remove-button {
                                    display: inline-flex;
                                    align-items: center;
                                    justify-content: center;
                                    cursor: pointer;
                                    background-color: #ff4b4b;
                                    color: white;
                                    border: none;
                                    padding: 2px 5px;
                                    font-size: 12px;
                                    border-radius: 3px;
                                }
                                .remove-button:hover {
                                    background-color: #ff1a1a;
                                }
                            </style>
                        """, unsafe_allow_html=True)

                        # Initialize session state for rows
                        if 'source_row_ids' not in st.session_state:
                            st.session_state.source_row_ids = [1]
                        #if 'source_row_data' not in st.session_state:
                        #st.session_state.source_row_data = []
                        # Create rows based on session state
                        src_rows_data = []
                        src_all_fields_filled = False

                        for row_id in st.session_state.source_row_ids:
                            log_info(f"source row ids {st.session_state.source_row_ids}")
                            src_row_data, src_all_fields_filled = create_row(src_all_fields_filled,row_id,src_col_list,"source",max(st.session_state.source_row_ids))
                            src_rows_data.append(src_row_data)

                        print(src_all_fields_filled)

                        # Add a plus symbol to add more rows
                        if st.button("➕ Add Row",disabled=not src_all_fields_filled,key = "source_add"):
                            new_row_id = max(st.session_state.source_row_ids) + 1
                            st.session_state.source_row_ids.append(new_row_id)
                            src_all_fields_filled = False
                            st.rerun()

                        # Add a submit button to generate JSON output

                with tab2:
                    if target_df.empty:
                        st.error("Target Data is empty please check connection & retry.")
                    else:
                        tgt_col_list = target_df.columns[1:]
                        st.markdown("""
                            <style>
                                .streamlit-expanderHeader {
                                    white-space: normal;
                                }
                                .stSelectbox div[data-baseweb="select"] {
                                    width: auto;  # Automatically adjust width based on content
                                    min-width: 300px;
                                }
                                .stSelectbox div[data-baseweb="select"] > div {
                                    overflow: visible;  # Ensure the values are completely visible
                                }
                                .stSelectbox {
                                    margin-right: 10px;  # Add equal gap between dropdowns
                                }
                                .stSelectbox div[data-baseweb="select"] ul {
                                    overflow: visible;  # Ensure the dropdown options are completely visible
                                }
                                .remove-button {
                                    display: inline-flex;
                                    align-items: center;
                                    justify-content: center;
                                    cursor: pointer;
                                    background-color: #ff4b4b;
                                    color: white;
                                    border: none;
                                    padding: 2px 5px;
                                    font-size: 12px;
                                    border-radius: 3px;
                                }
                                .remove-button:hover {
                                    background-color: #ff1a1a;
                                }
                            </style>
                        """, unsafe_allow_html=True)

                        # Initialize session state for rows
                        if 'target_row_ids' not in st.session_state:
                            st.session_state.target_row_ids = [1]

                        # Create rows based on session state
                        tgt_rows_data = []
                        tgt_all_fields_filled = False

                        for row_id in st.session_state.target_row_ids:
                            tgt_row_data, tgt_all_fields_filled = create_row(tgt_all_fields_filled,row_id, tgt_col_list,"target",max(st.session_state.target_row_ids))
                            tgt_rows_data.append(tgt_row_data)

                        # Add a plus symbol to add more rows
                        if st.button("➕ Add Row", disabled=not tgt_all_fields_filled,key = "target_add"):
                            new_row_id = max(st.session_state.target_row_ids) + 1
                            st.session_state.target_row_ids.append(new_row_id)
                            tgt_all_fields_filled = False
                            st.rerun()

                if tgt_all_fields_filled and src_all_fields_filled:
                    all_fields_filled = True
                else:
                    all_fields_filled = False

                if 'submitted' not in st.session_state:
                    st.session_state.submitted = False

                if st.session_state.submitted:
                    st.button("🔍 Analyze & Generate Report - Source & Target", disabled=True,
                              key="submit")
                else:
                    if st.button("🔍 Analyze & Generate Report - Source & Target",
                                 disabled=not all_fields_filled, key="submit"):
                        with st.spinner(
                                'DQ Execution In-Progress. Please wait...(this may take a while)'):

                            srcfile_name = f"{selected_testcase}_source_data.json"
                            srcfile_path = os.path.join(dq_data_path, srcfile_name)

                            src_rows_data = [{k: (None if v is None else v) for k, v in row.items()} for
                                             row in src_rows_data]
                            create_json_file(src_rows_data, srcfile_path)
                            log_info(f"Source DQ JSON file has been created in path {srcfile_path}")
                            tgtfile_name = f"{selected_testcase}_target_data.json"
                            tgtfile_path = os.path.join(dq_data_path, tgtfile_name)

                            tgt_rows_data = [{k: (None if v is None else v) for k, v in row.items()} for row in
                                             tgt_rows_data]

                            create_json_file(tgt_rows_data, tgtfile_path)
                            log_info(f"Target DQ JSON file has been created in path {tgtfile_path}")
                            srcdetailreport, srcsummaryreport = startdqtest(source_df,
                                                                            selected_testcase, srcfile_path,
                                                                            "source",createdate)
                            tgtdetailreport, tgtsummaryreport = startdqtest(target_df,
                                                                            selected_testcase, tgtfile_path,
                                                                            "target",createdate)
                            # st.success("Completed. Click below to check results...")
                            st.success("Completed. Pdf results are generated and displayed below. Please check")
                            div1, div2, div3, div4 = st.tabs(["SourceDetailReport", "SourceSummaryReport", "TargetDetailReport", "TargetSummaryReport"])
                            if 'source_details_pdf_report' not in st.session_state:
                                st.session_state.source_details_pdf_report = srcdetailreport
                            if 'source_summary_pdf_report' not in st.session_state:
                                st.session_state.source_summary_pdf_report = srcsummaryreport
                            if 'target_details_pdf_report' not in st.session_state:
                                st.session_state.target_details_pdf_report = tgtdetailreport
                            if 'target_summary_pdf_report' not in st.session_state:
                                st.session_state.target_summary_pdf_report = tgtsummaryreport
                            with div1:
                                display_pdf(st.session_state.source_details_pdf_report)
                            with div2:
                                display_pdf(st.session_state.source_summary_pdf_report)
                            # st.session_state.sourcesubmitted = True
                            with div3:
                                display_pdf(st.session_state.target_details_pdf_report)
                            with div4:
                                display_pdf(st.session_state.target_summary_pdf_report)


            if selected_option == "Source":
                st.subheader("DQ check at Source Level")

                if source_df.empty:
                    st.error("Source Data is empty please check connection & retry.")
                else:
                    src_col_list = source_df.columns[1:]
                    st.markdown("""
                        <style>
                            .streamlit-expanderHeader {
                                white-space: normal;
                            }
                            .stSelectbox div[data-baseweb="select"] {
                                width: auto;  # Automatically adjust width based on content
                                min-width: 300px;
                            }
                            .stSelectbox div[data-baseweb="select"] > div {
                                overflow: visible;  # Ensure the values are completely visible
                            }
                            .stSelectbox {
                                margin-right: 10px;  # Add equal gap between dropdowns
                            }
                            .stSelectbox div[data-baseweb="select"] ul {
                                overflow: visible;  # Ensure the dropdown options are completely visible
                            }
                            .remove-button {
                                display: inline-flex;
                                align-items: center;
                                justify-content: center;
                                cursor: pointer;
                                background-color: #ff4b4b;
                                color: white;
                                border: none;
                                padding: 2px 5px;
                                font-size: 12px;
                                border-radius: 3px;
                            }
                            .remove-button:hover {
                                background-color: #ff1a1a;
                            }
                        </style>
                    """, unsafe_allow_html=True)

                    # Initialize session state for rows
                    if 'source_row_ids' not in st.session_state:
                        st.session_state.source_row_ids = [1]
                    #if 'source_row_data' not in st.session_state:
                    #st.session_state.source_row_data = []
                    # Create rows based on session state
                    src_rows_data = []
                    src_all_fields_filled = False

                    for row_id in st.session_state.source_row_ids:
                        log_info(f"source row ids {st.session_state.source_row_ids}")
                        src_row_data, src_all_fields_filled = create_row(src_all_fields_filled, row_id, src_col_list,
                                                                         "source", max(st.session_state.source_row_ids))
                        src_rows_data.append(src_row_data)

                    # Add a plus symbol to add more rows
                    if st.button("➕ Add Row",disabled=not src_all_fields_filled,key = "source_add"):
                        new_row_id = max(st.session_state.source_row_ids) + 1
                        st.session_state.source_row_ids.append(new_row_id)
                        st.rerun()

                    # Add a submit button to generate JSON output
                    if 'sourcesubmitted' not in st.session_state:
                        st.session_state.sourcesubmitted = False

                    if st.session_state.sourcesubmitted:
                        st.button("🔍 Analyze & Generate Report", disabled=True, key = "source_submit")
                    else:
                        if st.button("🔍 Analyze & Generate Report", disabled=not src_all_fields_filled,key = "source_submit"):
                            with st.spinner('DQ Execution at Source In-Progress. Please wait...(this may take a while)'):
                                file_name = f"{selected_testcase}_source_data.json"
                                file_path = os.path.join(dq_data_path, file_name)
                                src_rows_data = [{k: (None if v is None else v) for k, v in row.items()} for row in src_rows_data]
                                create_json_file(src_rows_data,file_path)
                                log_info(f"Source JSON file has been created in path {file_path}")
                                detailreport,summaryreport = startdqtest(source_df, selected_testcase, file_path,"source",createdate)
                                st.success("Completed. Pdf results are generated and displayed below. Please check")
                                div1, div2 = st.tabs(["Detail Report","Summary Report"])
                                if 'source_details_pdf_report' not in st.session_state:
                                    st.session_state.source_details_pdf_report = detailreport
                                with div1:
                                    display_pdf(st.session_state.source_details_pdf_report)
                                with div2:
                                    if 'source_summary_pdf_report' not in st.session_state:
                                        st.session_state.source_summary_pdf_report = summaryreport
                                    display_pdf(st.session_state.source_summary_pdf_report)


            if selected_option == "Target":
                st.subheader("DQ check at Target Level")
                if target_df.empty:
                    st.error("Target Data is empty please check connection & retry.")
                else:
                    tgt_col_list = target_df.columns[1:]
                    st.markdown("""
                        <style>
                            .streamlit-expanderHeader {
                                white-space: normal;
                            }
                            .stSelectbox div[data-baseweb="select"] {
                                width: auto;  # Automatically adjust width based on content
                                min-width: 300px;
                            }
                            .stSelectbox div[data-baseweb="select"] > div {
                                overflow: visible;  # Ensure the values are completely visible
                            }
                            .stSelectbox {
                                margin-right: 10px;  # Add equal gap between dropdowns
                            }
                            .stSelectbox div[data-baseweb="select"] ul {
                                overflow: visible;  # Ensure the dropdown options are completely visible
                            }
                            .remove-button {
                                display: inline-flex;
                                align-items: center;
                                justify-content: center;
                                cursor: pointer;
                                background-color: #ff4b4b;
                                color: white;
                                border: none;
                                padding: 2px 5px;
                                font-size: 12px;
                                border-radius: 3px;
                            }
                            .remove-button:hover {
                                background-color: #ff1a1a;
                            }
                        </style>
                    """, unsafe_allow_html=True)

                    # Initialize session state for rows
                    if 'target_row_ids' not in st.session_state:
                        st.session_state.target_row_ids = [1]

                    # Create rows based on session state
                    tgt_rows_data = []
                    tgt_all_fields_filled = False

                    for row_id in st.session_state.target_row_ids:
                        tgt_row_data, tgt_all_fields_filled = create_row(tgt_all_fields_filled, row_id,
                                                                         tgt_col_list, "target",
                                                                         max(st.session_state.target_row_ids))
                        tgt_rows_data.append(tgt_row_data)

                    # Add a plus symbol to add more rows
                    if st.button("➕ Add Row", disabled=not tgt_all_fields_filled,key = "target_add"):
                        new_row_id = max(st.session_state.target_row_ids) + 1
                        st.session_state.target_row_ids.append(new_row_id)
                        st.rerun()

                    # Add a submit button to generate JSON output
                    if 'targetsubmitted' not in st.session_state:
                        st.session_state.targetsubmitted = False

                    if st.session_state.targetsubmitted:
                        st.button("🔍 Analyze & Generate Report", disabled=True,key = "target_submit")
                    else:
                        if st.button("🔍 Analyze & Generate Report", disabled=not tgt_all_fields_filled,key = "target_submit"):
                            with st.spinner('DQ Execution at Target In-Progress. Please wait...(this may take a while)'):
                                # folder_path = r"F:\GitHub_Workspaces\DATF\QE_ATF\datf_core\test\data\dataquality"
                                file_name = f"{selected_testcase}_target_data.json"
                                file_path = os.path.join(dq_data_path, file_name)
                                tgt_rows_data = [{k: (None if v is None else v) for k, v in row.items()} for row in
                                                 tgt_rows_data]
                                create_json_file(tgt_rows_data, file_path)
                                log_info(f"Target JSON file has been created in path {file_path}")
                                detailreport,summaryreport = startdqtest(target_df, selected_testcase, file_path,"target",createdate)
                                st.success("Completed. Pdf results are generated and displayed below. Please check")
                                div1, div2 = st.tabs(["Detail Report", "Summary Report"])
                                with div1:
                                    if 'target_details_pdf_report' not in st.session_state:
                                        st.session_state.target_details_pdf_report = detailreport
                                    display_pdf(st.session_state.target_details_pdf_report)
                                with div2:
                                    if 'target_summary_pdf_report' not in st.session_state:
                                        st.session_state.target_summary_pdf_report = summaryreport
                                    display_pdf(st.session_state.target_summary_pdf_report)


if __name__ == "__main__":
    data_quality_checks()
