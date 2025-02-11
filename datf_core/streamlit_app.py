import streamlit as st
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql import Row

st.set_page_config(page_title="Data Quality Analyzer", layout="wide")

st.title("Data Quality Analyzer")

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
        .stButton button {
            border-radius: 5px;
            padding: 10px 20px;
            font-size: 16px;
        }
        .stButton button:disabled {
            background-color: #cccccc;
            color: #666666;
        }
        .stButton button:hover {
            background-color: #0056b3;
        }
        .stMarkdown h1 {
            color: #007bff;
            font-size: 36px;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)

# Initialize session state for rows
if 'row_ids' not in st.session_state:
    st.session_state.row_ids = [1]
if 'submitted' not in st.session_state:
    st.session_state.submitted = False

# Function to create a row of dropdowns and inputs
def create_row(row_id, disabled=False):
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 0.5])

    with col1:
        option1 = st.selectbox(f"Select DQ check", ["Length", "Null", "NotBeNull", "Unique", "UniqueCount", "DistinctSet", "ColumnCount", "Regexp"], index=0, key=f"col2_{row_id}", disabled=disabled)
    
    with col2:
        if option1 != "ColumnCount":
            option2 = st.selectbox(f"Select Column", ["Column1", "Column2", "Column3"], index=0, key=f"col1_{row_id}", disabled=disabled)
        else:
            option2 = st.selectbox(f"Select Column", ["N/A"], index=0, key=f"col1_{row_id}", disabled=True)

    with col3:
        if option1 == "Length":
            option3 = st.selectbox(f"Select check", ["Equal", "Between"], index=0, key=f"col3_{row_id}", disabled=disabled)
        elif option1 == "UniqueCount":
            option3 = st.selectbox(f"Select check", ["Between"], index=0, key=f"col3_{row_id}", disabled=disabled)
        else:
            option3 = st.selectbox(f"Select check", ["N/A"], index=0, key=f"col3_{row_id}", disabled=True)

    with col4:
        if option1 == "Length" and option3 == "Equal":
            option4 = st.text_input(f"Type Len Equal ToBe", key=f"col4_{row_id}", help="Example - Value should be like '6'", disabled=disabled)
        elif option1 == "Length" and option3 == "Between":
            option4 = st.text_input(f"Type Len Between", key=f"col4_{row_id}", help="Example - Value should be like '5-10'", disabled=disabled)
        elif option1 == "UniqueCount" and option3 == "Between":
            option4 = st.text_input(f"Type Count Between", key=f"col4_{row_id}", help="Example - Value should be like '5-10'", disabled=disabled)
        elif option1 in ["DistinctSet", "Regexp"]:
            option4 = st.text_input(f"Enter value for {option1}", key=f"col4_{row_id}", help="Example - ['value1', 'value2'] for DistinctSet or 'regex_pattern' for Regexp", disabled=disabled)
        elif option1 == "ColumnCount":
            option4 = st.text_input(f"Enter expected column count", key=f"col4_{row_id}", help="Example - Value should be like '5'", disabled=disabled)
        else:
            option4 = st.text_input(f"N/A", key=f"col4_{row_id}", disabled=True)

        if option4 and not disabled:
            if option1 == "Length" and option3 == "Equal" and not option4.isdigit():
                st.error(f"Row {row_id}: Please enter a valid integer")
            if option1 == "Length" and option3 == "Between":
                if not re.match(r'^\d+-\d+$', option4):
                    st.error(f"Row {row_id}: Please enter a range in the format 'integer-integer'.")
                else:
                    start, end = map(int, option4.split('-'))
                    if start >= end:
                        st.error(f"Row {row_id}: The first integer must be smaller than the second integer.")
            if option1 == "UniqueCount" and option3 == "Between":
                if not re.match(r'^\d+-\d+$', option4):
                    st.error(f"Row {row_id}: Please enter a range in the format 'integer-integer'.")
                else:
                    start, end = map(int, option4.split('-'))
                    if start >= end:
                        st.error(f"Row {row_id}: The first integer must be smaller than the second integer.")
            if option1 == "Regexp":
                try:
                    re.compile(option4)
                except re.error:
                    st.error(f"Row {row_id}: Please enter a valid regular expression.")
            if option1 == "DistinctSet":
                try:
                    eval_option4 = eval(option4)
                    if not isinstance(eval_option4, list):
                        raise ValueError
                except:
                    st.error(f"Row {row_id}: Please enter a valid list format. Example - ['value1', 'value2']")
            if option1 == "ColumnCount" and not option4.isdigit():
                st.error(f"Row {row_id}: Please enter a valid integer")

    with col5:
        if row_id > 1 and not disabled:
            if st.button("🗑️", key=f"remove_{row_id}", help="Remove this row"):
                st.session_state.row_ids.remove(row_id)
                st.rerun()

    return {
        "DQ Check": option1,
        "Column": option2,
        "Check Type": option3,
        "Value": option4
    }

# Create rows based on session state
rows_data = []
all_fields_filled = True
for row_id in st.session_state.row_ids:
    row_data = create_row(row_id, disabled=st.session_state.submitted)
    rows_data.append(row_data)
    if row_data["DQ Check"] in ["Length", "UniqueCount"]:
        if not row_data["Value"] or (row_data["Check Type"] == "Equal" and not row_data["Value"].isdigit()) or (row_data["Check Type"] == "Between" and not re.match(r'^\d+-\d+$', row_data["Value"])):
            all_fields_filled = False
        if row_data["Check Type"] == "Between" and re.match(r'^\d+-\d+$', row_data["Value"]):
            start, end = map(int, row_data["Value"].split('-'))
            if start >= end:
                all_fields_filled = False
    elif row_data["DQ Check"] in ["DistinctSet", "Regexp"]:
        if not row_data["Value"]:
            all_fields_filled = False
        if row_data["DQ Check"] == "DistinctSet":
            try:
                eval_option4 = eval(row_data["Value"])
                if not isinstance(eval_option4, list):
                    all_fields_filled = False
            except:
                all_fields_filled = False
        if row_data["DQ Check"] == "Regexp":
            try:
                re.compile(row_data["Value"])
            except re.error:
                all_fields_filled = False
    elif row_data["DQ Check"] == "ColumnCount":
        if not row_data["Value"] or not row_data["Value"].isdigit():
            all_fields_filled = False

# Add a plus symbol to add more rows
if not st.session_state.submitted and st.button("➕ Add another row"):
    new_row_id = max(st.session_state.row_ids) + 1
    st.session_state.row_ids.append(new_row_id)
    st.rerun()

# Add a submit button to generate JSON output
if st.button("Submit", disabled=not all_fields_filled):
    st.session_state.submitted = True
    json_data = json.dumps(rows_data, separators=(',', ':'), default=lambda x: None if x is None else x)
    st.json(json_data)
    print(json_data)



