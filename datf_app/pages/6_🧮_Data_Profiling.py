import streamlit as st
from datf_app.common.commonmethods import *


def data_profiling():

    st.set_page_config(
        page_title="Generate Data Profile"
    )
    st.title("Source or Target Data Profile Generator")

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



if __name__ == "__main__":
    data_profiling()
