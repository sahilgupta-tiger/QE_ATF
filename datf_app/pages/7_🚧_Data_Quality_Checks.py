import streamlit as st
from datf_app.common.commonmethods import *


def data_quality_checks():

    st.set_page_config(
        page_title="Apply Data Quality Checks"
    )
    st.title("Source & Target Data Quality Validations")
    st.header("***Page Under Construction***")

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

            if st.button("Start Profiling for Source & Target"):
                with st.spinner('Processing, Please wait...'):
                    source_df, target_df = test_connectivity_from_testcase(selected_protocol, selected_testcase)

                if not source_df.empty and not target_df.empty:
                    st.success("Connection Success. Proceed below...")
                else:
                    st.error("Unable to connect either Source or Target. Please check test Configs and retry.")

                st.divider()


if __name__ == "__main__":
    data_quality_checks()
