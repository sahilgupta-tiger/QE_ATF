import streamlit as st
from datf_app.common.commonmethods import *
import streamlit.components.v1 as components


def data_profiling():

    st.set_page_config(
        page_title="Generate Data Profiles"
    )
    st.title("Source & Target Data Profile Generator")

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

            if st.button("Connect & Generate for Source & Target"):
                with st.spinner('Processing, Please wait...'):
                    source_df, target_df = test_connectivity_from_testcase(selected_protocol, selected_testcase)

                if not source_df.empty and not target_df.empty:
                    st.success("Connection Success. Proceed below...")
                else:
                    st.error("Unable to connect either Source or Target. Please check test Configs and retry.")

                st.divider()
                st.markdown("**Note:** Sample Data for profiling is limited to the number of rows prescribed as per \
                Test Case Config in the field 'dataprofilelimit'. Please update if needed.")

                st.header("Generated Data Profiles")
                tab1, tab2 = st.tabs(["Source Profile", "Target Profile"])

                with tab1:
                    if source_df.empty:
                        st.error("Source Data is empty please check connection & retry.")
                    else:
                        with st.spinner('Loading Report, Please wait...'):
                            src_profile_path = create_data_profile_report(source_df, "Source Dataset")
                            src_html_file = open(src_profile_path, 'r', encoding='utf-8')
                            source_code = src_html_file.read()
                            components.html(source_code, height=800, width=850, scrolling=True)

                with tab2:
                    if target_df.empty:
                        st.error("Target Data is empty please check connection & retry.")
                    else:
                        with st.spinner('Loading Report, Please wait...'):
                            tgt_profile_path = create_data_profile_report(target_df, "Target Dataset")
                            tgt_html_file = open(tgt_profile_path, 'r', encoding='utf-8')
                            target_code = tgt_html_file.read()
                            components.html(target_code, height=800, width=850, scrolling=True)


if __name__ == "__main__":
    data_profiling()
