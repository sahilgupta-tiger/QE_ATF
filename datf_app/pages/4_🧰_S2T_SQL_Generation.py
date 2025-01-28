import streamlit as st
import openai
from datf_app.common.commonmethods import *

if 'source_columns' not in st.session_state:
    st.session_state['source_columns'] = []
if 'target_columns' not in st.session_state:
    st.session_state['target_columns'] = []


def s2t_sql_generation():

    st.set_page_config(
        page_title="S2T SQL Generator"
    )
    st.title("Source to Target SQL Generator")

    onlyfiles = read_test_protocol()
    selected_protocol = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", selected_protocol)

    src_column_list = []
    tgt_column_list = []

    if selected_protocol is not None:
        onlytestcases = read_test_cases(selected_protocol)
        selected_testcase = st.selectbox(
            "Choose one from Test Case below...",
            onlytestcases, index=None, placeholder="type to search",
        )
        st.write("You selected: ", selected_testcase)

        if selected_testcase is not None:

            if st.button("Click to Connect Source & Target systems"):
                with st.spinner('Processing, Please wait...'):
                    source_df, target_df = test_connectivity_from_testcase(
                        selected_protocol, selected_testcase)

                source_collist = list(source_df.columns[1:])
                target_collist = list(target_df.columns[1:])
                src_column_list = source_collist.copy()
                tgt_column_list = target_collist.copy()

                if len(source_collist) != 0 and len(target_collist) != 0:
                    st.success("Connection Success. Proceed with query generation below...")
                else:
                    st.error("Unable to load columns from either Source or Target. Please check test configs and retry.")

            st.header("Generate SQLs using GenAI LLM")
            tab1, tab2 = st.tabs(["Source Test Query", "Target Test Query"])

            with tab1:
                with st.form("src_query"):
                    # Function to update session state with columns
                    def update_source_columns():
                        st.session_state['source_columns'] = src_column_list
                    if st.session_state['source_columns']:
                        src_column_list = st.session_state['source_columns']
                    try:
                        source_column_selection = st.multiselect("Select Source Columns", src_column_list)
                        prompt = st.text_area(key="src_txt_area", label="Enter your prompt for SQL generation")
                        if st.form_submit_button("Generate Source SQL", on_click=update_source_columns):
                            final_prompt = f"Generate a SQL query with following requirements: {prompt}"
                            final_prompt += f"\nAnd use these Columns names as reference: {', '.join(source_column_selection)}"
                            with st.spinner("Getting results from AI now..."):
                                response = get_queries_from_ai(final_prompt)
                            sql_query = response.strip()
                            st.code(sql_query, language='sql')
                            sql_query += " LIMIT 5"
                            source_result = source_df.query(sql_query)
                            st.write("Running Query and Output Results from Source:")
                            st.dataframe(source_result, hide_index=True, use_container_width=True)
                    except openai.APIConnectionError:
                        st.error("Unable to connect to GenAI API. Please check Network Settings!")
                    except Exception as error:
                        st.error("EXECUTION ERRORED! Please check logs.")
                        print(error)

            with tab2:
                with st.form("tgt_query"):
                    # Function to update session state with selected columns
                    def update_target_columns():
                        st.session_state['target_columns'] = tgt_column_list
                    if st.session_state['target_columns']:
                        tgt_column_list = st.session_state['target_columns']
                    try:
                        target_column_selection = st.multiselect("Select Target Columns", tgt_column_list)
                        tgt_prompt = st.text_area(key="tgt_txt_area", label="Enter your prompt for SQL generation")
                        if st.form_submit_button("Generate Target SQL", on_click=update_target_columns):
                            final_tgt_prompt = f"Generate a SQL query with following requirements-{tgt_prompt}"
                            final_tgt_prompt += f"\nAnd use these Columns names as reference: {', '.join(target_column_selection)}"
                            with st.spinner("Getting results from AI now..."):
                                tgt_response = get_queries_from_ai(final_tgt_prompt)
                            tgt_sql_query = tgt_response.strip()
                            st.code(tgt_sql_query, language='sql')
                            tgt_sql_query += " LIMIT 5"
                            target_result = target_df.query(sql_query)
                            st.write("Running Query and Output Results from Target:")
                            st.dataframe(target_result, hide_index=True, use_container_width=True)
                    except openai.APIConnectionError:
                        st.error("Unable to connect to GenAI API. Please check Network Settings!")
                    except Exception as error:
                        st.error("EXECUTION ERRORED! Please check logs.")
                        print(error)


if __name__ == "__main__":
    s2t_sql_generation()
