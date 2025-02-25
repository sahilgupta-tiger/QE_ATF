import pandas as pd
import streamlit as st
import openai
from datf_app.common.commonmethods import *

if 'source_columns' not in st.session_state:
    st.session_state['source_columns'] = []
if 'target_columns' not in st.session_state:
    st.session_state['target_columns'] = []

source_df = pd.DataFrame()
target_df = pd.DataFrame()
temp_table = "source_table"
temp_tgt_table = "target_table"

def s2t_sql_generation():

    st.set_page_config(
        page_title="S2T SQL Generator"
    )
    st.title("Source & Target SQL Generator")

    # Choose the generation type
    gen_type = st.radio(
        "Choose the generation type:", ["Native Tool", "GenAI Assisted"],
        captions=["(Limited Features)", "(Advanced Features)"],
        horizontal=True)

    if gen_type is not None:
        st.write(f"You selected: {gen_type}.")
    else:
        st.write("Please select a generation type above.")

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

            button_press = st.button("Click to Connect Source & Target systems")
            if not button_press:
                st.write("Before proceeding, please click button above.")
            else:
                global source_df, target_df
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

            if gen_type is not None and (gen_type == "GenAI Assisted"):
                st.header("Generate SQLs using GenAI Tool")
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
                                final_prompt = build_sql_generation_prompt(prompt, source_column_selection, temp_table)
                                with st.spinner("Getting results from AI now..."):
                                    response = get_queries_from_ai(final_prompt)
                                sql_query = response.strip()
                                st.code(sql_query, language='sql')

                                st.write("Running Query and Output Results from Source:")
                                source_result = running_sql_query_on_df(source_df, temp_table, sql_query)
                                st.dataframe(source_result, hide_index=True, use_container_width=True)

                        except openai.APIConnectionError:
                            st.error("Unable to connect to GenAI API. Please check Network Settings!")
                        except QueryRunFailed as q:
                            st.write(str(q))
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
                                final_tgt_prompt = build_sql_generation_prompt(tgt_prompt, target_column_selection, temp_tgt_table)
                                with st.spinner("Getting results from AI now..."):
                                    tgt_response = get_queries_from_ai(final_tgt_prompt)
                                tgt_sql_query = tgt_response.strip()
                                st.code(tgt_sql_query, language='sql')

                                st.write("Running Query and Output Results from Target:")
                                target_result = running_sql_query_on_df(target_df, temp_tgt_table, tgt_sql_query)
                                st.dataframe(target_result, hide_index=True, use_container_width=True)

                        except openai.APIConnectionError:
                            st.error("Unable to connect to GenAI API. Please check Network Settings!")
                        except QueryRunFailed as q:
                            st.write(str(q))
                        except Exception as error:
                            st.error("EXECUTION ERRORED! Please check logs.")
                            print(error)

            if gen_type is not None and (gen_type == "Native Tool"):
                st.header("Generate SQLs using DATF Tool")
                tab3, tab4 = st.tabs(["Source Test Query", "Target Test Query"])

                # Read JSON file
                with open(gen_queries_path, "r", encoding="utf-8") as file:
                    query_data = json.load(file)

                with tab3:
                    try:
                        if len(src_column_list) != 0 and len(tgt_column_list) != 0:
                            src_sql_query = query_data['sourcequery']
                        else:
                            src_sql_query = "A None"
                        get_src_tblname = get_next_word(src_sql_query)
                        st.code(src_sql_query, language='sql')

                        # st.write("Running Query and Output Results from Source:")
                        # src_query_result = running_sql_query_on_df(source_df, get_src_tblname, src_sql_query)
                        # st.dataframe(src_query_result, hide_index=True, use_container_width=True)

                    except QueryRunFailed as q:
                        st.write(str(q))
                    except Exception as error:
                        st.error("EXECUTION ERRORED! Please check logs.")
                        print(error)

                with tab4:
                    try:
                        if len(src_column_list) != 0 and len(tgt_column_list) != 0:
                            tgt_sql_query = query_data['targetquery']
                        else:
                            tgt_sql_query = "A None"
                        get_tgt_tblname = get_next_word(tgt_sql_query)
                        st.code(tgt_sql_query, language='sql')

                        # st.write("Running Query and Output Results from Target:")
                        # tgt_query_result = running_sql_query_on_df(target_df, get_tgt_tblname, tgt_sql_query)
                        # st.dataframe(tgt_query_result, hide_index=True, use_container_width=True)

                    except QueryRunFailed as q:
                        st.write(str(q))
                    except Exception as error:
                        st.error("EXECUTION ERRORED! Please check logs.")
                        print(error)


if __name__ == "__main__":
    s2t_sql_generation()
