import streamlit as st
from datf_app.common.commonmethods import *


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

    src_table = exec_table_name

    if st.button("Test Connection with Source & Target"):
        pass

    with (st.expander("Click to Generate Source SQL Query")):
        source_columns = get_column_names(conn_exe, src_table)
        source_column_selection = st.multiselect("Select Source Columns", source_columns)
        prompt = st.text_area(key="src_txt_area", label="Enter your prompt for SQL generation")
        if st.button("Generate Source SQL"):
            final_prompt = f"Generate a SQL query with following requirements-{prompt}"
            final_prompt += f"\nAnd use these Columns names as reference: {', '.join(source_column_selection)}"
            response = get_queries_from_ai(final_prompt)
            sql_query = response.strip()
            st.code(sql_query, language='sql')
            # Execute the SQL on the source or target connection
            if st.button("Run SQL on Source"):
                sql_query += " LIMIT 5"
                source_result = pd.read_sql(sql_query, conn_exe)
                st.write("Query Results from Source:")
                st.dataframe(source_result)

    with (st.expander("Click to Generate Target SQL Query")):
        target_columns = get_column_names(conn_exe, src_table)
        target_column_selection = st.multiselect("Select Target Columns", target_columns)
        tgt_prompt = st.text_area(key="tgt_txt_area", label="Enter your prompt for SQL generation")
        if st.button("Generate Target SQL"):
            final_tgt_prompt = f"Generate a SQL query with following requirements-{tgt_prompt}"
            final_tgt_prompt += f"\nAnd use these Columns names as reference: {', '.join(target_column_selection)}"
            tgt_response = get_queries_from_ai(final_tgt_prompt)
            tgt_sql_query = tgt_response.strip()
            st.code(tgt_sql_query, language='sql')
            tgt_sql_query += " LIMIT 5"
            # Execute the SQL on the source or target connection
            if st.button("Run SQL on Target"):
                target_result = pd.read_sql(sql_query, conn_exe)
                st.write("Query Results from Target:")
                st.dataframe(target_result)


if __name__ == "__main__":
    s2t_sql_generation()
