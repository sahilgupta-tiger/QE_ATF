import streamlit as st
from datf_app.common.commonmethods import *


def bulk_generation():

    st.set_page_config(
        page_title="Bulk SQL Generator"
    )
    st.title("Bulk SQL Generator using GenAI")

    onlyfiles = read_sqlbulk_files()
    selected_bulkfile = st.selectbox(
        "Choose one from Bulk Files below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", selected_bulkfile)

    if selected_bulkfile is not None:
        if st.button("Run and Validate Generated SQL Queries"):
            with st.spinner('Processing, Please wait...(this may take a while)'):
                html = generate_bulk_sql_queries(selected_bulkfile)
            st.divider()
            if html is not None:
                st.html(html)  # Display HTML content
                st.success("Queries Generated and Results are validated.")
            else:
                st.error("Unable to load SQL report. Please check test configs in excel and retry.")


if __name__ == "__main__":
    bulk_generation()
