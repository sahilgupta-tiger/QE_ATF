import streamlit as st
from datf_app.common.commonmethods import *


def bulk_generation():

    st.set_page_config(
        page_title="Bulk SQL Generator"
    )
    st.title("Bulk Test Configs SQL Generator")

    onlyfiles = read_sqlbulk_files()
    selected_bulkfile = st.selectbox(
        "Choose one from Bulk Files below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", selected_bulkfile)

    if selected_bulkfile is not None:
        if st.button("Run and Validate Generated SQL Queries"):
            with st.spinner('Processing, Please wait...'):
                html = generate_bulk_sql_queries(selected_bulkfile)
            st.divider()
            st.markdown(html, unsafe_allow_html=True)  # Display HTML content
            st.write("Query Generated and results are validated")


if __name__ == "__main__":
    bulk_generation()
