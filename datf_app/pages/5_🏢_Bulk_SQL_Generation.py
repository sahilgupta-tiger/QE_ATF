import streamlit as st
from datf_app.common.commonmethods import *


def bulk_generation():

    st.set_page_config(
        page_title="Bulk SQL Generator"
    )
    st.title("Bulk Test Configs SQL Generator")

    st.divider()
    testcase_file = st.file_uploader("Template Excel file",
                                     type='xlsx', accept_multiple_files=False)
    convention = 'bulk_'
    upload_status = file_upload_all(testcase_file, 'sqlbulk', convention)
    if upload_status is not None:
        if upload_status == "issue1":
            st.error("Bulk Upload filename is already in use. Please rename and reupload.")
        elif upload_status == "issue2":
            st.error(f"Filename must start with '{convention}'. Please rename and reupload.")
        else:
            st.success(upload_status)


if __name__ == "__main__":
    bulk_generation()
