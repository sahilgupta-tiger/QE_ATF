import streamlit as st
from datf_app.common.commonmethods import *


def upload_new_test_page():

    st.set_page_config(
        page_title="Upload New TC"
    )
    st.title("Upload new Test Configuration")

    testcase_file = st.file_uploader("Test Case Protocol Excel file",
                                     type='xlsx', accept_multiple_files=False)
    conv_test = 'test_'
    upl_tc = file_upload_all(testcase_file, 'testprotocol', conv_test)
    if upl_tc is not None:
        if upl_tc == "issue1":
            st.error("Protocol filename is already in use. Please rename and reupload.")
        elif upl_tc == "issue2":
            st.error(f"Filename must start with '{conv_test}'. Please rename and reupload.")
        else:
            st.success(upl_tc)

    connection_file = st.file_uploader("Credential JSON file",
                                     type='json', accept_multiple_files=False)
    conv_conn = 'conn_'
    upl_con = file_upload_all(connection_file, 'connections', conv_conn)
    if upl_con is not None:
        if upl_con == "issue1":
            st.error("Connection filename is already in use. Please rename and reupload.")
        elif upl_con == "issue2":
            st.error(f"Filename must start with '{conv_conn}'. Please rename and reupload.")
        else:
            st.success(upl_con)

    s2t_file = st.file_uploader("Source to Target Mapping file",
                                     type='xlsx', accept_multiple_files=False)
    conv_s2t = 's2t_'
    upl_s2t = file_upload_all(s2t_file, 's2t', conv_s2t)
    if upl_s2t is not None:
        if upl_s2t == "issue1":
            st.error("S2T Mapping Filename is already in use. Please rename and reupload.")
        elif upl_s2t == "issue2":
            st.error(f"Filename must start with '{conv_s2t}'. Please rename and reupload.")
        else:
            st.success(upl_s2t)

    bulk_generate_file = st.file_uploader("Bulk SQL Generation Excel file",
                                          type='xlsx', accept_multiple_files=False)
    convention = 'bulk_'
    upload_status = file_upload_all(bulk_generate_file, 'sqlbulk', convention)
    if upload_status is not None:
        if upload_status == "issue1":
            st.error("Bulk Upload filename is already in use. Please rename and reupload.")
        elif upload_status == "issue2":
            st.error(f"Filename must start with '{convention}'. Please rename and reupload.")
        else:
            st.success(upload_status)

    st.divider()
    st.markdown("**👈 Select the required page from the sidebar** to continue!")


if __name__ == "__main__":
    upload_new_test_page()
