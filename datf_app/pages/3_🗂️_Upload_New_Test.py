from os import listdir
from os.path import isfile, join
import streamlit as st
from datf_app.common.commonmethods import *


def file_upload_all(uploaded_file, file_type, convention):

    if uploaded_file is not None:
        name_present = False
        tc_path = f"{root_path}/test/{file_type}"
        onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
        for loop in onlyfiles:
            if uploaded_file.name == loop:
                name_present = True
                break

        if name_present:
            st.error("Filename is already in use. Please rename and reupload.")
            return False
        elif not uploaded_file.name.startswith(convention):
            st.error(f"Filename must start with '{convention}'. Please rename and reupload.")
            return False
        else:
            success_message = save_uploadedfile(uploaded_file, tc_path)
            st.success(success_message)
            return True


def upload_new_test_page():

    st.set_page_config(
        page_title="Upload New TC"
    )
    st.title("Upload new Test Configuration")

    st.divider()
    testcase_file = st.file_uploader("Test Case Protocol Excel file",
                                     type='xlsx', accept_multiple_files=False)
    upl_tc = file_upload_all(testcase_file, 'testprotocol', 'test_')

    connection_file = st.file_uploader("Credential JSON file",
                                     type='json', accept_multiple_files=False)
    upl_con = file_upload_all(connection_file, 'connections', 'conn_')

    s2t_file = st.file_uploader("Source to Target Mapping file (optional)",
                                     type='xlsx', accept_multiple_files=False)
    upl_s2t = file_upload_all(s2t_file, 's2t', 's2t_')

    if upl_tc or upl_con or upl_s2t:
        st.divider()
        st.markdown("**👈 Select the required page from the sidebar** to continue!")


if __name__ == "__main__":
    upload_new_test_page()
