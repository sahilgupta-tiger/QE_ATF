from os import listdir
from os.path import isfile, join
import os
import streamlit as st
from datf_core.src.testconfig import *


def save_uploadedfile(uploadedfile, filepath):
    with open(os.path.join(filepath, uploadedfile.name), "wb") as f:
        f.write(uploadedfile.getbuffer())
    return st.success(f"Saved File: {uploadedfile.name} to '{filepath}' in framework!")


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
            save_uploadedfile(uploaded_file, tc_path)
            return True


def upload_new_test_page():

    st.set_page_config(
        page_title="Upload New TC"
    )
    st.title("Upload new Test Configuration")

    st.divider()
    testcase_file = st.file_uploader("Test Case Excel file",
                                     type='xlsx', accept_multiple_files=False)
    upl_tc = file_upload_all(testcase_file, 'testcases', 'testcase')

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
