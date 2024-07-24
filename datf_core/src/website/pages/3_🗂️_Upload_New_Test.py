from datf_core.src.website.setpaths import *
from os import listdir
from os.path import isfile, join
import streamlit as st


def save_uploadedfile(uploadedfile, filepath):
    with open(os.path.join(filepath, uploadedfile.name), "wb") as f:
        f.write(uploadedfile.getbuffer())
    return st.success(f"Saved File: {uploadedfile.name} to {filepath} in framework!")


def file_upload_test_case():
    uploaded_file = st.file_uploader("Test Case Excel file",
                                     type='xlsx', accept_multiple_files=False)

    if uploaded_file is not None:
        name_present = False
        tc_path = f"{core_path}/test/testcases"
        onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
        for loop in onlyfiles:
            if uploaded_file.name == loop:
                name_present = True
                break

        if name_present:
            st.error("Filename is already in use. Please rename and reupload.")
            return False
        else:
            save_uploadedfile(uploaded_file, tc_path)
            return True


def file_upload_connections():
    uploaded_file = st.file_uploader("Credentials JSON file",
                                     type='json', accept_multiple_files=False)

    if uploaded_file is not None:
        name_present = False
        tc_path = f"{core_path}/test/connections"
        onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
        for loop in onlyfiles:
            if uploaded_file.name == loop:
                name_present = True
                break

        if name_present:
            st.error("Filename is already in use. Please rename and reupload.")
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
    upl_tc = file_upload_test_case()
    upl_con = file_upload_connections()

    if upl_tc or upl_con:
        st.divider()
        st.markdown("**👈 Select the required page from the sidebar** to continue!")


if __name__ == "__main__":
    upload_new_test_page()
