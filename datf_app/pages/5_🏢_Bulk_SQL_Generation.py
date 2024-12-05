import streamlit as st
import json
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI
from datf_core.src.testconfig import *
from os import listdir
from os.path import isfile, join


openai_json = json.load(open(f"{root_path}/test/connections/azure_open_ai_connection.json"))
os.environ["AZURE_OPENAI_API_KEY"] = decryptcredential(openai_json['apikey'])
os.environ["AZURE_OPENAI_ENDPOINT"] = openai_json['endpoint']
openai_api_version = openai_json['apiversion']
azure_deployment = openai_json['deployment']


def get_queries_from_ai(prompt):

    model = AzureChatOpenAI(
        openai_api_version=openai_api_version,
        azure_deployment=azure_deployment,
    )
    message = HumanMessage(
        content=prompt
    )
    output_value=model([message])
    return output_value.content


def save_uploadedfile(uploadedfile, filepath):
    with open(os.path.join(filepath, uploadedfile.name), "wb") as f:
        f.write(uploadedfile.getbuffer())
    return st.success(f"Saved File: {uploadedfile.name} to '{filepath}' in framework!")


def file_upload_all(uploaded_file, file_type, convention):

    if uploaded_file is not None:
        name_present = False
        tc_path = f"{core_path}/test/{file_type}"
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


def bulk_generation():

    st.set_page_config(
        page_title="Bulk Generator"
    )
    st.title("Bulk Test Configs SQL Generator")

    st.divider()
    testcase_file = st.file_uploader("Template Excel file",
                                     type='xlsx', accept_multiple_files=False)
    upl_bulk = file_upload_all(testcase_file, 'sqlbulk', 'bulk_')


if __name__ == "__main__":
    bulk_generation()
