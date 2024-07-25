from datf_core.src.website.setpaths import *
import streamlit as st
from os import listdir
from os.path import isfile, join
import json
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI
from datf_core.src.testconfig import *


openai_json = json.load(open(f"{core_path}/test/connections/azure_open_ai_connection.json"))
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


def read_test_configs():
    tc_path = f"{core_path}/test/testcases"
    onlyfiles = [f for f in listdir(tc_path) if isfile(join(tc_path, f))]
    for loop in onlyfiles:
        if loop.find("template") != -1:
            onlyfiles.remove(loop)

    option = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", option)


def s2t_sql_generation():

    st.set_page_config(
        page_title="S2T Generator"
    )
    st.title("Source to Target SQL Generator")
    read_test_configs()


if __name__ == "__main__":
    s2t_sql_generation()
