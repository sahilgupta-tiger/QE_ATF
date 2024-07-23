from datf_core.src.website.setpaths import *
import streamlit as st
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


def bulk_generation():

    st.set_page_config(
        page_title="Bulk Generator"
    )
    st.title("Bulk Test Configs SQL Generator")


if __name__ == "__main__":
    bulk_generation()
