import os
import streamlit as st
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI


os.environ["AZURE_OPENAI_API_KEY"] = "4fed2bedb59744a99b0424622f6d9d1b"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://qepracticekey.openai.azure.com/"
openai_api_version="2023-05-15"
azure_deployment="qepracticekey"


def get_queries_from_ai(prompt):

    model = AzureChatOpenAI(
        openai_api_version=openai_api_version,
        azure_deployment=azure_deployment,
    )
    message = HumanMessage(
        content=prompt
    )
    output_value=model([message])
    return(output_value.content)


if __name__ == "__main__":
    get_queries_from_ai("")

