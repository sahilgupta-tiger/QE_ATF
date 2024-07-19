import os
import json
import streamlit as st
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI
from ..testconfig import decryptcredential


openai_json = json.load(open("../../../test/connections/azure_open_ai_connection.json"))
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


def load_first_page():

    st.set_page_config(
        page_title="QE SQL Generator",
        page_icon="👋",
    )

    st.write("# Welcome to Tiger QE *GenAI* SQL Generator! 👋")

    st.sidebar.success("Select an option page above.")

    st.markdown(
        """
        Our OpenAI powered SQL Generator along with PySpark technology helps you
        achieve the best of Data Testing and new Generative AI capabilities.
        **👈 Select a page from the sidebar** to see some examples
        of what the SQL Generator can do!
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
    """
    )


if __name__ == "__main__":
    load_first_page()

