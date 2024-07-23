import os
import json
import streamlit as st
from cryptography.fernet import Fernet
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI


def decryptcredential(encodedstring):
    cryptokey = b'K_QLpmYNUy6iHP4m73k2Q2brMfFy2nmJJK61HlSOTQI='
    encrypted = str.encode(encodedstring)
    fer = Fernet(cryptokey)
    decrypted = fer.decrypt(encrypted).decode('utf-8')
    return decrypted


core_path = 'datf_core'
full_path = f'D:/My_Workspaces/GitHub/DATF_Other/Pyspark/QE_ATF/{core_path}'
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


def load_home_page():

    st.set_page_config(
        page_title="TigerQE DATF SQL Generator",
        page_icon="✨",
    )

    st.write("# Welcome to Tiger QE *GenAI* SQL Generator for DATF Accelerator! 👋")

    st.sidebar.success("Select an option page above.")

    st.markdown(
        """
        Our OpenAI powered SQL Generator along with PySpark technology helps you
        achieve the best of both worlds namely, Data-validation Automated Testing 
        Framework (DATF) and new Generative AI capabilities.
        
        **👈 Select a page from the sidebar** to see some examples
        of what the SQL Generator can do!
        
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
    """
    )


if __name__ == "__main__":
    load_home_page()

