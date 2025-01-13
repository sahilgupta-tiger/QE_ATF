import pathlib
import sys; sys.path.append(str(pathlib.Path(__file__).parent.parent))
import streamlit as st
import openpyxl; openpyxl.reader.excel.warnings.simplefilter(action='ignore')
from common.commonmethods import *


def load_home_page():

    st.set_page_config(
        page_title="TigerQE DATF SQL Generator",
        page_icon="✨",
    )

    st.write("# Welcome to Tiger QE DATF with *GenAI* SQL Generator! 👋")

    st.sidebar.success("Select an option page above")

    st.markdown(
        """
        Our OpenAI powered SQL Generator along with Data-validation Automated Testing 
        Framework (DATF) **Spark Edition** helps you achieve the best of both worlds namely, 
        High Volume Big Data Validations using Spark technology (clusters or compute) 
        and new Generative AI LLM capabilities.
        \n\n
        **👈 Select a page from the sidebar** to see some examples
        of what our Accelerator can do!
        \n\n
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
        
        ### Contact Us
        - Reach us at [QE Core Team](mailto:sahil.gupta@tigeranalytics.com)
    """
    )

    create_execution_db()


if __name__ == "__main__":
    load_home_page()

