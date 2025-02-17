import pathlib
import sys; sys.path.append(str(pathlib.Path(__file__).parent.parent))
import streamlit as st
import openpyxl; openpyxl.reader.excel.warnings.simplefilter(action='ignore')
from common.commonmethods import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)


def load_home_page():

    st.set_page_config(
        page_title="Tiger QE DATF Accelerator",
        page_icon="✨",
    )

    st.write("# Welcome to Tiger QE DATF Tool with SQL Generator & Data Profiling! 👋")

    st.sidebar.success("Select an option page above")

    st.markdown(
        """
        Our OpenAI powered SQL Generator along with Data-validation Automated Testing 
        Framework (DATF) **Spark Edition**, which helps you achieve the best solution namely, 
        High Volume Big Data Validations using Spark technology (clusters or compute), 
        brand new GenAI LLM capabilities. 
        Additionally understanding the data using profiling methods and data quality validations.
        
        
        **👈 Select a page from the sidebar** to see some examples
        of what our Accelerator can do!
        
        
        ### Contact Us
        - Reach us at [QE Core Team](mailto:sahil.gupta@tigeranalytics.com)
        
        
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
        
    """
    )

    create_execution_db()


if __name__ == "__main__":
    load_home_page()

