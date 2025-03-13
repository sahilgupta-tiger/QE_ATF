import pathlib
import sys; sys.path.append(str(pathlib.Path(__file__).parent.parent))
import streamlit as st
from common.commonmethods import *


def load_home_page():

    st.set_page_config(
        page_title="Tiger QE DATF Accelerator",
        page_icon="✨",
    )

    st.write("# Welcome to Tiger QE DATF website! 👋")
    st.sidebar.success("Select an option page above")

    font_pixel = 14
    explain_text1 = f'<span style="font-size: {font_pixel}px;">:  A unified solution that automates the entire data validation process, ensuring accuracy, consistency, and reliability across all datasets.</span>'
    explain_text2 = f'<span style="font-size: {font_pixel}px;">:  Enables seamless execution of test scripts to validate data transformations between source and target systems.</span>'
    explain_text3 = f'<span style="font-size: {font_pixel}px;">:  Leverages Generative AI or native tools to create optimized SQL scripts for complex queries, reducing manual effort and improving efficiency.</span>'
    explain_text4 = f'<span style="font-size: {font_pixel}px;">:  Provides in-depth insights into source or target datasets by analyzing patterns, anomalies, and key metrics.</span>'
    explain_text5 = f'<span style="font-size: {font_pixel}px;">:  Automates the validation based on DQ rules that ensure datasets meet quality standards, preventing errors and enhancing trust in data-driven decisions.</span>'

    st.markdown(f"""
        <table style="margin-left: auto; margin-right: auto; border-collapse: collapse; border: none;">
            <tr><th style="text-align: center;"> Salient - Features </th></tr>
            <tr><td>Comprehensive Automated Data Validation Platform</td></tr>
            <tr><td>End-to-End Test Execution for Source-to-Target Mappings</td></tr>
            <tr><td>AI-Powered SQL Generation Tool</td></tr>
            <tr><td>Advanced Data Profiling Capabilities</td></tr>
            <tr><td>Robust Data Quality Checks for Source and Target Systems</td></tr>
        </table>
    """, unsafe_allow_html=True)

    st.markdown("""
        **👈 Select a page from the sidebar** to see some examples
        of what our Accelerator can do!
        """)

    st.divider()

    st.markdown("""    
        ### Contact Us
        - Reach us at [QE Core Team](mailto:sahil.gupta@tigeranalytics.com)
        
        
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
        
    """)

    create_execution_db()


if __name__ == "__main__":
    load_home_page()

