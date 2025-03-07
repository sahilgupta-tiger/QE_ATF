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

    st.markdown(f"""
        <table style="margin-left: auto; margin-right: auto; border-collapse: collapse; border: none;">
            <tr><th> Salient - Features </th></tr>
            <tr><td><b>Comprehensive Automated Data Validation Platform:</b><span style="font-size: {font_pixel}px;">  A unified solution that automates the entire data validation process, ensuring accuracy, consistency, and reliability across all datasets. This eliminates manual errors, enhances data quality, and streamlines operations for large-scale data management.</span></td></tr>
            <tr><td><b>End-to-End Test Execution for Source-to-Target Mappings:</b><span style="font-size: {font_pixel}px;">  Enables seamless execution of test scripts to validate data transformations between source and target systems. This ensures accurate data migration and integration while maintaining consistency across environments.</span></td></tr>
            <tr><td><b>AI-Powered SQL Generation Tool:</b><span style="font-size: {font_pixel}px;">  Leverages Generative AI or native tools to create optimized SQL scripts for complex queries, reducing manual effort and improving efficiency in database operations.</span></td></tr>
            <tr><td><b>Advanced Data Profiling Capabilities:</b><span style="font-size: {font_pixel}px;">  Provides in-depth insights into source or target datasets by analyzing patterns, anomalies, and key metrics. This supports better decision-making and ensures data readiness for downstream processes.</span></td></tr>
            <tr><td><b>Robust Data Quality Checks for Source and Target Systems:</b><span style="font-size: {font_pixel}px;">  Automates the application of predefined rules and validations to ensure datasets meet quality standards, preventing errors and enhancing trust in data-driven decisions.</span></td></tr>
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

