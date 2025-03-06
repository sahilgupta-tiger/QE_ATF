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

    st.markdown("""
        <table style="margin-left: auto; margin-right: auto; border-collapse: collapse; border: none;">
            <tr><th> Salient - Features </th></tr>
            <tr><td><li> Execute Test Scripts across Source to Target mappings </li></td></tr>
            <tr><td><li> SQL Generation with GenAI Assistance or Native Tool </li></td></tr>
            <tr><td><li> Data Profiling for Source or Target </li></td></tr>
            <tr><td><li> Apply Data Quality checks on Source or Target </li></td></tr>
        </table>
    """, unsafe_allow_html=True)

    st.divider()

    st.markdown(
        """
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

