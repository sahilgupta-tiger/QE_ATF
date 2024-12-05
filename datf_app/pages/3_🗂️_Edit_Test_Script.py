from os import listdir
from os.path import isfile, join
import os
import streamlit as st


def edit_test_cases():

    st.set_page_config(
        page_title="Edit Test Scripts"
    )
    st.title("Edit Existing Test Configurations")

    st.divider()
    st.markdown("**👈 Select the required page from the sidebar** to continue!")


if __name__ == "__main__":
    edit_test_cases()
