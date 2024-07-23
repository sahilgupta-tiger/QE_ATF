import streamlit as st


def upload_new_test_page():

    st.set_page_config(
        page_title="Upload New TC"
    )
    st.title("Upload a new Test Config file")


if __name__ == "__main__":
    upload_new_test_page()
