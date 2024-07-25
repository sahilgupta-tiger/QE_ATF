import setpaths
import streamlit as st


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
        Framework (DATF) helps you achieve the best of both worlds namely, 
        High Volume Data Validations using PySpark technology and new Generative AI capabilities.
        
        **👈 Select a page from the sidebar** to see some examples
        of what our Accelerator can do!
        
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
        
        ### Contact Us
        - Reach us at [QE Core Team](mailto:sahil.gupta@tigeranalytics.com)
    """
    )


if __name__ == "__main__":
    load_home_page()

