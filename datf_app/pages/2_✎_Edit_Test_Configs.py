import streamlit as st
from datf_app.common.commonmethods import *


def edit_test_cases():

    st.set_page_config(
        page_title="Edit Test Configs"
    )
    st.title("Edit Existing Test Configurations")

    onlyfiles = read_test_protocol()
    selected_protocol = st.selectbox(
        "Choose one from Test Configs below...",
        onlyfiles, index=None, placeholder="type to search",
    )
    st.write("You selected: ", selected_protocol)

    if selected_protocol is not None:
        df = pd.read_sql_query(f"SELECT * FROM '{selected_protocol}'", conn_exe)

        # Load the Test Cases as an interactive table
        edited_df = st.data_editor(df, key='Sno.',
                           column_config={
                               "execute": st.column_config.CheckboxColumn(
                                   "Execute?",
                                   help="Select the test cases for execution.",
                                   default=False,
                               )
                           },
                           hide_index=True, use_container_width=True)

        edited_df.to_sql(con=conn_exe, name=selected_protocol, if_exists="replace", index=False)

        st.divider()
        st.markdown("**The Data Grid above will Auto-Save, if changes are made.**")


if __name__ == "__main__":
    edit_test_cases()
