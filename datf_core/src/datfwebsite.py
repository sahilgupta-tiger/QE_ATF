from tabulate import tabulate
import sqlite3
import pandas as pd
from atf.common.atf_common_functions import log_info
from constants import *

conn = sqlite3.connect('../utils/DATF_EXECUTION.db')
cur = conn.cursor()

protocol_file_path = "../test/testprotocol/testprotocol.xlsx"
selection_file_path = "../test/testprotocol/testselection.xlsx"
df = pd.read_excel(protocol_file_path, sheet_name=exec_sheet_name)


def import_excel_to_db():

    df.to_sql(exec_table_name, conn, if_exists='replace', index=False)
    # displaying the DataFrame
    print(tabulate(df, headers='keys', tablefmt='psql'))


def export_db_to_excel(xls_file):
    # readdb_df = pd.read_sql_table(exec_table_name, conn)
    df.to_excel(xls_file, sheet_name=exec_sheet_name, index=False)
    # displaying the DataFrame
    print(tabulate(df, headers='keys', tablefmt='psql'))


if __name__ == "__main__":
    import_excel_to_db()
    log_info(f"Protocol Config path :{protocol_file_path}")
    log_info(f"Test Selection path :{selection_file_path}")
    export_db_to_excel(selection_file_path)
