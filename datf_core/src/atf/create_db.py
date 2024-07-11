import sqlite3
import pandas as pd
from tabulate import tabulate
from constants import *

# Establish connection with the database and create one if it does not exist
exec_db_name = 'DATF_EXECUTION'
conn = sqlite3.connect(f'{root_path}utils/{exec_db_name}.db')
cur = conn.cursor()


def import_excel_to_db(xls_file, tbl_name):
    df = pd.read_excel(xls_file, sheet_name=exec_sheet_name)
    df.to_sql(tbl_name, conn, if_exists='replace', index=False)
    # displaying the DataFrame
    print(tabulate(df, headers='keys', tablefmt='psql'))
    del df


def table_exists(table):
    cur.execute('''SELECT count(name) FROM sqlite_master
        WHERE TYPE = 'table' AND name = '{}' '''.format(table))
    if cur.fetchone()[0] == 1:
        return True
    return False


def create_db(excel_file):
    try:
        # Create table from Excel and save in DB file locally
        import_excel_to_db(excel_file, exec_table_name)
    finally:
        conn.commit()
        cur.close()
        conn.close()


"""
if __name__ == "__main__":
    protocol_file_path = "../test/testprotocol/testprotocol.xlsx"
    create_db(protocol_file_path)
"""

