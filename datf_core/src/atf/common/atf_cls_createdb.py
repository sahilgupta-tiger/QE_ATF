import os
import sqlite3
import pandas as pd
from tabulate import tabulate
from constants import *


# Establish connection with the database and create one if it does not exist
conn = sqlite3.connect(f'{root_path}utils/{exec_db_name}.db')
cur = conn.cursor()


def import_excel_to_db(xls_file):
    writedb_df = pd.read_excel(xls_file, sheet_name=exec_sheet_name)
    writedb_df.to_sql(exec_table_name, conn, if_exists='replace', index=False)
    writedb_df['execute'].replace({'Y': True, 'N': False}, inplace=True)
    # displaying the DataFrame
    print(tabulate(writedb_df, headers='keys', tablefmt='psql'))
    del writedb_df


def export_db_to_excel(xls_file):
    readdb_df = pd.read_sql_table(exec_table_name, conn)
    readdb_df.to_excel(xls_file, sheet_name=exec_sheet_name)
    # displaying the DataFrame
    print(tabulate(readdb_df, headers='keys', tablefmt='psql'))
    del readdb_df


def table_exists(table):
    cur.execute('''SELECT count(name) FROM sqlite_master
        WHERE TYPE = 'table' AND name = '{}' '''.format(table))
    if cur.fetchone()[0] == 1:
        return True
    return False


def create_db(excel_file):
    # Create table from Excel and save in DB file locally
    import_excel_to_db(excel_file)
    conn.commit()


def create_sheet(excel_file):
    # Create table from Excel and save in DB file locally
    export_db_to_excel(excel_file)
    conn.commit()

"""
if __name__ == "__main__":
    protocol_file_path = "../test/testprotocol/testprotocol.xlsx"
    create_db(protocol_file_path)
"""


def change_permission(folder):
    for path_ in os.listdir(folder):
        abs_path = os.path.join(folder, path_)
        if os.path.isdir(abs_path):
            change_permission(abs_path)
        os.chmod(abs_path, 0o777)

