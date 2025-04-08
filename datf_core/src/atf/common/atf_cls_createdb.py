import os
import sqlite3
import pandas as pd
import re
import stat
from testconfig import *


# Establish connection with the database and create one if it does not exist
sqllite_db_name = f'{root_path}utils/{exec_db_name}.db'


def import_excel_to_db(xls_file):
    with sqlite3.connect(sqllite_db_name) as conn:
        writedb_df = pd.read_excel(xls_file, sheet_name=exec_sheet_name)
        writedb_df['execute'].replace({'Y': True, 'N': False}, inplace=True)
        writedb_df.to_sql(exec_table_name, conn, if_exists='replace', index=False)
        # displaying the DataFrame
        # print(tabulate(writedb_df, headers='keys', tablefmt='psql'))
    del writedb_df


def export_db_to_excel(xls_file):
    with sqlite3.connect(sqllite_db_name) as conn:
        readdb_df = pd.read_sql_table(exec_table_name, conn)
        readdb_df.to_excel(xls_file, sheet_name=exec_sheet_name)
        # displaying the DataFrame
        # print(tabulate(readdb_df, headers='keys', tablefmt='psql'))
    del readdb_df


def table_exists(table):
    with sqlite3.connect(sqllite_db_name) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(name) FROM sqlite_master WHERE TYPE = 'table' "
                        f"AND name = '{re.sub('[^a-zA-Z]+', '', table)}';")
            result = cur.fetchone()[0]

    if result == 1:
        return True
    return False


def create_db(excel_file):
    with sqlite3.connect(sqllite_db_name) as conn:
        # Create table from Excel and save in DB file locally
        import_excel_to_db(excel_file)
        conn.commit()


def create_sheet(excel_file):
    with sqlite3.connect(sqllite_db_name) as conn:
        # Create table from Excel and save in DB file locally
        export_db_to_excel(excel_file)
        conn.commit()


def change_permission(folder):
    for path_ in os.listdir(folder):
        abs_path = os.path.join(folder, path_)
        if os.path.isdir(abs_path):
            change_permission(abs_path)
        os.chmod(abs_path, stat.S_IRWXU | stat.S_IRWXG)

