"""
constants.py
"""
import pytz
from cryptography.fernet import Fernet

root_path = '/app/'
table_name = 'historical_trends'
utctimezone = pytz.timezone("UTC")
conn_file_name = "test\\connections\\raw_snowflake_sql_connection.json"


def decryptcredentials(enodedstring):
    cryptokey = b'K_QLpmYNUy6iHP4m73k2Q2brMfFy2nmJJK61HlSOTQI='
    encrypted = str.encode(enodedstring)
    fer = Fernet(cryptokey)
    decrypted = fer.decrypt(encrypted).decode('utf-8')
    return decrypted
