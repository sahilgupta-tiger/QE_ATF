#!/usr/bin/env python
# encoding: utf-8
"""
constants.py
"""
import pytz
from cryptography.fernet import Fernet

# root_path = '/Workspace/Repos/<user>/QE_ATF/datf_core/'
root_path = '/app/'
# root path must be CWD path
protocol_location = f"{root_path}test/testprotocol/testprotocol.xlsx"
conn_file_name = f"{root_path}test/connections/raw_snowflake_sql_connection.json"

table_name = 'historical_trends'
utctimezone = pytz.timezone("UTC")

conf_JSON = """ {
    "spark.executor.instances": "18",
    "spark.executor.cores": "8",
    "spark.executor.memory": "6g",
    "spark.default.parallelism": "56",
    "spark.sql.shuffle.partitions": "250",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "2g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.6",
    "spark.sql.debug.maxToStringFields": "300",
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    "spark.sql.autoBroadcastJoinThreshold": "-1"
} """


def decryptcredentials(enodedstring):
    cryptokey = b'K_QLpmYNUy6iHP4m73k2Q2brMfFy2nmJJK61HlSOTQI='
    encrypted = str.encode(enodedstring)
    fer = Fernet(cryptokey)
    decrypted = fer.decrypt(encrypted).decode('utf-8')
    return decrypted