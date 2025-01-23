#!/usr/bin/env python
# encoding: utf-8
"""
testconfig.py
"""
import pytz
import os
from cryptography.fernet import Fernet

protocol_engine = "docker" # options: default, databricks, docker

if protocol_engine == "databricks":
    root_path = os.getenv('CWD')
elif protocol_engine == "docker":
    root_path = "datf_core/"
else:
    root_path = "datf_core/"


# *** DO NOT CHANGE BELOW VALUES ***
results_db_name = 'DATF_RESULTS'
rept_table_name = 'historical_trends'
exec_db_name = 'DATF_EXECUTION'
exec_table_name = 'testselection'
exec_sheet_name = 'protocoltestcasedetails'
protocol_tab_name = 'protocol'
utctimezone = pytz.timezone("UTC")
genai_conn_json = "azure_open_ai_connection"

spark_conf_JSON = """ {
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


def decryptcredential(encodedstring):
    cryptokey = b'K_QLpmYNUy6iHP4m73k2Q2brMfFy2nmJJK61HlSOTQI='
    encrypted = str.encode(encodedstring)
    fer = Fernet(cryptokey)
    decrypted = fer.decrypt(encrypted).decode('utf-8')
    return decrypted

