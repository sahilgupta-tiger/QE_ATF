#!/usr/bin/env python
# encoding: utf-8
"""
constants.py
"""
import pytz
import os

root_path = os.getenv('CWD')
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
