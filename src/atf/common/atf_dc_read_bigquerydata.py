# Databricks notebook source
# DBTITLE 1, Import Required Libraries
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info
import os

# COMMAND ----------

# DBTITLE 1,Load Common Functions notebook
# MAGIC %run ./atf_common_functions

# COMMAND ----------

# DBTITLE 1,Function to read Big Query data
def read_bigquerydata(tc_datasource_config, spark):
  log_info("Reading from Big Query Table")

  connectionname = tc_datasource_config['connectionname']
  connectiontype = tc_datasource_config['connectiontype']
  resourceformat = tc_datasource_config['format']
  resourcename = tc_datasource_config['filename']

  bqprojecttable = resourcename.split(".")

  datafilter = tc_datasource_config['filter']
  excludecolumns = tc_datasource_config['excludecolumns']
  excludecolumns = str(excludecolumns)
  exclude_cols = excludecolumns.split(',')
  datafilter = str(datafilter)
  selectallcolqry = f"SELECT * FROM {resourcename} "
  if len(datafilter) > 0:
    selectallcolqry = selectallcolqry + datafilter

  # Authenticating to BigQuery GCP project using Base64 string of the Service Account JSON key
  bqcredentials = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiaW5kaWdvLWFsbWFuYWMtMzg0MDExIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiNDc5ODdkZWY4Yzg5NGVkNjljNTg5N2E1ZGFjN2I3NGQ0Y2E0NzQzOCIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZnSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2d3Z2dTa0FnRUFBb0lCQVFDbWxIN1d2M3A1dHRqYVxucjQ3Si9CVFFwM012ZXdSR2hZQnNZNnJ6ZEZhbXJwSjVXcWVCM01iWVljZGZ5OTZIdHFGV1Y4UXBKb3d4WXRwL1xuejFUTkxmVGxPZFRMYnNTenQ0RnBUTFBia0pwMWsxeUlyZ2UzbjZwQmxpdDNjUTZzZ1puYVJqQjNTbFhZd2gxYlxubWtTaGI3cjg5cCtJNUdOaWNhWmxyMEZPSnZWbkRWQnpCTGhxcVdhdGR5T2oza05QQ2JLYWFnYWRBWVR2dEx0TVxuNis1Uk4wNE0xaVVrTEFPeXVjcGVERHNlQUVMQ2F2dnkvZzkvcDU5SEduRUNRR2dYYnFBWFZ1V1BqTzU1ZmVxc1xuS3BmeHVuMXpKNDZCUy80dkZMck91MWg0U1FXNms5U05Fa2tJYmkwYVpmS2hHNXk0RGpaNklsYndXa3o3NnFLOFxuL0dzS2NiLzNBZ01CQUFFQ2dnRUFGbzZVTUd0RVgvZjVISC9hWTVLZGxQV2YzT1daR2gyWTJ0U1NRUjVYWDRIVFxuTnRpbVJ5a0l5aU5QZHJMcUl0R0J2bW5yRjdGdkdYN3IzSlBNUlc1OE01QWx0eGg4VFdQRGcrbE5qNlZnclJ3elxuMHhSUjJqVW1QblJoMVc0N1VQUEp6Q3RtQTdsT3I2ZjFoc0o0bWEzQ0VlQXBrVVZ3Q2RRZTlyVUpNRmFKTTVkN1xuZVhXdzR0eXBBV1FlVUE4WEplYnNBSkNUN01XZGZQRHA3SWdZaG1GUVlQdXpaSGVVOStJcy9LV2lwYXcrRkI5NlxuV0FKSFBmMW4wTytKbFRYbFFxeEx2WnVldVBZd0cvL3Q1cmszeEZ6ZVBzZGUyVktvdVI5c3NxbGswZ1IvckNIb1xud1FqbEtPWDczeGw2TTIwUk1mLzV6c05EY21TaE5ZeWcxb25FcVRlR2tRS0JnUURZS3dqZk5sT01XTDd0SWRTYlxuVlU3S01YZzdrZFlkaHZDSFFoRnlNenFYeDlraU1kY3hFcEtnbEdkdW9walFicU5Cc1c0eXM1UnY2dm9MZHY2MlxuREVoVURzZ2RNK1NaMTlRQWlSTEhMSWRFcjhPc2NScjl6UGIyZC9aS01CREZ6SGVyMHhLeTZWbnhEcjdiOVBZQVxuRjdJV3JEUk5MWnk3d1FucTQrS0JRWEllY1FLQmdRREZSbElBNlE3K3FYQlNNaGVHZzNpWGtVVVdtUE1zbW9laFxuRGo0TUxhQndPSnhkUHhCMVBFUmpnQ2M4S3VWcHNRbzRYQnlnaXdrcWx4OU15VTUyQXZoTnJqTVJ5UElpQUFqYVxuR0FxVFBPeWxLalg3K2ZiOVRZOEo1bmVINDA0R0JEOWE5dllwa0JUeXErbzB4dG5xSXJMZkxsR3JKMGlQNll5YVxudktmQzE3REk1d0tCZ1FETERibWF1dFJHUEVLMFN4Z0VCV0VxYXNldTVUSEVzTEdWekVqSkJqZ2V4UHBBdGFLWFxuRlFRbzFONUh2WEpnTDkwbzR6dEpZd2luRElsdVpZdWpnK3daRVgyR2VPMWVXYTlxQlZoZlFZT2EwS08wZ3RaQVxuMjl5TDdtbXFqSFhrNTBqdVhTbnVaMlkyR296TDN0R0ppcmlkdFlxM2lJWDJrOEQySkxQcWlJaGk4UUtCZ1FDblxueU5OZUt0cXlCZktlTmpTRUY3V3RVOFRySDFOVUVHcGl0UFpZN1VXVHJOSnhESDQyOS9kQ21YREZYOGFYU2hoalxuTHhTNkJ3Yll6b2UwaTFMTVZHSlR6MkZvTXZ5ZHBtSGcvS21oNlNOc3BxWWJsZEpZUm1ydUJMS05ST3JMZXpsWFxuYU15ZE5RcGNDNU83SjVXWDZjTXd4dmgzRllaRndHSHlDWnExd1RWKzZRS0JnRWlqQTNhcFpsTlQ1L1FWRXQ3SlxuRXh4TUl2RTkzTFRWRVBZRTk2RDJxQUZWQitLNnNZNGszU1JxSVZ3b2hmSyt6ajVmUWhGNEtYbHQzYXdnWm9EY1xua3FjYmNVK1RYcDY2U2htQnVwMC9FYXd4dE1XWGl0VW13K0lmV2pwMFFIRTJEWmVncHRwOGFHQmNzRU9SYk5OZVxucGd4M2FZRHVySlFkSDRQVjBCTFhxejhXXG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiaW5kaWdvLWFsbWFuYWMtMzg0MDExQGFwcHNwb3QuZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICIxMTA4MzI1Njg3OTE2NTYzOTA2NzIiLAogICJhdXRoX3VyaSI6ICJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20vby9vYXV0aDIvYXV0aCIsCiAgInRva2VuX3VyaSI6ICJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsCiAgImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLAogICJjbGllbnRfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9yb2JvdC92MS9tZXRhZGF0YS94NTA5L2luZGlnby1hbG1hbmFjLTM4NDAxMSU0MGFwcHNwb3QuZ3NlcnZpY2VhY2NvdW50LmNvbSIKfQo="
  
  df_bigquerydata = (spark.read
                    .format("bigquery")
                    .option("parentProject", bqprojecttable[0])
                    .option("credentials", bqcredentials)
                    .option("project", bqprojecttable[0])
                    .option("dataset", bqprojecttable[1])
                    .option("table", f"{bqprojecttable[1]}.{bqprojecttable[2]}")
                    .load())

  columns = df_bigquerydata.columns
  columnlist = list(set(columns) - set(exclude_cols))
  columnlist.sort()
  
  columnlist = ','.join(columnlist)

  df_bigquerydata.createOrReplaceTempView("bigqueryview")
  selectcolqry = "SELECT " + columnlist + " FROM bigqueryview"
  selectcolqry_ret = "SELECT " + columnlist + f" FROM {resourcename}"
  df_out = spark.sql(selectcolqry)
  df_out.printSchema()
  df_out.show()
  log_info("Returning the DataFrame from read_biquerydata Function")
  return df_out, selectcolqry_ret