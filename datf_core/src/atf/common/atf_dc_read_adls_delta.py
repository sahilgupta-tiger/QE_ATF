from pyspark.sql.functions import *
from pyspark.sql.types import *
from atf.common.atf_common_functions import log_info, readconnectionconfig
from constants import *

def read_adls_deltadata(dict_configdf, spark):
  log_info("Reading delta Data from ADLS storage")
  resourcename = dict_configdf['filename']
  comparetype = dict_configdf['testquerygenerationmode']
  connectionname = dict_configdf['connectionname']
  connectionconfig = readconnectionconfig(connectionname)
  storage_account = connectionconfig['STORAGE_ACCOUNT_NAME']
  container_name = connectionconfig['CONTAINER_NAME']  # Assuming you have this in your config
  delta_path = dict_configdf['path']  # Relative path within the container
  sas_token = connectionconfig.get('SAS_TOKEN', None)  # Optional SAS token

  # Set the configuration using Service Principal
  spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
  spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", f"{connectionconfig['CLIENT_ID']}")
  spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", f"{connectionconfig['CLIENT_SECRET']}")
  spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{connectionconfig['TENANT_ID']}/oauth2/token")

  # Construct the full URL
  if sas_token:
    deltapath = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{delta_path}?{sas_token}"
  else:
    deltapath = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{delta_path}"

  if comparetype == 'Auto':
    datafilter = dict_configdf['filter']
    excludecolumns = dict_configdf['excludecolumns']
    excludecolumns = str(excludecolumns)
    exclude_cols = excludecolumns.split(',')
    datafilter = str(datafilter)

    descquery = f'DESCRIBE delta.`{deltapath}`;'
    col_df = spark.sql(descquery)
    col_df = col_df.filter(
      (col("col_name") != "") &
      (col("col_name") != "# Partitioning") &
      (~col("col_name").contains("Part ")) &
      (col("col_name") != "Not partitioned")
    )
    columns = list(col_df.select('col_name').toPandas()['col_name'])
    columnlist = list(set(columns) - set(exclude_cols))
    columnlist.sort()
    columnlist = ','.join(columnlist)
    query_delta = f"SELECT {columnlist} FROM delta.`{deltapath}`"

    if len(datafilter) >= 5:
      query_delta += f" WHERE {datafilter}"

    df_deltadata = spark.sql(query_delta)

  else:
    querypath = root_path + dict_configdf['querypath']
    query_delta = spark.read.text(querypath)
    df_deltadata = spark.sql(query_delta.head()[0])

  df_deltadata.printSchema()
  df_deltadata.show()
  log_info("Returning the DataFrame")

  return df_deltadata, query_delta