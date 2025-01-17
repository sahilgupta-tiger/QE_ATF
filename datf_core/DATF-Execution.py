# Databricks notebook source
dbutils.widgets.text('test_protocol_name', 'traversedtestprotocollatin')
dbutils.widgets.dropdown("test_type", "content", ['null','count', 'duplicate', 'content'])
dbutils.widgets.text('test_names', 'all')
dbutils.widgets.text('work_path', '/Workspace/Shared/QE_ATF_Latest/datf_core')

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
install_path = f"{work_path}/scripts/requirements.txt"
%pip install -r $install_path


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

#python
import os
work_path = dbutils.widgets.get("work_path")
os.environ['CWD'] = work_path
py_file = f"{work_path}/src/s2ttester.py"
test_type = dbutils.widgets.get("test_type")
test_names = dbutils.widgets.get("test_names")
test_protocol_name = dbutils.widgets.get("test_protocol_name")
params = {
    "test_protocol_name": test_protocol_name,
    "test_type": test_type,
    "test_names": test_names
}

test_protocol = f"{work_path}/test/testprotocol/{test_protocol_name}.xlsx"

runner = f"{py_file} {test_protocol} {test_type} {test_names}"
%run $runner

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
html_file_content = open(f"{work_path}/utils/reports/datfreport.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
html_file_content = open(f"{work_path}/utils/reports/datf_trends_report.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

dbutils.fs.ls("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/patients_source_parquet")

# COMMAND ----------

'''import pyspark.sql.functions as F
df = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/patients_source_parquet/part-00000-2138c990-5aab-4a32-9f94-39ca44b8f791-c000.snappy.parquet")
print(df.count())
df.select("id").show(truncate=False)
df = df.withColumn(
    "DName",
    F.when(
        F.col("id") == "1d604da9-9a81-4ba9-80c2-de3375d59b40",
        F.lit("ACANA®Freeze+")
    ).when(
        F.col("id") == "034e9e3b-2def-4559-bb2a-7850888ae060",
        F.lit("Chefâ€™s")
    ).otherwise(F.col("FIRST"))
)
df.write.mode("overwrite").parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_source/")

df1 = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_source/")
print(df1.count())
df1.printSchema()'''


# COMMAND ----------

'''import pyspark.sql.functions as F
df = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/patients_source_parquet/part-00000-2138c990-5aab-4a32-9f94-39ca44b8f791-c000.snappy.parquet")
df.select("id").show(truncate=False)
df = df.withColumn(
    "DName",
    F.when(
        F.col("id") == "1d604da9-9a81-4ba9-80c2-de3375d59b40",
        F.lit("abcdtest")
    ).otherwise(F.col("FIRST"))
)
df.show()
df.printSchema()
df.write.mode("overwrite").parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_target/")

df1 = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_target/")
print(df1.count())
df1.printSchema()'''

