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

runner = f"{py_file} {test_protocol} {test_type} {test_names} {work_path}"
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

'''import pyspark.sql.functions as F
df = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/patients_source_parquet/part-00000-2138c990-5aab-4a32-9f94-39ca44b8f791-c000.snappy.parquet")
print(df.count())
df.select("id").show(truncate=False)
df = df.withColumn(
    "DName",
    F.when(
        F.col("id") == "1d604da9-9a81-4ba9-80c2-de3375d59b40",
        F.lit("Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)")
    ).when(
        F.col("id") == "034e9e3b-2def-4559-bb2a-7850888ae060",
        F.lit("Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)")
    ).otherwise(F.col("FIRST"))
)
df.write.mode("overwrite").parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_source/")

df1 = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_source/")
print(df1.count())
df1.printSchema()'''
#◆ � Tin 🍒 ✔ Sign Beware Ç 𝐃 Dog Sign 8 ⧫🌟


# COMMAND ----------

pip install pydeequ

# COMMAND ----------

print("HI")

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/patients_source_parquet/part-00000-2138c990-5aab-4a32-9f94-39ca44b8f791-c000.snappy.parquet")
df.select("id").show(truncate=False)
df = df.withColumn(
    "DName",
    F.when(
        F.col("id") == "1d604da9-9a81-4ba9-80c2-de3375d59b40",
        F.lit("Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible  Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)")
    ).otherwise(F.col("FIRST"))
)
df.show()
df.printSchema()
df.write.mode("overwrite").parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_target/")

df1 = spark.read.parquet("file:/Workspace/Shared/QE_ATF_Latest/datf_core/test/data/source/latin_target/")
print(df1.count())
df1.printSchema()


# COMMAND ----------

from pydeequ.analyzers import AnalysisRunner, Size, Completeness
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
analysisResult = AnalysisRunner(spark) \
                    .onData(df1) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("CITY")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()


# COMMAND ----------

from pydeequ import ColumnProfilerRunner
os.environ["SPARK_VERSION"] = "3.5"
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Data Profiling with PyDeequ") \
    .getOrCreate()
result = ColumnProfilerRunner(spark).onData(df1).run()

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Data Profiling with PyDeequ") \
    .getOrCreate()
spark_version = spark.version



# COMMAND ----------



import os
from pyspark.sql import SparkSession


# Initialize the Spark session
os.environ["SPARK_VERSION"] = "3.3"
from pydeequ.profiles import *
spark = SparkSession.builder \
    .appName("Deequ Profiling") \
    .getOrCreate()

# Assuming df1 is a valid DataFrame
result = ColumnProfilerRunner(spark).onData(df1).run()

for col, profile in result.profiles.items():
    print(profile)

# COMMAND ----------

from pydeequ import ColumnProfilerRunner

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Deequ Profiling") \
    .getOrCreate()

# Check if the ColumnProfilerRunner can be instantiated properly
try:
    profiler = ColumnProfilerRunner(spark)
    print(f"Successfully created ColumnProfilerRunner instance: {profiler}")
except Exception as e:
    print(f"Error instantiating ColumnProfilerRunner: {e}")

# COMMAND ----------

java -version

# COMMAND ----------

pip show pydeequ


# COMMAND ----------


pip install pyspark

# COMMAND ----------

pip install pandas-profiling

# COMMAND ----------


from pandas_profiling import ProfileReport
import pandas as pd
from pyspark.sql import DataFrame
profile = ProfileReport(df1, title="Data Profiling Report", explorative=True)

# Save the report as an HTML file
profile.to_file("/Workspace/Shared/QE_ATF_Latest/datf_core/data_profiling_report.html")
#summary = dbutils.data.summarize(df1)
#print(summary)
#summary_df = pd.DataFrame(summary)
#print(summary_df)

#summary_df.to_html('/Workspace/Shared/QE_ATF_Latest/datf_core/summary_report.html', index=False)

# COMMAND ----------

pip install sweetviz

# COMMAND ----------

import sweetviz as sv

# Convert Spark DataFrame to Pandas DataFrame
df1_pandas = df1.toPandas()

# Analyze the Pandas DataFrame
report = sv.analyze([df1_pandas, "Sample Dataset"])

# Generate the report as an HTML file
report.show_html('/Workspace/Shared/QE_ATF_Latest/datf_core/data_profiling_report.html')

# COMMAND ----------

result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

for col, profile in result.profiles.items():
    print(profile)

# COMMAND ----------

pip install reportlab

# COMMAND ----------


from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# Summarize the dataframe
summary_df = df1.describe().toPandas()

# Create a PDF file
pdf_path = "/Workspace/Shared/QE_ATF_Latest/datf_core/summary_report.pdf"
c = canvas.Canvas(pdf_path, pagesize=letter)

# Define some text formatting
c.setFont("Helvetica", 10)

# Write summary into the PDF
text = "Summary Report\n\n"
for index, row in summary_df.iterrows():
    text += f"{index}\n"
    for column, value in row.items():
        text += f"  {column}: {value}\n"

c.drawString(100, 750, text)

# Save the PDF
c.save()

# COMMAND ----------

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# Summarize the dataframe
summary = dbutils.data.summarize(df1)

# Create a PDF file
pdf_path = "/Workspace/Shared/QE_ATF_Latest/datf_core/summary_report.pdf"
c = canvas.Canvas(pdf_path, pagesize=letter)

# Define some text formatting
c.setFont("Helvetica", 10)

# Write summary into the PDF
text = "Summary Report\n\n"
for column, value in summary.items():
    text += f"{column}: {value}\n"

c.drawString(100, 750, text)

# Save the PDF
c.save()

# COMMAND ----------

dbutils.fs.ls("/Workspace/Shared/QE_ATF_Latest/datf_core/dejavu/DejaVuSansCondensed.ttf")

# COMMAND ----------

'''from fpdf import FPDF
pdf = FPDF(format='A4', unit='mm')
pdf.add_page()
pdf.add_font('DejaVu', '', '/Workspace/Shared/QE_ATF_Latest/datf_core/dejavu/DejaVuSansCondensed.ttf', uni=True)'''

# COMMAND ----------

pip install fpdf

# COMMAND ----------

'''from fpdf import FPDF

# Create PDF instance
pdf = FPDF()

# Add a page
pdf.add_page()

# Set font
pdf.set_font('Arial', size=12)


col_widths = [40, 40, 60, 60]

# Row height
row_height = 10
data = [
    ["Value 1", "Value 2", "Very long text that should wrap inside column 3", "Very long text that should wrap inside column 4"],
    ["Value 1", "Value 2", "Another large value that needs to wrap inside column 3", "Another large value to wrap in column 4"]
]

# Add data rows
for row in data:
    for i in range(4):
        pdf.multi_cell(col_widths[i], row_height, row[i], border=1, align='L')

# Output the PDF
pdf.output("/Workspace/Shared/QE_ATF_Latest/datf_core/example_rect.pdf")'''

# COMMAND ----------

td = [['id=034e9e3b-2def-4559-bb2a-7850888ae060', 'Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)', 'Milo271'], ['id=1d604da9-9a81-4ba9-80c2-de3375d59b40', 'Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4) Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)', 'Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible with Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)Community Coffee Vanilla Waffle Cone 96 Count Coffee Pods, Ice Cream Flavored, Compatible  Keurig 2.0 K-Cup Brewers, 24 Count (Pack of 4)']]
td1 = b = [[None for _ in row] for row in td]
print(td1)
for i,row in enumerate(td):
    for j,data in enumerate(row):
        if(j==1 and len(str(data)))>200:
            data = str(data)[:200] + " NOTE"
        td1[i][j] = data
print(td1)
        
        


# COMMAND ----------

pip install sweetviz
