# Databricks notebook source
dbutils.widgets.text('test_protocol_name', 'contenttestprotocol')
dbutils.widgets.dropdown("test_type", "count", ['count', 'null', ''duplicate', 'fingerprint', 'content', 'schema'])
dbutils.widgets.text('test_names', 'all')
dbutils.widgets.text('work_path', '/Workspace/Shared/QE_ATF/datf_core/')

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
install_path = f"{work_path}/scripts/requirements.txt"
%pip install -r $install_path
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
html_file_content = open(f"{work_path}utils/reports/datfreport.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
html_file_content = open(f"{work_path}utils/reports/datf_trends_report.html", 'r').read()
displayHTML(html_file_content)
