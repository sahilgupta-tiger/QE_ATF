# Databricks notebook source
dbutils.widgets.text('test_protocol_name', 'traversedtestprotocol')
dbutils.widgets.dropdown("test_type", "content", ['count', 'duplicate', 'content'])
dbutils.widgets.text('test_names', 'all')
dbutils.widgets.text('work_path', '/Workspace/Shared/QE_ATF_Enhanced_03')

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
install_path = f"{work_path}/datf_core/scripts/requirements.txt"
%pip install -r $install_path

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

#python
import os
work_path = dbutils.widgets.get("work_path")
os.environ['CWD'] = work_path
py_file = f"{work_path}/datf_core/src/s2ttester.py"
test_type = dbutils.widgets.get("test_type")
test_names = dbutils.widgets.get("test_names")
test_protocol_name = dbutils.widgets.get("test_protocol_name")
params = {
    "test_protocol_name": test_protocol_name,
    "test_type": test_type,
    "test_names": test_names
}
test_protocol = f"{work_path}/datf_core/test/testprotocol/{test_protocol_name}.xlsx"
runner = f"{py_file} {test_protocol} {test_type} {test_names}"
%run $runner

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
html_file_content = open(f"{work_path}/datf_core/utils/reports/datfreport.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

work_path = dbutils.widgets.get("work_path")
html_file_content = open(f"{work_path}/datf_core/utils/reports/datf_trends_report.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

pip install cryptography

# COMMAND ----------

from cryptography.fernet import Fernet

key = b'K_QLpmYNUy6iHP4m73k2Q2brMfFy2nmJJK61HlSOTQI='
f = Fernet(key)

def createencryptedpasscode(inputtext):
    bytecode = str.encode(inputtext) # convert string to bytes
    token = f.encrypt(bytecode)
    print("Encrypted Passcode: " + str(token, 'utf-8'))
    return token

#gettext = input("Enter password for encryption: ")
encryptedtoken = createencryptedpasscode("JiDNSpow9283HJ!w")
print("Passing the generated token to decryption method...")
decrypted = f.decrypt(encryptedtoken).decode('utf-8')
print("Decrypted Passcode: " + decrypted)

# COMMAND ----------

def decryptcredential(encodedstring):
    cryptokey = b'K_QLpmYNUy6iHP4m73k2Q2brMfFy2nmJJK61HlSOTQI='
    encrypted = str.encode(encodedstring)
    fer = Fernet(cryptokey)
    decrypted = fer.decrypt(encrypted).decode('utf-8')
    print(decrypted)

decryptcredential("gAAAAABn74sZLTNfU-alTQBlVKZqV8PzizIfeCiq0P9bpXaBtq8pPXJ_bx8e5hvd8ROI1-woVWsQxbd91EAGMRBeuVkrcoAkIiz4-p-QKwdQ-vpKSUpOkaI=")
