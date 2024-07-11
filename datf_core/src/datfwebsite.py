import sys

from datf_core.src.atf.common.atf_common_functions import log_info
from datf_core.src.constants import root_path
from datf_core.utils.create_db import create_db

if __name__ == "__main__":
    testcasesrunlist = []
    protocol_file_path = f"{root_path}test/testprotocol/testprotocol.xlsx"
    create_db(protocol_file_path)
    testtype = sys.argv[1]
    temporaryrunlist=sys.argv[2].rstrip()
    if "," in sys.argv[2]:
        testcasesrunlist = temporaryrunlist.split(",")
    else:
        testcasesrunlist.append(temporaryrunlist)
    log_info(f"Protocol Config path :{protocol_file_path}")
    log_info(f"TestType: {testtype}")
    log_info(f"TestCasesRunList: {testcasesrunlist}")
