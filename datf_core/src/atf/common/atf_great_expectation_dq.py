import great_expectations as gx
import os
import pandas as pd
import json
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_pdf_constants import *

def dq_analyser(tcname,dejavu_path, rows, df):
    pdfobj = generatePDF(dejavu_path)
    pdfobj.write_text("Data Quality Analysis Report", 'report header')
    resultfolder =  dejavu_path + "/test/results/data_quality/"+tcname
    if not os.path.exists(resultfolder):
        log_info(f"Creating directory - {resultfolder}")
        os.mkdir(resultfolder)
    else:
        log_info(f"The directory - `{resultfolder}` already exists.")

    print(rows)
    for item in rows:
        if item["DQ Check"] == "Regexp" or item["DQ Check"] == "DistinctSet":
            item["Value"] = json.loads(item["Value"])
    context = gx.get_context()
    # Define the Data Source name
    data_source_name = "my_data_source"
    # Add the Data Source to the Data Context
    data_source = context.data_sources.add_spark(name=data_source_name)
    print(data_source)
    data_asset_name = "my_dataframe_data_asset"
    # Add a Data Asset to the Data Source
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)
    print(data_asset)
    batch_definition_name = "my_batch_definition"
    # Add a Batch Definition to the Data Asset
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )
    batch_parameters = {"dataframe": df}
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    for i,row in enumerate(rows):
        column = row["Column"]
        value = row["Value"]
        check = row["DQ Check"]
        check_type = row["Check Type"]
        print(column, value, check, check_type)
        if check == "Length" and check_type == "Equal":
            expectation  = gx.expectations.ExpectColumnValueLengthsToEqual(column=column, value = value)
            validation_results = batch.validate(expectation)
            #print(validation_results)
            status = validation_results.success
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
        if check == "Length" and check_type == "Between":
            expectation  = gx.expectations.ExpectColumnValueLengthsToBeBetween(column=column, min_value = value.split("-")[0], max_value = value.split("-")[1])
            validation_results = batch.validate(expectation)
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
        if check == "Null":
            expectation  = gx.expectations.ExpectColumnValuesToBeNull(column=column)
            validation_results = batch.validate(expectation)
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = "'null'"
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
        if check == "NotBeNull":
            expectation  = gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            validation_results = batch.validate(expectation)
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = "'not null'"
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
        if check == "Unique":
            expectation  = gx.expectations.ExpectColumnValuesToBeUnique(column=column)
            validation_results = batch.validate(expectation)
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = "to be unique"
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
        if check == "DistinctSet":
            expectation  = gx.expectations.ExpectColumnDistinctValuesToBeInSet(column=column, value_set=value)
            validation_results = batch.validate(expectation)    
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            count = None
            unexpected_count = None
            status = validation_results.success
            observed_value = validation_results.result["observed_value"][:50]
            columnname = validation_results.expectation_config.kwargs["column"]
        if check == "ColumnCount":
            status = validation_results.success
            expectation  = gx.expectations.ExpectTableColumnCountToEqual(value=value)
            validation_results = batch.validate(expectation)
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            count = None
            unexpected_count = None
            observed_value = validation_results.result["observed_value"]
            columnname = column
        if check == "Regexp":
            expectation = gx.expectations.ExpectColumnValuesToMatchRegexList(column=column,regex_list=value,match_on="any")
            validation_results = batch.validate(expectation)
            #print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
        if status:
            Result = "Passed"
        else:
            Result = "Failed"
        pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}", 'section heading')
        dict_result = {"Test_Status": Result, "column Name": columnname, "DQ Check": Dqtype, "Expected Value": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Observed Value": observed_value}
        pdfobj.create_table_summary(dict_result)
        pdfobj.write_text("Refer below for more information", 'section heading')
        pdfobj.create_table_summary(validation_results)
    timenow = datetime.now(utctimezone)
    created_time = str(timenow.astimezone(utctimezone).strftime("%d_%b_%Y_%H_%M_%S_%Z"))
    resultpath = resultfolder + "/data_quality_analysis_report_"+created_time+".pdf"
    pdfobj.pdf.output(resultpath, 'F')
