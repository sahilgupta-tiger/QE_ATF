import great_expectations as gx
def ge_test_initalization(df):
    df = df
    # Retrieve your Data Context
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
    return batch

def ge_test_execution(pdfobj,batch,rows):
    pdfobj = pdfobj
    batch = batch
    rows = rows
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
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_{dqtype_valid}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Length": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
        if check == "Length" and check_type == "Between":
            expectation  = gx.expectations.ExpectColumnValueLengthsToBeBetween(column=column, min_value = value.split("-")[0], max_value = value.split("-")[1])
            validation_results = batch.validate(expectation)
            print(validation_results)
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
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_{dqtype_valid}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Length Range": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
        if check == "Null":
            expectation  = gx.expectations.ExpectColumnValuesToBeNull(column=column)
            validation_results = batch.validate(expectation)
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = "'null'"
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Value": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
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
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Value": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
        if check == "Unique":
            expectation  = gx.expectations.ExpectColumnValuesToBeUnique(column=column)
            validation_results = batch.validate(expectation)
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = "to be unique"
            status = validation_results.success
            count = validation_results.result["element_count"]
            unexpected_count = validation_results.result["unexpected_count"]
            observed_value = validation_results.result["partial_unexpected_list"]
            columnname = validation_results.expectation_config.kwargs["column"]
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Value": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Duplicate Values": observed_value}
        if check == "DistinctSet":
            expectation  = gx.expectations.ExpectColumnDistinctValuesToEqualSet(column=column, value_set=value)
            validation_results = batch.validate(expectation)
            
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            count = None
            unexpected_count = None
            status = validation_results.success
            observed_value = validation_results.result["observed_value"][:50]
            columnname = validation_results.expectation_config.kwargs["column"]
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Value": expected, "Actual Value": observed_value}
        if check == "ColumnCount":
            status = validation_results.success
            expectation  = gx.expectations.ExpectTableColumnCountToEqual(value=value)
            validation_results = batch.validate(expectation)
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            count = None
            unexpected_count = None
            observed_value = validation_results.result["observed_value"]
            columnname = column
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "DQ Validation": Dqtype, "Expected Column Count": expected,  "Actual Column Count": observed_value}
        if check == "Regexp":
            print(value)
            regex_list = [item["Value"] for item in rows if item["DQ Check"] == "Regexp"]
            print(regex_list)
            expectation = gx.expectations.ExpectColumnValuesToMatchRegex(column=column,regex=value)
            validation_results = batch.validate(expectation)
            print(validation_results)
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
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "column Name": columnname, "DQ Validation": Dqtype, "Expected Pattern": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
        if check == "Regexplist":
            print(value)
            print(type(value))
            #regex_list = [item["Value"] for item in rows if item["DQ Check"] == "Regexplist"]
            #print(regex_list)
            expectation = gx.expectations.ExpectColumnValuesToMatchRegexList(column=column,regex_list=value,match_on = "any")
            validation_results = batch.validate(expectation)
            print(validation_results)
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
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "column Name": columnname, "DQ Validation": Dqtype, "Expected Pattern": expected,  "Element Count": count, "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
        if check == "Sum" and check_type == "Between":
            expectation  = gx.expectations.ExpectColumnSumToBeBetween(column=column, min_value = value.split("-")[0], max_value = value.split("-")[1])
            validation_results = batch.validate(expectation)
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            status = validation_results.success
            observed_value = validation_results.result["observed_value"]
            print(observed_value)
            columnname = validation_results.expectation_config.kwargs["column"]
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{columnname}_{Dqtype}_{dqtype_valid}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype, "Expected Sum Range": expected,  "Actual Sum": observed_value}
        if check == "ColumnOrder":
            expectation  = gx.expectations.ExpectTableColumnsToMatchOrderedList(column_list=value)
            validation_results = batch.validate(expectation)
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            status = validation_results.success
            observed_value = validation_results.result["observed_value"]
            mismatch = validation_results.result["details"]["mismatched"]
            if status:
                Result = "Passed"
            else:
                Result = "Failed"
            pdfobj.write_text(f"{i+1}.Testcase_{Dqtype}_Validation", 'section heading')
            dict_result = {"Test Status": Result, "DQ Validation": Dqtype, "Expected Column Order": expected,  "Actual Column Order": observed_value, "Column Order Mismatch": mismatch}
        pdfobj.create_table_summary(dict_result)
    return pdfobj