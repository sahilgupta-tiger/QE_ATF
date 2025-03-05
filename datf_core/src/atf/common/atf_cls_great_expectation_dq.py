import great_expectations as gx
import ast
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType


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


def ge_test_execution(pdfobj, pdfobj_summary, testsuite, batch, rows, spark,createdate,tc_data):
    src_info = tc_data[0]
    tgt_info = tc_data[1]
    print(src_info)
    print(tgt_info)
    utctimezone = timezone.utc
    DQValidation_starttime = datetime.now(utctimezone)
    pdfobj = pdfobj
    batch = batch
    rows = rows
    ftccount = 0
    ptccount = 0
    df_testsuite_summary = []
    df_testsuite_summary = pd.DataFrame(
        columns=['Testcase Name', 'ColumnName', 'Data Quality Check', 'Test Result', 'Reason', 'ExecutionTime'])
    tccount = len(rows)
    pdfobj.write_text("1. Configuration Details",'section heading' )
    pdfobj.write_text("1.1. Source Configuration Details", 'section sub heading')
    pdfobj.create_table_summary(src_info)
    pdfobj.write_text("1.2. Target Configuration Details", 'section sub heading')
    pdfobj.create_table_summary(tgt_info)
    pdfobj.write_text("2. Testcase Details", 'section heading')
    for i, row in enumerate(rows):

        column = row["Column"]
        value = row["Value"]
        check = row["DQ Check"]
        check_type = row["Check Type"]
        print(column, value, check, check_type)
        if check == "Length" and check_type == "Equal":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnValueLengthsToEqual(column=column, value=value)
            validation_results = batch.validate(expectation)
            # print(validation_results)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = f"{unexpected_count} - No of values are not having length as {expected}"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_{dqtype_valid}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Length": expected, "Element Count": count, "Unexpected Count": unexpected_count,
                           "Sample Mismatches": observed_value}
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))

        if check == "Length" and check_type == "Between":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnValueLengthsToBeBetween(column=column,
                                                                              min_value=value.split("-")[0],
                                                                              max_value=value.split("-")[1])
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = f"{unexpected_count} - No of values are not having length as {expected}"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_{dqtype_valid}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Length Range": expected, "Element Count": count,
                           "Unexpected Count": unexpected_count, "Sample Mismatches": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "Null":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnValuesToBeNull(column=column)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = f"{unexpected_count} - No of values are not having null value"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Value": expected, "Element Count": count, "Unexpected Count": unexpected_count,
                           "Sample Mismatches": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]

        if check == "NotBeNull":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            validation_results = batch.validate(expectation)
            # print(validation_results)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = f"{unexpected_count} - No of values are having null value"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Value": expected, "Element Count": count, "Unexpected Count": unexpected_count,
                           "Sample Mismatches": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "Unique":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnValuesToBeUnique(column=column)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = f"{unexpected_count} - No of values are duplicated"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Value": expected, "Element Count": count, "Unexpected Count": unexpected_count,
                           "Sample Duplicate Values": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "DistinctSet":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnDistinctValuesToEqualSet(column=column, value_set=value)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = "Actual distinct values are not macthing with Expected distinct values in the column"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Distinct Value": expected, "Actual Distinct Value": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "ColumnCount":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectTableColumnCountToEqual(value=value)
            validation_results = batch.validate(expectation)
            print(validation_results)
            Dqtype = check
            dqtype_valid = check_type
            expected = value
            count = None
            unexpected_count = None
            status = validation_results.success
            observed_value = validation_results.result["observed_value"]
            columnname = column
            if status:
                Result = "Passed"
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = "Actual column count is not matching with Expected count"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "DQ Validation": Dqtype, "Expected Column Count": expected,
                           "Actual Column Count": observed_value}
            #df_testsuite_summary.append((tcname, '', Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, '', Dqtype, Result, Reason, testcase_exectime]
        if check == "Regexp":
            testcase_starttime = datetime.now(utctimezone)
            print(value)
            regex_list = [item["Value"] for item in rows if item["DQ Check"] == "Regexp"]
            print(regex_list)
            expectation = gx.expectations.ExpectColumnValuesToMatchRegex(column=column, regex=value)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = "Column has some other patterns apart from the expected one"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Pattern": expected, "Element Count": count, "Unexpected Count": unexpected_count,
                           "Sample Mismatches": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "Regexplist":
            testcase_starttime = datetime.now(utctimezone)
            print(value)
            print(type(value))
            reg_list = ast.literal_eval(value)
            print(type(reg_list))
            # regex_list = [item["Value"] for item in rows if item["DQ Check"] == "Regexplist"]
            # print(regex_list)
            expectation = gx.expectations.ExpectColumnValuesToMatchRegexList(column=column, regex_list=reg_list,
                                                                             match_on="any")
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = "Column has some other patterns apart from the expected one"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Pattern": expected, "Element Count": count, "Unexpected Count": unexpected_count,
                           "Sample Mismatches": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "Sum" and check_type == "Between":
            testcase_starttime = datetime.now(utctimezone)
            expectation = gx.expectations.ExpectColumnSumToBeBetween(column=column, min_value=value.split("-")[0],
                                                                     max_value=value.split("-")[1])
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = "Actual Sum is not matching with the range"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{columnname}_{Dqtype}_{dqtype_valid}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "Column Name": columnname, "DQ Validation": Dqtype,
                           "Expected Sum Range": expected, "Actual Sum": observed_value}
            #df_testsuite_summary.append((tcname, column, Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, column, Dqtype, Result, Reason, testcase_exectime]
        if check == "ColumnOrder":
            testcase_starttime = datetime.now(utctimezone)
            col_list = ast.literal_eval(value)
            print(type(col_list))
            expectation = gx.expectations.ExpectTableColumnsToMatchOrderedList(column_list=col_list)
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
                ptccount = ptccount + 1
                Reason = "Matched"
            else:
                Result = "Failed"
                ftccount = ftccount + 1
                Reason = "Actual Column Order is not matching with the expected order"
            testcase_endtime = datetime.now(utctimezone)
            testcase_exectime = testcase_endtime - testcase_starttime
            testcase_exectime = str(testcase_exectime).split('.')[0]
            tcname = f"2.{i + 1}. Testcase_{Dqtype}_Validation"
            pdfobj.write_text(tcname, 'section heading')
            dict_result = {"Test Status": Result, "DQ Validation": Dqtype, "Expected Column Order": expected,
                           "Actual Column Order": observed_value, "Column Order Mismatch": mismatch}
            #df_testsuite_summary.append((tcname, '', Dqtype, Result, Reason, testcase_exectime))
            df_testsuite_summary.loc[i] = [tcname, '', Dqtype, Result, Reason, testcase_exectime]
        pdfobj.create_table_summary(dict_result)
    DQValidation_endtime = datetime.now(utctimezone)
    dqvalidation_exectime = DQValidation_endtime - DQValidation_starttime
    dqvalidation_exectime = str(dqvalidation_exectime).split('.')[0]
    protocol_run_params = {"Application Name": "Data Quality Analyser", "Test Suite Name": testsuite,
                           "Execution Start Time": DQValidation_starttime, "Execution End Time": DQValidation_endtime,
                           "Execution Duartion": dqvalidation_exectime, "Total No of Testcases": tccount,
                           "Total No of Testcases Pased": ptccount, "Total No of Testcases failed": ftccount}
    #df_testsuite_summary.show()
    print(type(df_testsuite_summary))
    schema = StructType([
        StructField('Testcase Name', StringType(), True),
        StructField('ColumnName', StringType(), True),
        StructField('Data Quality Check', StringType(), True),
        StructField('Test Result', StringType(), True),
        StructField('Reason', StringType(), True),
        StructField('ExecutionTime', StringType(), True)
    ])
    if (len(df_testsuite_summary) != 0):
        df_testsuite_summary = spark.createDataFrame(data=df_testsuite_summary)
    else:
        df_testsuite_summary = None

    #df_testsuite_summary = spark.createDataFrame(data=df_testsuite_summary, schema=schema)


    #df_testsuite_summary.show()
    print(type(df_testsuite_summary))
    pdfobj_summary = generate_protocol_summary_report(df_testsuite_summary, testsuite, protocol_run_params,
                                                      pdfobj_summary, spark,src_info,tgt_info)
    return pdfobj, pdfobj_summary


def generate_protocol_summary_report(df_testsuite_summary, testsuite, protocol_run_params, pdfobj_summary, spark,src_info,tgt_info):
    pdfobj_summary.write_text('Data Quality Analysis Summary Report', 'report header')
    pdfobj_summary.write_text(testsuite, 'subheading')
    pdfobj_summary.write_text("1. Configuration Details ", 'section heading')
    pdfobj_summary.write_text("1.1. Source Configuration Details", 'section sub heading')
    pdfobj_summary.create_table_summary(src_info)
    pdfobj_summary.write_text("1.2. Target Configuration Details", 'section sub heading')
    pdfobj_summary.create_table_summary(tgt_info)

    pdfobj_summary.write_text("2. Test Suite Run Summary ", 'section heading')
    pdfobj_summary.create_table_summary(protocol_run_params)
    pdfobj_summary.write_text("3. Test Result Summary", 'section heading')
    table_type = 'dqsummary'
    sno = 1
    test_results_list = ['Failed', 'Passed']
    for test_result in test_results_list:
        testheader = "3." + str(sno) + ". " + test_result + " Testcases"
        pdfobj_summary.write_text(testheader, 'section heading')
        print(f" 2--- {type(df_testsuite_summary)}")
        if (df_testsuite_summary is not None):
            df_testsuite_summary_temp = df_testsuite_summary.select('*') \
                .filter(col('Test Result') == test_result)
            df_testsuite_summary_temp.show()
            if (df_testsuite_summary_temp.count() == 0):
                df_testsuite_summary_temp = None
        else:
            print("second if else loop")
            df_testsuite_summary_temp = None
        pdfobj_summary.create_table_details(df_testsuite_summary_temp, table_type)
        print(type(df_testsuite_summary_temp))
        sno = sno + 1
    return pdfobj_summary

