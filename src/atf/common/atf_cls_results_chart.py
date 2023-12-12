from IPython.display import HTML
from src.atf.common.atf_common_functions import *
import pandas as pd


def generate_results_charts(df_protocol_summary, protocol_run_details, protocol_run_params, output_path, created_time, testcasetype):

    log_info("Printing Results from Protcol below ---")
    log_info(df_protocol_summary.show())
    log_info(protocol_run_details)
    log_info(protocol_run_params)
    log_info(output_path)
    log_info(created_time)
    log_info(testcasetype)
    protocol_run_params_html = ""
    protocol_run_details_html = ""

    for key, value in protocol_run_params.items():
        protocol_run_params_html += f"<p><span style='font-weight:bold'>{key}</span> : {value}</p>"

    for key, value in protocol_run_details.items():
        protocol_run_details_html += f"<p><span style='font-weight:bold'>{key}</span> : {value}</p>"

    # Create the data table for Google Chart
    data_table = [['Task', 'Hours per Day']]
    test_results = {
        "Passed": 11,
        "Failed": 2
    }
    for label, value in test_results.items():
        data_table.append([label, value])

    '''try:
        # Convert DataFrame df_protocol_summary to an HTML table
        df_html_table = df_protocol_summary.to_html(index=False)
    except AttributeError:
        # If to_html method is not available, use tabulate library
        df_html_table = tabulate(df_protocol_summary, headers='keys', tablefmt='html', showindex=False)'''

    # Construct the JavaScript code for Google Chart
    chart_code = f"""
        <html>
        <head>
            <title>DATF Test Run Report</title>
        </head>
        <body>
            <h1>Summary</h1>
            <h2><b>1. Test Protocol Run Summary</b></h2>
            {protocol_run_details_html}
            <h2><b>2. Protocol Run Parameters</b></h2>
            {protocol_run_params_html}

        </body>
        </html>
    """
    # Display the chart
    file_path = f"/app/charts/chart_report_{created_time}.html"

    with open(file_path, 'w') as file:
        file.write(chart_code)
    log_info(f"Chart generated at: {file_path}")


