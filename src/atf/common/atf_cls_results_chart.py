from IPython.display import HTML, display
import shutil
import os
from pretty_html_table import build_table
import pandas as pd
from atf.common.atf_common_functions import log_info, log_error


def generate_results_charts(df_protocol_summary, protocol_run_details, protocol_run_params, created_time, testcasetype, output_path, combined_path, summary_path):

    log_info("Printing Results from Protcol below ---")
    # converting pyspark dataframe to pandas dataframe for html rendering
    df_pd_summary = df_protocol_summary.toPandas()
    log_info(df_pd_summary.to_string(index=False))
    log_info(protocol_run_details)
    log_info(protocol_run_params)
    log_info(output_path)
    log_info(created_time)
    log_info(testcasetype)
    protocol_run_params_html = ""
    protocol_run_details_html = ""

    for key, value in protocol_run_params.items():
        protocol_run_params_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"

    for key, value in protocol_run_details.items():
        protocol_run_details_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"

    # Construct the JavaScript code for Google Chart
    chart_code = f"""
        <html>
        <head>
            <title>DATF Test Run Report</title>
        </head>
        <body>
            <h1>DATF Test Summary</h1>
            <h2><b>1. Protocol Run Summary</b></h2>
            {protocol_run_details_html}
            <h2><b>2. Protocol Run Parameters</b></h2>
            {protocol_run_params_html}
            <h2><b>3. Protocol Test Results</b></h2>
            {build_table(df_pd_summary,'orange_light')}

        </body>
        </html>
    """
    # do not delete or modify below function, very important!
    create_html_report(chart_code, created_time, output_path, combined_path, summary_path)


def create_html_report(chart_code, created_time, output_path, combined_path, summary_path):
    # Create the HTML file
    html_file_name = f"chart_report_{created_time}.html"
    html_file_path = f"/app/test/results/charts/{html_file_name}"

    with open(html_file_path, 'w') as file:
        file.write(chart_code)
    log_info(f"Chart generated at: {html_file_path}")

    # copy all current reports to single folder after emptying it
    final_report_path = "/app/utils/reports"
    for filename in os.listdir(final_report_path):
        file_path = os.path.join(final_report_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            log_info('Failed to delete %s. Reason: %s' % (file_path, e))

    log_info(f"Summary PDF Path: {summary_path}")
    log_info(f"Combined PDF Path: {combined_path}")
    summary_file = summary_path.replace(output_path, '')
    combined_file = combined_path.replace(output_path, '')
    log_info(f"File Names : {summary_file} | {combined_file}")
    shutil.copytree(output_path, final_report_path, dirs_exist_ok=True)
    shutil.copy(html_file_path, final_report_path)
    os.rename(fr"{final_report_path}/{html_file_name}", fr"{final_report_path}/datfreport.html")
    os.rename(fr"{final_report_path}/{summary_file}", fr"{final_report_path}/datf_summary.pdf")
    os.rename(fr"{final_report_path}/{combined_file}", fr"{final_report_path}/datf_combined.pdf")
    log_info(f"Reports copied over to: {final_report_path}")


