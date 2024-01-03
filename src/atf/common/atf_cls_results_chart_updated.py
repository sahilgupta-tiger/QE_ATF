from IPython.display import HTML, display
import shutil
import os
from pretty_html_table import build_table
import pandas as pd
import matplotlib.pyplot as plt
from atf.common.atf_common_functions import log_info, log_error
#import pymssql
#from sqlalchemy import create_engine
import base64
from io import BytesIO
import sqlite3
from tabulate import tabulate
from collections import defaultdict
import plotly.graph_objects as go
from datetime import datetime

def generate_results_charts_updated(df_protocol_summary, protocol_run_details, protocol_run_params, created_time, testcasetype, output_path, combined_path, summary_path):

    log_info("Printing Results from Protcol below ---")
    # converting pyspark dataframe to pandas dataframe for html rendering
    df_pd_summary = df_protocol_summary.toPandas()
    colors = {'Passed': 'green', 'Failed': 'red', 'Broken': 'yellow'}
    test_results_count = df_pd_summary['Test Result'].value_counts()
    log_info("Test results count is")
    log_info(test_results_count)
    log_info(df_pd_summary)
    log_info(df_pd_summary.to_string(index=False))
    log_info(protocol_run_details)
    log_info(protocol_run_params)
    new_df = df_pd_summary[['Testcase Name', 'Test Result', 'Runtime']]
    # Displaying the new DataFrame
    log_info(new_df)
    log_info(protocol_run_details)
    protocol_start_time = protocol_run_details.get('Test Protocol Start Time', '')
    # Add 'Test Protocol Start Time' column with the same value for all rows
    new_df['Test Protocol Start Time'] = protocol_start_time
    log_info("updated df")
    log_info(new_df)
    #his_df=store_results_into_db(df_pd_summary, protocol_run_details)
    #log_info("Results from db")
    #log_info(his_df)
    duration_chart_html =create_duration_chart(new_df)
    pie_chart_html = generate_pie_chart_html(new_df)
    protocol_run_params_html = "<p>"
    for key, value in protocol_run_params.items():
        protocol_run_params_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"
    protocol_run_params_html += "</p>"

    protocol_run_details_html = "<p>"
    for key, value in protocol_run_details.items():
        protocol_run_details_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"
    protocol_run_details_html += "</p>"


    # Construct the JavaScript code for Google Chart
    chart_code = fr"""
        <html>
        <head>
            <title>DATF Test Run Report</title>
            <style>
                /* Define a CSS style for the headers */
                h2, h3, h4 {{
                    font-size: 18px;
                    font-family: Arial, sans-serif;
                    /* Add other styles as needed */
                }}
                /* Style to display elements side by side */
                .flex-container {{
                    display: flex;
                    justify-content: space-between;
                }}

            </style>

        </head>
        <body>
            <h1>DATF Test Summary</h1>
            <h2><b>1. Protocol Test Results</b></h2>
            {build_table(df_pd_summary,'orange_dark')}
            <h2><b>2. Protocol Run Details</b></h2>
            {protocol_run_details_html}
            <h2><b>3. Protocol Run Parameters</b></h2>
            {protocol_run_params_html}
            <div class="flex-container">
                <div>
                    <h3><b>4. Durations</b></h3>
                    {duration_chart_html}  <!-- Include the historical chart HTML content here -->
                </div>
                <div>
                    <h3><b>5. Status</b></h3>
                    <div>
                        {pie_chart_html}  <!-- Embed the pie chart here -->
                    </div>
                </div>
            </div>

            
            
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

    summary_file = summary_path.replace(output_path, '')
    combined_file = combined_path.replace(output_path, '')
    shutil.copytree(output_path, final_report_path, dirs_exist_ok=True)
    shutil.copy(html_file_path, final_report_path)
    # rename files for Archiving Artefacts within CT Pipeline integration
    os.rename(fr"{final_report_path}/{html_file_name}", fr"{final_report_path}/datfreport.html")
    os.rename(fr"{final_report_path}/{summary_file}", fr"{final_report_path}/datf_summary.pdf")
    os.rename(fr"{final_report_path}/{combined_file}", fr"{final_report_path}/datf_combined.pdf")
    log_info(f"Reports copied over to: {final_report_path}")

def store_results_into_db(df_pd_summary,protocol_run_details):

    #log_info(df_pd_summary)
    new_df = df_pd_summary[['Testcase Name', 'Test Result', 'Runtime']]
    # Displaying the new DataFrame
    log_info(new_df)
    log_info(protocol_run_details)
    protocol_start_time = protocol_run_details.get('Test Protocol Start Time', '')
    # Add 'Test Protocol Start Time' column with the same value for all rows
    new_df['Test Protocol Start Time'] = protocol_start_time

    log_info("Updated dataframe is")
    log_info(new_df)
    conn = sqlite3.connect('SQLITE_Sample.db')
    table_name = 'historical_trends'
    
    new_df.to_sql(table_name, conn, if_exists='append', index=False)
    query = f"SELECT * FROM {table_name}"
    df_from_db = pd.read_sql_query(query, conn)
    # Display the retrieved data
    log_info("Data retrieved from db")
    log_info(df_from_db)
    conn.close()
    return df_from_db
   
def create_duration_chart(new_df):

    new_df = new_df[['Testcase Name', 'Test Result', 'Runtime']]
    log_info("checking")
    log_info(new_df)
    log_info(tabulate(new_df, headers='keys', tablefmt='psql'))

    time_intervals = {
        '0s-20s': {'count': 0, 'testcases': []},
        '20s-40s': {'count': 0, 'testcases': []},
        '40s-1m': {'count': 0, 'testcases': []},
        '1m+': {'count': 0, 'testcases': []},
    }

    # Converting runtime strings to duration in seconds
    for index, row in new_df.iterrows():
        runtime_str = row['Runtime']
        runtime_obj = datetime.strptime(runtime_str, '%H:%M:%S')
        duration_seconds = runtime_obj.hour * 3600 + runtime_obj.minute * 60 + runtime_obj.second

        if duration_seconds < 20:
            time_intervals['0s-20s']['count'] += 1
            time_intervals['0s-20s']['testcases'].append(row['Testcase Name'])
        elif 20 <= duration_seconds < 40:
            time_intervals['20s-40s']['count'] += 1
            time_intervals['20s-40s']['testcases'].append(row['Testcase Name'])
        elif 40 <= duration_seconds < 60:
            time_intervals['40s-1m']['count'] += 1
            time_intervals['40s-1m']['testcases'].append(row['Testcase Name'])
        else:
            time_intervals['1m+']['count'] += 1
            time_intervals['1m+']['testcases'].append(row['Testcase Name'])

    # Generating HTML file with Google Charts
    html_content = '''
    <!DOCTYPE html>
    <html>
    <head>
      <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
      <script type="text/javascript">
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(drawCharts);

        function drawCharts() {
          // Drawing Bar Chart for Test Case Execution Time Intervals
          var timeData = new google.visualization.DataTable();
          timeData.addColumn('string', 'Time Intervals');
          timeData.addColumn('number', 'Number of Test Cases');
          timeData.addColumn({type: 'string', role: 'tooltip', 'p': {'html': true}});

          timeData.addRows([
    '''

    # Adding data rows for each time interval
    for interval, values in time_intervals.items():
        html_content += f"        ['{interval}', {values['count']}, '{'<br>'.join(values['testcases']) if values['testcases'] else 'No test cases'}'],\n"

    html_content += '''
          ]);

          var timeOptions = {
            title: 'Test Case Execution Time Intervals',
            chartArea: {width: '40%'},
            hAxis: {
              title: 'Number of Test Cases',
              minValue: 0,
            },
            vAxis: {
              title: 'Time Intervals',
              textStyle: { fontSize: 12 }
            },
            tooltip: { isHtml: true }
          };

          var timeChart = new google.visualization.BarChart(document.getElementById('time_chart'));
          timeChart.draw(timeData, timeOptions);
        }
      </script>
    </head>
    <body>
      <div id="time_chart" style="width: 700px; height: 500px; display: inline-block;"></div>
    </body>
    </html>
    '''
    return html_content  # Return the generated HTML content

    '''file_path = f"/app/test/results/historical_trends/test_status_chart.html"

    # Writing HTML content to a file
    with open(file_path, 'w') as html_file:
        html_file.write(html_content)

    print("HTML file generated successfully!")'''
    
def generate_pie_chart_html(dataframe):
    result_counts = dataframe['Test Result'].value_counts()

    labels = result_counts.index.tolist()
    sizes = result_counts.values.tolist()
    colors = ['green', 'red', 'yellow']
    
    # Get the test case names for each test result
    testcase_names = [dataframe[dataframe['Test Result'] == label]['Testcase Name'].iloc[0] for label in labels]
    explode = tuple(0.1 if i == 0 else 0 for i in range(len(labels)))
    '''def func(pct, allvals):
        absolute = int(pct / 100. * sum(allvals))
        return f"{absolute}\n{testcase_names.pop(0)}"'''

    plt.figure(figsize=(6, 6))
    patches, texts, autotexts = plt.pie(sizes, explode=explode, labels=labels, colors=colors,
                                        autopct='%1.1f%%', startangle=140)
    # Generate tooltip information
    tooltip_info = [f'<area alt="{testcase}" title="{testcase}" shape="circle" coords="{str(pie.center[0])},{str(pie.center[1])},{str(pie.r*2)}" />' 
                    for pie, testcase in zip(patches, testcase_names)]

    plt.axis('equal')
    plt.title('Test Results Distribution')

    image_stream = BytesIO()
    plt.savefig(image_stream, format='png')
    image_stream.seek(0)

    image_base64 = base64.b64encode(image_stream.getvalue()).decode('utf-8')
    chart_image_tag = f'''
        <img src="data:image/png;base64,{image_base64}" usemap="#testcase_map" alt="Test Results Pie Chart">
        <map name="testcase_map">
            {''.join(tooltip_info)}
        </map>
    '''

    return chart_image_tag
