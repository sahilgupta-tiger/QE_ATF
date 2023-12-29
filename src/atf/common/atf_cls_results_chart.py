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


def generate_results_charts(df_protocol_summary, protocol_run_details, protocol_run_params, created_time, testcasetype, output_path, combined_path, summary_path):

    log_info("Printing Results from Protcol below ---")
    # converting pyspark dataframe to pandas dataframe for html rendering
    df_pd_summary = df_protocol_summary.toPandas()
    colors = {'Passed': 'green', 'Failed': 'red', 'Broken': 'yellow'}
    test_results_count = df_pd_summary['Test Result'].value_counts()
    log_info("Test results count is")
    log_info(test_results_count)
    log_info(df_pd_summary.to_string(index=False))
    log_info(protocol_run_details)
    log_info(protocol_run_params)

    protocol_run_params_html = "<p>"
    for key, value in protocol_run_params.items():
        protocol_run_params_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"
    protocol_run_params_html += "</p>"

    protocol_run_details_html = "<p>"
    for key, value in protocol_run_details.items():
        protocol_run_details_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"
    protocol_run_details_html += "</p>"

    # Creating the trends graph
    plt.figure(figsize=(8, 6))
    plt.bar(test_results_count.index, test_results_count.values, color=[colors[result] for result in test_results_count.index])
    plt.xlabel('Test Result')
    plt.ylabel('Number of Test Cases')
    plt.title('TREND')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    
    # Save the graph as a base64-encoded string
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')



    # Construct the JavaScript code for Google Chart
    chart_code = fr"""
        <html>
        <head>
            <title>DATF Test Run Report</title>

        </head>
        <body>
            <h1>DATF Test Summary</h1>
            <h2><b>1. Protocol Test Results</b></h2>
            {build_table(df_pd_summary,'orange_dark')}
            <h2><b>2. Protocol Run Details</b></h2>
            {protocol_run_details_html}
            <h2><b>3. Protocol Run Parameters</b></h2>
            {protocol_run_params_html}
            <h2><b>4. Trends of Test Results</b></h2>
            <img src="data:image/png;base64,{image_base64}" alt="Trends Graph">    
            
        </body>
        </html>
    """
    

    # do not delete or modify below function, very important!
    create_html_report(chart_code, created_time, output_path, combined_path, summary_path)
    #create_historical_trends_report()
    his_df=store_results_into_db(df_pd_summary, protocol_run_details)
    create_historical_trends(his_df)
    
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

    # Get the index for the new row
    '''new_index = len(new_df)-1

    # Update the DataFrame with the new 'Test Protocol Start Time' value at the specific index
    new_df.loc[new_index, 'Test Protocol Start Time'] = protocol_run_details['Test Protocol Start Time']'''
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
    
def create_historical_trends_report():
   # Sample data (replace this with your fetched data from the database)
    data_from_db = [
        {'testcase': 'Test A', 'status': 'passed', 'duration': 35},
        {'testcase': 'Test B', 'status': 'failed', 'duration': 75},
        {'testcase': 'Test C', 'status': 'broken', 'duration': 40},
        {'testcase': 'Test D', 'status': 'passed', 'duration': 50},
        # Add more data here...
    ]

    # Processing data to count statuses
    status_count_per_testcase = {}

    for entry in data_from_db:
        status = entry['status']
        if status not in status_count_per_testcase:
            status_count_per_testcase[status] = 0
        status_count_per_testcase[status] += 1

    # Processing data to categorize test cases based on execution time intervals
    time_intervals = {
        '0s-20s': {'count': 0, 'testcases': []},
        '20s-40s': {'count': 0, 'testcases': []},
        '40s-1m': {'count': 0, 'testcases': []},
        '1m+': {'count': 0, 'testcases': []},
    }

    for entry in data_from_db:
        duration = entry['duration']
        if duration < 20:
            time_intervals['0s-20s']['count'] += 1
            time_intervals['0s-20s']['testcases'].append(entry['testcase'])
        elif 20 <= duration < 40:
            time_intervals['20s-40s']['count'] += 1
            time_intervals['20s-40s']['testcases'].append(entry['testcase'])
        elif 40 <= duration < 60:
            time_intervals['40s-1m']['count'] += 1
            time_intervals['40s-1m']['testcases'].append(entry['testcase'])
        else:
            time_intervals['1m+']['count'] += 1
            time_intervals['1m+']['testcases'].append(entry['testcase'])

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
          // Drawing Pie Chart for Test Case Status
          var statusData = google.visualization.arrayToDataTable([
            ['Status', 'Count'],
    '''

    # Adding data rows for each status
    for status, count in status_count_per_testcase.items():
        html_content += f"        ['{status}', {count}],\n"

    html_content += '''
          ]);

          var statusOptions = {
            title: 'Test Case Status',
            pieHole: 0.4,
            slices: {
              0: {color: 'green'}, // Green color for 'passed'
              1: {color: 'red'},   // Red color for 'failed'
              2: {color: 'orange'},// Orange color for 'broken'
            }
          };

          var statusChart = new google.visualization.PieChart(document.getElementById('status_chart'));
          statusChart.draw(statusData, statusOptions);

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
            chartArea: {width: '50%'},
            hAxis: {
              title: 'Number of Test Cases',
              minValue: 0,
            },
            vAxis: {
              title: 'Time Intervals',
            },
            tooltip: { isHtml: true }
          };

          var timeChart = new google.visualization.BarChart(document.getElementById('time_chart'));
          timeChart.draw(timeData, timeOptions);
        }
      </script>
    </head>
    <body>
      <div id="status_chart" style="width: 900px; height: 500px; display: inline-block;"></div>
      <div id="time_chart" style="width: 900px; height: 500px; display: inline-block;"></div>
    </body>
    </html>
    '''
    file_path = f"/app/test/results/historical_trends/test_status_chart.html"
    # Writing HTML content to a file
    with open(file_path, 'w') as html_file:
        html_file.write(html_content)

    print("HTML file generated successfully!")
    
def create_historical_trends(df):

    #log_info("Output from the db")
    #log_info(df)
    

    data = {
        'Testcase Name': ['testcase21_mysql_csv_match', 'testcase21_mysql_csv_match'],
        'Test Result': ['Passed', 'Failed'],
        'Runtime': ['0:01:03', '0:00:56'],
        'Test Protocol Start Time': ['27-Dec-2023 14:36:02 UTC', '27-Dec-2023 14:36:02 UTC']
    }

    df = pd.DataFrame(data)

    # Counting the number of test cases for each Test Protocol Start Time and Test Result
    grouped = df.groupby(['Test Protocol Start Time', 'Test Result']).size().unstack(fill_value=0).reset_index()

    # Prepare data for Google Charts
    chart_data = grouped.rename(columns={'Passed': 'green', 'Failed': 'red'}).copy()
    chart_data['Test Protocol Start Time'] = pd.to_datetime(chart_data['Test Protocol Start Time'])
    chart_data = chart_data.sort_values('Test Protocol Start Time')

    # Generate Google Chart (JavaScript code)
    chart_script = f"""
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {{'packages':['corechart']}});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {{
        var data = new google.visualization.DataTable();
        data.addColumn('datetime', 'Test Protocol Start Time');
        data.addColumn('number', 'Passed');
        data.addColumn('number', 'Failed');

        data.addRows([
    """

    # Append the data rows to the chart script
    for _, row in chart_data.iterrows():
        chart_script += f"      [new Date('{row['Test Protocol Start Time']}'), {row['green']}, {row['red']}],\n"

    # Complete the chart script
    chart_script += """
        ]);

        var options = {{
          title: 'Test Results Over Time',
          hAxis: {{ title: 'Test Protocol Start Time' }},
          vAxis: {{ title: 'Number of Test Cases' }},
          legend: {{ position: 'top' }},
          series: {{
            0: {{ color: 'green', visibleInLegend: true }},
            1: {{ color: 'red', visibleInLegend: true }}
          }},
          pointSize: 5
        }};

        var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
        chart.draw(data, options);
      }}
    </script>
    """

    # HTML content with the Google Chart JavaScript code
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Status Chart</title>
    </head>
    <body>
        <h1>Test Status Chart</h1>
        <div id="chart_div" style="width: 800px; height: 600px;"></div>
        {chart_script}
    </body>
    </html>
    """

    # File path to save the HTML file
    file_path = "/app/test/results/historical_trends/test_status_chart.html"

    # Save the HTML content to the specified file path
    with open(file_path, 'w') as file:
        file.write(html_content)

    print(f"HTML file '{file_path}' with the Google Chart JavaScript code has been created.")
