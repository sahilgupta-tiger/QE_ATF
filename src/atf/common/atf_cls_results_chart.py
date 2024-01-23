import shutil
import os
from pretty_html_table import build_table
import pandas as pd
from atf.common.atf_common_functions import log_info, log_error
import sqlite3
from tabulate import tabulate
from datetime import datetime

table_name = 'historical_trends'


def generate_results_charts(df_protocol_summary, protocol_run_details, protocol_run_params, created_time,
                            testcasetype, output_path, combined_path, summary_path):

    log_info("Generating Charts and Trends from Results of the Protcol Execution")
    # converting pyspark dataframe to pandas dataframe for html rendering
    df_pd_summary = df_protocol_summary.toPandas()

    # Creating a New DataFrame for Pie Chart and Duration Chart
    new_df = df_pd_summary[['Testcase Name', 'Test Result', 'Runtime']]

    # Call methods to generate the pie chart and duration chart
    duration_chart_html = create_duration_chart(new_df)
    pie_chart_html = create_pie_chart(new_df)

    protocol_run_details_html = "<p>"
    for key, value in protocol_run_details.items():
        protocol_run_details_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"
    protocol_run_details_html += "</p>"

    # Removing a value from protocol run parameters
    del protocol_run_params['Testcases Executed']

    protocol_run_params_html = "<p>"
    for key, value in protocol_run_params.items():
        protocol_run_params_html += f"<span style='font-weight:bold'>{key}</span><span class='tab'></span>: {value}<br>"
    protocol_run_params_html += "</p>"

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
                }}
                /* Style to display elements side by side */
                .flex-container {{
                    display: flex;
                    justify-content: space-between;
                }}

            </style>

        </head>
        <body>
            <h1>DATF Test Execution Summary</h1>
            <div class="flex-container">
                <div>
                    <h3><b>1. Overall Status</b></h3>
                    <div>
                        {pie_chart_html}  <!-- Embed the pie chart here -->
                    </div>
                </div>
                <div>
                    <h3><b>2. Duration Summary</b></h3>
                    {duration_chart_html}  <!-- Include the duration chart here -->
                </div>
            </div>
            <h2><b>2. Protocol Test Results</b></h2>
            {build_table(df_pd_summary,'orange_dark')}
            <h2><b>3. Protocol Run Details</b></h2>
            {protocol_run_details_html}
            <h2><b>4. Protocol Run Parameters</b></h2>
            {protocol_run_params_html}
           
        </body>
        </html>
    """

    # Save all resutlts in a SQLITE3 DB which can be used for Dashboards as well
    store_results_into_db(df_pd_summary, protocol_run_details, testcasetype, created_time)

    # Call the method to generate and save the Trends HTML content
    trends_code = historical_trends()

    # Do not delete or modify below function, very important it needs to be LAST!
    create_html_report(trends_code, chart_code, created_time, output_path, combined_path, summary_path)


def create_html_report(trends_code, chart_code, created_time, output_path, combined_path, summary_path):
    # Create the HTML file
    html_file_name = f"chart_report_{created_time}.html"
    html_file_path = f"/app/test/results/charts/{html_file_name}"

    with open(html_file_path, 'w') as file:
        file.write(chart_code)
    log_info(f"Chart generated at: {html_file_path}")

    trends_path = f"/app/test/results/trends/datf_trends_report.html"

    with open(trends_path, 'w') as file:
        file.write(trends_code)
    log_info(f"Trends generated at: {trends_path}")

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
    shutil.copy(trends_path, final_report_path)
    # rename files for Archiving Artefacts within CT Pipeline integration
    os.rename(fr"{final_report_path}/{html_file_name}", fr"{final_report_path}/datfreport.html")
    os.rename(fr"{final_report_path}/{summary_file}", fr"{final_report_path}/datf_summary.pdf")
    os.rename(fr"{final_report_path}/{combined_file}", fr"{final_report_path}/datf_combined.pdf")
    log_info(f"HTML & PDF Reports copied over to: {final_report_path}")


def store_results_into_db(df_pd_summary, protocol_run_details, testcasetype, created_time):

    new_df = df_pd_summary[['Testcase Name', 'Test Result', 'Runtime']].copy()

    # Grab values from protocol run details to insert into dataframe DB
    protocol_start_time = protocol_run_details.get('Test Protocol Start Time', '')
    protocol_end_time = protocol_run_details.get('Test Protocol End Time', '')
    protocol_totalrun_time = protocol_run_details.get('Total Protocol Run Time', '')

    # Add Start Time, End Time, Created Time and Test Case Type column with the same value for all rows
    new_df['Testcase Type'] = testcasetype
    new_df['Run Created Time'] = created_time
    new_df['Protocol Start Time'] = protocol_start_time
    new_df['Protocol End Time'] = protocol_end_time
    new_df['Protocol Total Run Time'] = protocol_totalrun_time

    # Connect to SQLITE DB and update the table if exists
    conn = sqlite3.connect('/app/utils/DATF_SQLITE.db')
    new_df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()


def retrieve_from_db(sql_query):

    conn = sqlite3.connect('/app/utils/DATF_SQLITE.db')
    # Filter data from DB using SQL and create a DF
    df_from_db = pd.read_sql_query(sql_query, conn)

    # Display the retrieved data
    log_info("Data retrieved from DB successfully.")
    # print(tabulate(df_from_db, headers='keys', tablefmt='psql'))

    conn.close()
    return df_from_db


def create_duration_chart(new_df):

    dur_df = new_df[['Testcase Name', 'Test Result', 'Runtime']].copy()

    time_intervals = {
        '0s-45s': {'count': 0, 'testcases': []},
        '45s-2m': {'count': 0, 'testcases': []},
        '2m-5m': {'count': 0, 'testcases': []},
        '5m+': {'count': 0, 'testcases': []},
    }

    # Converting runtime strings to duration in seconds
    for index, row in dur_df.iterrows():
        runtime_str = row['Runtime']
        if runtime_str != '':
            runtime_obj = datetime.strptime(runtime_str, '%H:%M:%S')
            duration_seconds = runtime_obj.hour * 3600 + runtime_obj.minute * 60 + runtime_obj.second

            if duration_seconds < 45:
                time_intervals['0s-45s']['count'] += 1
                time_intervals['0s-45s']['testcases'].append(row['Testcase Name'])
            elif 45 <= duration_seconds < 120:
                time_intervals['45s-2m']['count'] += 1
                time_intervals['45s-2m']['testcases'].append(row['Testcase Name'])
            elif 120 <= duration_seconds < 300:
                time_intervals['2m-5m']['count'] += 1
                time_intervals['2m-5m']['testcases'].append(row['Testcase Name'])
            else:
                time_intervals['5m+']['count'] += 1
                time_intervals['5m+']['testcases'].append(row['Testcase Name'])

    # Generating HTML file with Google Charts
    duration_html = '''
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
        duration_html += f"        ['{interval}', {values['count']}, '{'<br>'.join(values['testcases']) if values['testcases'] else 'No test cases'}'],\n"

    duration_html += '''
          ]);

          var timeOptions = {
            title: 'Test Execution Time Intervals',
            legend: {position: 'none'},
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
    return duration_html  # Return the generated HTML code


def create_pie_chart(new_df):

    pie_df = new_df[['Test Result']].copy()

    # Generating HTML file with Google Charts
    pie_html = '''
    <!DOCTYPE html>
    <html>
    <head>
      <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
      <script type="text/javascript">
        google.charts.load('current', { packages: ['corechart'] });
        google.charts.setOnLoadCallback(drawChart);
        
    '''
    pie_list_table = pie_df.groupby('Test Result').size().reset_index(name='Count').values.tolist()
    columns = ['Status', 'Count']
    pie_list_table.insert(0, columns)

    pie_html += f"        var pieChartData = {pie_list_table}\n"
    pie_html += '''
        function drawChart() {
          // Drawing Pie Chart for Test Case Execution Results
          var pieData = google.visualization.arrayToDataTable(pieChartData);       
    
          var pieOptions = {
            title: 'Test Result Distribution',
            is3D: true,
            legend: 'none',
        '''
    pie_colors = "colors:['red','green']"
    if len(pie_list_table) < 3:
        if pie_list_table[1][0] == 'Passed':
            pie_colors = "colors:['green']"
        elif pie_list_table[1][0] == 'Failed':
            pie_colors = "colors:['red']"

    pie_html += f"           {pie_colors}\n"
    pie_html += '''
          };

          var pieChart = new google.visualization.PieChart(document.getElementById('pie_chart'));
          pieChart.draw(pieData, pieOptions);
        }
      </script>
    </head>
    <body>
      <div id="pie_chart" style="width: 700px; height: 500px; display: inline-block;"></div>
    </body>
    </html>
    '''
    return pie_html  # Return the generated HTML code


def historical_trends():

    # Start the HTML page contents from here
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>Historical Trends Dashboard</title>
      <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    </head>
    <body>    
    """

    # the 1st graph
    trends_query = f'''
        SELECT [Run Created Time] as "Created Time",
        count(*) as "Total",
        sum(case when [Test Result] = 'Failed' then 1 else 0 end) as "Failed",
        sum(case when [Test Result] = 'Passed' then 1 else 0 end) as "Passed",
        ROUND((count(*)/2.1)+count(*),2) as "Average"
        FROM {table_name} GROUP BY [Run Created Time] 
        ORDER BY [Run Created Time] DESC LIMIT 25;
    '''
    trends_df = retrieve_from_db(trends_query)
    print(tabulate(trends_df, headers='keys', tablefmt='psql'))
    trends_data = trends_df.values.tolist()

    html_content += f"""
      <h2>1. Historical Trends Graph (Last 25 Runs)</h2>
      <div id="trends_chart"></div>
      
        <script type="text/javascript">
            google.charts.load('current', {{ packages: ['corechart'], callback: drawTrends }});
            
            var trendsData = {trends_data}
            function drawTrends() {{
                var data = google.visualization.arrayToDataTable([
                ['Created Time', 'Total', 'Failed', 'Passed', 'Average'],
                ...trendsData
                ]);
                
                var options = {{
                    vAxis: {{title: 'Results'}},
                    hAxis: {{title: 'Timeline'}},
                    legend: {{position: 'bottom', alignment: 'center', maxLines: 1}},
                    isStacked: true,
                    seriesType: 'bars',
                    series: {{3: {{type: 'line'}}}}
                }};
                
                var chart = new google.visualization.ComboChart(document.getElementById('trends_chart'));
                chart.draw(data, options);
            }}
        </script>
    """

    # the 2nd graph
    hist_query = f"SELECT * FROM {table_name} ORDER BY [Run Created Time] DESC"
    his_df = retrieve_from_db(hist_query)

    # Creating separate DataFrames for each 'Testcase Type'
    count_df = his_df[his_df['Testcase Type'] == 'count'].copy()
    duplicate_df = his_df[his_df['Testcase Type'] == 'duplicate'].copy()
    content_df = his_df[his_df['Testcase Type'] == 'content'].copy()

    count_data = count_df.groupby('Test Result').size().reset_index(name='Count').values.tolist()
    duplicate_data = duplicate_df.groupby('Test Result').size().reset_index(name='Count').values.tolist()
    content_data = content_df.groupby('Test Result').size().reset_index(name='Count').values.tolist()

    if len(count_data) > 1:
        count_data[0].append('color:#DD4477')
        count_data[1].append('color:#329262')
    if len(duplicate_data) > 1:
        duplicate_data[0].append('color:#DD4477')
        duplicate_data[1].append('color:#329262')
    if len(content_data) > 1:
        content_data[0].append('color:#DD4477')
        content_data[1].append('color:#329262')

    html_content += f"""
      <h2>2. No. of Test Cases Executed (All Time)</h2>
      <div>
        <label for="filter">Test Case Type Filter:</label>
        <select id="filter" onchange="updateGraph()">
          <option value="count">Counting Rows</option>
          <option value="duplicate">Finding Duplicates</option>
          <option value="content">Matching Contents</option>
        </select>
      </div>
      <div id="chart_div"></div>

      <script type="text/javascript">
        google.charts.load('current', {{ packages: ['corechart'], callback: drawChart }});

        var countData = {count_data};
        var duplicateData = {duplicate_data};
        var contentData = {content_data};

        function drawChart() {{
          var data = google.visualization.arrayToDataTable([
            ['Test Result', 'Count', {{role:'style'}}],
            ...countData
          ]);

          var options = {{
            title: 'Bar Graph based on Filter',
            legend: {{position: 'none'}},
            hAxis: {{minValue: 0}},
            is3D: true,
            // Other chart options
          }};

          var chart = new google.visualization.BarChart(document.getElementById('chart_div'));
          chart.draw(data, options);
        }}

        function updateGraph() {{
          var selectedFilter = document.getElementById('filter').value;
          var newData;

          switch (selectedFilter) {{
            case 'count':
              newData = countData;
              break;
            case 'duplicate':
              newData = duplicateData;
              break;
            case 'content':
              newData = contentData;
              break;
            default:
              newData = countData;
              break;
          }}

          var data = google.visualization.arrayToDataTable([
            ['Test Result', 'Count', {{role:'style'}}],
            ...newData
          ]);

          var options = {{
            title: 'Bar Graph based on Filter',
            legend: {{position: 'none'}},
            hAxis: {{minValue: 0}},
            is3D: true,
            // Other chart options
          }};

          var chart = new google.visualization.BarChart(document.getElementById('chart_div'));
          chart.draw(data, options);
        }}
      </script>
    """

    html_content += """
    </body>
    </html>
    """

    return html_content




