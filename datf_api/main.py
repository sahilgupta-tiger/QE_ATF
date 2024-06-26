from flask import Flask, send_file,jsonify
import subprocess

app = Flask(__name__)

@app.route('/execute')
def execute_command():
    subprocess.run(["docker", "exec", "-ti", "-u", "root", "DATF", "bash", "-c",
                    "cd / && cd app/scripts && sh testingstart.sh count testcase14_csv_parquet_mismatch_manua"])

    return jsonify({"summary":"Commands executed successfully"})

@app.route('/summary')
def display_html():
    html_file_path = 'C:/Users/dhamodharan.sesh/PycharmProjects/pythonProject1/QE_ATF-codebase/QE_ATF-codebase/datf_core/utils/reports/datfreport.html'
    return send_file(html_file_path)


if __name__ == '__main__':
    app.run(debug=True)
