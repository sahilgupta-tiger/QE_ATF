from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    data=get_data_from_sh_scripts()
    return render_template('dashboard.html', data=data)

if __name__=='__main__':
    app.run(debug=True)