import flask
app = flask.Flask(__name__)

def process():
    return 'Process'

@app.route('/', methods=['GET', 'POST'])
def main():
    if flask.request.method == 'POST':
        return process()
    else:
        return flask.render_template('main.html')

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')


