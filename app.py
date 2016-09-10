#!/usr/bin/env python
'''
    Front end flask implementation
'''

import flask
app = flask.Flask(__name__)

def process():
    print("1")
    filter_type = flask.request.form['filter_type']
    print("2")
    if filter_type not in ('cadd', 'condel', 'sift', 'polyphen'):
        flask.render_template('main.html', error='Invalid filter type')
    try:
        print("a")
        filter_value = float(flask.request.form['filter_value'])
        print("b")
    except ValueError:
        print("c")
        return flask.render_template('main.html', error='Invalid filter value')
    print("3")

    burdens = flask.request.form['burdens'].split('\n')
    result = []
    for line, burden in enumerate(burdens):
        fields = burden.strip().split(',')
        if len(fields) != 2:
            return flask.render_template('main.html', error='Incorrect burden format on line {}'.format(line))
        result.append({'gene': fields[0], 'count': fields[1]})

    return flask.render_template('results.html', result=result)

@app.route('/', methods=['GET', 'POST'])
def main():
    if flask.request.method == 'POST':
        return process()
    else:
        return flask.render_template('main.html')

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')

