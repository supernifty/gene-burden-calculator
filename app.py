#!/usr/bin/env python
'''
    Front end flask implementation
'''

import flask
import sqlite3

app = flask.Flask(__name__)

DB = "./exac.db"

### database access
def db():
    db_instance = getattr(flask.g, '_database', None)
    if db_instance is None:
        db_instance = flask.g._database = sqlite3.connect(DB)
    return db_instance

def query_db(query, args=(), one=False):
    cur = db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

@app.teardown_appcontext
def close_connection(exception):
    db_instance = getattr(flask.g, '_database', None)
    if db_instance is not None:
        db_instance.close()

### business logic
def process():
    '''
        analysis
    '''
    filter_type = flask.request.form['filter_type']
    if filter_type not in ('cadd', 'condel', 'sift', 'polyphen'):
        flask.render_template('main.html', error='Invalid filter type')
    try:
        filter_value = float(flask.request.form['filter_value'])
    except ValueError:
        return flask.render_template('main.html', error='Invalid filter value')

    burdens = flask.request.form['burdens'].split('\n')
    result = []
    for line, burden in enumerate(burdens):
        fields = burden.strip().split(',')
        if len(fields) != 2:
            return flask.render_template('main.html', error='Incorrect burden format on line {}'.format(line))

        # find matching genes
        matches = query_db("select count(*) from exac where gene = ? and {} >= ?".format(filter_type), [fields[0], filter_value], one=True)[0]
        
        result.append({'gene': fields[0], 'burden': fields[1], 'matches': matches})

    return flask.render_template('results.html', result=result, filter_type=filter_type, filter_value=filter_value)

### front end
@app.route('/', methods=['GET', 'POST'])
def main():
    '''
        main entry point
    '''
    if flask.request.method == 'POST':
        return process()
    else:
        return flask.render_template('main.html')

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')

