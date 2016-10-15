#!/usr/bin/env python
'''
    Front end flask implementation
'''

import flask
import sqlite3

import calculate

app = flask.Flask(__name__)

DB = "./exac.db"
EXAC_POPULATION = 53105

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
    errors = []
    filter_type = flask.request.form['filter_type']
    if filter_type not in ('cadd', 'condel', 'sift', 'polyphen'):
        flask.render_template('main.html', errors=['Invalid filter type'])
    try:
        filter_value = float(flask.request.form['filter_value'])
    except ValueError:
        errors.append('Filter value must be numeric')

    try:
        cases = int(flask.request.form['cases'])
    except ValueError:
        errors.append('Number of cases must be numeric')

    burdens = flask.request.form['burdens'].split('\n')

    result = []
    warnings = []
    for line, burden in enumerate(burdens):
        fields = burden.strip().split(',')
        if len(fields) != 2:
            errors.append('Incorrect burden format on line {}'.format(line + 1))
            continue

        try:
            case_burden = float(fields[1])
        except ValueError:
            errors.append('Incorrect burden format on line {}'.format(line + 1))
            continue

        # find matching genes
        matches = query_db("select count(*), protein_length from exac left join protein_length on exac.gene=protein_length.gene where exac.gene=? and exac.{} >= ?".format(filter_type), 
                           [fields[0], filter_value], one=True)
        
        if matches[1] is not None:
            statistics = calculate.calculate_burden_statistics(case_burden=case_burden, total_cases=cases, population_burden=matches[0], total_population=EXAC_POPULATION)
            result.append({'gene': fields[0], 'burden': fields[1], 'matches': matches[0], 'protein_length': matches[1], 'z_test': statistics[0], 'binomial_test': statistics[1]})
        else:
            # gene is no good
            warnings.append( 'Gene "{}" had no matches'.format(fields[0]))

    if len(errors) == 0:
        return flask.render_template('results.html', 
            result=result, 
            filter_type=filter_type, 
            filter_value=filter_value, 
            cases=cases,
            gene_list = ','.join(["'{}'".format(item['gene'].replace("'", "\\'")) for item in result if item['protein_length'] is not None]),
            protein_lengths = ','.join([ str(item['protein_length']) for item in result if item['protein_length'] is not None]),
            binomial_pvalues = ','.join([ '{0:0.3e}'.format(item['binomial_test']) for item in result if item['protein_length'] is not None]),
            warnings = warnings
        )
    else:
        return flask.render_template('main.html', errors=errors, form=flask.request.form)

### front end
@app.route('/', methods=['GET', 'POST'])
def main():
    '''
        main entry point
    '''
    if flask.request.method == 'POST':
        return process()
    else:
        return flask.render_template('main.html', form=flask.request.form)

if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0')

