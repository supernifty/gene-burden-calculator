#!/usr/bin/env python
'''
    Front end flask implementation
'''

import flask
import os
import sqlite3
import uuid

import calculate
import runner

DB = "./exac.db"
RUNNER_DB = "./runner.db"

EXAC_POPULATION = 53105.0
UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = set(['vcf'])

app = flask.Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

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

    # case count
    try:
        cases = int(flask.request.form['cases'])
        if cases <= 0:
            errors.append('Number of cases must be greater than zero')
    except ValueError:
            errors.append('Number of cases must be numeric')

    # parse overall settings - filtering
    filter_type = flask.request.form['filter_type']
    if filter_type not in ('cadd', 'condel', 'sift', 'polyphen'):
        flask.render_template('main.html', errors=['Invalid filter type'], form=flask.request.form)
    try:
        filter_value = float(flask.request.form['filter_value'])
    except ValueError:
        errors.append('Filter value must be numeric')

    # check filter types options
    try:
        include_high_impact = flask.request.form.getlist('impacts')
        if len(include_high_impact)==0:
            errors.append('Missing variant impact type')
    except ValueError:
        errors.append('Missing variant impact type')

    # population filter
    try:
        filter_af_pop = flask.request.form.getlist('filter_af_pop')
        if len(filter_af_pop)==0:
            errors.append('Invalid population name')
    except ValueError:
        flask.render_template('main.html', errors=['Invalid population name'], form=flask.request.form)
        # errors.append('Invalid population name')

    # check filter popultaion names?
    if not set(filter_af_pop).issubset(set(['exac_all', 'exac_african', 'exac_latino', 'exac_east_asian', 'exac_fin', 'exac_nonfin_eur', 'exac_south_asian', 'exac_other'])):
         flask.render_template('main.html', errors=['Invalid population name'], form=flask.request.form)
    # filter AF value
    try:
        filter_af_value = float(flask.request.form['filter_af_value'])
    except ValueError:
        errors.append('Filter allele frequency must be numeric')

    # input variants per gene count
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
            if case_burden <= 0:
                errors.append('Incorrect burden format on line {}: burden count {} is less than 0'.format(line + 1, fields[1]))
                continue
        except ValueError:
            errors.append('Incorrect burden format on line {}: burden count "{}" is not numeric'.format(line + 1, fields[1]))
            continue

        sql_parameters = [fields[0], filter_value]

        # additional
        additional_filter = " and ("
        if len(include_high_impact)>0:
            for impact in include_high_impact[:-1]:
                additional_filter += (" impact = ? or ")
                sql_parameters.append(impact)
            additional_filter += (" impact = ? ")
            sql_parameters.append(include_high_impact[-1])
        additional_filter += ")"

        # population filter for a list of selected populations
        population_filter = ''
        if filter_af_pop:
            for pop_value in filter_af_pop:
                population_filter += (" and exac.{} < ?").format(pop_value)
                sql_parameters.append(filter_af_value)
        print additional_filter + " " + population_filter

        # find matching genes
        query = "select count(*), protein_length from exac left join protein_length on exac.gene=protein_length.gene where exac.gene=? and exac.{} >= ? {} {}".format(
                filter_type,
                additional_filter,
                population_filter)
        matches = query_db(
                query,
                sql_parameters,
                one=True)

        if matches[1] is not None:
            statistics = calculate.calculate_burden_statistics(case_burden=case_burden, total_cases=cases, population_burden=matches[0], total_population=EXAC_POPULATION)
            result.append({'gene': fields[0], 'burden': fields[1], 'matches': matches[0], 'protein_length': matches[1], \
                'z_test': statistics[0], 'binomial_test': statistics[1], 'relative_risk': statistics[2], 'rr_conf_interval': statistics[3]})
        else:
            # gene is no good
            warnings.append( 'Gene "{}" had no matches'.format(fields[0]))

    if len(errors) == 0:
        return flask.render_template('results.html',
            result=result,
            filter_type=filter_type,
            filter_value=filter_value,
            filter_af_pop=','.join(filter_af_pop),
            filter_af_value=filter_af_value,
            cases=cases,
            include_high_impact=','.join(include_high_impact),
            gene_list = ','.join(["'{}'".format(item['gene'].replace("'", "\\'")) for item in result if item['protein_length'] is not None]),
            protein_lengths = ','.join([ str(item['protein_length']) for item in result if item['protein_length'] is not None]),
            binomial_pvalues = ','.join([ '{0:0.3e}'.format(item['binomial_test']) for item in result if item['protein_length'] is not None]),
            relative_risk = ','.join([ '{0:0.3e}'.format(item['relative_risk']) for item in result if item['protein_length'] is not None]),
            rr_conf_interval = ','.join([ str(item['rr_conf_interval']) for item in result if item['protein_length'] is not None]),
            warnings = warnings
        )
    else:
        return flask.render_template('main.html', errors=errors, form=flask.request.form)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

def generate_id():
    return str(uuid.uuid4())

def process_upload():
    errors = []
    if 'vcf' not in flask.request.files:
        errors.append('Please specify a file')
        return flask.render_template('upload.html', errors=errors, form=flask.request.form)

    vcf_file = flask.request.files['vcf']
    # if user does not select file, browser also
    # submit a empty part without filename
    if vcf_file.filename == '':
        errors.append('No selected file')
        return flask.render_template('upload.html', errors=errors, form=flask.request.form)

    if vcf_file and allowed_file(vcf_file.filename):
        job_id = generate_id()
        vcf_file.save(os.path.join(app.config['UPLOAD_FOLDER'], '{}.vcf'.format(job_id)))
        # start processing
        runner.add_to_queue(RUNNER_DB, job_id, os.path.join(app.config['UPLOAD_FOLDER'], '{}.vcf'.format(job_id)), os.path.join(app.config['UPLOAD_FOLDER'], '{}.out'.format(job_id)))
        return flask.redirect(flask.url_for('process_vcf', job=job_id))

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

@app.route('/vcf_result/<job>')
def vcf_result(job):
    status = runner.job_status(RUNNER_DB, job)
    if status is None:
        return flask.render_template('upload.html', form=flask.request.form)
    else:
        data = open(os.path.join(app.config['UPLOAD_FOLDER'], '{}.out'.format(job)), 'r').readlines()
        return flask.render_template('vcf_result.html', job=job, data=data)

@app.route('/process_vcf/<job>')
def process_vcf(job):
    status = runner.job_status(RUNNER_DB, job)
    if status is None:
        return flask.render_template('upload.html', form=flask.request.form)
    elif status['status'] == 'F':
        return flask.redirect(flask.url_for("vcf_result", job=job))
    else:
        return flask.render_template('process_vcf.html', job=job, status=status)

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if flask.request.method == 'POST':
        return process_upload()
    else:
        return flask.render_template('upload.html', form=flask.request.form)

@app.route('/about')
def about():
    return flask.render_template('about.html')

@app.route('/contact')
def contact():
    return flask.render_template('contact.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
