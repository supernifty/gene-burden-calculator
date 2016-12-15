#!/usr/bin/env python
'''
    Front end flask implementation
'''

import flask
import os
import json
import sqlite3
import uuid

import calculate
import helpers
import runner

DB = "./exac.db"
RUNNER_DB = "./runner.db"

EXAC_POPULATION = 53105.0
UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = set(['vcf'])

# output from annotation tool
OUTPUT_FIELDS = ['gene', 'chrom', 'start', 'end', 'ref', 'alt', 'impact', 'impact_type', 'allele_count', 'allele_number', 'exac_all_pop_ac', 'exac_all_pop_an', 'exac_all_pop', 'exac_african_ac', 'exac_african_an', 'exac_african', 'exac_latino_ac', 'exac_latino_an', 'exac_latino', 'exac_east_asian_ac', 'exac_east_asian_an', 'exac_east_asian', 'exac_fin_ac', 'exac_fin_an', 'exac_fin', 'exac_nonfin_eur_ac', 'exac_nonfin_eur_anexac_nonfin_eur', 'exac_south_asian_ac', 'exac_south_asian_an', 'exac_south_asian', 'exac_other_ac', 'exac_other_ac', 'exac_other', 'sift', 'polyphen', 'condel_score', 'cadd']

app = flask.Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024 * 1024 # 1G

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
        analysis when details provided as gene/burden
    '''

    # is it the vcf?
    if flask.request.files['vcf'] is not None and flask.request.files['vcf'].filename is not None and flask.request.files['vcf'].filename != '':
        return process_upload()

    # it's the csv
    settings = helpers.parse_settings(flask.request.form)

    if len(settings['errors']) > 0:
        flask.render_template('main.html', errors=settings['errors'], form=flask.request.form)

    # input variants per gene count
    burdens = flask.request.form['burdens'].split('\n')

    result = []
    warnings = []
    for line, burden in enumerate(burdens):
        fields = burden.strip().split(',')
        if len(fields) != 2:
            settings['errors'].append('Incorrect burden format on line {}'.format(line + 1))
            continue

        try:
            case_burden = float(fields[1])
            if case_burden <= 0:
                settings['errors'].append('Incorrect burden format on line {}: burden count {} is less than 0'.format(line + 1, fields[1]))
                continue
        except ValueError:
            settings['errors'].append('Incorrect burden format on line {}: burden count "{}" is not numeric'.format(line + 1, fields[1]))
            continue

        # --- determine exac count for this gene
        matches = helpers.get_exac_detail(query_db=query_db, gene=fields[0], settings=settings)

        if matches[1] is not None:
            statistics = calculate.calculate_burden_statistics(case_burden=case_burden, total_cases=settings['cases'], population_burden=matches[0], total_population=EXAC_POPULATION)
            result.append({'gene': fields[0], 'burden': fields[1], 'matches': matches[0], 'protein_length': matches[1], \
                'z_test': statistics[0], 'binomial_test': statistics[1], 'relative_risk': statistics[2], 'rr_conf_interval': statistics[3]})
        else:
            # gene is no good
            warnings.append( 'Gene "{}" or variants not found in the selected database'.format(fields[0]))

    if len(settings['errors']) == 0:
        return flask.render_template('results.html',
            result=result,
            filter_type=settings['filter_type'],
            filter_value=settings['filter_value'],
            filter_af_pop=','.join(settings['filter_af_pop']),
            filter_af_value=settings['filter_af_value'],
            cases=settings['cases'],
            include_impacts=','.join(settings['include_impacts']),
            gene_list = ','.join(["'{}'".format(item['gene'].replace("'", "\\'")) for item in result if item['protein_length'] is not None]),
            protein_lengths = ','.join([ str(item['protein_length']) for item in result if item['protein_length'] is not None]),
            binomial_pvalues = ','.join([ '{0:0.3e}'.format(item['binomial_test']) for item in result if item['protein_length'] is not None]),
            relative_risk = ','.join([ '{0:0.3e}'.format(item['relative_risk']) for item in result if item['protein_length'] is not None]),
            rr_conf_interval = ','.join([ str(item['rr_conf_interval']) for item in result if item['protein_length'] is not None]),
            warnings = warnings
        )
    else:
        return flask.render_template('main.html', errors=settings['errors'], form=flask.request.form)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

def generate_id():
    return str(uuid.uuid4())

def process_upload():
    '''
        adds the annotation job to the queue
    '''
    errors = []
    if 'vcf' not in flask.request.files:
        errors.append('Please specify a file')
        return flask.render_template('main.html', errors=errors, form=flask.request.form)

    vcf_file = flask.request.files['vcf']
    # if user does not select file, browser also
    # submit a empty part without filename
    if vcf_file.filename == '':
        errors.append('No selected file')
        return flask.render_template('main.html', errors=errors, form=flask.request.form)

    if not allowed_file(vcf_file.filename):
        errors.append('Invalid file extension')
        return flask.render_template('main.html', errors=errors, form=flask.request.form)

    settings = helpers.parse_settings(flask.request.form)

    if len(settings['errors']) > 0:
        return flask.render_template('main.html', errors=errors, form=flask.request.form)

    # looks ok

    job_id = generate_id()
    vcf_file.save(os.path.join(app.config['UPLOAD_FOLDER'], '{}.vcf'.format(job_id)))

    # start processing
    runner.add_to_queue(RUNNER_DB, job_id, os.path.join(app.config['UPLOAD_FOLDER'], '{}.vcf'.format(job_id)), os.path.join(app.config['UPLOAD_FOLDER'], '{}.out'.format(job_id)))
    redirect = flask.redirect(flask.url_for('process_vcf', job=job_id))
    response = flask.current_app.make_response(redirect)
    response.set_cookie('settings', value=json.dumps(settings))
    return response

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

@app.route('/vcf_result/<job>/')
def vcf_result(job):
    '''
        redirected to this after annotation has finished
    '''
    status = runner.job_status(RUNNER_DB, job)
    if status is None: # no output
        return flask.render_template('main.html', errors=['Job not found'], form=flask.request.form)

    # determine counts for genes
    settings_str = flask.request.cookies.get('settings')
    settings = json.loads(settings_str)
    result = []
    warnings = []
    current = [None, 0]
    for line in open(os.path.join(app.config['UPLOAD_FOLDER'], '{}.out'.format(job)), 'r'):
        fields = line.strip('\n').split('\t')
        if current[0] == fields[0]: # same gene
            current[1] += 1 # TODO
        else: # new gene
            if current[0] is not None:
                matches = helpers.get_exac_detail(query_db=query_db, gene=current[0], settings=settings)
                if matches[1] is not None:
                    statistics = calculate.calculate_burden_statistics(case_burden=current[1], total_cases=settings['cases'], population_burden=matches[0], total_population=EXAC_POPULATION)
                    result.append({'gene': current[0], 'burden': current[1], 'matches': matches[0], 'protein_length': matches[1], \
                        'z_test': statistics[0], 'binomial_test': statistics[1], 'relative_risk': statistics[2], 'rr_conf_interval': statistics[3]})
                else:
                    # gene is no good
                    warnings.append( 'Gene "{}" or variants not found in the selected database'.format(current[0]))
            # start counting new gene
            current = [fields[0], 1] # TODO
    if current[1] > 0:
        matches = helpers.get_exac_detail(query_db=query_db, gene=current[0], settings=settings)
        if matches[1] is not None:
            statistics = calculate.calculate_burden_statistics(case_burden=current[1], total_cases=settings['cases'], population_burden=matches[0], total_population=EXAC_POPULATION)
            result.append({'gene': current[0], 'burden': current[1], 'matches': matches[0], 'protein_length': matches[1], \
                'z_test': statistics[0], 'binomial_test': statistics[1], 'relative_risk': statistics[2], 'rr_conf_interval': statistics[3]})
        else:
            # gene is no good
            warnings.append( 'Gene "{}" or variants not found in the selected database'.format(current[0]))

    return flask.render_template('results.html',
        result=result,
        filter_type=settings['filter_type'],
        filter_value=settings['filter_value'],
        filter_af_pop=','.join(settings['filter_af_pop']),
        filter_af_value=settings['filter_af_value'],
        cases=settings['cases'],
        include_impacts=','.join(settings['include_impacts']),
        gene_list = ','.join(["'{}'".format(item['gene'].replace("'", "\\'")) for item in result if item['protein_length'] is not None]),
        protein_lengths = ','.join([ str(item['protein_length']) for item in result if item['protein_length'] is not None]),
        binomial_pvalues = ','.join([ '{0:0.3e}'.format(item['binomial_test']) for item in result if item['protein_length'] is not None]),
        relative_risk = ','.join([ '{0:0.3e}'.format(item['relative_risk']) for item in result if item['protein_length'] is not None]),
        rr_conf_interval = ','.join([ str(item['rr_conf_interval']) for item in result if item['protein_length'] is not None]),
        warnings = warnings
    )

@app.route('/process_vcf/<job>/')
def process_vcf(job):
    '''
        wait for the annotation job to finish by continually checking the job status
    '''
    status = runner.job_status(RUNNER_DB, job)
    if status is None:
        return flask.render_template('main.html', form=flask.request.form)
    elif status['status'] == 'F': # finished
        return flask.redirect(flask.url_for("vcf_result", job=job))
    else: # still in progress
        return flask.render_template('process_vcf.html', job=job, status=status)

@app.route('/about')
def about():
    return flask.render_template('about.html')

@app.route('/contact')
def contact():
    return flask.render_template('contact.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
