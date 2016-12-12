
def parse_settings(form):
    result = {'errors': []}

    # case count
    try:
        result['cases'] = int(form['cases'])
        if result['cases'] <= 0:
            result['errors'].append('Number of cases must be greater than zero')
    except ValueError:
            result['errors'].append('Number of cases must be numeric')

    # parse overall settings - filtering
    result['filter_type'] = form['filter_type']
    if result['filter_type'] not in ('cadd', 'condel', 'sift', 'polyphen'):
        result['errors'] = ['Invalid filter type']
        return result
    try:
        result['filter_value'] = float(form['filter_value'])
    except ValueError:
        result['errors'].append('Filter value must be numeric')

    # check filter types options
    try:
        result['include_impacts'] = form.getlist('impacts')
        if len(result['include_impacts'])==0:
            result['errors'].append('Missing variant impact type')
    except ValueError:
        result['errors'].append('Missing variant impact type')

    # population filter
    try:
        result['filter_af_pop'] = form.getlist('filter_af_pop')
        if len(result['filter_af_pop']) == 0:
            result['errors'].append('Invalid population name')
    except ValueError:
        result['errors'] = ['Invalid population name']
        return result

    # check filter popultaion names?
    if not set(result['filter_af_pop']).issubset(set(['exac_all', 'exac_african', 'exac_latino', 'exac_east_asian', 'exac_fin', 'exac_nonfin_eur', 'exac_south_asian', 'exac_other'])):
         result['errors'] = ['Invalid population name']
         return result

    # filter AF value
    try:
        result['filter_af_value'] = float(form['filter_af_value'])
    except ValueError:
        result['errors'].append('Filter allele frequency must be numeric')

    return result

def get_exac_detail(query_db, gene, settings):
    sql_parameters = [gene, settings['filter_value']]

    # additional
    additional_filter = " and ("
    if len(settings['include_impacts'])>0:
        for impact in settings['include_impacts'][:-1]:
            additional_filter += (" impact = ? or ")
            sql_parameters.append(impact)
        additional_filter += (" impact = ? ")
        sql_parameters.append(settings['include_impacts'][-1])
    additional_filter += ")"

    # population filter for a list of selected populations
    population_filter = ''
    if settings['filter_af_pop']:
        for pop_value in settings['filter_af_pop']:
            population_filter += (" and exac.{} < ?").format(pop_value)
            sql_parameters.append(settings['filter_af_value'])

    # find matching genes
    query = "select count(*), protein_length from exac left join protein_length on exac.gene=protein_length.gene where exac.gene=? and (exac.{} >= ? or exac.{} is null) {} {}".format(
            settings['filter_type'],
            settings['filter_type'],
            additional_filter,
            population_filter)
    matches = query_db(
            query,
            sql_parameters,
            one=True)

    # count, length
    return matches 
