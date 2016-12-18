#!/usr/bin/env python

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

# output from annotation tool
#OUTPUT_FIELDS = {'gene': 0, 'chrom': 1, 'start': 2, 'end': 3, 'ref': 4, 'alt': 5, 'impact': 6, 'impact_type': 7, 'allele_count': 8, 'allele_number': 9, 'exac_all_pop_ac': 10, 'exac_all_pop_an': 11, 'exac_all_pop': 12, 'exac_african_ac': 13, 'exac_african_an': 14, 'exac_african': 15, 'exac_latino_ac': 16, 'exac_latino_an': 17, 'exac_latino': 18, 'exac_east_asian_ac': 19, 'exac_east_asian_an': 20, 'exac_east_asian': 21, 'exac_fin_ac': 22, 'exac_fin_an': 23, 'exac_fin': 24, 'exac_nonfin_eur_ac': 25, 'exac_nonfin_eur_anexac_nonfin_eur': 26, 'exac_south_asian_ac': 27, 'exac_south_asian_an': 28, 'exac_south_asian': 29, 'exac_other_ac': 30, 'exac_other_ac': 31, 'exac_other': 32, 'sift': 33, 'polyphen': 34, 'condel': 35, 'cadd': 36}
OUTPUT_FIELDS = {'gene': 0, 'chrom': 1, 'start': 2, 'end': 3, 'ref': 4, 'alt': 5, 'impact': 6, 'impact_type': 7, 'allele_count': 8, 'allele_number': 9, 'exac_all_pop_ac': 10, 'exac_all_pop_an': 11, 'exac_all': 12, 'exac_african_ac': 13, 'exac_african_an': 14, 'exac_african': 15, 'exac_latino_ac': 16, 'exac_latino_an': 17, 'exac_latino': 18, 'exac_east_asian_ac': 19, 'exac_east_asian_an': 20, 'exac_east_asian': 21, 'exac_fin_ac': 22, 'exac_fin_an': 23, 'exac_fin': 24, 'exac_nonfin_eur_ac': 25, 'exac_nonfin_eur_anexac_nonfin_eur': 26, 'exac_south_asian_ac': 27, 'exac_south_asian_an': 28, 'exac_south_asian': 29, 'exac_other_ac': 30, 'exac_other_ac': 31, 'exac_other': 32, 'sift': 33, 'polyphen': 34, 'condel': 35, 'cadd': 36}

def get_vcf_match(fields, settings):
    '''
        returns 1 if the input line passes the filter, 0 otherwise
    '''
    # filter type
    if fields[OUTPUT_FIELDS[settings['filter_type']]] != 'None' and float(fields[OUTPUT_FIELDS[settings['filter_type']]]) < float(settings['filter_value']):
        return 0

    # impact filter
    if len(settings['include_impacts']) > 0:
        found = False
        for impact in settings['include_impacts']:
            if fields[OUTPUT_FIELDS['impact_type']] == impact: # match any impact to pass
                found = True
                break
        if not found:
            return 0

    # population filter for a list of selected populations
    population_filter = ''
    if settings['filter_af_pop']:
        for pop_value in settings['filter_af_pop']:
            if fields[OUTPUT_FIELDS[pop_value]] == 'None':
                continue # this is ok
            if float(fields[OUTPUT_FIELDS[pop_value]]) >= float(settings['filter_af_value']): # field must be less for each pop
                return 0

    return int(fields[OUTPUT_FIELDS['allele_count']])

def get_exac_detail(query_db, gene, settings):
    sql_parameters = [gene, settings['filter_value']]

    # impact filter
    additional_filter = " and ("
    if len(settings['include_impacts']) > 0:
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
    query = "select sum(allele_count), protein_length from exac left join protein_length on exac.gene=protein_length.gene where exac.gene=? and (exac.{} >= ? or exac.{} is null) {} {}".format(
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
