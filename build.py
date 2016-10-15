#!/usr/bin/env python
'''
    Build sqlite database from exac data
'''

import argparse
import sys

import sqlite3

FIELDS=17

def main(db_file, exac_files, protein_lengths):
    db = sqlite3.connect(db_file)
    # create
    # v1
    #gene    impact  impact_type     allele_count    total_alleles   exac_aaf        cadd    condel  sift    polyphen
    #IGHV1OR21-1     initiator_codon_variant MED     3       106210  2.4841428879e-05        8.94    None    0.0     0.895
    # v2
    # gene    impact  impact_type     allele_count    total_alleles   exac_all_pop    exac_african    exac_latino     exac_east_asian exac_fin        exac_nonfin_eur exac_south_asian        exac_other      cadd    cadd_indel      condel_score    sift    polyphen
    # OR4F5   missense_variant        MED     1811    53105   0.0246222927882 0.00255297421496        0.00804493017608        0.0     0.0462395543175 0.0405799922149 0.00488782051282        0.0271565495208 12.14   None    0.270106915236  0.0     0.568
    # v3
    #gene   impact impact_type  	allele_count   	total_alleles  	exac_all_pop   	exac_african   	exac_latino    	exac_east_asian	exac_fin       	exac_nonfin_eur	exac_south_asian       	exac_other     	caddcondel_score   	sift   	polyphen
    # OR4F5  	missense_variant       	MED    	1811   	53105  	0.0246222927882	0.00255297421496       	0.00804493017608       	0      	0.0462395543175	0.0405799922149	0.00488782051282       	0.0271565495208	10.53      	0.270106915236 	0.0    	0.568
    if exac_files is not None:
        #db.execute('''CREATE TABLE if not exists exac (gene text, impact text, impact_type text, allele_count int, total_alleles int, exac_aaf real, cadd read, condel real, sift real, polyphen real)''')
        db.execute('''CREATE TABLE if not exists exac (gene text, impact text, impact_type text, allele_count int, total_alleles int, exac_all real, exac_african real, exac_latino real, exac_east_asian real, exac_fin real, exac_nonfin_eur real, exac_south_asian real, exac_other real, cadd read, condel real, sift real, polyphen real)''')
        db.execute('''CREATE INDEX if not exists exac_gene ON exac(gene)''')
    
        # exac
        for src_file in exac_files:
            sys.stderr.write('processing {}\n'.format(src_file))
            with open(src_file, 'r') as src:
                first = True
                for idx, line in enumerate(src):
                    if first:
                        first = False
                        continue
                    fields = line.strip('\n').split('\t')
                    if len(fields) != FIELDS:
                        sys.stderr.write('skipping {}. expected {} got {}\n'.format(line, FIELDS, len(fields)))
                        continue
                    for i in range(len(fields)):
                        if fields[i] == 'None':
                            fields[i] = 'null'
                    db.execute('''INSERT INTO exac VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'''.format(
                        fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9],
                        fields[10], fields[11], fields[12], fields[13], fields[14], fields[15], fields[16]))
                    if idx % 1000 == 0:
                        db.commit()
                        sys.stderr.write('processing exac {}: {} lines processed\n'.format(src_file, idx))
            db.commit()

    # gene
    if protein_lengths is not None:
        db.execute('''CREATE TABLE if not exists protein_length (gene text, protein_length int)''')
        db.execute('''CREATE INDEX if not exists protein_length_gene ON protein_length(gene)''')
        first = True
        for idx, line in enumerate(open(protein_lengths, 'r')):
            if first:
                first = False
                continue
            fields = line.strip('\n').split('\t')
            db.execute('''INSERT INTO protein_length VALUES('{}', {})'''.format(fields[0], fields[1]))
            if idx % 1000 == 0:
                db.commit()
                sys.stderr.write('processing gene lengths {}: {} lines processed\n'.format(protein_lengths, idx))
        db.commit()

    db.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build Exac DB for burden calculation')
    parser.add_argument('--db', required=True, help='db to write to')
    parser.add_argument('--exac', nargs='*', help='exac files to read from')
    parser.add_argument('--length', help='gene length files to read from')
    args = parser.parse_args()
    main(args.db, args.exac, args.length)
