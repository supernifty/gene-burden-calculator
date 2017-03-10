#!/usr/bin/env python
'''
    Build sqlite database from exac data
'''

import argparse
import sys

import sqlite3

FIELDS=38

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
    # v4
    # gene,chrom,start,end,ref,alt,impact,impact_type,allele_count,allele_number,
    # exac_all_pop_ac,exac_all_pop_an,exac_all_pop,exac_african_ac,exac_african_an,exac_african,exac_latino_ac,exac_latino_an,exac_latino,exac_east_asian_ac,
    # exac_east_asian_an,exac_east_asian,exac_fin_ac,exac_fin_an,exac_fin,exac_nonfin_eur_ac,exac_nonfin_eur_an,exac_nonfin_eur,exac_south_asian_ac,exac_south_asian_an,
    # exac_south_asian,exac_other_ac,exac_other_an,exac_other,sift,polyphen,condel_score,cadd
    if exac_files is not None:
        db.execute('''CREATE TABLE if not exists exac (gene text, chrom text, start int, end int, ref text, alt text, impact text, impact_type text, allele_count int,
            allele_number int, exac_all_ac int, exac_all_an int, exac_all real, exac_african_ac int, exac_african_an int, exac_african real, exac_latino_ac int, exac_latino_an int, exac_latino real,
            exac_east_asian_ac int, exac_east_asian_an int, exac_east_asian real, exac_fin_ac int, exac_fin_an int, exac_fin real, exac_nonfin_eur_ac int, exac_nonfin_eur_an int, exac_nonfin_eur real, exac_south_asian_ac int,
            exac_south_asian_an int, exac_south_asian real, exac_other_ac int, exac_other_an int, exac_other real, sift real, polyphen real, condel real, cadd real)''')
        #db.execute('''CREATE TABLE if not exists exac (gene text, impact text, impact_type text, allele_count int, total_alleles int, exac_all real, exac_african real, exac_latino real, exac_east_asian real, exac_fin real, exac_nonfin_eur real, exac_south_asian real, exac_other real, cadd read, condel real, sift real, polyphen real)''')
        db.execute('''CREATE INDEX if not exists exac_gene ON exac(gene)''')
        db.execute('''CREATE INDEX if not exists exac_impact ON exac(impact)''')
        db.execute('''CREATE INDEX if not exists exac_impact_type ON exac(impact_type)''')

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
                    db.execute('''INSERT INTO exac VALUES('{}', '{}', {}, {}, '{}', '{}', '{}', '{}', {}, {},
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                    {}, {}, {}, {}, {}, {}, {}, {})'''.format(
                        fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9],
                        fields[10], fields[11], fields[12], fields[13], fields[14], fields[15], fields[16], fields[17], fields[18], fields[19],
                        fields[20], fields[21], fields[22], fields[23], fields[24], fields[25], fields[26], fields[27], fields[28], fields[29],
                        fields[30], fields[31], fields[32], fields[33], fields[34], fields[35], fields[36], fields[37]))
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
