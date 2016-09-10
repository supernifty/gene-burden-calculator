#!/usr/bin/env python
'''
    Build sqlite database from exac data
'''

import argparse
import sys

import sqlite3

def main(db_file, src_files):
    db = sqlite3.connect(db_file)
    # create
    #gene    impact  impact_type     allele_count    total_alleles   exac_aaf        cadd    condel  sift    polyphen
    #IGHV1OR21-1     initiator_codon_variant MED     3       106210  2.4841428879e-05        8.94    None    0.0     0.895
    db.execute('''CREATE TABLE exac (gene text, impact text, impact_type text, allele_count int, total_alleles int, exac_aaf real, cadd read, condel real, sift real, polyphen real)''')
    db.execute('''CREATE INDEX exac_gene ON exac(gene)''')

    # load
    for src_file in src_files:
        sys.stderr.write('processing {}\n'.format(src_file))
        with open(src_file, 'r') as src:
            first = True
            for idx, line in enumerate(src):
                if first:
                    first = False
                    continue
                fields = line.strip('\n').split('\t')
                if len(fields) != 10:
                    sys.stderr.write('skipping {}\n'.format(line))
                    continue
                for i in range(len(fields)):
                    if fields[i] == 'None':
                        fields[i] = 'null'
                db.execute('''INSERT INTO exac VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {})'''.format(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9]))
                db.commit()
                if idx % 1000 == 0:
                    sys.stderr.write('processing {}: {} lines processed\n'.format(src_file, idx))
    db.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare BAMs')
    parser.add_argument('--db', required=True, help='db to write to')
    parser.add_argument('files', nargs='*', help='exac files to read from')
    args = parser.parse_args()
    main(args.db, args.files)
