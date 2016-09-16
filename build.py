#!/usr/bin/env python
'''
    Build sqlite database from exac data
'''

import argparse
import sys

import sqlite3

def main(db_file, exac_files, protein_lengths):
    db = sqlite3.connect(db_file)
    # create
    #gene    impact  impact_type     allele_count    total_alleles   exac_aaf        cadd    condel  sift    polyphen
    #IGHV1OR21-1     initiator_codon_variant MED     3       106210  2.4841428879e-05        8.94    None    0.0     0.895
    if exac_files is not None:
        db.execute('''CREATE TABLE if not exists exac (gene text, impact text, impact_type text, allele_count int, total_alleles int, exac_aaf real, cadd read, condel real, sift real, polyphen real)''')
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
                    if len(fields) != 10:
                        sys.stderr.write('skipping {}\n'.format(line))
                        continue
                    for i in range(len(fields)):
                        if fields[i] == 'None':
                            fields[i] = 'null'
                    db.execute('''INSERT INTO exac VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {})'''.format(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9]))
                    db.commit()
                    if idx % 1000 == 0:
                        sys.stderr.write('processing exac {}: {} lines processed\n'.format(src_file, idx))

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
            db.commit()
            if idx % 1000 == 0:
                sys.stderr.write('processing gene lengths {}: {} lines processed\n'.format(protein_lengths, idx))

    db.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare BAMs')
    parser.add_argument('--db', required=True, help='db to write to')
    parser.add_argument('--exac', nargs='*', help='exac files to read from')
    parser.add_argument('--length', help='exac files to read from')
    args = parser.parse_args()
    main(args.db, args.exac, args.length)
