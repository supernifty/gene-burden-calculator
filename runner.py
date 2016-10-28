#!/usr/bin/env python
'''
  simple queueing job manager

  statuses: A=available, R=running, F=finished
'''

import argparse
import datetime
import os
import sqlite3
import sys
import time
import traceback

DEBUG = False
COMMAND = "wc < {input} > {output}"

def log(sev, msg):
    if DEBUG or sev != 'DEBUG':
        sys.stderr.write('{} {}: {}\n'.format(datetime.datetime.now().strftime('%y%m%d %H:%M:%S'), sev, msg))

def query_db(db, query, args=(), one=False):
    cur = db.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def run_queue(db_file):
    db = sqlite3.connect(db_file)
    log('INFO', 'checking for jobs to run...')
    
    next_job = query_db(db, '''select rowid, input, output from job where status = 'A' order by created limit 1''', one=True)
    if next_job is None:
        log('INFO', 'nothing to do')
    else:
        parameters = {'input': next_job[1], 'output': next_job[2], 'id': next_job[0]}
        db.execute('''update job set status = 'R', started = ? where rowid = ?''', (datetime.datetime.utcnow(), next_job[0])) # R = running
        db.commit()

        log('INFO', 'running command with parameters {}...'.format(parameters))
        os.system(COMMAND.format(**parameters))
        log('INFO', 'running command with parameters {}: done'.format(parameters))

        db.execute('''update job set status = 'F', finished = ? where rowid = ?''', (datetime.datetime.utcnow(), next_job[0])) # F = finished
        db.commit()
    
    log('INFO', 'checking for jobs to run: done')

def add_to_queue(db_file, job_id, src_file, dest_file):
    log('INFO', 'adding item to queue...')
    db = sqlite3.connect(db_file)
    # generate schema if not already present
    db.execute('''create table if not exists job (job_id text, created real, started real, finished real, input text, output text, status char)''')

    # add item
    db.execute('''insert into job (job_id, created, input, output, status) values (?, ?, ?, ?, ?)''', (job_id, datetime.datetime.utcnow(), src_file, dest_file, 'A')) # A = available

    db.commit()
    log('INFO', 'adding item to queue: done')

def job_status(db_file, job_id):
    db = sqlite3.connect(db_file)
    status = query_db(db, '''select created, started, finished, status from job where job_id = ?''', [job_id], one=True)
    return { 'created': status[0], 'started': status[1], 'finished': status[2], 'status': status[3] }

def main():
    parser = argparse.ArgumentParser(description='Update job status database')
    parser.add_argument('--db', required=True, help='target database')
    parser.add_argument('--sleep', required=False, type=int, help='repeatedly update the database every sleep seconds')
    parser.add_argument('--debug', required=False, default=False, action='store_true', help='write additional debugging info')
    parser.add_argument('--input', required=False, help='input file')
    parser.add_argument('--output', required=False, help='output file')
    args = parser.parse_args()
    DEBUG = args.debug

    if args.input and args.output:
        add_to_queue(args.db, "test_job", args.input, args.output)

    if args.sleep:
        while True:
            try:
                run_queue(args.db)
            except Exception as err:
                traceback.print_exc()
            time.sleep(args.sleep)
    else:
        run_queue(args.db)

if __name__ == '__main__':
  main()
