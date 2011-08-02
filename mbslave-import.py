#!/usr/bin/env python

import ConfigParser
import psycopg2
import tarfile
import sys
import os


def load_tar(filename, db, schema, ignored_tables):
    print "Importing data from", filename
    tar = tarfile.open(filename, 'r:bz2')
    cursor = db.cursor()
    for member in tar:
        if not member.name.startswith('mbdump/'):
            continue
        table = member.name.split('/')[1].replace('_sanitised', '')
        fulltable = schema + "." + table
        if table in ignored_tables:
            print " - Ignoring", fulltable
            continue
        cursor.execute("SELECT 1 FROM %s LIMIT 1" % fulltable)
        if cursor.fetchone():
            print " - Skipping", fulltable, "(already contains data)"
            continue
        print " - Loading", fulltable
        cursor.copy_from(tar.extractfile(member), fulltable)
        db.commit()


config = ConfigParser.RawConfigParser()
config.read(os.path.dirname(__file__) + '/mbslave.conf')

opts = {}
opts['database'] = config.get('DATABASE', 'name')
opts['user'] = config.get('DATABASE', 'user')
if config.has_option('DATABASE', 'password'):
	opts['password'] = config.get('DATABASE', 'password')
if config.has_option('DATABASE', 'host'):
	opts['host'] = config.get('DATABASE', 'host')
if config.has_option('DATABASE', 'port'):
	opts['port'] = config.get('DATABASE', 'port')
db = psycopg2.connect(**opts)

schema = config.get('DATABASE', 'schema')
ignored_tables = set(config.get('TABLES', 'ignore').split(','))
for filename in sys.argv[1:]:
    load_tar(filename, db, schema, ignored_tables)

