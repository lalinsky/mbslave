#!/usr/bin/env python

import ConfigParser
#import psycopg2
import MySQLdb
import tarfile
import sys
import os
import shutil
import tempfile


def table_name(schema, table):
    if False:
        return schema + "." + table
    else:
        return "`" + table + "`"

def load_tar(filename, db, schema, ignored_tables):
    print "Importing data from", filename
    tar = tarfile.open(filename, 'r:bz2')
    cursor = db.cursor()
    for member in tar:
        if not member.name.startswith('mbdump/'):
            continue
        table = member.name.split('/')[1].replace('_sanitised', '')
        fulltable = table_name(schema, table)
        if table in ignored_tables:
            print " - Ignoring", fulltable
            continue
        cursor.execute("SELECT 1 FROM %s LIMIT 1" % fulltable)
        if cursor.fetchone():
            print " - Skipping", fulltable, "(already contains data)"
            continue
        print " - Loading", fulltable
        tmp = tempfile.NamedTemporaryFile(suffix='.txt')
        shutil.copyfileobj(tar.extractfile(member), tmp)
        cursor.execute("LOAD DATA LOCAL INFILE %%s INTO TABLE %s" % (fulltable,), (tmp.name,))
        #cursor.copy_from(tar.extractfile(member), fulltable)
        db.commit()


config = ConfigParser.RawConfigParser()
config.read(os.path.dirname(__file__) + '/mbslave.conf')

opts = {}
opts['charset'] = 'utf8'
opts['local_infile'] = True
opts['db'] = config.get('DATABASE', 'name')
opts['user'] = config.get('DATABASE', 'user')
if config.has_option('DATABASE', 'host'):
	opts['host'] = config.get('DATABASE', 'host')
if config.has_option('DATABASE', 'port'):
	opts['port'] = config.get('DATABASE', 'port')
db = MySQLdb.connect(**opts)

schema = config.get('DATABASE', 'schema')
ignored_tables = set(config.get('TABLES', 'ignore').split(','))
for filename in sys.argv[1:]:
    load_tar(filename, db, schema, ignored_tables)

