#!/usr/bin/env python

import ConfigParser
import psycopg2
import tarfile
import sys
import os
import re


def parse_data_fields(s):
    fields = {}
    for name, value in re.findall(r'''"([^"]+)"=('(?:''|[^'])*') ''', s):
        if not value:
            value = None
        else:
            value = value[1:-1].replace("''", "'").replace("\\\\", "\\")
        fields[name] = value
    return fields


def parse_bool(s):
    return s == 't'


ESCAPES = (('\\b', '\b'), ('\\f', '\f'), ('\\n', '\n'), ('\\r', '\r'),
           ('\\t', '\t'), ('\\v', '\v'), ('\\\\', '\\'))

def unescape(s):
    if s == '\\N':
        return None
    for orig, repl in ESCAPES:
        s = s.replace(orig, repl)
    return s


def read_psql_dump(fp, types):
    for line in fp:
        values = map(unescape, line.rstrip('\r\n').split('\t'))
        for i, value in enumerate(values):
            if value is not None:
                values[i] = types[i](value)
        yield values


class PacketImporter(object):

    def __init__(self, db, schema, ignored_tables):
        self._db = db
        self._data = {}
        self._transactions = {}
        self._schema = schema
        self._ignored_tables = ignored_tables

    def load_pending_data(self, fp):
        dump = read_psql_dump(fp, [int, parse_bool, parse_data_fields])
        for id, key, values in dump:
            self._data[(id, key)] = values

    def load_pending(self, fp):
        dump = read_psql_dump(fp, [int, str, str, int])
        for id, table, type, xid in dump:
            table = table.split(".")[1].strip('"')
            transaction = self._transactions.setdefault(xid, [])
            transaction.append((id, table, type))

    def process(self):
        cursor = self._db.cursor()
        for xid in sorted(self._transactions.keys()):
            transaction = self._transactions[xid]
            #print ' - Running transaction', xid
            #print 'BEGIN; --', xid
            for id, table, type in sorted(transaction):
                if table in self._ignored_tables:
                    continue
                fulltable = self._schema + '.' + table
                if type == 'd':
                    sql = 'DELETE FROM %s' % (fulltable,)
                    params = []
                elif type == 'u':
                    values = self._data[(id, False)]
                    sql_values = ', '.join('%s=%%s' % i for i in values)
                    sql = 'UPDATE %s SET %s' % (fulltable, sql_values)
                    params = values.values()
                elif type == 'i':
                    values = self._data[(id, False)]
                    sql_columns = ', '.join(values.keys())
                    sql_values = ', '.join(['%s'] * len(values))
                    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (fulltable, sql_columns, sql_values)
                    params = values.values()
                if type == 'd' or type == 'u':
                    values = self._data[(id, True)]
                    sql += ' WHERE ' + ' AND '.join('%s=%%s' % i for i in values.keys())
                    params.extend(values.values())
                cursor.execute(sql, params)
                #print sql, params
            #print 'COMMIT; --', xid
        self._db.commit()


def process_tar(filename, db, schema, ignored_tables):
    print "Processing", filename
    tar = tarfile.open(filename, 'r:bz2')
    importer = PacketImporter(db, schema, ignored_tables)
    for member in tar:
        if member.name == 'mbdump/Pending':
            importer.load_pending(tar.extractfile(member))
        elif member.name == 'mbdump/PendingData':
            importer.load_pending_data(tar.extractfile(member))
    importer.process()


config = ConfigParser.RawConfigParser()
config.read(os.path.dirname(__file__) + '/mbslave.conf')

opts = {}
opts['database'] = config.get('DATABASE', 'name')
opts['user'] = config.get('DATABASE', 'user')
if config.has_option('DATABASE', 'host'):
	opts['host'] = config.get('DATABASE', 'host')
if config.has_option('DATABASE', 'port'):
	opts['port'] = config.get('DATABASE', 'port')
db = psycopg2.connect(**opts)

schema = config.get('DATABASE', 'schema')
ignored_tables = set(config.get('TABLES', 'ignore').split(','))
for filename in sys.argv[1:]:
    process_tar(filename, db, schema, ignored_tables)

