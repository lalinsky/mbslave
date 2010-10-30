#!/usr/bin/env python

import ConfigParser
import psycopg2
import tarfile
import sys
import os
import re
import urllib2
import shutil
import tempfile


def parse_data_fields(s):
    fields = {}
    for name, value in re.findall(r'''"([^"]+)"=('(?:''|[^'])*')? ''', s):
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
        stats = {}
        for xid in sorted(self._transactions.keys()):
            transaction = self._transactions[xid]
            #print ' - Running transaction', xid
            #print 'BEGIN; --', xid
            for id, table, type in sorted(transaction):
                if table in self._ignored_tables:
                    continue
                fulltable = self._schema + '.' + table
                if fulltable not in stats:
                    stats[fulltable] = {'d': 0, 'u': 0, 'i': 0}
                stats[fulltable][type] += 1
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
                #print sql, params
                cursor.execute(sql, params)
            #print 'COMMIT; --', xid
        print ' - Statistics:'
        for table in sorted(stats.keys()):
            print '   * %-30s\t%d\t%d\t%d' % (table, stats[table]['i'], stats[table]['u'], stats[table]['d'])
        self._db.commit()


def process_tar(fileobj, db, schema, ignored_tables, expected_schema_seq):
    print "Processing", fileobj.name
    tar = tarfile.open(fileobj=fileobj, mode='r:bz2')
    importer = PacketImporter(db, schema, ignored_tables)
    for member in tar:
        if member.name == 'SCHEMA_SEQUENCE':
            schema_seq = int(tar.extractfile(member).read().strip())
            if schema_seq != expected_schema_seq:
                raise Exception("Mismatched schema sequence, %d (database) vs %d (replication packet)" % (expected_schema_seq, schema_seq))
        elif member.name == 'TIMESTAMP':
            ts = tar.extractfile(member).read().strip()
            print ' - Packet was produced at', ts
        elif member.name == 'mbdump/Pending':
            importer.load_pending(tar.extractfile(member))
        elif member.name == 'mbdump/PendingData':
            importer.load_pending_data(tar.extractfile(member))
    importer.process()


def download_packet(replication_seq):
    url = "http://ftp.musicbrainz.org/pub/musicbrainz/data/replication/replication-%d.tar.bz2" % replication_seq
    print "Downloading", url
    try:
        data = urllib2.urlopen(url)
    except urllib2.URLError, e:
        if e.code == 404:
            return None
        raise
    tmp = tempfile.NamedTemporaryFile(suffix='.tar.bz2')
    shutil.copyfileobj(data, tmp)
    data.close()
    tmp.seek(0)
    return tmp

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

cursor = db.cursor()
cursor.execute("SELECT current_schema_sequence, current_replication_sequence FROM %s.replication_control" % schema)
schema_seq, replication_seq = cursor.fetchone()

while True:
    replication_seq += 1
    tmp = download_packet(replication_seq)
    if tmp is None:
        print 'Not found, stopping'
        break
    process_tar(tmp, db, schema, ignored_tables, schema_seq)
    tmp.close()

