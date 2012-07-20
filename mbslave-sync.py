#!/usr/bin/env python

import ConfigParser
import psycopg2
import tarfile
import sys
import os
import time
import re
import urllib2
import shutil
import tempfile
from mbslave import Config, ReplicationHook, connect_db
from mbslave.monitoring import StatusReport


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

    def __init__(self, db, schema, not_ignored_tables, replication_seq, hook):
        self._db = db
        self._data = {}
        self._transactions = {}
        self._schema = schema
        self.not_ignored_tables = not_ignored_tables
        self._hook = hook
        self._replication_seq = replication_seq

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

        self._hook.begin(self._replication_seq)
        for xid in sorted(self._transactions.keys()):
            transaction = self._transactions[xid]
            #print ' - Running transaction', xid
            #print 'BEGIN; --', xid
            for id, table, type in sorted(transaction):
                if table not in self.not_ignored_tables:
                    continue
                fulltable = self._schema + '.' + table
                if fulltable not in stats:
                    stats[fulltable] = {'d': 0, 'u': 0, 'i': 0}
                stats[fulltable][type] += 1
                keys = self._data.get((id, True), {})
                values = self._data.get((id, False), {})
                if type == 'd':
                    sql = 'DELETE FROM %s' % (fulltable,)
                    params = []
                    self._hook.before_delete(table, keys)
                elif type == 'u':
                    sql_values = ', '.join('%s=%%s' % i for i in values)
                    sql = 'UPDATE %s SET %s' % (fulltable, sql_values)
                    params = values.values()
                    self._hook.before_update(table, keys, values)
                elif type == 'i':
                    sql_columns = ', '.join(values.keys())
                    sql_values = ', '.join(['%s'] * len(values))
                    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (fulltable, sql_columns, sql_values)
                    params = values.values()
                    self._hook.before_insert(table, values)
                if type == 'd' or type == 'u':
                    sql += ' WHERE ' + ' AND '.join('%s%s%%s' % (i, ' IS ' if keys[i] is None else '=') for i in keys.keys())
                    params.extend(keys.values())
                #print sql, params
                try:
                    cursor.execute(sql, params)
                except Exception as err:
                    print "Error: "
                    print err
                self._db.commit()

                    
                if type == 'd':
                    self._hook.after_delete(table, keys)
                elif type == 'u':
                    self._hook.after_update(table, keys, values)
                elif type == 'i':
                    self._hook.after_insert(table, values)
            #print 'COMMIT; --', xid
        print ' - Statistics:'
        for table in sorted(stats.keys()):
            print '   * %-30s\t%d\t%d\t%d' % (table, stats[table]['i'], stats[table]['u'], stats[table]['d'])
        self._hook.before_commit()
        self._hook.after_commit()


def process_tar(fileobj, db, schema, ignored_tables, expected_schema_seq, replication_seq, hook):
    print "Processing", fileobj.name
    tar = tarfile.open(fileobj=fileobj, mode='r:bz2')
    importer = PacketImporter(db, schema, ignored_tables, replication_seq, hook)
    for member in tar:
        if member.name == 'SCHEMA_SEQUENCE':
            schema_seq = int(tar.extractfile(member).read().strip())
            if schema_seq != expected_schema_seq:
                raise Exception("Mismatched schema sequence, %d (database) vs %d (replication packet)" % (expected_schema_seq, schema_seq))
        elif member.name == 'TIMESTAMP':
            ts = tar.extractfile(member).read().strip()
            print ' - Packet was produced at', ts
        elif member.name in ('mbdump/Pending', 'mbdump/dbmirror_pending'):
            importer.load_pending(tar.extractfile(member))
        elif member.name in ('mbdump/PendingData', 'mbdump/dbmirror_pendingdata'):
            importer.load_pending_data(tar.extractfile(member))
    importer.process()


def download_packet(base_url, replication_seq):
    url = base_url + "/replication-%d.tar.bz2" % replication_seq
    print "Downloading", url
    try:
        data = urllib2.urlopen(url)
    except urllib2.HTTPError, e:
        if e.code == 404:
            return None
        raise
    tmp = tempfile.NamedTemporaryFile(suffix='.tar.bz2')
    shutil.copyfileobj(data, tmp)
    data.close()
    tmp.seek(0)
    return tmp

config = Config(os.path.dirname(__file__) + '/mbslave.conf')
db = connect_db(config)
cursor = db.cursor()

schema = config.get('DATABASE', 'schema')
base_url = config.get('MUSICBRAINZ', 'base_url')
cursor.execute("SELECT array_to_string((SELECT array_agg(table_name::TEXT) FROM information_schema.tables WHERE table_schema = '%s')::text[], ',');" % schema)
not_ignored_tables = cursor.fetchall()[0][0].split(',')
if config.solr.enabled:
    from mbslave.search import SolrReplicationHook
    hook_class = SolrReplicationHook
else:
    hook_class = ReplicationHook

cursor.execute("SELECT current_schema_sequence, current_replication_sequence FROM %s.replication_control" % schema)
schema_seq, replication_seq = cursor.fetchone()

status = StatusReport(schema_seq, replication_seq)
if config.monitoring.enabled:
    status.load(config.monitoring.status_file)

start = time.time()

while True:
    replication_seq += 1
    print replication_seq
    hook = hook_class(config, db, schema)
    tmp = download_packet(base_url, replication_seq)
    if tmp is None:
        print 'Not found, stopping'
        status.end()
        break
    process_tar(tmp, db, schema, not_ignored_tables, schema_seq, replication_seq, hook)
    tmp.close()
    status.update(replication_seq)
    print time.time() - start

if config.monitoring.enabled:
    status.save(config.monitoring.status_file)

