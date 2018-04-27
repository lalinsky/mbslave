#!/usr/bin/env python2

import tarfile
import os
import re
import urllib2
import shutil
import tempfile
from mbslave import Config, ReplicationHook, connect_db, parse_name, fqn
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

    def __init__(self, db, config, ignored_schemas, ignored_tables, replication_seq, hook):
        self._db = db
        self._data = {}
        self._transactions = {}
        self._config = config
        self._ignored_schemas = ignored_schemas
        self._ignored_tables = ignored_tables
        self._hook = hook
        self._replication_seq = replication_seq

    def load_pending_data(self, fp):
        dump = read_psql_dump(fp, [int, parse_bool, parse_data_fields])
        for id, key, values in dump:
            self._data[(id, key)] = values

    def load_pending(self, fp):
        dump = read_psql_dump(fp, [int, str, str, int])
        for id, table, type, xid in dump:
            schema, table = parse_name(self._config, table)
            transaction = self._transactions.setdefault(xid, [])
            transaction.append((id, schema, table, type))

    def process(self):
        cursor = self._db.cursor()
        stats = {}
        self._hook.begin(self._replication_seq)
        for xid in sorted(self._transactions.keys()):
            transaction = self._transactions[xid]
            #print ' - Running transaction', xid
            #print 'BEGIN; --', xid
            for id, schema, table, type in sorted(transaction):
                if schema == '<ignore>':
                    continue
                if schema in self._ignored_schemas:
                    continue
                if table in self._ignored_tables:
                    continue
                fulltable = fqn(schema, table)
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
                cursor.execute(sql, params)
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
        self._db.commit()
        self._hook.after_commit()


def process_tar(fileobj, db, schema, ignored_schemas, ignored_tables, expected_schema_seq, replication_seq, hook):
    print "Processing", fileobj.name
    tar = tarfile.open(fileobj=fileobj, mode='r:bz2')
    importer = PacketImporter(db, schema, ignored_schemas, ignored_tables, replication_seq, hook)
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


def download_packet(base_url, token, replication_seq):
    url = base_url.rstrip("/") + "/replication-%d.tar.bz2" % replication_seq
    if token:
        url += '?token=' + token
    print "Downloading", url
    try:
        data = urllib2.urlopen(url, timeout=60)
    except urllib2.HTTPError, e:
        if e.code == 404:
            return None
        raise
    tmp = tempfile.NamedTemporaryFile(suffix='.tar.bz2')
    shutil.copyfileobj(data, tmp)
    data.close()
    tmp.seek(0)
    return tmp

config = Config(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mbslave.conf'))
db = connect_db(config)

base_url = config.get('MUSICBRAINZ', 'base_url')
if config.has_option('MUSICBRAINZ', 'token'):
    token = config.get('MUSICBRAINZ', 'token')
else:
    token = None
ignored_schemas = set(config.get('schemas', 'ignore').split(','))
ignored_tables = set(config.get('TABLES', 'ignore').split(','))

hook_class = ReplicationHook

cursor = db.cursor()
cursor.execute("SELECT current_schema_sequence, current_replication_sequence FROM %s.replication_control" % config.schema.name('musicbrainz'))
schema_seq, replication_seq = cursor.fetchone()

status = StatusReport(schema_seq, replication_seq)
if config.monitoring.enabled:
    status.load(config.monitoring.status_file)

while True:
    replication_seq += 1
    hook = hook_class(config, db, config)
    tmp = download_packet(base_url, token, replication_seq)
    if tmp is None:
        print 'Not found, stopping'
        status.end()
        break
    process_tar(tmp, db, config, ignored_schemas, ignored_tables, schema_seq, replication_seq, hook)
    tmp.close()
    status.update(replication_seq)

if config.monitoring.enabled:
    status.save(config.monitoring.status_file)

