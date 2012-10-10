import itertools
import urllib2
import psycopg2.extras
from collections import namedtuple
from lxml import etree as ET
from lxml.builder import E
from mbslave.replication import ReplicationHook

Entity = namedtuple('Entity', ['name', 'fields'])
Field = namedtuple('Field', ['name', 'column'])
MultiField = namedtuple('MultiField', ['name', 'column'])


class Schema(object):

    def __init__(self, entities):
        self.entities = entities
        self.entities_by_id = dict((e.name, e) for e in entities)

    def __getitem__(self, name):
        return self.entities_by_id[name]


class Entity(object):

    def __init__(self, name, fields):
        self.name = name
        self.fields = fields

    def iter_single_fields(self, name=None):
        for field in self.fields:
            if isinstance(field, Field):
                if name is not None and field.name != name:
                    continue
                yield field

    def iter_multi_fields(self, name=None):
        for field in self.fields:
            if isinstance(field, MultiField):
                if name is not None and field.name != name:
                    continue
                yield field


class Column(object):

    def __init__(self, name, foreign=None):
        self.name = name
        self.foreign = foreign


class ForeignColumn(Column):

    def __init__(self, table, name, foreign=None):
        super(ForeignColumn, self).__init__(name, foreign=foreign)
        self.table = table


schema = Schema([
    Entity('artist', [
        Field('id', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('artist_name', 'name'))),
        Field('sort_name', Column('sort_name', ForeignColumn('artist_name', 'name'))),
        Field('country', Column('country', ForeignColumn('country', 'name'))),
        Field('country_code', Column('country', ForeignColumn('country', 'iso_code'))),
        Field('gender', Column('gender', ForeignColumn('gender', 'name'))),
        Field('type', Column('type', ForeignColumn('artist_type', 'name'))),
        MultiField('ipi', ForeignColumn('artist_ipi', 'ipi')),
        MultiField('alias', ForeignColumn('artist_alias', 'name', ForeignColumn('artist_name', 'name'))),
    ]),
    Entity('label', [
        Field('id', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('label_name', 'name'))),
        Field('sort_name', Column('sort_name', ForeignColumn('label_name', 'name'))),
        Field('country', Column('country', ForeignColumn('country', 'name'))),
        Field('country_code', Column('country', ForeignColumn('country', 'iso_code'))),
        Field('type', Column('type', ForeignColumn('label_type', 'name'))),
        MultiField('ipi', ForeignColumn('label_ipi', 'ipi')),
        MultiField('alias', ForeignColumn('label_alias', 'name', ForeignColumn('label_name', 'name'))),
    ]),
    Entity('work', [
        Field('id', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('work_name', 'name'))),
        Field('type', Column('type', ForeignColumn('work_type', 'name'))),
        MultiField('iswc', ForeignColumn('iswc', 'iswc')),
        MultiField('alias', ForeignColumn('work_alias', 'name', ForeignColumn('work_name', 'name'))),
    ]),
    Entity('release_group', [
        Field('id', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('release_name', 'name'))),
        Field('type', Column('type', ForeignColumn('release_group_primary_type', 'name'))),
        MultiField('type',
            ForeignColumn('release_group_secondary_type_join', 'secondary_type',
                ForeignColumn('release_group_secondary_type', 'name'))),
        Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
    ]),
    Entity('release', [
        Field('id', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('barcode', Column('barcode')),
        Field('name', Column('name', ForeignColumn('release_name', 'name'))),
        Field('status', Column('status', ForeignColumn('release_status', 'name'))),
        Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
        MultiField('catno', ForeignColumn('release_label', 'catno')),
        MultiField('label', ForeignColumn('release_label', 'label', ForeignColumn('label', 'name'))),
    ]),
    Entity('recording', [
        Field('id', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('release_name', 'name'))),
        Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
    ]),
])


SQL_SELECT_TPL = "SELECT\n%(columns)s\nFROM\n%(joins)s\nORDER BY %(sort_column)s"


def generate_iter_query(columns, joins, ids=()):
    id_column = columns[0]
    tpl = ["SELECT", "%(columns)s", "FROM", "%(joins)s"]
    if ids:
        tpl.append("WHERE %(id_column)s IN (%(ids)s)")
    tpl.append("ORDER BY %(id_column)s")
    sql_columns = ',\n'.join('  ' + i for i in columns)
    sql_joins = '\n'.join('  ' + i for i in joins)
    sql = "\n".join(tpl) % dict(columns=sql_columns, joins=sql_joins,
                                id_column=id_column, ids=placeholders(ids))
    return sql


def iter_main(db, name, ids=()):
    entity = schema[name]
    joins = [name]
    tables = set([name])
    columns = ['%s.id' % (name,)]
    names = []
    for field in entity.iter_single_fields():
        table = name
        column = field.column
        while column.foreign is not None:
            foreign_table = table + '__' + column.name + '__' + column.foreign.table
            if foreign_table not in tables:
                joins.append('JOIN %(parent)s AS %(label)s ON %(label)s.%(child)s = %(child)s.id' % dict(
                    parent=column.foreign.table, child=table, label=foreign_table))
                tables.add(foreign_table)
            table = foreign_table
            column = column.foreign
        columns.append('%s.%s' % (table, column.name))
        names.append(field.name)

    query = generate_iter_query(columns, joins, ids)

    cursor = db.cursor()
    cursor.execute(query, ids)

    for row in cursor:
        id = row[0]
        fields = [E.field(name, name='kind')]
        for name, value in zip(names, row[1:]):
            if isinstance(value, str):
                value = value.decode('utf8')
            fields.append(E.field(value, name=name))
        yield id, fields


def iter_sub(db, name, subtable, ids=()):
    entity = schema[name]
    joins = []
    tables = set()
    columns = []
    names = []
    for field in entity.iter_multi_fields():
        if field.column.table != subtable:
            continue
        last_column = column = field.column
        table = column.table
        while True:
            if last_column is column:
                if table not in tables:
                    joins.append(table)
                    tables.add(table)
                    columns.append('%s.%s' % (table, name))
            else:
                foreign_table = table + '__' + last_column.name + '__' + column.table
                if foreign_table not in tables:
                    joins.append('JOIN %(parent)s AS %(label)s ON %(label)s.id = %(child)s.%(child_column)s' % dict(
                        parent=column.table, child=table, child_column=column.name, label=foreign_table))
                    tables.add(foreign_table)
                table = foreign_table
            if column.foreign is None:
                break
            last_column = column
            column = column.foreign
        columns.append('%s.%s' % (table, column.name))
        names.append(field.name)

    query = generate_iter_query(columns, joins, ids)

    cursor = db.cursor()
    cursor.execute(query, ids)

    fields = []
    last_id = None
    for row in cursor:
        id = row[0]
        if last_id != id:
            if values:
                yield last_id, fields
            last_id = id
            fields = []
        for name, value in zip(fields, row[1:]):
            if isinstance(value, str):
                value = value.decode('utf8')
            fields.append(E.field(value, name=name))
    if values:
        yield last_id, fields


def placeholders(ids):
    return ", ".join(["%s" for i in ids])


def iter_list(db, query, field, ids=()):
    cursor = db.cursor()
    cursor.execute(query, ids)
    last_id = None
    values = None
    for id, value in cursor:
        if last_id != id:
            if values:
                yield {'_id': last_id, field: values}
            last_id = id
            values = []
        values.append(value)
    if values:
        yield {'_id': last_id, field: values}


def iter_ipi(db, entity, ids=()):
    query = """
        SELECT %(entity)s, ipi
        FROM %(entity)s_ipi
    """ % dict(entity=entity)
    if ids:
        ids = tuple(set(ids))
        query += " WHERE %s IN (%s)" % (entity, placeholders(ids))
    query += " ORDER BY %s" % (entity,)
    return iter_list(db, query, 'ipi', ids)


def iter_iswc(db, ids=()):
    query = "SELECT work, iswc FROM iswc"
    if ids:
        ids = tuple(set(ids))
        query += " WHERE work IN (%s)" % (placeholders(ids),)
    query += " ORDER BY work"
    return iter_list(db, query, 'iswc', ids)


def iter_aliases(db, entity, ids=()):
    query = """
        SELECT a.%(entity)s, an.name
        FROM %(entity)s_alias a
        JOIN %(entity)s_name an ON a.name=an.id
    """ % dict(entity=entity)
    if ids:
        ids = tuple(set(ids))
        query += " WHERE a.%s IN (%s)" % (entity, placeholders(ids))
    query += " ORDER BY a.%s" % (entity,)
    cursor = db.cursor()
    cursor.execute(query, ids)
    last_id = None
    aliases = None
    for id, alias in cursor:
        if last_id != id:
            if aliases:
                yield {'_id': last_id, 'aliases': aliases}
            last_id = id
            aliases = []
        aliases.append(alias)
    if aliases:
        yield {'_id': last_id, 'aliases': aliases}


def iter_artists(db, ids=()):
    query = """
        SELECT
            a.id AS _id,
            a.gid AS id,
            a.comment AS disambiguation,
            an.name AS name,
            asn.name AS sortname,
            at.name AS type,
            c.name AS country,
            c.iso_code AS country_code,
            g.name AS gender
        FROM artist a
        JOIN artist_name an ON a.name=an.id
        JOIN artist_name asn ON a.sort_name=asn.id
        LEFT JOIN artist_type at ON a.type=at.id
        LEFT JOIN country c ON a.country=c.id
        LEFT JOIN gender g ON a.gender=g.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE a.id IN (%s)" % placeholders(ids)
    query += " ORDER BY a.id"
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, ids)
    return cursor


def grab_next(iter):
    try:
        return iter.next()
    except StopIteration:
        return None


def merge(main, *extra):
    current = map(grab_next, extra)
    for row in main:
        row = dict(row)
        for i, val in enumerate(current):
            if val is not None:
                if val['_id'] == row['_id']:
                    row.update(val)
                    current[i] = grab_next(extra[i])
        yield row


def add_country_fields(fields, name, code):
    fields.append(E.field(name.decode('utf8'), name='country'))
    fields.append(E.field(code, name='country'))
    if code == 'GB':
        fields.append(E.field('UK', name='country'))


def add_list_fields(fields, values, name):
    for value in values:
        fields.append(E.field(value.decode('utf8'), name=name))


def fetch_artists(db, ids=()):
    iter = merge(iter_artists(db, ids), iter_aliases(db, 'artist', ids), iter_ipi(db, 'artist', ids))
    for row in iter:
        fields = [
            E.field('artist', name='kind'),
            E.field(row['id'], name='id'),
            E.field(row['name'].decode('utf8'), name='name'),
            E.field(row['sortname'].decode('utf8'), name='sortname'),
        ]
        if row['disambiguation']:
            fields.append(E.field(row['disambiguation'].decode('utf8'), name='disambiguation'))
        if row['type']:
            fields.append(E.field(row['type'], name='type'))
        if row['gender']:
            fields.append(E.field(row['gender'], name='gender'))
        if row['country']:
            add_country_fields(fields, row['country'], row['country_code'])
        if 'aliases' in row and row['aliases']:
            add_list_fields(fields, row['aliases'], 'alias')
        if 'ipi' in row and row['ipi']:
            add_list_fields(fields, row['ipi'], 'ipi')
        yield E.doc(*fields)


def iter_labels(db, ids=()):
    query = """
        SELECT
            l.id AS _id,
            l.gid AS id,
            l.comment AS disambiguation,
            ln.name AS name,
            lsn.name AS sortname,
            lt.name AS type,
            c.name AS country,
            c.iso_code AS country_code,
            l.label_code AS code
        FROM label l
        JOIN label_name ln ON l.name=ln.id
        JOIN label_name lsn ON l.sort_name=lsn.id
        LEFT JOIN label_type lt ON l.type=lt.id
        LEFT JOIN country c ON l.country=c.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE l.id IN (%s)" % placeholders(ids)
    query += " ORDER BY l.id"
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, ids)
    return cursor


def fetch_labels(db, ids=()):
    iter = merge(iter_labels(db, ids), iter_aliases(db, 'label', ids), iter_ipi(db, 'label', ids))
    for row in iter:
        fields = [
            E.field('label', name='kind'),
            E.field(row['id'], name='id'),
            E.field(row['name'].decode('utf8'), name='name'),
            E.field(row['sortname'].decode('utf8'), name='sortname'),
        ]
        if row['disambiguation']:
            fields.append(E.field(row['disambiguation'].decode('utf8'), name='disambiguation'))
        if row['type']:
            fields.append(E.field(row['type'], name='type'))
        if row['country']:
            add_country_fields(fields, row['country'], row['country_code'])
        if row['code']:
            fields.append(E.field('LC-%04d' % row['code'], name='code'))
            fields.append(E.field('LC%04d' % row['code'], name='code'))
        if 'aliases' in row and row['aliases']:
            add_list_fields(fields, row['aliases'], 'alias')
        if 'ipi' in row and row['ipi']:
            add_list_fields(fields, row['ipi'], 'ipi')
        yield E.doc(*fields)


def fetch_release_groups(db, ids=()):
    query = """
        SELECT
            rg.gid,
            rgn.name,
            rgt.name,
            an.name
        FROM release_group rg
        JOIN release_name rgn ON rg.name = rgn.id
        JOIN artist_credit ac ON rg.artist_credit = ac.id
        JOIN artist_name an ON ac.name = an.id
        LEFT JOIN release_group_type rgt ON rg.type = rgt.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE rg.id IN (%s)" % placeholders(ids)
    query += " ORDER BY rg.id"
    cursor = db.cursor()
    cursor.execute(query, ids)
    for gid, name, type, artist in cursor:
        fields = [
            E.field('releasegroup', name='kind'),
            E.field(gid, name='id'),
            E.field(name.decode('utf8'), name='name'),
            E.field(artist.decode('utf8'), name='artist'),
        ]
        if type:
            fields.append(E.field(type, name='type'))
        yield E.doc(*fields)


def fetch_recordings(db, ids=()):
    query = """
        SELECT
            r.gid,
            rn.name,
            an.name
        FROM recording r
        JOIN track_name rn ON r.name = rn.id
        JOIN artist_credit ac ON r.artist_credit = ac.id
        JOIN artist_name an ON ac.name = an.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE r.id IN (%s)" % placeholders(ids)
    query += " ORDER BY r.id"
    cursor = db.cursor()
    cursor.execute(query, ids)
    for gid, name, artist in cursor:
        fields = [
            E.field('recording', name='kind'),
            E.field(gid, name='id'),
            E.field(name.decode('utf8'), name='name'),
            E.field(artist.decode('utf8'), name='artist'),
        ]
        yield E.doc(*fields)


def iter_release_labels(db, ids=()):
    query = """
        SELECT rl.release, rl.catalog_number, ln.name
        FROM release_label rl
        LEFT JOIN label l ON rl.label = l.id
        LEFT JOIN label_name ln ON l.name = ln.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE rl.release IN (%s)" % (placeholders(ids),)
    query += " ORDER BY rl.release"
    cursor = db.cursor()
    cursor.execute(query, ids)
    last_id = None
    catnos = set()
    labels = set()
    for id, catno, label in cursor:
        if last_id != id:
            if catnos or labels:
                yield {'_id': last_id, 'catnos': catnos, 'labels': labels}
            last_id = id
            catnos = set()
            labels = set()
        if catno:
            catnos.add(catno)
        if label:
            labels.add(label)
    if catnos or labels:
        yield {'_id': last_id, 'catnos': catnos, 'labels': labels}


def iter_releases(db, ids=()):
    query = """
        SELECT
            r.id AS _id,
            r.gid AS id,
            rn.name AS name,
            rgt.name AS type,
            rs.name AS status,
            an.name AS artist,
            r.barcode AS barcode
        FROM release r
        JOIN release_name rn ON r.name = rn.id
        JOIN artist_credit ac ON r.artist_credit = ac.id
        JOIN artist_name an ON ac.name = an.id
        JOIN release_group rg ON r.release_group = rg.id
        LEFT JOIN release_group_type rgt ON rg.type = rgt.id
        LEFT JOIN release_status rs ON r.status = rs.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE r.id IN (%s)" % placeholders(ids)
    query += " ORDER BY r.id"
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, ids)
    return cursor


def fetch_releases(db, ids=()):
    iter = merge(iter_releases(db, ids), iter_release_labels(db, ids))
    for row in iter:
        fields = [
            E.field('release', name='kind'),
            E.field(row['id'], name='id'),
            E.field(row['name'].decode('utf8'), name='name'),
            E.field(row['artist'].decode('utf8'), name='artist'),
        ]
        if row['type']:
            fields.append(E.field(row['type'], name='type'))
        if row['status']:
            fields.append(E.field(row['status'], name='status'))
        if row['barcode']:
            fields.append(E.field(row['barcode'], name='barcode'))
        if 'catnos' in row and row['catnos']:
            for catno in row['catnos']:
                fields.append(E.field(catno.decode('utf8'), name='catno'))
        if 'labels' in row and row['labels']:
            for label in row['labels']:
                fields.append(E.field(label.decode('utf8'), name='label'))
        yield E.doc(*fields)


def iter_works(db, ids=()):
    query = """
        SELECT
            w.id AS _id,
            w.gid AS id,
            wn.name AS name,
            wt.name AS type
        FROM work w
        JOIN work_name wn ON w.name = wn.id
        LEFT JOIN work_type wt ON w.type = wt.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE w.id IN (%s)" % placeholders(ids)
    query += " ORDER BY w.id"
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, ids)
    return cursor


def fetch_works(db, ids=()):
    iter = merge(iter_works(db, ids), iter_aliases(db, 'work', ids), iter_iswc(db, ids))
    for row in iter:
        fields = [
            E.field('work', name='kind'),
            E.field(row['id'], name='id'),
            E.field(row['name'].decode('utf8'), name='name'),
        ]
        if row['type']:
            fields.append(E.field(row['type'].decode('utf8'), name='type'))
        if 'aliases' in row and row['aliases']:
            add_list_fields(fields, row['aliases'], 'alias')
        if 'iswc' in row and row['iswc']:
            add_list_fields(fields, row['iswc'], 'iswc')
        yield E.doc(*fields)


def fetch_all(cfg, db):
    return itertools.chain(
        fetch_works(db) if cfg.solr.index_works else [],
        fetch_recordings(db) if cfg.solr.index_recordings else [],
        fetch_releases(db) if cfg.solr.index_releases else [],
        fetch_release_groups(db) if cfg.solr.index_release_groups else [],
        fetch_artists(db) if cfg.solr.index_artists else [],
        fetch_labels(db) if cfg.solr.index_labels else [])


class SolrReplicationHook(ReplicationHook):

    def __init__(self, cfg, db, schema):
        super(SolrReplicationHook, self).__init__(cfg, db, schema)

    def begin(self, seq):
        self.deleted = {}
        self.added = set()
        self.seq = seq

    def add_update(self, table, id):
        key = table, id
        if key in self.deleted:
            del self.deleted[key]
        self.added.add(key)

    def after_insert(self, table, values):
        if table in ('artist', 'label', 'release', 'release_group', 'recording', 'work'):
            self.add_update(table, values['id'])
        elif table == 'artist_alias':
            self.add_update('artist', values['artist'])
        elif table == 'label_alias':
            self.add_update('label', values['label'])
        elif table == 'work_alias':
            self.add_update('work', values['work'])
        elif table == 'release_label':
            self.add_update('release', values['release'])

    def after_update(self, table, keys, values):
        if table in ('artist', 'label', 'release', 'release_group', 'recording', 'work'):
            id = keys['id']
            self.add_update(table, id)
            if table == 'release_group':
                cursor = self.db.cursor()
                cursor.execute("SELECT id FROM %s.release WHERE release_group = %%s" % (self.schema,), (id,))
                for release_id, in cursor:
                    self.add_update('release', release_id)
            elif table == 'label':
                cursor = self.db.cursor()
                cursor.execute("SELECT release FROM %s.release_label WHERE label = %%s" % (self.schema,), (id,))
                for release_id, in cursor:
                    self.add_update('release', release_id)
        elif table == 'artist_alias':
            self.add_update('artist', values['artist'])
        elif table == 'label_alias':
            self.add_update('label', values['label'])
        elif table == 'work_alias':
            self.add_update('work', values['work'])
        elif table == 'release_label':
            self.add_update('release', values['release'])

    def before_delete(self, table, keys):
        if table in ('artist', 'label', 'release', 'release_group', 'recording', 'work'):
            key = table, keys['id']
            if key in self.added:
                self.added.remove(key)
            cursor = self.db.cursor()
            cursor.execute("SELECT gid FROM %s.%s WHERE id = %%s" % (self.schema, table), (key[1],))
            row = cursor.fetchone()
            if row is not None:
                self.deleted[key] = row[0]
        elif table == 'artist_alias':
            cursor = self.db.cursor()
            cursor.execute("SELECT artist FROM %s.artist_alias WHERE id = %%s" % (self.schema,), (keys['id'],))
            for artist_id, in cursor:
                self.add_update('artist', artist_id)
        elif table == 'label_alias':
            cursor = self.db.cursor()
            cursor.execute("SELECT label FROM %s.label_alias WHERE id = %%s" % (self.schema,), (keys['id'],))
            for label_id, in cursor:
                self.add_update('label', label_id)
        elif table == 'work_alias':
            cursor = self.db.cursor()
            cursor.execute("SELECT work FROM %s.work_alias WHERE id = %%s" % (self.schema,), (keys['id'],))
            for work_id, in cursor:
                self.add_update('work', work_id)
        elif table == 'release_label':
            cursor = self.db.cursor()
            cursor.execute("SELECT release FROM %s.release_label WHERE id = %%s" % (self.schema,), (keys['id'],))
            for release_id, in cursor:
                self.add_update('release', release_id)

    def after_commit(self):
        xml = []
        xml.append('<update>')
        xml.append(ET.tostring(E.deleted(*map(E.id, set(self.deleted.values())))))
        update = {}
        for table, id in self.added:
            update.setdefault(table, set()).add(id)
        xml.append('<add>')
        for table, ids in update.iteritems():
            fetch_func = globals()['fetch_%ss' % table]
            for doc in fetch_func(self.db, ids):
                xml.append(ET.tostring(doc))
        xml.append('</add>')
        xml.append('</update>')
        filename = '/tmp/mb_solr_data_%d.xml' % self.seq
        print ' - Saved Solr update packet to', filename
        f = open(filename, 'w')
        f.writelines(xml)
        f.close()
        req = urllib2.Request(self.cfg.solr.url + '/update', ''.join(xml),
            {'Content-Type': 'application/xml; encoding=UTF-8'})
        print ' - Updated Solr index at', self.cfg.solr.url
        resp = urllib2.urlopen(req)
        the_page = resp.read()

