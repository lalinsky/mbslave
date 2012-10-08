import itertools
import urllib2
import psycopg2.extras
from lxml import etree as ET
from lxml.builder import E
from mbslave.replication import ReplicationHook


def placeholders(ids):
    return ", ".join(["%s" for i in ids])


def iter_ipi(db, entity, ids=()):
    query = """
        SELECT %(entity)s, ipi
        FROM %(entity)s_ipi
    """ % dict(entity=entity)
    if ids:
        ids = tuple(set(ids))
        query += " WHERE %s IN (%s)" % (entity, placeholders(ids))
    query += " ORDER BY %s" % (entity,)
    cursor = db.cursor()
    cursor.execute(query, ids)
    last_id = None
    aliases = None
    for id, alias in cursor:
        if last_id != id:
            if aliases:
                yield {'_id': last_id, 'ipi': aliases}
            last_id = id
            aliases = []
        aliases.append(alias)
    if aliases:
        yield {'_id': last_id, 'ipi': aliases}


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


def add_alias_fields(fields, aliases):
    for alias in aliases:
        fields.append(E.field(alias.decode('utf8'), name='alias'))


def add_ipi_fields(fields, codes):
    for code in codes:
        fields.append(E.field(code.decode('utf8'), name='ipi'))


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
            add_alias_fields(fields, row['aliases'])
        if 'ipi' in row and row['ipi']:
            add_ipi_fields(fields, row['ipi'])
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
            add_alias_fields(fields, row['aliases'])
        if 'ipi' in row and row['ipi']:
            add_ipi_fields(fields, row['ipi'])
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
            wt.name AS type,
            w.iswc
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
    iter = merge(iter_works(db, ids), iter_aliases(db, 'work', ids))
    for row in iter:
        fields = [
            E.field('work', name='kind'),
            E.field(row['id'], name='id'),
            E.field(row['name'].decode('utf8'), name='name'),
        ]
        if row['type']:
            fields.append(E.field(row['type'].decode('utf8'), name='type'))
        if row['iswc']:
            fields.append(E.field(row['iswc'].decode('utf8'), name='iswc'))
        if 'aliases' in row and row['aliases']:
            add_alias_fields(fields, row['aliases'])
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

