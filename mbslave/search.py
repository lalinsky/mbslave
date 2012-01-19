import urllib2
from lxml import etree as ET
from lxml.builder import E
from mbslave.replication import ReplicationHook


def placeholders(ids):
    return ", ".join(["%s" for i in ids])


def fetch_artists(db, ids=()):
    query = """
        SELECT
            a.gid,
            an.name,
            at.name,
            c.name,
            c.iso_code,
            a.ipi_code,
            g.name
        FROM artist a
        JOIN artist_name an ON a.name=an.id
        LEFT JOIN artist_type at ON a.type=at.id
        LEFT JOIN country c ON a.country=c.id
        LEFT JOIN gender g ON a.gender=g.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE a.id IN (%s)" % placeholders(ids)
    query += " ORDER BY a.id"
    cursor = db.cursor()
    cursor.execute(query, ids)
    for gid, name, type, country_name, country_iso_code, ipi_code, gender in cursor:
        fields = [
            E.field('artist', name='kind'),
            E.field(gid, name='id'),
            E.field(name.decode('utf8'), name='name'),
        ]
        if type:
            fields.append(E.field(type, name='type'))
        if gender:
            fields.append(E.field(gender, name='gender'))
        if ipi_code:
            fields.append(E.field(ipi_code, name='ipi'))
        if country_name:
            fields.append(E.field(country_name.decode('utf8'), name='country'))
            fields.append(E.field(country_iso_code, name='country'))
        yield E.doc(*fields)


def fetch_labels(db, ids=()):
    query = """
        SELECT
            l.gid,
            ln.name,
            lt.name,
            c.name,
            c.iso_code,
            l.ipi_code,
            l.label_code
        FROM label l
        JOIN label_name ln ON l.name=ln.id
        LEFT JOIN label_type lt ON l.type=lt.id
        LEFT JOIN country c ON l.country=c.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE l.id IN (%s)" % placeholders(ids)
    query += " ORDER BY l.id"
    cursor = db.cursor()
    cursor.execute(query, ids)
    for gid, name, type, country_name, country_iso_code, ipi_code, label_code in cursor:
        fields = [
            E.field('label', name='kind'),
            E.field(gid, name='id'),
            E.field(name.decode('utf8'), name='name'),
        ]
        if type:
            fields.append(E.field(type, name='type'))
        if ipi_code:
            fields.append(E.field(ipi_code, name='ipi'))
        if label_code:
            fields.append(E.field('LC-%04d' % label_code, name='code'))
        if country_name:
            fields.append(E.field(country_name.decode('utf8'), name='country'))
            fields.append(E.field(country_iso_code, name='country'))
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


def fetch_releases(db, ids=()):
    query = """
        SELECT
            r.gid,
            rn.name,
            rgt.name,
            rs.name,
            an.name,
            r.barcode
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
    cursor = db.cursor()
    cursor.execute(query, ids)
    for gid, name, type, status, artist, barcode in cursor:
        fields = [
            E.field('release', name='kind'),
            E.field(gid, name='id'),
            E.field(name.decode('utf8'), name='name'),
            E.field(artist.decode('utf8'), name='artist'),
        ]
        if type:
            fields.append(E.field(type, name='type'))
        if status:
            fields.append(E.field(status, name='status'))
        if barcode:
            fields.append(E.field(barcode, name='barcode'))
        yield E.doc(*fields)


def fetch_works(db, ids=()):
    query = """
        SELECT
            w.gid,
            wn.name,
            wt.name,
            w.iswc
        FROM work w
        JOIN work_name wn ON w.name = wn.id
        LEFT JOIN work_type wt ON w.type = wt.id
    """
    if ids:
        ids = tuple(set(ids))
        query += " WHERE w.id IN (%s)" % placeholders(ids)
    query += " ORDER BY w.id"
    cursor = db.cursor()
    cursor.execute(query, ids)
    for gid, name, type, iswc in cursor:
        fields = [
            E.field('work', name='kind'),
            E.field(gid, name='id'),
            E.field(name.decode('utf8'), name='name'),
        ]
        if type:
            fields.append(E.field(type.decode('utf8'), name='type'))
        if iswc:
            fields.append(E.field(iswc, name='iswc'))
        yield E.doc(*fields)


def fetch_all(cfg, db):
    return itertools.chain(
        fetch_works(db) if cfg.solr.index_works else [],
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

    def after_insert(self, table, values):
        if table in ('artist', 'label', 'release', 'release_group', 'work'):
            key = table, values['id']
            if key in self.deleted:
                del self.deleted[key]
            self.added.add(key)

    def after_update(self, table, keys, values):
        if table in ('artist', 'label', 'release', 'release_group', 'work'):
            key = table, keys['id']
            if key in self.deleted:
                del self.deleted[key]
            self.added.add(key)

    def before_delete(self, table, keys):
        if table in ('artist', 'label', 'release', 'release_group', 'work'):
            key = table, keys['id']
            if key in self.added:
                self.added.remove(key)
            cursor = self.db.cursor()
            cursor.execute("SELECT gid FROM %s.%s WHERE id = %%s" % (self.schema, table), (key[1],))
            row = cursor.fetchone()
            if row is not None:
                self.deleted[key] = row[0]

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

