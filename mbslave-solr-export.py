#!/usr/bin/env python

import os
import itertools
from lxml import etree as ET
from lxml.builder import E
from mbslave import Config, connect_db

cfg = Config(os.path.join(os.path.dirname(__file__), 'mbslave.conf'))
db = connect_db(cfg)


def fetch_artists():
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
        ORDER BY a.id
    """
    cursor = db.cursor()
    cursor.execute(query)
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


def fetch_labels():
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
        ORDER BY l.id
    """
    cursor = db.cursor()
    cursor.execute(query)
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


def fetch_release_groups():
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
        ORDER BY rg.id
    """
    cursor = db.cursor()
    cursor.execute(query)
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


def fetch_releases():
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
        ORDER BY r.id
    """
    cursor = db.cursor()
    cursor.execute(query)
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


def fetch_all():
    return itertools.chain(
        fetch_releases(),
        fetch_release_groups(),
        fetch_artists(),
        fetch_labels())


print '<add>'
for doc in fetch_all():
    print ET.tostring(doc)
print '</add>'

