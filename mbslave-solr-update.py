#!/usr/bin/env python2

import os
import urllib2
from cStringIO import StringIO
from lxml import etree as ET
from mbslave import Config, connect_db
from mbslave.search import fetch_all_updated

cfg = Config(os.path.join(os.path.dirname(__file__), 'mbslave.conf'))
db = connect_db(cfg, True)

xml = StringIO()
xml.write('<update>\n')
for doc in fetch_all_updated(cfg, db):
    xml.write(ET.tostring(doc))
    xml.write('\n')
xml.write('</update>\n')

req = urllib2.Request(cfg.solr.url + '/update?commit=true', xml.getvalue(),
    {'Content-Type': 'application/xml; encoding=UTF-8'})
resp = urllib2.urlopen(req)
the_page = resp.read()

doc = ET.fromstring(the_page)
status = doc.find("lst[@name='responseHeader']/int[@name='status']")
if status.text != '0':
    print the_page
    raise SystemExit(1)

db.commit()

