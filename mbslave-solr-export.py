#!/usr/bin/env python2

import os
from lxml import etree as ET
from mbslave import Config, connect_db
from mbslave.search import fetch_all

cfg = Config(os.path.join(os.path.dirname(__file__), 'mbslave.conf'))
db = connect_db(cfg, True)

print '<add>'
for id, doc in fetch_all(cfg, db):
    print ET.tostring(doc)
print '</add>'

