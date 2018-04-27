#!/usr/bin/env python2

import os
from mbslave import Config, connect_db
from mbslave.search import generate_triggers

cfg = Config(os.path.join(os.path.dirname(__file__), 'mbslave.conf'))
db = connect_db(cfg)

print '\set ON_ERROR_STOP 1'
print 'BEGIN;'

for code in generate_triggers():
    print code

print 'COMMIT;'

