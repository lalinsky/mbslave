#!/usr/bin/env python2

import re
import os
import sys
from mbslave import Config

config = Config(os.path.dirname(__file__) + '/mbslave.conf')

def update_search_path(m):
    schemas = m.group(2).replace("'", '').split(',')
    schemas = [config.schema.name(s.strip()) for s in schemas]
    return m.group(1) + ', '.join(schemas) + ';'

def update_schema(m):
    return m.group(1) + config.schema.name(m.group(2)) + m.group(3)

for line in sys.stdin:
    line = re.sub(r'(SET search_path = )(.+?);', update_search_path, line)
    line = re.sub(r'(\b)(\w+)(\.)', update_schema, line)
    line = re.sub(r'( SCHEMA )(\w+)(;)', update_schema, line)
    sys.stdout.write(line)

