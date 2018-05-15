#!/usr/bin/env python2

import os
from optparse import OptionParser
from mbslave import Config

parser = OptionParser()
parser.add_option("-S", "--no-schema", action="store_true", dest="public", default=False, help="don't configure the default schema")
parser.add_option("-s", "--schema", dest="schema", default='musicbrainz', help="default schema")
options, args = parser.parse_args()

config = Config(os.path.dirname(__file__) + '/mbslave.conf')

args = ['psql']
args.append('-U')
args.append(config.get('DATABASE', 'user'))
if config.has_option('DATABASE', 'host'):
    args.append('-h')
    args.append(config.get('DATABASE', 'host'))
if config.has_option('DATABASE', 'port'):
    args.append('-p')
    args.append(config.get('DATABASE', 'port'))
args.append(config.get('DATABASE', 'name'))

if not options.public:
    schema = config.schema.name(options.schema)
    os.environ['PGOPTIONS'] = '-c search_path=%s,public' % schema
if config.has_option('DATABASE', 'password'):
    os.environ['PGPASSWORD'] = config.get('DATABASE', 'password')
os.execvp("psql", args)
