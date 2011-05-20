#!/usr/bin/env python

import ConfigParser
import os

config = ConfigParser.RawConfigParser()
config.read(os.path.dirname(__file__) + '/mbslave.conf')

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

os.environ['PGOPTIONS'] = '-c search_path=%s' % config.get('DATABASE', 'schema')
os.execvp("psql", args)
