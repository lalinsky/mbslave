import ConfigParser


class ConfigSection(object):
    pass


class SolrConfig(object):

    def __init__(self):
        self.enabled = False
        self.url = 'http://localhost:8983/solr/musicbrainz'
        self.index_artists = True
        self.index_labels = True
        self.index_releases = True
        self.index_release_groups = True
        self.index_recordings = True
        self.index_works = True

    def parse(self, parser, section):
        if parser.has_option(section, 'enabled'):
            self.enabled = parser.getboolean(section, 'enabled')
        if parser.has_option(section, 'url'):
            self.url = parser.get(section, 'url').rstrip('/')
        for name in ('artists', 'labels', 'releases', 'release_groups', 'recordings', 'works'):
            key = 'index_%s' % name
            if parser.has_option(section, key):
                setattr(self, key, parser.getboolean(section, key))


class MonitoringConfig(object):

    def __init__(self):
        self.enabled = False
        self.status_file = '/tmp/mbslave-status.xml'

    def parse(self, parser, section):
        if parser.has_option(section, 'enabled'):
            self.enabled = parser.getboolean(section, 'enabled')
        if parser.has_option(section, 'status_file'):
            self.status_file = parser.get(section, 'status_file')


class SchemasConfig(dict):

    def name(self, name):
        return self.get(name, name)

    def parse(self, parser, section):
        for name, value in parser.items(section):
            self[name] = value


class Config(object):

    def __init__(self, path):
        self.path = path
        self.cfg = ConfigParser.RawConfigParser()
        self.cfg.read(self.path)
        self.get = self.cfg.get
        self.has_option = self.cfg.has_option
        self.database = ConfigSection()
        self.solr = SolrConfig()
        if self.cfg.has_section('solr'):
            self.solr.parse(self.cfg, 'solr')
        self.monitoring = MonitoringConfig()
        if self.cfg.has_section('monitoring'):
            self.monitoring.parse(self.cfg, 'monitoring')
        self.schema = SchemasConfig()
        if self.cfg.has_section('schemas'):
            self.schema.parse(self.cfg, 'schemas')

    def make_psql_args(self):
        opts = {}
        opts['database'] = self.cfg.get('DATABASE', 'name')
        opts['user'] = self.cfg.get('DATABASE', 'user')
        if self.cfg.has_option('DATABASE', 'password'):
	        opts['password'] = self.cfg.get('DATABASE', 'password')
        if self.cfg.has_option('DATABASE', 'host'):
	        opts['host'] = self.cfg.get('DATABASE', 'host')
        if self.cfg.has_option('DATABASE', 'port'):
	        opts['port'] = self.cfg.get('DATABASE', 'port')
        return opts

