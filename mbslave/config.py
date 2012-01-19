import ConfigParser


class ConfigSection(object):
    pass


class Config(object):

    def __init__(self, path):
        self.path = path
        self.cfg = ConfigParser.RawConfigParser()
        self.cfg.read(self.path)
        self.get = self.cfg.get
        self.has_option = self.cfg.has_option
        self.database = ConfigSection()
        self.database.schema = self.cfg.get('DATABASE', 'schema')

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

