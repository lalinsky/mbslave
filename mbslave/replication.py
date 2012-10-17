import datetime
import shutil

class ReplicationHook(object):

    def __init__(self, cfg, db, schema):
        self.cfg = cfg
        self.db = db
        self.schema = schema
        self.logging = cfg.has_option('LOGGING', 'directory')

        if self.logging:
            time = datetime.datetime.today().strftime('%Y-%m-%d-%H%M%S')
            directory = cfg.get('LOGGING', 'directory')
            self.logfile = '%s/mbslave-%s.log' % (directory,time) 
            self.logtmp = self.logfile + '.tmp'

    def begin(self, seq):
        self.log = open(self.logtmp, 'w')
        print 'Logging to %s' % self.logfile

    def before_delete(self, table, keys):
        if self.logging:
            self.log.write('delete;%s;%s\n' % (table,repr(keys)))
        
    def before_update(self, table, keys, values):
        if self.logging:
            self.log.write('update;%s;%s\n' % (table,repr(values)))
        
    def before_insert(self, table, values):
        if self.logging:
            self.log.write('insert;%s;%s\n' % (table,repr(values)))

    def after_delete(self, table, keys):
        pass

    def after_update(self, table, keys, values):
        pass

    def after_insert(self, table, values):
        pass

    def before_commit(self):
        pass

    def after_commit(self):
        if self.logging:
            self.log.close()
            shutil.move(self.logtmp, self.logfile)

