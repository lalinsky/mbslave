
class ReplicationHook(object):

    def __init__(self, cfg, db, schema):
        self.cfg = cfg
        self.db = db
        self.schema = schema

    def begin(self, seq):
        pass

    def before_commit(self):
        pass

    def after_commit(self):
        pass

    def before_delete(self, table, keys):
        pass

    def before_update(self, table, keys, values):
        pass

    def before_insert(self, table, values):
        pass

    def after_delete(self, table, keys):
        pass

    def after_update(self, table, keys, values):
        pass

    def after_insert(self, table, values):
        pass

