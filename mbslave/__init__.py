import psycopg2
from mbslave.config import Config
from mbslave.replication import ReplicationHook


def connect_db(cfg):
    return psycopg2.connect(**cfg.make_psql_args())


def parse_name(config, table):
    if '.' in table:
        schema, table = table.split('.', 1)
    else:
        schema = 'musicbrainz'
    schema = config.schema.name(schema.strip('"'))
    table = table.strip('"')
    return schema, table


def fqn(schema, table):
    return '%s.%s' % (schema, table)


def check_table_exists(db, schema, table):
    cursor = db.cursor()
    cursor.execute("SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s", (schema, table))
    if not cursor.fetchone():
        return False
    return True

