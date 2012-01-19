import psycopg2
from mbslave.config import Config
from mbslave.replication import ReplicationHook

def connect_db(cfg):
    db = psycopg2.connect(**cfg.make_psql_args())
    db.cursor().execute("SET search_path TO %s", (cfg.database.schema,))
    return db

