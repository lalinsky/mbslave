Installation
============

1. Setup a database and create mbslave.conf by copying and editing
   mbslave.conf.default. If you are starting completely from scratch,
   you can use the following commands to setup a clean database:

   $ sudo su - postgres
   $ createuser musicbrainz
   $ createdb -O musicbrainz musicbrainz
   $ createlang plpgsql musicbrainz

2. Prepare empty schema for the MusicBrainz database (skip this if you
   want to use the default 'public' schema), install the cube extension into
   this new schema and create the table structure:

   $ echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql.py
   $ sed 's/public/musicbrainz/' `pg_config --sharedir`/contrib/cube.sql | psql -U postgres musicbrainz
   $ ./mbslave-psql.py <sql/CreateTables.sql

3. Download the MusicBrainz database dump files from
   http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/

4. Import the data dumps, for example:

   $ ./mbslave-import.py mbdump.tar.bz2 mbdump-derived.tar.bz2

5. Setup primary keys, indexes, views and functions:

   $ ./mbslave-psql.py <sql/CreatePrimaryKeys.sql
   $ ./mbslave-psql.py <sql/CreateFunctions.sql
   $ grep -vE '(collate|page_index)' sql/CreateIndexes.sql | ./mbslave-psql.py
   $ ./mbslave-psql.py <sql/CreateViews.sql
   $ ./mbslave-psql.py <sql/CreateSimpleViews.sql

6. Vacuum the newly created database (optional)

   $ echo 'VACUUM ANALYZE;' | ./mbslave-psql.py

7. Run the initial replication:

   $ ./mbslave-sync.py

Replication
===========

In order to regularly update your database with the latest changes, setup
a cron job like this that runs every hour:

15 * * * * $HOME/mbslave/mbslave-sync.py >>/var/log/mbslave.log

Upgrading
=========

Release 2011-07-13
~~~~~~~~~~~~~~~~~~

 $ ./mbslave-psql.py <sql/updates/20110624-cdtoc-indexes.sql
 $ ./mbslave-psql.py <sql/updates/20110710-tracklist-index-slave-before.sql
 $ echo "TRUNCATE url_gid_redirect" | ./mbslave-psql.py
 $ echo "TRUNCATE work_alias" | ./mbslave-psql.py
 $ curl -O "ftp://data.musicbrainz.org/pub/musicbrainz/data/20110711-update.tar.bz2"
 $ curl -O "ftp://data.musicbrainz.org/pub/musicbrainz/data/20110711-update-derived.tar.bz2"
 $ ./mbslave-import.py 20110711-update.tar.bz2 20110711-update-derived.tar.bz2
 $ ./mbslave-psql.py <sql/updates/20110710-tracklist-index-slave-after.sql
 $ echo "UPDATE replication_control SET current_schema_sequence = 13, current_replication_sequence = 51420;" | ./mbslave-psql.py

Optionally, if you want to integrate tables from the old rawdata database,
in case they start being replicated in the future, you can also run these
commands:

 $ ./mbslave-psql.py <sql/vertical/rawdata/CreateTables.sql
 $ ./mbslave-psql.py <sql/vertical/rawdata/CreateIndexes.sql
 $ ./mbslave-psql.py <sql/vertical/rawdata/CreatePrimaryKeys.sql
 $ ./mbslave-psql.py <sql/vertical/rawdata/CreateFunctions.sql
 $ echo "ALTER TABLE edit_artist ADD status smallint NOT NULL;" | ./mbslave-psql.py
 $ echo "CREATE INDEX edit_artist_idx_status ON edit_artist (status);" | ./mbslave-psql.py
 $ echo "ALTER TABLE edit_label ADD status smallint NOT NULL;" | ./mbslave-psql.py
 $ echo "CREATE INDEX edit_label_idx_status ON edit_label (status);" | ./mbslave-psql.py

