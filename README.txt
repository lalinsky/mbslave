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
   want to use the default 'public' schema) and create the table
   structure:

   $ echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql.py
   $ ./mbslave-psql.py <sql/CreateTables.sql

   If you are using a newer version of PostgreSQL, you might want to prefer
   to use the UUID type instead of CHAR(36) to store UUIDs. A simple
   search/replace will do the trick:
   
   $ sed 's/CHAR(36)/UUID/' sql/CreateTables.sql | ./mbslave-psql.py

3. Download the MusicBrainz database dump files from
   http://musicbrainz.org/doc/Database_Download

4. Import the data dumps, for example:

   $ ./mbslave-import.py mbdump.tar.bz2 mbdump-derived.tar.bz2

5. Setup primary keys, indexes, views and functions:

   $ ./mbslave-psql.py <sql/CreatePrimaryKeys.sql
   $ ./mbslave-psql.py <sql/CreateIndexes.sql
   $ ./mbslave-psql.py <sql/CreateViews.sql
   $ ./mbslave-psql.py <sql/CreateFunctions.sql

6. Vacuum the newly created database (optional)

   $ echo 'VACUUM ANALYZE;' | ./mbslave-psql.py

7. Run the initial replication:

   $ ./mbslave-sync.py

