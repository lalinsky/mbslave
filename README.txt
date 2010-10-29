Installation
============

1. Setup a database and create mbslave.conf by copying and editing
   mbslave.conf.default

2. Prepare empty schema for the MusicBrainz database and create the
   table structure:

   $ echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql
   $ ./mbslave-psql.py <sql/CreateTables.sql

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

