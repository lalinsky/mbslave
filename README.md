# MusicBrainz Database Setup

This repository contains a collection of scripts that can help you setup a local
copy of the MusicBrainz database and keep it up to date. There is no script that
does everything for you though. The main motivation for writing these scripts was
to customize the database, so the way to install the database might differ from
user to user.

## Installation

 0. Make sure you have [Python](http://python.org/) and [psycopg2](http://initd.org/psycopg/) installed.

 1. Setup a database and create `mbslave.conf` by copying and editing
    mbslave.conf.default. If you are starting completely from scratch,
    you can use the following commands to setup a clean database:

        sudo su - postgres
        createuser musicbrainz
        createdb -l C -E UTF-8 -T template0 -O musicbrainz musicbrainz
        createlang plpgsql musicbrainz

 2. Prepare empty schemas for the MusicBrainz database and create the table structure:

        echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA statistics;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA cover_art_archive;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA wikidocs;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA documentation;' | ./mbslave-psql.py -S
        ./mbslave-remap-schema.py <sql/CreateTables.sql | sed 's/CUBE/TEXT/' | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/statistics/CreateTables.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/caa/CreateTables.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/wikidocs/CreateTables.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/documentation/CreateTables.sql | ./mbslave-psql.py

 3. Download the MusicBrainz database dump files from
    http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/

 4. Import the data dumps, for example:

        ./mbslave-import.py mbdump.tar.bz2 mbdump-derived.tar.bz2

 5. Setup primary keys, indexes and views:

        ./mbslave-remap-schema.py <sql/CreatePrimaryKeys.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/statistics/CreatePrimaryKeys.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/caa/CreatePrimaryKeys.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/wikidocs/CreatePrimaryKeys.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/documentation/CreatePrimaryKeys.sql | ./mbslave-psql.py

        ./mbslave-remap-schema.py <sql/CreateIndexes.sql | grep -vE '(collate|page_index|medium_index)' | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/statistics/CreateIndexes.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/caa/CreateIndexes.sql | ./mbslave-psql.py

        ./mbslave-remap-schema.py <sql/CreateViews.sql | ./mbslave-psql.py

 6. Vacuum the newly created database (optional)

        echo 'VACUUM ANALYZE;' | ./mbslave-psql.py

## Replication

After the initial database setup, you might want to update the database with the latest data.
The `mbslave-sync.py` script will fetch updates from MusicBrainz and apply it to your local database:

```sh
./mbslave-sync.py
```

In order to update your database regularly, add a cron job like this that runs every hour:

```cron
15 * * * * $HOME/mbslave/mbslave-sync.py >>/var/log/mbslave.log
```

## Upgrading

When the MusicBrainz database schema changes, the replication will stop working.
This is usually announced on the [MusicBrainz blog](http://blog.musicbrainz.org/).
When it happens, you need to upgrade the database.

### Release 2014-05-14 (20)

Download some new data to import:

```sh
wget http://ftp.musicbrainz.org/pub/musicbrainz/data/schema-change-2014-05/mbdump-derived.tar.bz2
wget http://ftp.musicbrainz.org/pub/musicbrainz/data/schema-change-2014-05/mbdump-documentation.tar.bz2
```

Import the data dumps:

```sh
echo 'TRUNCATE TABLE release_tag;' | ./mbslave-psql.py
./mbslave-import.py mbdump-derived.tar.bz2
./mbslave-import.py mbdump-documentation.tar.bz2 
```

Run the upgrade scripts:

```sh
./mbslave-remap-schema.py <sql-extra/CreateUUIDFunctions.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/schema-change/20.slave.sql | grep -v to_tsvector | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20140509-place-example-pkeys.sql | ./mbslave-psql.py
echo 'UPDATE replication_control SET current_schema_sequence = 20;' | ./mbslave-psql.py
echo 'VACUUM ANALYZE;' | ./mbslave-psql.py
```

## Solr Search Index (Work-In-Progress)

If you would like to also build a Solr index for searching, mbslave includes a script to
export the MusicBrainz into XML file that you can feed to Solr:

    ./mbslave-solr-export.py >/tmp/mbslave-solr-data.xml

Once you have generated this file, you for example start a local instance of Solr:

    java -Dsolr.solr.home=/path/to/mbslave/solr/ -jar start.jar

Import the XML file:

    curl http://localhost:8983/solr/musicbrainz/update -F stream.file=/tmp/mbslave-solr-data.xml -F commit=true

Install triggers to queue database updates:

    echo 'CREATE SCHEMA mbslave;' | ./mbslave-psql.py -S
    ./mbslave-remap-schema.py <sql-extra/solr-queue.sql | ./mbslave-psql.py -s mbslave
    ./mbslave-solr-generate-triggers.py | ./mbslave-remap-schema.py | ./mbslave-psql.py -s mbslave

Update the index:

    ./mbslave-solr-update.py

