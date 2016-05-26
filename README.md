# MusicBrainz Database Setup

This repository contains a collection of scripts that can help you setup a local
copy of the MusicBrainz database and keep it up to date. There is no script that
does everything for you though. The main motivation for writing these scripts was
to customize the database, so the way to install the database might differ from
user to user.

## Installation

 0. Make sure you have [Python](http://python.org/) and [psycopg2](http://initd.org/psycopg/) installed.
    On Debian and Ubuntu, that means installing these packages:

        sudo apt install python-psycopg2

 1. Create `mbslave.conf` by copying and editing `mbslave.conf.default`.
    You will need to get the API token on the [MetaBrainz website](https://test.metabrainz.org/).

 1. Setup the database. If you are starting completely from scratch,
    you can use the following commands to setup a clean database:

        sudo su - postgres
        createuser musicbrainz
        createdb -l C -E UTF-8 -T template0 -O musicbrainz musicbrainz
        createlang plpgsql musicbrainz
        psql musicbrainz -c 'CREATE EXTENSION cube;'
        psql musicbrainz -c 'CREATE EXTENSION earthdistance;'

 2. Prepare empty schemas for the MusicBrainz database and create the table structure:

        echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA statistics;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA cover_art_archive;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA wikidocs;' | ./mbslave-psql.py -S
        echo 'CREATE SCHEMA documentation;' | ./mbslave-psql.py -S

        ./mbslave-remap-schema.py <sql/CreateTables.sql | ./mbslave-psql.py
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

        ./mbslave-remap-schema.py <sql/CreateIndexes.sql | grep -v musicbrainz_collate | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/CreateSlaveIndexes.sql | ./mbslave-psql.py
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

### Release 2016-05-24 (23)

You need to do a full import for this version.
