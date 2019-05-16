# MusicBrainz Database Setup

This repository contains a collection of scripts that can help you setup a local
copy of the MusicBrainz database and keep it up to date. There is no script that
does everything for you though. The main motivation for writing these scripts was
to customize the database, so the way to install the database might differ from
user to user.

## Installation

 0. Make sure you have [Python 2](http://python.org/) and [psycopg2](http://initd.org/psycopg/) installed.
    On Debian and Ubuntu, that means installing these packages:

        sudo apt install python python-psycopg2

 1. Create `mbslave.conf` by copying and editing `mbslave.conf.default`.
    You will need to get the API token on the [MetaBrainz website](https://metabrainz.org/supporters/account-type).

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
        ./mbslave-remap-schema.py <sql/CreateFunctions.sql | ./mbslave-psql.py

 6. Vacuum the newly created database (optional)

        echo 'VACUUM ANALYZE;' | ./mbslave-psql.py

## Tips and Tricks

### Single Database Schema

MusicBrainz used a number of schemas by default. If you are embedding the MusicBrainz database into
an existing database for your application, it's convenient to merge them all into a single schema.
That can be done by changing your config like this:

    [schemas]
    musicbrainz=musicbrainz
    statistics=musicbrainz
    cover_art_archive=musicbrainz
    wikidocs=musicbrainz
    documentation=musicbrainz

After this, you only need to create the `musicbrainz` schema and import all the tables there.

### No PostgreSQL Extensions

You can avoid installing the `cube` and `earthdistance` extensions if you map the `CUBE` column to `TEXT`
and remove some indexes.

You might do a similar thing with `JSONB` columns, if you don't want to upgrade to PostgreSQL 9.5 yet.

Replace the commands above with something like his:

    ./mbslave-remap-schema.py <sql/CreateTables.sql | \
        perl -pe 's{\b(CUBE|JSONB)\b}{TEXT}' | \
        ./mbslave-psql.py

    ./mbslave-remap-schema.py <sql/CreateIndexes.sql | \
        grep -v musicbrainz_collate | \
        grep -v ll_to_earth | \
        grep -v medium_index | \
        perl -pe 's{\bUSING BRIN\b}{}' | \
        perl -pe 'BEGIN { undef $/; } s{^CREATE INDEX edit_data_idx_link_type .*?;}{}smg' | \
        ./mbslave-psql.py

### Full Import Upgrade

You can use the schema mapping feature to do zero-downtime upgrade of the database with full
data import. You can temporarily map all schemas to e.g. `musicbrainz_NEW`, import your new
database there and then rename it.

    echo 'BEGIN; ALTER SCHEMA musicbrainz RENAME TO musicbrainz_OLD; ALTER SCHEMA musicbrainz_NEW RENAME TO musicbrainz; COMMIT;' | ./mbslave-psql.py -S

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

### Release 2019-05-13 (25)

Run the upgrade scripts:

```
./mbslave-remap-schema.py <sql/updates/schema-change/25.slave.sql | ./mbslave-psql.py
echo 'UPDATE replication_control SET current_schema_sequence = 25;' | ./mbslave-psql.py
```
