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

    ```sh
    sudo su - postgres
    createuser musicbrainz
    createdb -l C -E UTF-8 -T template0 -O musicbrainz musicbrainz
    createlang plpgsql musicbrainz
    ```

 2. Prepare empty schema for the MusicBrainz database (skip this if you
    want to use the default `public` schema) and create the table structure:

    ```sh
    echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql.py -S
    sed 's/CUBE/TEXT/' sql/CreateTables.sql | ./mbslave-psql.py
    ```

 3. Download the MusicBrainz database dump files from
    http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/

 4. Import the data dumps, for example:

    ```sh
    ./mbslave-import.py mbdump.tar.bz2 mbdump-derived.tar.bz2
    ```

 5. Setup primary keys, indexes and views:

    ```sh
    ./mbslave-psql.py <sql/CreatePrimaryKeys.sql
    grep -vE '(collate|page_index|tracklist_index)' sql/CreateIndexes.sql | ./mbslave-psql.py
    ./mbslave-psql.py <sql/CreateSimpleViews.sql
    ```

 6. Vacuum the newly created database (optional)

    ```sh
    echo 'VACUUM ANALYZE;' | ./mbslave-psql.py
    ```

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

### Release 2011-07-13

```sh
./mbslave-psql.py <sql/updates/20110624-cdtoc-indexes.sql
./mbslave-psql.py <sql/updates/20110710-tracklist-index-slave-before.sql
echo "TRUNCATE url_gid_redirect" | ./mbslave-psql.py
echo "TRUNCATE work_alias" | ./mbslave-psql.py
curl -O "ftp://data.musicbrainz.org/pub/musicbrainz/data/20110711-update.tar.bz2"
curl -O "ftp://data.musicbrainz.org/pub/musicbrainz/data/20110711-update-derived.tar.bz2"
./mbslave-import.py 20110711-update.tar.bz2 20110711-update-derived.tar.bz2
./mbslave-psql.py <sql/updates/20110710-tracklist-index-slave-after.sql
echo "UPDATE replication_control SET current_schema_sequence = 13, current_replication_sequence = 51420;" | ./mbslave-psql.py
```

Optionally, if you want to integrate tables from the old rawdata database,
in case they start being replicated in the future, you can also run these
commands:

```sh
./mbslave-psql.py <sql/vertical/rawdata/CreateTables.sql
./mbslave-psql.py <sql/vertical/rawdata/CreateIndexes.sql
./mbslave-psql.py <sql/vertical/rawdata/CreatePrimaryKeys.sql
./mbslave-psql.py <sql/vertical/rawdata/CreateFunctions.sql
echo "ALTER TABLE edit_artist ADD status smallint NOT NULL;" | ./mbslave-psql.py
echo "CREATE INDEX edit_artist_idx_status ON edit_artist (status);" | ./mbslave-psql.py
echo "ALTER TABLE edit_label ADD status smallint NOT NULL;" | ./mbslave-psql.py
echo "CREATE INDEX edit_label_idx_status ON edit_label (status);" | ./mbslave-psql.py
```

### Release 2012-01-12

```sh
./mbslave-psql.py <sql/updates/20120105-caa-flag.sql
echo "UPDATE replication_control SET current_schema_sequence = 14;" | ./mbslave-psql.py
```

### Release 2012-05-15

```sh
grep 'CREATE VIEW' sql/CreateSimpleViews.sql | sed 's/CREATE/DROP/' | sed 's/ AS/;/' | ./mbslave-psql.py
./mbslave-psql.py <sql/updates/20120420-editor-improvements.sql
./mbslave-psql.py <sql/updates/20120417-improved-aliases.sql
./mbslave-psql.py <sql/updates/20120423-release-group-types.sql
./mbslave-psql.py <sql/updates/20120320-remove-url-refcount.sql
./mbslave-psql.py <sql/updates/20120410-multiple-iswcs-per-work.sql
./mbslave-psql.py <sql/updates/20120430-timeline-events.sql
./mbslave-psql.py <sql/updates/20120501-timeline-events.sql
./mbslave-psql.py <sql/updates/20120405-rename-language-columns.sql
./mbslave-psql.py <sql-extra/20120406-update-language-codes.sql
./mbslave-psql.py <sql/updates/20120411-add-work-language.sql
./mbslave-psql.py <sql/updates/20120314-add-tracknumber.sql
./mbslave-psql.py <sql/updates/20120412-add-ipi-tables.sql
./mbslave-psql.py <sql/updates/20120508-unknown-end-dates.sql
./mbslave-psql.py <sql/CreateSimpleViews.sql
echo "UPDATE replication_control SET current_schema_sequence = 15;" | ./mbslave-psql.py
```

## Solr Search Index (Work-In-Progress)

If you would like to also build a Solr index for searching, mbslave includes a script to
export the MusicBrainz into XML file that you can feed to Solr:

    ./mbslave-solr-export.py >/tmp/solr-data.xml

Once you have generated this file, you for example start a local instance of Solr:

    java -Dsolr.solr.home=/path/to/mbslave/solr/ -jar start.jar

And tell it to import the XML file:

    curl http://localhost:8983/solr/musicbrainz/update -F stream.file=/tmp/solr-data.xml -F commit=true

