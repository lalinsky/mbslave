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
        ./mbslave-remap-schema.py <sql/CreateTables.sql | sed 's/CUBE/TEXT/' | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/statistics/CreateTables.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/caa/CreateTables.sql | ./mbslave-psql.py

 3. Download the MusicBrainz database dump files from
    http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/

 4. Import the data dumps, for example:

        ./mbslave-import.py mbdump.tar.bz2 mbdump-derived.tar.bz2

 5. Setup primary keys, indexes and views:

        ./mbslave-remap-schema.py <sql/CreatePrimaryKeys.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/statistics/CreatePrimaryKeys.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/caa/CreatePrimaryKeys.sql | ./mbslave-psql.py

        ./mbslave-remap-schema.py <sql/CreateIndexes.sql | grep -vE '(collate|page_index|tracklist_index)' | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/statistics/CreateIndexes.sql | ./mbslave-psql.py
        ./mbslave-remap-schema.py <sql/caa/CreateIndexes.sql | ./mbslave-psql.py

        ./mbslave-remap-schema.py <sql/CreateSimpleViews.sql | ./mbslave-psql.py

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

### Release 2012-10-15

This release introduces two new database schemas. You can decide whether you want
to have the three schemas in your database, or if you want to merge them. For this
you need to update your `mbslave.conf` configuration file (see `mbslave.conf.dist`
for usage of the new options). The rest of the guide will assume that you will
keep have all schemas.

Reimporting your database from data dumps is the recommended approach, unless you
are aware what changes were done in this release.

The upgrade scripts for this release do not support PostgreSQL 8.4. If you are,
using this or an older version of PostgreSQL, you have to drop your database
and import from data dumps.

Create the cover_art_archive schema:

```sh
echo 'CREATE SCHEMA cover_art_archive;' | ./mbslave-psql.py -S
./mbslave-remap-schema.py <sql/updates/20121015-caa-as-of-schema-15.sql | ./mbslave-psql.py
wget http://ftp.musicbrainz.org/pub/musicbrainz/data/schema-change-2012-10-15/mbdump-cover-art-archive.tar.bz2
./mbslave-import.py mbdump-cover-art-archive.tar.bz2
./mbslave-remap-schema.py <sql/updates/20120919-caa-edits-pending.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20120921-release-group-cover-art.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/caa/CreatePrimaryKeys.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/caa/CreateIndexes.sql | ./mbslave-psql.py
```

Move tables to the statistics schema:

```sh
./mbslave-remap-schema.py <sql/updates/20120922-move-statistics-tables.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20120927-add-log-statistics.sql | ./mbslave-psql.py
```

Upgrade the musicbrainz schema (not completely tested, if it doesn't work, please let me know):

```sh
grep 'VIEW' sql/CreateSimpleViews.sql | sed 's/CREATE OR REPLACE/DROP/' | sed 's/ AS/;/' | ./mbslave-psql.py
./mbslave-psql.py <sql-extra/20121017-create-controlled-for-whitespace.sql
./mbslave-psql.py <sql/SetSequences.sql 
./mbslave-psql.py <sql/updates/20120220-merge-duplicate-credits.sql
./mbslave-psql.py <sql/updates/20120822-more-text-constraints.sql
./mbslave-psql.py <sql/updates/20120917-rg-st-created.sql
./mbslave-psql.py <sql/updates/20120921-drop-url-descriptions.sql
./mbslave-psql.py <sql/updates/20120911-not-null-comments.sql
./mbslave-psql.py <sql/CreateSimpleViews.sql
echo "UPDATE replication_control SET current_schema_sequence = 16;" | ./mbslave-psql.py
```

### Release 2013-05-15

There are again two new schemas, so before you begin you need to update your
`mbslave.conf` to define the mapping for the new schemas. See
`mbslave.conf.default` for the default configuration.

Assuming the schemas are not renamed, or they are renamed to names that do not yet exist in the database, so you can run the following:

```sh
./mbslave-remap-schema.py <sql/updates/20130222-transclusion-table.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130313-relationship-documentation.sql | ./mbslave-psql.py
```

Alternatively, if you want to map them both to for example `musicbrainz` which already exists, use this:

```sh
./mbslave-remap-schema.py <sql/updates/20130222-transclusion-table.sql | grep -v 'CREATE SCHEMA' | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130313-relationship-documentation.sql | grep -v 'CREATE SCHEMA' | ./mbslave-psql.py

```

Because this documentation uses a slightly non-standard setup, we need to prepare the database for upgrade:

```sh
grep 'VIEW' sql/CreateSimpleViews.sql | sed 's/CREATE OR REPLACE/DROP/' | sed 's/ AS/;/' | ./mbslave-psql.py
tail -n+61 sql/CreateFunctions.sql | head -n 64 | ./mbslave-remap-schema.py | ./mbslave-psql.py

Now run the actual upgrade:

```sh
./mbslave-remap-schema.py <sql/updates/20130414-work-attributes.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130117-cover-image-types.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130312-collection-descriptions.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130313-instrument-credits.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130222-drop-work.artist_credit.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130322-multiple-country-dates.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130225-rename-link_type.short_link_phrase.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/SetSequences.sql | ./mbslave-psql.py # ignore errors
./mbslave-remap-schema.py <sql/updates/20130301-areas.sql | grep -vE '(to_tsvector|page_index)' | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130425-edit-area.sql | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20130318-track-mbid-reduplicate-tracklists.sql | grep -v 'USING GIST' | ./mbslave-psql.py
./mbslave-remap-schema.py <sql/updates/20120914-isni.sql | ./mbslave-psql.py
```

Re-create the simple views and increase the schema number:

```sh
./mbslave-psql.py <sql/CreateSimpleViews.sql
echo "UPDATE replication_control SET current_schema_sequence = 17;" | ./mbslave-psql.py
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

