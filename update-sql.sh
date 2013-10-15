#!/bin/sh

CLONEDIR="${1:-/tmp/mbserver-clone}"
rm -rf sql
[ -z "$1" ] && git clone git://github.com/metabrainz/musicbrainz-server.git "$CLONEDIR"
cp -r "$CLONEDIR/admin/sql" .
[ -z "$1" ] && rm -rf $CLONEDIR

grep 'VIEW' sql-extra/CreateSimpleViews.sql | sed 's/CREATE OR REPLACE/DROP/' | sed 's/ AS/;/' | perl -e 'print reverse <>' >sql-extra/DropSimpleViews.sql

