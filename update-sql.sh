#!/bin/sh

CLONEDIR=/tmp/mbserver-clone
git clone git://git.musicbrainz.org/musicbrainz-server.git $CLONEDIR
rm -rf sql
mv $CLONEDIR/admin/sql .
rm -rf $CLONEDIR

