#!/usr/bin/perl -w
# vi: set ts=4 sw=4 :
#____________________________________________________________________________
#
#   MusicBrainz -- the open internet music database
#
#   Copyright (C) 2009 Lukas Lalinsky
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
#   $Id: 20090524-1.pl 11612 2009-05-25 00:34:31Z robert $
#____________________________________________________________________________

use strict;

use FindBin;
use lib "$FindBin::Bin/../../../cgi-bin";

use DBDefs;
use MusicBrainz::Server::Database;
use MusicBrainz;
use Sql;

my $mb = MusicBrainz->new;
$mb->Login(db => "READWRITE");
my $sql = Sql->new($mb->{DBH});

if (defined MusicBrainz::Server::Database->get("READONLY"))
{
	$sql->Begin;
	$sql->Do("GRANT SELECT ON release_group TO ". MusicBrainz::Server::Database->get("READONLY")->username);
	$sql->Do("GRANT SELECT ON release_group_meta TO ". MusicBrainz::Server::Database->get("READONLY")->username);
	$sql->Do("GRANT SELECT ON release_groupwords TO ". MusicBrainz::Server::Database->get("READONLY")->username);
	$sql->Do("GRANT SELECT ON isrc TO ". MusicBrainz::Server::Database->get("READONLY")->username);
	$sql->Commit;
}
