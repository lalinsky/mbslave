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
#   $Id$
#____________________________________________________________________________

use strict;

use FindBin;
use lib "$FindBin::Bin/../../../cgi-bin";

use DBDefs;
use MusicBrainz;
use Sql;
use MusicBrainz::Server::Replication ':replication_type';

my $mb = MusicBrainz->new;
$mb->Login(db => "READWRITE");
my $sql = Sql->new($mb->{DBH});

$sql->Begin;
eval {

$sql->Do("DROP INDEX album_artistindex");
$sql->Do("DROP INDEX album_gidindex");
$sql->Do("DROP INDEX album_nameindex");
$sql->Do("DROP INDEX album_pageindex");

if (&DBDefs::REPLICATION_TYPE ne RT_SLAVE) {
	$sql->Do("ALTER TABLE album DROP CONSTRAINT album_fk_artist");
	$sql->Do("ALTER TABLE album DROP CONSTRAINT album_fk_language");
	$sql->Do("ALTER TABLE album DROP CONSTRAINT album_fk_script");
	$sql->Do("ALTER TABLE album DISABLE TRIGGER a_upd_album");
}

printf "Initializing default release_groups...\n";
$sql->Do("ALTER TABLE album ADD release_group INTEGER");
$sql->Do("UPDATE album SET release_group=id");

$sql->Do("CREATE INDEX album_artistindex ON album (artist)");

printf "Loading releases...\n";
my $albums = $sql->SelectListOfHashes("SELECT id, name, artist FROM album");
my %release_groups = map { $_->{id} => $_ } @$albums;
my %album_rg = map { $_->{id} => $_->{id} } @$albums;

sub find_rg
{
	my $album_id = shift;
	while ($album_id != $album_rg{$album_id}) {
		$album_id = $album_rg{$album_id};
	}
	return $album_id;
}

my $links;

$sql->Do("CREATE AGGREGATE array_accum (basetype = anyelement, sfunc = array_append, stype = anyarray, initcond = '{}')");

printf "Loading 'first album release' ARs...\n";
$links = $sql->SelectListOfLists("
	SELECT link0, link1
	FROM l_album_album LEFT JOIN album a0 ON a0.id=link0 LEFT JOIN album a1 ON a1.id=link1
	WHERE link_type=2 AND link0!=link1 AND a0.attributes=a1.attributes AND
		a0.artist=a1.artist AND
		regexp_replace(a0.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', '') =
		regexp_replace(a1.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', '')
	");
printf "Processing 'first album release' ARs...\n";
foreach my $row (@$links) {
	my $link0 = $row->[0];
	my $link1 = $row->[1];
	$album_rg{$link1} = find_rg($link0);
}

printf "Loading 'transliteration' ARs...\n";
$links = $sql->SelectListOfLists("
	SELECT link0, link1
	FROM l_album_album LEFT JOIN album a0 ON a0.id=link0 LEFT JOIN album a1 ON a1.id=link1
	WHERE link_type=15 AND link0!=link1 AND a0.artist=a1.artist
	");
printf "Processing 'transliteration' ARs...\n";
foreach my $row (@$links) {
	my $link0 = $row->[0];
	my $link1 = $row->[1];
	$album_rg{$link1} = find_rg($link0);
}

printf "Loading 'part of set' ARs...\n";
$links = $sql->SelectListOfLists("
	SELECT link0, link1
	FROM l_album_album LEFT JOIN album a0 ON a0.id=link0 LEFT JOIN album a1 ON a1.id=link1
	WHERE link_type=17 AND link0!=link1 AND
		a0.artist=a1.artist AND
		regexp_replace(a0.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', '') =
		regexp_replace(a1.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', '')
	");
printf "Processing 'part of set' ARs...\n";
foreach my $row (@$links) {
	my $link0 = $row->[0];
	my $link1 = $row->[1];
	$album_rg{$link1} = find_rg($link0);
}

printf "Loading 'remaster' ARs...\n";
$links = $sql->SelectListOfLists("
	SELECT link0, link1
	FROM l_album_album LEFT JOIN album a0 ON a0.id=link0 LEFT JOIN album a1 ON a1.id=link1
	WHERE link_type=3 AND link0!=link1 AND a0.attributes=a1.attributes AND
		a0.artist=a1.artist AND
		regexp_replace(a0.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', '') =
		regexp_replace(a1.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', '')
	");
printf "Processing 'remaster' ARs...\n";
foreach my $row (@$links) {
	my $link0 = $row->[1];
	my $link1 = $row->[0];
	$album_rg{$link1} = find_rg($link0);
}

printf "Loading discogs ARs...\n";
$links = $sql->SelectSingleColumnArray("
	SELECT array_accum(link0)
	FROM l_album_url JOIN album a ON a.id=l_album_url.link0
	WHERE link_type=24
	GROUP BY link1,
		regexp_replace(a.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', ''),
		a.artist
	HAVING count(*) > 1");
printf "Processing discogs ARs...\n";
foreach my $ids (@$links) {
	if (ref $ids ne 'ARRAY') {
		$ids = [ $ids =~ /(\d+)/g ];
	}
	my @ids = sort { $a <=> $b } @$ids;
	my $target_id = $ids[0];
	foreach my $id (@ids) {
		if ($album_rg{$id} == $id) {
			$target_id = $id;
		}
	}
	my $target_rg_id = find_rg($target_id);	
	foreach my $id (@ids) {
		if ($id != $target_id) {
			$album_rg{$id} = $target_rg_id;
		}
	}
}

printf "Loading wikipedia ARs...\n";
$links = $sql->SelectSingleColumnArray("
	SELECT array_accum(link0)
	FROM l_album_url JOIN album a ON a.id=l_album_url.link0
	WHERE link_type=23
	GROUP BY link1,
		regexp_replace(a.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', ''),
		a.artist
	HAVING count(*) > 1");
printf "Processing wikipedia ARs...\n";
foreach my $ids (@$links) {
	if (ref $ids ne 'ARRAY') {
		$ids = [ $ids =~ /(\d+)/g ];
	}
	my @ids = sort { $a <=> $b } @$ids;
	my $target_id = $ids[0];
	foreach my $id (@ids) {
		if ($album_rg{$id} == $id) {
			$target_id = $id;
		}
	}
	my $target_rg_id = find_rg($target_id);	
	foreach my $id (@ids) {
		if ($id != $target_id) {
			$album_rg{$id} = $target_rg_id;
		}
	}
}

printf "Loading identical release events (barcode optional)...\n";
$links = $sql->SelectSingleColumnArray("
	SELECT array_accum(album) FROM release JOIN album a ON a.id=release.album
	WHERE releasedate!='0000-00-00' and releasedate is not null
		and country is not null and label is not null
		and catno is not null
	GROUP BY releasedate, country, label, catno, barcode, format,
		regexp_replace(a.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', ''),
		a.artist
	HAVING COUNT(*) > 1");
printf "Processing identical release events...\n";
foreach my $ids (@$links) {
	if (ref $ids ne 'ARRAY') {
		$ids = [ $ids =~ /(\d+)/g ];
	}
	my @ids = sort { $a <=> $b } @$ids;
	my $target_id = $ids[0];
	foreach my $id (@ids) {
		if ($album_rg{$id} == $id) {
			$target_id = $id;
		}
	}
	my $target_rg_id = find_rg($target_id);	
	foreach my $id (@ids) {
		if ($id != $target_id) {
			$album_rg{$id} = $target_rg_id;
		}
	}
}

printf "Loading identical release events (excluding cat#, barcode required)...\n";
$links = $sql->SelectSingleColumnArray("
	SELECT array_accum(album) FROM release JOIN album a ON a.id=release.album
	WHERE releasedate!='0000-00-00' and releasedate is not null
		and country is not null and label is not null
		and barcode is not null
	GROUP BY releasedate, country, label, barcode, format,
		regexp_replace(a.name, E'\\\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]\$', ''),
		a.artist
	HAVING COUNT(*) > 1");
printf "Processing identical release events...\n";
foreach my $ids (@$links) {
	if (ref $ids ne 'ARRAY') {
		$ids = [ $ids =~ /(\d+)/g ];
	}
	my @ids = sort { $a <=> $b } @$ids;
	my $target_id = $ids[0];
	foreach my $id (@ids) {
		if ($album_rg{$id} == $id) {
			$target_id = $id;
		}
	}
	my $target_rg_id = find_rg($target_id);	
	foreach my $id (@ids) {
		if ($id != $target_id) {
			$album_rg{$id} = $target_rg_id;
		}
	}
}


my $i = 0;
my $cnt = 0;
my $c = scalar(keys %album_rg);

foreach my $album_id (keys %album_rg) {
	my $rg_id = find_rg($album_id);
	if ($album_id != $rg_id) {
		$sql->Do("UPDATE album SET release_group=? WHERE id=?", $rg_id, $album_id);
		# Forge a merge edit
		my %new = ( "ReleaseGroupId0" => $rg_id, "ReleaseGroupName0" => $release_groups{$rg_id}->{name},
				"ReleaseGroupId1" => $album_id, "ReleaseGroupName1" => $release_groups{$album_id}->{name} );

		$cnt += 1;
	}
	if ($i % 100 == 0) {
		printf STDERR "$i/$c\r";
	}
	$i += 1;
}
printf STDERR "$cnt release groups merged\n";

$sql->Do("ALTER TABLE album ALTER COLUMN release_group SET NOT NULL");

$sql->Do("CREATE UNIQUE INDEX album_gidindex ON album (gid)");
$sql->Do("CREATE INDEX album_nameindex ON album (name)");
$sql->Do("CREATE INDEX album_pageindex ON album (page)");

if (&DBDefs::REPLICATION_TYPE ne RT_SLAVE) {

	$sql->Do("
ALTER TABLE album
    ADD CONSTRAINT album_fk_artist
    FOREIGN KEY (artist)
    REFERENCES artist(id)
");

	$sql->Do("
ALTER TABLE album
    ADD CONSTRAINT album_fk_language
    FOREIGN KEY (language)
    REFERENCES language(id)
");

	$sql->Do("
ALTER TABLE album
    ADD CONSTRAINT album_fk_script
    FOREIGN KEY (script)
    REFERENCES script(id)
");

	$sql->Do("ALTER TABLE album ENABLE TRIGGER a_upd_album");

}

#$sql->Do("DROP AGGREGATE array_accum (anyelement)");

	$sql->Commit;
};
if ($@) {
	$sql->Rollback;
}
