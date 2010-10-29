\set ON_ERROR_STOP 1

BEGIN;

CREATE INDEX album_release_groupindex ON album (release_group);

CREATE OR REPLACE FUNCTION from_hex(t text) RETURNS integer
    AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN EXECUTE 'SELECT x'''||t||'''::integer AS hex' LOOP
        RETURN r.hex;
    END LOOP;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- NameSpace_URL = '6ba7b8119dad11d180b400c04fd430c8'
CREATE OR REPLACE FUNCTION generate_uuid_v3(namespace varchar, name varchar) RETURNS varchar
    AS $$
DECLARE
    value varchar(36);
    bytes varchar;
BEGIN
    bytes = md5(decode(namespace, 'hex') || decode(name, 'escape'));
    value = substr(bytes, 1+0, 8);
    value = value || '-';
    value = value || substr(bytes, 1+2*4, 4);
    value = value || '-';
    value = value || lpad(to_hex((from_hex(substr(bytes, 1+2*6, 2)) & 15) | 48), 2, '0');
    value = value || substr(bytes, 1+2*7, 2);
    value = value || '-';
    value = value || lpad(to_hex((from_hex(substr(bytes, 1+2*8, 2)) & 63) | 128), 2, '0');
    value = value || substr(bytes, 1+2*9, 2);
    value = value || '-';
    value = value || substr(bytes, 1+2*10, 12);
    return value;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE STRICT;


CREATE TABLE release_group (
    id                  SERIAL,
    gid                 CHAR(36),
    name                VARCHAR(255),
    page                INTEGER NOT NULL,
    artist              INTEGER NOT NULL, -- references artist
    type                INTEGER,
    modpending          INTEGER DEFAULT 0
);

INSERT INTO release_group (id, gid, name, page, artist, type)
    SELECT a.id,
        generate_uuid_v3('6ba7b8119dad11d180b400c04fd430c8', 'http://musicbrainz.org/show/release-group/?id=' || a.id),
        regexp_replace(a.name, E'\\s+[(](disc [0-9]+(: .*?)?|bonus disc(: .*?)?)[)]$', ''), a.page, a.artist,
        CASE
            WHEN 1 = ANY(a.attributes[2:10]) THEN 1
            WHEN 2 = ANY(a.attributes[2:10]) THEN 2
            WHEN 3 = ANY(a.attributes[2:10]) THEN 3
            WHEN 4 = ANY(a.attributes[2:10]) THEN 4
            WHEN 5 = ANY(a.attributes[2:10]) THEN 5
            WHEN 6 = ANY(a.attributes[2:10]) THEN 6
            WHEN 7 = ANY(a.attributes[2:10]) THEN 7
            WHEN 8 = ANY(a.attributes[2:10]) THEN 8
            WHEN 9 = ANY(a.attributes[2:10]) THEN 9
            WHEN 10 = ANY(a.attributes[2:10]) THEN 10
            WHEN 11 = ANY(a.attributes[2:10]) THEN 11
            WHEN 0 = ANY(a.attributes[2:10]) THEN 0
            ELSE NULL
        END
    FROM album a WHERE a.id = a.release_group;

-- insert release-groups rename edits
INSERT INTO moderation_closed (id, tab, col, rowid, prevvalue, newvalue, moderator, artist, type, depmod, status, opentime, closetime, expiretime, yesvotes, novotes, automod)
	SELECT nextval('moderation_open_id_seq'), 'release_group', 'name', rg.id, al.name, rg.name, 4, rg.artist, 65, 0, 2, NOW(), NOW(), NOW(), 0, 0, 1
	FROM album al INNER JOIN release_group rg ON (al.id = rg.id)
	WHERE al.name <> rg.name;

SELECT SETVAL('release_group_id_seq', (SELECT MAX(id) FROM release_group));

ALTER TABLE release_group ADD CONSTRAINT release_group_pkey  PRIMARY KEY (id);
CREATE INDEX release_group_artistindex ON release_group (artist);
CREATE UNIQUE INDEX release_group_gidindex ON release_group (gid);
CREATE INDEX release_group_nameindex ON release_group (name);
CREATE INDEX release_group_pageindex ON release_group (page);

CREATE TABLE release_group_meta (
    id                  INTEGER NOT NULL,
    lastupdate          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    firstreleasedate    CHAR(10),
    releasecount        INTEGER DEFAULT 0
);

SELECT release_group, COUNT(*) AS cnt
    INTO TEMPORARY tmp_releasecount
    FROM album GROUP BY release_group;

CREATE UNIQUE INDEX tmp_releasecount_idx_release_group ON tmp_releasecount (release_group);

-- release-groups having a firstreleasedate
INSERT INTO release_group_meta (id, firstreleasedate, releasecount)
    SELECT rg.id, MIN(firstreleasedate), rc.cnt
    FROM release_group rg, album a, albummeta am, tmp_releasecount rc
    WHERE am.id = a.id AND a.release_group = rg.id AND firstreleasedate <> '0000-00-00' AND firstreleasedate IS NOT NULL
        AND rg.id=rc.release_group
    GROUP BY rg.id, rc.cnt;

-- remaining release-groups
INSERT INTO release_group_meta (id, firstreleasedate, releasecount)
    SELECT rg.id, null, rc.cnt
    FROM release_group rg
        LEFT JOIN release_group_meta rg_meta on (rg.id = rg_meta.id)
        LEFT JOIN tmp_releasecount rc on (rg.id = rc.release_group)
    WHERE rg_meta.id is null;

ALTER TABLE release_group_meta ADD CONSTRAINT release_group_meta_pkey PRIMARY KEY (id);

END;
-- vi: set ts=4 sw=4 et :
