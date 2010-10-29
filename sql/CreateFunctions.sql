\set ON_ERROR_STOP 1

--'-----------------------------------------------------------------
-- The join(VARCHAR) aggregate
--'-----------------------------------------------------------------

CREATE OR REPLACE FUNCTION join_append(VARCHAR, VARCHAR)
RETURNS VARCHAR AS '
DECLARE
    state ALIAS FOR $1;
    value ALIAS FOR $2;
BEGIN
    IF (value IS NULL) THEN RETURN state; END IF;
    IF (state IS NULL) THEN
        RETURN value;
    ELSE
        RETURN(state || '' '' || value);
    END IF;
END;
' LANGUAGE 'plpgsql';

CREATE AGGREGATE join(BASETYPE = VARCHAR, SFUNC=join_append, STYPE=VARCHAR);

--'-----------------------------------------------------------------
-- Populate the albummeta table, one-to-one join with album.
-- All columns are non-null integers, except firstreleasedate
-- which is CHAR(10) WITH NULL
--'-----------------------------------------------------------------

create or replace function fill_album_meta () returns integer as '
declare

   table_count integer;

begin

   raise notice ''Truncating table albummeta'';
   truncate table albummeta;

   raise notice ''Counting tracks'';
   create temporary table albummeta_tracks as select album.id, count(albumjoin.album) 
                from album left join albumjoin on album.id = albumjoin.album group by album.id;

   raise notice ''Counting discids'';
   create temporary table albummeta_discids as select album.id, count(album_cdtoc.album) 
                from album left join album_cdtoc on album.id = album_cdtoc.album group by album.id;

   raise notice ''Counting puids'';
   create temporary table albummeta_puids as select album.id, count(puidjoin.track) 
                from album, albumjoin left join puidjoin on albumjoin.track = puidjoin.track 
                where album.id = albumjoin.album group by album.id;

    raise notice ''Finding first release dates'';
    CREATE TEMPORARY TABLE albummeta_firstreleasedate AS
        SELECT  album AS id, MIN(releasedate)::CHAR(10) AS firstreleasedate
        FROM    release
        GROUP BY album;

   raise notice ''Filling albummeta table'';
   insert into albummeta (id, tracks, discids, puids, firstreleasedate, asin, coverarturl, dateadded, lastupdate)
   select a.id,
            COALESCE(t.count, 0) AS tracks,
            COALESCE(d.count, 0) AS discids,
            COALESCE(p.count, 0) AS puids,
            r.firstreleasedate,
            aws.asin,
            aws.coverarturl,
            timestamp ''1970-01-01 00:00:00-00'',
            NULL
    FROM    album a
            LEFT JOIN albummeta_tracks t ON t.id = a.id
            LEFT JOIN albummeta_discids d ON d.id = a.id
            LEFT JOIN albummeta_puids p ON p.id = a.id
            LEFT JOIN albummeta_firstreleasedate r ON r.id = a.id
            LEFT JOIN album_amazon_asin aws on aws.album = a.id
            ;

   drop table albummeta_tracks;
   drop table albummeta_discids;
   drop table albummeta_puids;
   drop table albummeta_firstreleasedate;

   return 1;

end;
' language 'plpgsql';

--'-----------------------------------------------------------------
-- Keep rows in albummeta in sync with album
--'-----------------------------------------------------------------

create or replace function insert_album_meta () returns TRIGGER as $$
begin 
    insert into albummeta (id, tracks, discids, puids, lastupdate) values (NEW.id, 0, 0, 0, now()); 
    insert into album_amazon_asin (album, lastupdate) values (NEW.id, '1970-01-01 00:00:00'); 
    PERFORM propagate_lastupdate(NEW.id, CAST('album' AS name));
    UPDATE release_group_meta SET releasecount = releasecount + 1 WHERE id=NEW.release_group;
    
    return NEW; 
end; 
$$ language 'plpgsql';

create or replace function update_album_meta () returns TRIGGER as $$
begin
    IF (NEW.name != OLD.name) 
    THEN
        UPDATE album_amazon_asin SET lastupdate = '1970-01-01 00:00:00' WHERE album = NEW.id; 
    END IF;
    IF (NEW.release_group != OLD.release_group)
    THEN
        PERFORM set_release_group_firstreleasedate(OLD.release_group);
        PERFORM set_release_group_firstreleasedate(NEW.release_group);
        UPDATE release_group_meta SET releasecount = releasecount - 1 WHERE id=OLD.release_group;
        UPDATE release_group_meta SET releasecount = releasecount + 1 WHERE id=NEW.release_group;
    END IF;
    IF (NEW.modpending = OLD.modpending)
    THEN
        UPDATE albummeta SET lastupdate = now() WHERE id = NEW.id; 
        PERFORM propagate_lastupdate(NEW.id, CAST('album' AS name));
    END IF;
   return NULL;
end;
$$ language 'plpgsql';

--'-----------------------------------------------------------------
-- Keep rows in <entity>_meta table in sync with table <entity>
-- Deletion is done by cascade with foreign keys
--'-----------------------------------------------------------------

create or replace function a_iu_entity () returns TRIGGER as $$
begin 
    IF (TG_OP = 'INSERT') 
    THEN
        EXECUTE 'INSERT INTO ' || TG_RELNAME || '_meta (id) VALUES (' || NEW.id || ')';
        PERFORM propagate_lastupdate(NEW.id, TG_RELNAME);
    ELSIF (TG_OP = 'UPDATE')
    THEN
        IF (NEW.modpending = OLD.modpending)
        THEN
            IF (TG_RELNAME != 'track')
            THEN
                EXECUTE 'UPDATE ' || TG_RELNAME || '_meta SET lastupdate = now() WHERE id = ' || NEW.id; 
            END IF;
            PERFORM propagate_lastupdate(NEW.id, TG_RELNAME);
        END IF;             
    END IF;
    RETURN NULL; 
end; 
$$ language 'plpgsql';

create or replace function b_del_entity () returns TRIGGER as $$
begin 
    IF (TG_RELNAME = 'album')
    THEN
        PERFORM set_release_group_firstreleasedate(OLD.release_group);
        UPDATE release_group_meta SET releasecount = releasecount - 1 WHERE id=OLD.release_group;
    END IF;
    PERFORM propagate_lastupdate(OLD.id, TG_RELNAME);
    RETURN OLD; 
end;
$$ language 'plpgsql';

--'-----------------------------------------------------------------
-- Propagates changes on entity to linked entities 
--'-----------------------------------------------------------------
create or replace function propagate_lastupdate (entity_id integer, relname name) returns VOID as $$
begin 

--- This function caused the entire database to slow to a crawl and has been removed for now.
--- This functionality will have to be carefully re-considered in the future.

end; 
$$ language 'plpgsql';

--'-----------------------------------------------------------------
-- Changes to albumjoin could cause changes to albummeta.tracks
-- and/or albummeta.puids and/or albummeta.puids
--'-----------------------------------------------------------------

create or replace function a_ins_albumjoin () returns trigger as $$
begin
    UPDATE  albummeta
    SET     tracks = tracks + 1,
            puids = puids + (SELECT COUNT(*) FROM puidjoin WHERE track = NEW.track)
    WHERE   id = NEW.album;
    PERFORM propagate_lastupdate(NEW.track, CAST('track' AS name));

    return NULL;
end;
$$ language 'plpgsql';
--'--
create or replace function a_upd_albumjoin () returns trigger as $$
begin
    if NEW.album = OLD.album AND NEW.track = OLD.track
    then
        -- Sequence has been changed
        IF (NEW.modpending = OLD.modpending) 
        THEN
            PERFORM propagate_lastupdate(OLD.track, CAST('track' AS name));
        END IF;

    elsif NEW.track = OLD.track
    then
        -- A track is moved from an album to another one
        UPDATE  albummeta
        SET     tracks = tracks - 1,
                puids = puids - (SELECT COUNT(*) FROM puidjoin WHERE track = OLD.track),
                lastupdate = now()
        WHERE   id = OLD.album;
        -- For the old album we can't do anything better than propagete lastupdate at the album level
        PERFORM propagate_lastupdate(OLD.album, CAST('album' AS name));

        UPDATE  albummeta
        SET     tracks = tracks + 1,
                puids = puids + (SELECT COUNT(*) FROM puidjoin WHERE track = NEW.track)
        WHERE   id = NEW.album;
        PERFORM propagate_lastupdate(NEW.track, CAST('track' AS name));

    elsif NEW.album = OLD.album
    then
        -- TODO: should not happen yet
    end if;

    return NULL;
end;
$$ language 'plpgsql';
--'--
create or replace function a_del_albumjoin () returns trigger as $$
begin
    UPDATE  albummeta
    SET     tracks = tracks - 1,
            puids = puids - (SELECT COUNT(*) FROM puidjoin WHERE track = OLD.track)
    WHERE   id = OLD.album;

    return NULL;
end;
$$ language 'plpgsql';

create or replace function b_del_albumjoin () returns TRIGGER as $$
begin 
    PERFORM propagate_lastupdate(OLD.track, CAST('track' AS name));
    RETURN OLD; 
end;
$$ language 'plpgsql';

--'-----------------------------------------------------------------
-- Changes to album_cdtoc could cause changes to albummeta.discids
--'-----------------------------------------------------------------

create or replace function a_ins_album_cdtoc () returns trigger as $$ 
begin
    UPDATE  albummeta
    SET     discids = discids + 1,
            lastupdate = now()
    WHERE   id = NEW.album;
    PERFORM propagate_lastupdate(NEW.album, CAST('album' AS name));

    return NULL;
end;
$$ language 'plpgsql';
--'--
create or replace function a_upd_album_cdtoc () returns trigger as $$
begin
    if NEW.album = OLD.album
    then
        return NULL;
    end if;

    UPDATE  albummeta
    SET     discids = discids - 1,
            lastupdate = now()
    WHERE   id = OLD.album;
    PERFORM propagate_lastupdate(OLD.album, CAST('album' AS name));

    UPDATE  albummeta
    SET     discids = discids + 1,
            lastupdate = now()
    WHERE   id = NEW.album;
    PERFORM propagate_lastupdate(NEW.album, CAST('album' AS name));

    return NULL;
end;
$$ language 'plpgsql';
--'--
create or replace function a_del_album_cdtoc () returns trigger as $$
begin
    UPDATE  albummeta
    SET     discids = discids - 1,
            lastupdate = now()
    WHERE   id = OLD.album;
    PERFORM propagate_lastupdate(OLD.album, CAST('album' AS name));

    return NULL;
end;
$$ language 'plpgsql';


--'-----------------------------------------------------------------
-- Changes to puidjoin could cause changes to albummeta.puids
--'-----------------------------------------------------------------

create or replace function a_ins_puidjoin () returns trigger as '
begin
    UPDATE  albummeta
    SET     puids = puids + 1
    WHERE   id IN (SELECT album FROM albumjoin WHERE track = NEW.track);

    return NULL;
end;
' language 'plpgsql';
--'--
create or replace function a_upd_puidjoin () returns trigger as '
begin
    if NEW.track = OLD.track
    then
        return NULL;
    end if;

    UPDATE  albummeta
    SET     puids = puids - 1
    WHERE   id IN (SELECT album FROM albumjoin WHERE track = OLD.track);

    UPDATE  albummeta
    SET     puids = puids + 1
    WHERE   id IN (SELECT album FROM albumjoin WHERE track = NEW.track);

    return NULL;
end;
' language 'plpgsql';
--'--
create or replace function a_del_puidjoin () returns trigger as '
begin
    UPDATE  albummeta
    SET     puids = puids - 1
    WHERE   id IN (SELECT album FROM albumjoin WHERE track = OLD.track);

    return NULL;
end;
' language 'plpgsql';
--'-----------------------------------------------------------------
-- When a moderation closes, move rows from _open to _closed
--'-----------------------------------------------------------------

CREATE OR REPLACE FUNCTION after_update_moderation_open () RETURNS TRIGGER AS '
begin

    if (OLD.status IN (1,8) and NEW.status NOT IN (1,8)) -- STATUS_OPEN, STATUS_TOBEDELETED
    then
        -- Create moderation_closed record
        INSERT INTO moderation_closed SELECT * FROM moderation_open WHERE id = NEW.id;
        -- and update the closetime
        UPDATE moderation_closed SET closetime = NOW() WHERE id = NEW.id;

        -- Copy notes
        INSERT INTO moderation_note_closed
            SELECT * FROM moderation_note_open
            WHERE moderation = NEW.id;

        -- Copy votes
        INSERT INTO vote_closed
            SELECT * FROM vote_open
            WHERE moderation = NEW.id;

        -- Delete the _open records
        DELETE FROM vote_open WHERE moderation = NEW.id;
        DELETE FROM moderation_note_open WHERE moderation = NEW.id;
        DELETE FROM moderation_open WHERE id = NEW.id;
    end if;

    return NEW;
end;
' LANGUAGE 'plpgsql';

--'-----------------------------------------------------------------
-- Ensure release.releasedate is always valid
--'-----------------------------------------------------------------

CREATE OR REPLACE FUNCTION before_insertupdate_release () RETURNS TRIGGER AS '
DECLARE
    y CHAR(4);
    m CHAR(2);
    d CHAR(2);
    teststr VARCHAR(10);
    testdate DATE;
BEGIN
    -- Check that the releasedate looks like this: yyyy-mm-dd
    IF (NOT(NEW.releasedate ~ ''^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$''))
    THEN
        RAISE EXCEPTION ''Invalid release date specification'';
    END IF;

    y := SUBSTR(NEW.releasedate, 1, 4);
    m := SUBSTR(NEW.releasedate, 6, 2);
    d := SUBSTR(NEW.releasedate, 9, 2);

    -- Disallow yyyy-00-dd
    IF (m = ''00'' AND d != ''00'')
    THEN
        RAISE EXCEPTION ''Invalid release date specification'';
    END IF;

    -- Check that the y/m/d combination is valid (e.g. disallow 2003-02-31)
    IF (m = ''00'') THEN m:= ''01''; END IF;
    IF (d = ''00'') THEN d:= ''01''; END IF;
    teststr := ( y || ''-'' || m || ''-'' || d );
    -- TO_DATE allows 2003-08-32 etc (it becomes 2003-09-01)
    -- So we will use the ::date cast, which catches this error
    testdate := teststr;

    RETURN NEW;
END;
' LANGUAGE 'plpgsql';

--'-----------------------------------------------------------------
-- Maintain albummeta.firstreleasedate
--'-----------------------------------------------------------------

CREATE OR REPLACE FUNCTION set_release_group_firstreleasedate(release_group_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE release_group_meta SET firstreleasedate = (
        SELECT MIN(firstreleasedate) FROM albummeta, album WHERE album.id = albummeta.id
           AND release_group = release_group_id AND firstreleasedate <> '0000-00-00' AND firstreleasedate IS NOT NULL
    ) WHERE id = release_group_id;
    RETURN;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION set_album_firstreleasedate(INTEGER)
RETURNS VOID AS $$
DECLARE
    release_group_id INTEGER;
BEGIN
    UPDATE albummeta SET firstreleasedate = (
        SELECT MIN(releasedate) FROM release WHERE album = $1
           AND releasedate <> '0000-00-00' AND releasedate IS NOT NULL
    ), lastupdate = now() WHERE id = $1;
    release_group_id := (SELECT release_group FROM album WHERE id = $1);
    EXECUTE set_release_group_firstreleasedate(release_group_id);
    RETURN;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_ins_release () RETURNS TRIGGER AS $$
BEGIN
    EXECUTE set_album_firstreleasedate(NEW.album);
    PERFORM propagate_lastupdate(NEW.id, CAST('release' AS name));
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_upd_release () RETURNS TRIGGER AS $$
BEGIN
    IF (OLD.modpending = NEW.modpending)
    THEN
        EXECUTE set_album_firstreleasedate(NEW.album);
        PERFORM propagate_lastupdate(NEW.id, CAST('release' AS name));

        IF (OLD.album != NEW.album)
        THEN
            EXECUTE set_album_firstreleasedate(OLD.album);
            -- propagate_lastupdate not called since OLD.album is probably
            -- being merged in NEW.album
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_del_release () RETURNS TRIGGER AS $$
BEGIN
    EXECUTE set_album_firstreleasedate(OLD.album);
    RETURN OLD;
END;
$$ LANGUAGE 'plpgsql';

--'-----------------------------------------------------------------
-- Changes to album_amazon_asin should cause changes to albummeta.asin
--'-----------------------------------------------------------------

CREATE OR REPLACE FUNCTION set_album_asin(INTEGER)
RETURNS VOID AS '
BEGIN
    UPDATE albummeta SET coverarturl = (
        SELECT coverarturl FROM album_amazon_asin WHERE album = $1
    ), asin = (
        SELECT asin FROM album_amazon_asin WHERE album = $1
    ) WHERE id = $1
        -- Test if album still exists (sanity check)
        AND EXISTS (SELECT 1 FROM album where id = $1);
    RETURN;
END;
' LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_ins_album_amazon_asin () RETURNS TRIGGER AS '
BEGIN
    EXECUTE set_album_asin(NEW.album);
    RETURN NEW;
END;
' LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_upd_album_amazon_asin () RETURNS TRIGGER AS '
BEGIN
    EXECUTE set_album_asin(NEW.album);
    IF (OLD.album != NEW.album)
    THEN
        EXECUTE set_album_asin(OLD.album);
    END IF;
    RETURN NEW;
END;
' LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_del_album_amazon_asin () RETURNS TRIGGER AS '
BEGIN
    EXECUTE set_album_asin(OLD.album);
    RETURN OLD;
END;
' LANGUAGE 'plpgsql';

--'-----------------------------------------------------------------------------------
-- Changes to puid_stat/puidjoin_stat causes changes to puid.lookupcount/puidjoin.usecount
--'-----------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION a_idu_puid_stat () RETURNS trigger AS '
BEGIN
    IF (TG_OP = ''INSERT'' OR TG_OP = ''UPDATE'')
    THEN
        UPDATE puid SET lookupcount = (SELECT COALESCE(SUM(puid_stat.lookupcount), 0) FROM puid_stat WHERE puid_id = NEW.puid_id) WHERE id = NEW.puid_id;
        IF (TG_OP = ''UPDATE'')
        THEN
            IF (NEW.puid_id != OLD.puid_id)
            THEN
                UPDATE puid SET lookupcount = (SELECT COALESCE(SUM(puid_stat.lookupcount), 0) FROM puid_stat WHERE puid_id = OLD.puid_id) WHERE id = OLD.puid_id;
            END IF;
        END IF;
    ELSE
        UPDATE puid SET lookupcount = (SELECT COALESCE(SUM(puid_stat.lookupcount), 0) FROM puid_stat WHERE puid_id = OLD.puid_id) WHERE id = OLD.puid_id;
    END IF;

    RETURN NULL;
END;
' LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION a_idu_puidjoin_stat () RETURNS trigger AS '
BEGIN
    IF (TG_OP = ''INSERT'' OR TG_OP = ''UPDATE'')
    THEN
        UPDATE puidjoin SET usecount = (SELECT COALESCE(SUM(puidjoin_stat.usecount), 0) FROM puidjoin_stat WHERE puidjoin_id = NEW.puidjoin_id) WHERE id = NEW.puidjoin_id;
        IF (TG_OP = ''UPDATE'')
        THEN
            IF (NEW.puidjoin_id != OLD.puidjoin_id)
            THEN
                UPDATE puidjoin SET usecount = (SELECT COALESCE(SUM(puidjoin_stat.usecount), 0) FROM puidjoin_stat WHERE puidjoin_id = OLD.puidjoin_id) WHERE id = OLD.puidjoin_id;
            END IF;
        END IF;
    ELSE
        UPDATE puidjoin SET usecount = (SELECT COALESCE(SUM(puidjoin_stat.usecount), 0) FROM puidjoin_stat WHERE puidjoin_id = OLD.puidjoin_id) WHERE id = OLD.puidjoin_id;
    END IF;

    RETURN NULL;
END;
' LANGUAGE 'plpgsql';

--'-----------------------------------------------------------------------------------
-- Maintain Tags refcount
--'-----------------------------------------------------------------------------------

create or replace function a_ins_tag () returns trigger as '
begin
    UPDATE  tag
    SET     refcount = refcount + 1
    WHERE   id = NEW.tag;

    return NULL;
end;
' language 'plpgsql';

create or replace function a_del_tag () returns trigger as '
declare
    ref_count integer;
begin

    SELECT INTO ref_count refcount FROM tag WHERE id = OLD.tag;
    IF ref_count = 1 THEN
         DELETE FROM tag WHERE id = OLD.tag;
    ELSE
         UPDATE  tag
         SET     refcount = refcount - 1
         WHERE   id = OLD.tag;
    END IF;

    return NULL;
end;
' language 'plpgsql';

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

CREATE OR REPLACE FUNCTION generate_uuid_v4() RETURNS varchar
    AS $$
DECLARE
    value VARCHAR(36);
BEGIN
    value =          lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || '-';
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || '-';
    value = value || lpad((to_hex((ceil(random() * 255)::int & 15) | 64)), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || '-';
    value = value || lpad((to_hex((ceil(random() * 255)::int & 63) | 128)), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || '-';
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    value = value || lpad(to_hex(ceil(random() * 255)::int), 2, '0');
    RETURN value;
END;
$$ LANGUAGE 'plpgsql';

--'-- vi: set ts=4 sw=4 et :
