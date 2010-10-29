\set ON_ERROR_STOP 1

BEGIN;

ALTER TABLE album
    ADD CONSTRAINT album_fk_release_group
    FOREIGN KEY (release_group)
    REFERENCES release_group(id);

ALTER TABLE release_group
    ADD CONSTRAINT release_group_fk_artist
    FOREIGN KEY (artist)
    REFERENCES artist(id);

ALTER TABLE release_group_meta
    ADD CONSTRAINT release_group_meta_fk_id
    FOREIGN KEY (id)
    REFERENCES release_group(id)
    ON DELETE CASCADE;

ALTER TABLE release_groupwords
    ADD CONSTRAINT release_groupwords_fk_release_groupid
    FOREIGN KEY (release_groupid)
    REFERENCES release_group (id)
    ON DELETE CASCADE;

CREATE TRIGGER a_iu_release_group AFTER INSERT OR UPDATE ON release_group
    FOR EACH ROW EXECUTE PROCEDURE a_iu_entity();

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

END;
-- vi: set ts=4 sw=4 et :
