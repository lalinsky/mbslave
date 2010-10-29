\set ON_ERROR_STOP 1

BEGIN;

CREATE TABLE release_groupwords
(
    wordid              INTEGER NOT NULL,
    release_groupid     INTEGER NOT NULL
);

ALTER TABLE wordlist ADD release_groupusecount SMALLINT NOT NULL DEFAULT 0;

INSERT INTO release_groupwords SELECT w.* FROM albumwords w JOIN release_group rg ON w.albumid=rg.id;

ALTER TABLE release_groupwords ADD CONSTRAINT release_groupwords_pkey PRIMARY KEY (wordid, release_groupid);
CREATE INDEX release_groupwords_release_groupidindex ON release_groupwords (release_groupid);

END;
-- vi: set ts=4 sw=4 et :
