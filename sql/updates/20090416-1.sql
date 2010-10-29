\unset ON_ERROR_STOP

BEGIN;

CREATE TABLE isrc
(
    id                  SERIAL,
    track               INTEGER NOT NULL, -- references track
    isrc                CHAR(12) NOT NULL,
    source              SMALLINT,
    modpending          INTEGER DEFAULT 0
);

ALTER TABLE isrc ADD CONSTRAINT isrc_pkey PRIMARY KEY (id);
CREATE UNIQUE INDEX isrc_isrc_track ON isrc (isrc, track);
CREATE INDEX isrc_isrc ON isrc (isrc);


COMMIT;


-- vi: set ts=4 sw=4 et :
