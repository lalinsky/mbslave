\unset ON_ERROR_STOP

BEGIN;

ALTER TABLE isrc
    ADD CONSTRAINT fk_isrc_track
    FOREIGN KEY (track)
    REFERENCES track(id);

COMMIT;


-- vi: set ts=4 sw=4 et :
