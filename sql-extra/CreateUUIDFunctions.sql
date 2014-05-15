\set ON_ERROR_STOP 1
BEGIN;

-- Generates UUID version 4 (random-based)
CREATE OR REPLACE FUNCTION generate_uuid_v4() RETURNS uuid
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
    RETURN value::uuid;
END;
$$ LANGUAGE 'plpgsql';

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
CREATE OR REPLACE FUNCTION generate_uuid_v3(namespace varchar, name varchar) RETURNS uuid
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
    return value::uuid;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE STRICT;

COMMIT;
-- vi: set ts=4 sw=4 et :
