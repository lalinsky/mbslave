CREATE TABLE mbslave_solr_queue (
    id serial NOT NULL PRIMARY KEY,
    entity_type text NOT NULL,
    entity_id int NOT NULL
);

