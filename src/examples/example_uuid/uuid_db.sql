-- Database for testing the uuids
-- Written by DEIMOS Space S.L. (dibb)
-- modeule eboa

---------------------- 
-- Execute this as postgres user with the following command:
-- psql eboadb -f $path_to_eboa/eboa/src/tests/test_uuid/uuid_db.sql
---------------------- 

-- Define table
CREATE TABLE e2espm.uuid(
	uuid uuid NOT NULL,
	CONSTRAINT unique_uuid UNIQUE (uuid)

);

-- Asigned owner as e2espm
ALTER TABLE e2espm.uuid OWNER TO e2espm;

-- Create index
CREATE INDEX idx_uuid_uuid ON e2espm.uuid
	USING btree
	(
	  uuid
	);

