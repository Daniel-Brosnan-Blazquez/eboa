-- Database generated with pgModeler (PostgreSQL Database Modeler).
-- pgModeler  version: 0.9.0
-- PostgreSQL version: 9.6
-- Project Site: pgmodeler.com.br
-- Model Author: Daniel Brosnan Blazquez

-- object: eboa | type: ROLE --
-- DROP ROLE IF EXISTS eboa;
CREATE ROLE eboa WITH 
	INHERIT
	LOGIN;
-- ddl-end --


-- Database creation must be done outside an multicommand file.
-- These commands were put in this file only for convenience.
-- -- object: eboadb | type: DATABASE --
-- -- DROP DATABASE IF EXISTS eboadb;
-- CREATE DATABASE eboadb
-- ;
-- -- ddl-end --
-- 

-- object: eboa | type: SCHEMA --
-- DROP SCHEMA IF EXISTS eboa CASCADE;
CREATE SCHEMA eboa;
-- ddl-end --
ALTER SCHEMA eboa OWNER TO eboa;
-- ddl-end --

SET search_path TO pg_catalog,public,eboa;
-- ddl-end --

-- object: eboa.event_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_tb CASCADE;
CREATE TABLE eboa.event_tb(
	event_uuid uuid NOT NULL,
	start timestamp NOT NULL,
	stop timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	visible boolean NOT NULL,
	gauge_id uuid NOT NULL,
	explicit_ref_id uuid,
	processing_uuid uuid NOT NULL,
	CONSTRAINT event_tb_pk PRIMARY KEY (event_uuid),
	CONSTRAINT unique_event UNIQUE (event_uuid)

);
-- ddl-end --
ALTER TABLE eboa.event_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.gauge_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.gauge_cnf_tb CASCADE;
CREATE TABLE eboa.gauge_cnf_tb(
	gauge_id uuid NOT NULL,
	system text,
	name text NOT NULL,
	dim_signature_id uuid NOT NULL,
	CONSTRAINT gauge_cnf_tb_pk PRIMARY KEY (gauge_id),
	CONSTRAINT unique_gauge_cnf UNIQUE (system,name)

);
-- ddl-end --
ALTER TABLE eboa.gauge_cnf_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.dim_processing_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.dim_processing_tb CASCADE;
CREATE TABLE eboa.dim_processing_tb(
	processing_uuid uuid NOT NULL,
	name text NOT NULL,
	validity_start timestamp,
	validity_stop timestamp,
	generation_time timestamp,
	ingestion_time timestamp,
	ingestion_duration interval,
	dim_exec_version text,
	content_json json,
	dim_signature_id uuid,
	content_text text,
	parse_error text,
	CONSTRAINT dim_processing_tb_pk PRIMARY KEY (processing_uuid)

);
-- ddl-end --
ALTER TABLE eboa.dim_processing_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.dim_signature_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.dim_signature_tb CASCADE;
CREATE TABLE eboa.dim_signature_tb(
	dim_signature_id uuid NOT NULL,
	dim_signature text NOT NULL,
	dim_exec_name text NOT NULL,
	CONSTRAINT dim_signature_tb_pk PRIMARY KEY (dim_signature_id),
	CONSTRAINT unique_dim_signature UNIQUE (dim_signature,dim_exec_name)

);
-- ddl-end --
ALTER TABLE eboa.dim_signature_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_ref_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_ref_tb CASCADE;
CREATE TABLE eboa.explicit_ref_tb(
	explicit_ref_id uuid NOT NULL,
	ingestion_time timestamp NOT NULL,
	explicit_ref text NOT NULL,
	expl_ref_cnf_id uuid,
	CONSTRAINT explicit_ref_tb_pk PRIMARY KEY (explicit_ref_id),
	CONSTRAINT unique_explicit_ref UNIQUE (explicit_ref)

);
-- ddl-end --
ALTER TABLE eboa.explicit_ref_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_text_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_text_tb CASCADE;
CREATE TABLE eboa.event_text_tb(
	name text NOT NULL,
	value text NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_text_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_double_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_double_tb CASCADE;
CREATE TABLE eboa.event_double_tb(
	name text NOT NULL,
	value double precision NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_double_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_object_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_object_tb CASCADE;
CREATE TABLE eboa.event_object_tb(
	name text NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_object_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_geometry_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_geometry_tb CASCADE;
CREATE TABLE eboa.event_geometry_tb(
	name text NOT NULL,
	value geometry NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_geometry_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_tb CASCADE;
CREATE TABLE eboa.annotation_tb(
	annotation_uuid uuid NOT NULL,
	ingestion_time timestamp NOT NULL,
	visible boolean NOT NULL,
	explicit_ref_id uuid NOT NULL,
	processing_uuid uuid NOT NULL,
	annotation_cnf_id uuid NOT NULL,
	CONSTRAINT annotation_tb_pk PRIMARY KEY (annotation_uuid),
	CONSTRAINT unique_annotation UNIQUE (annotation_uuid)

);
-- ddl-end --
ALTER TABLE eboa.annotation_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_text_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_text_tb CASCADE;
CREATE TABLE eboa.annotation_text_tb(
	name text NOT NULL,
	value text NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_text_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_double_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_double_tb CASCADE;
CREATE TABLE eboa.annotation_double_tb(
	name text NOT NULL,
	value double precision NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_double_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_object_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_object_tb CASCADE;
CREATE TABLE eboa.annotation_object_tb(
	name text NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_object_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_geometry_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_geometry_tb CASCADE;
CREATE TABLE eboa.annotation_geometry_tb(
	name text NOT NULL,
	value geometry NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_geometry_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_cnf_tb CASCADE;
CREATE TABLE eboa.annotation_cnf_tb(
	annotation_cnf_id uuid NOT NULL,
	name text NOT NULL,
	system text,
	dim_signature_id uuid NOT NULL,
	CONSTRAINT annotation_cnf_tb_pk PRIMARY KEY (annotation_cnf_id),
	CONSTRAINT unique_annotation_cnf UNIQUE (name,system)

);
-- ddl-end --
ALTER TABLE eboa.annotation_cnf_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_link_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_link_tb CASCADE;
CREATE TABLE eboa.event_link_tb(
	event_uuid_link uuid NOT NULL,
	name text NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_link_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_ref_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_ref_cnf_tb CASCADE;
CREATE TABLE eboa.explicit_ref_cnf_tb(
	expl_ref_cnf_id uuid NOT NULL,
	name text NOT NULL,
	CONSTRAINT explicit_ref_cnf_tb_pk PRIMARY KEY (expl_ref_cnf_id),
	CONSTRAINT unique_explicit_ref_group UNIQUE (name)

);
-- ddl-end --
ALTER TABLE eboa.explicit_ref_cnf_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_ref_link_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_ref_link_tb CASCADE;
CREATE TABLE eboa.explicit_ref_link_tb(
	explicit_ref_id_link uuid NOT NULL,
	name text NOT NULL,
	explicit_ref_id uuid NOT NULL,
	CONSTRAINT explicit_ref_links_tb_pk PRIMARY KEY (explicit_ref_id_link)

);
-- ddl-end --
ALTER TABLE eboa.explicit_ref_link_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.dim_processing_status_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.dim_processing_status_tb CASCADE;
CREATE TABLE eboa.dim_processing_status_tb(
	time_stamp timestamp NOT NULL,
	proc_status integer NOT NULL,
	log text,
	processing_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.dim_processing_status_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_boolean_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_boolean_tb CASCADE;
CREATE TABLE eboa.event_boolean_tb(
	name text NOT NULL,
	value boolean NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_boolean_tb OWNER TO eboa;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_boolean_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_boolean_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: eboa.annotation_boolean_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_boolean_tb CASCADE;
CREATE TABLE eboa.annotation_boolean_tb(
	name text NOT NULL,
	value boolean NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_boolean_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_timestamp_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_timestamp_tb CASCADE;
CREATE TABLE eboa.annotation_timestamp_tb(
	name text NOT NULL,
	value timestamp NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_timestamp_tb OWNER TO eboa;
-- ddl-end --

-- object: annotation_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_timestamp_tb DROP CONSTRAINT IF EXISTS annotation_tb_fk CASCADE;
ALTER TABLE eboa.annotation_timestamp_tb ADD CONSTRAINT annotation_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotation_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_boolean_tb DROP CONSTRAINT IF EXISTS annotation_tb_fk CASCADE;
ALTER TABLE eboa.annotation_boolean_tb ADD CONSTRAINT annotation_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotation_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.dim_processing_status_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE eboa.dim_processing_status_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid)
REFERENCES eboa.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE eboa.annotation_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id)
REFERENCES eboa.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: gauge_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_tb DROP CONSTRAINT IF EXISTS gauge_cnf_tb_fk CASCADE;
ALTER TABLE eboa.event_tb ADD CONSTRAINT gauge_cnf_tb_fk FOREIGN KEY (gauge_id)
REFERENCES eboa.gauge_cnf_tb (gauge_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE eboa.event_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid)
REFERENCES eboa.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: eboa.event_key_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_key_tb CASCADE;
CREATE TABLE eboa.event_key_tb(
	event_key text NOT NULL,
	visible boolean NOT NULL,
	event_uuid uuid NOT NULL,
	dim_signature_id uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_key_tb OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_timestamp_tb | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_timestamp_tb CASCADE;
CREATE TABLE eboa.event_timestamp_tb(
	name text NOT NULL,
	value timestamp NOT NULL,
	level_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_timestamp_tb OWNER TO eboa;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_timestamp_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_timestamp_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_double_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_double_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_text_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_text_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_object_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_object_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_geometry_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_geometry_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_text_tb DROP CONSTRAINT IF EXISTS annotation_tb_fk CASCADE;
ALTER TABLE eboa.annotation_text_tb ADD CONSTRAINT annotation_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotation_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_object_tb DROP CONSTRAINT IF EXISTS annotation_tb_fk CASCADE;
ALTER TABLE eboa.annotation_object_tb ADD CONSTRAINT annotation_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotation_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_double_tb DROP CONSTRAINT IF EXISTS annotation_tb_fk CASCADE;
ALTER TABLE eboa.annotation_double_tb ADD CONSTRAINT annotation_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotation_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_geometry_tb DROP CONSTRAINT IF EXISTS annotation_tb_fk CASCADE;
ALTER TABLE eboa.annotation_geometry_tb ADD CONSTRAINT annotation_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotation_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE eboa.event_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id)
REFERENCES eboa.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE eboa.annotation_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid)
REFERENCES eboa.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_tb DROP CONSTRAINT IF EXISTS annotation_cnf_tb_fk CASCADE;
ALTER TABLE eboa.annotation_tb ADD CONSTRAINT annotation_cnf_tb_fk FOREIGN KEY (annotation_cnf_id)
REFERENCES eboa.annotation_cnf_tb (annotation_cnf_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_link_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_link_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_tb DROP CONSTRAINT IF EXISTS explicit_ref_cnf_tb_fk CASCADE;
ALTER TABLE eboa.explicit_ref_tb ADD CONSTRAINT explicit_ref_cnf_tb_fk FOREIGN KEY (expl_ref_cnf_id)
REFERENCES eboa.explicit_ref_cnf_tb (expl_ref_cnf_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.dim_processing_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE eboa.dim_processing_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES eboa.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_cnf_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE eboa.annotation_cnf_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES eboa.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.gauge_cnf_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE eboa.gauge_cnf_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES eboa.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_link_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE eboa.explicit_ref_link_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id)
REFERENCES eboa.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: pk_events_tb | type: INDEX --
-- DROP INDEX IF EXISTS eboa.pk_events_tb CASCADE;
CREATE UNIQUE INDEX pk_events_tb ON eboa.event_tb
	USING btree
	(
	  event_uuid ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_visible | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_visible CASCADE;
CREATE INDEX idx_events_visible ON eboa.event_tb
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_events_start | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_start CASCADE;
CREATE INDEX idx_events_start ON eboa.event_tb
	USING btree
	(
	  start ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_stop | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_stop CASCADE;
CREATE INDEX idx_events_stop ON eboa.event_tb
	USING btree
	(
	  stop
	);
-- ddl-end --

-- object: idx_events_gauge_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_gauge_id CASCADE;
CREATE INDEX idx_events_gauge_id ON eboa.event_tb
	USING btree
	(
	  gauge_id
	);
-- ddl-end --

-- object: idx_events_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_ingestion_time CASCADE;
CREATE INDEX idx_events_ingestion_time ON eboa.event_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_events_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_explicit_ref_id CASCADE;
CREATE INDEX idx_events_explicit_ref_id ON eboa.event_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_event_boolean_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_boolean_event_uuid CASCADE;
CREATE INDEX idx_event_boolean_event_uuid ON eboa.event_boolean_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_boolean_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_boolean_value CASCADE;
CREATE INDEX idx_event_boolean_value ON eboa.event_boolean_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_boolean_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_boolean_name CASCADE;
CREATE INDEX idx_event_boolean_name ON eboa.event_boolean_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_text_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_text_event_uuid CASCADE;
CREATE INDEX idx_event_text_event_uuid ON eboa.event_text_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_text_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_text_value CASCADE;
CREATE INDEX idx_event_text_value ON eboa.event_text_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_text_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_text_name CASCADE;
CREATE INDEX idx_event_text_name ON eboa.event_text_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_double_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_double_event_uuid CASCADE;
CREATE INDEX idx_event_double_event_uuid ON eboa.event_double_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_double_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_double_value CASCADE;
CREATE INDEX idx_event_double_value ON eboa.event_double_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_double_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_double_name CASCADE;
CREATE INDEX idx_event_double_name ON eboa.event_double_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_timestamp_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_timestamp_event_uuid CASCADE;
CREATE INDEX idx_event_timestamp_event_uuid ON eboa.event_timestamp_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_timestamp_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_timestamp_value CASCADE;
CREATE INDEX idx_event_timestamp_value ON eboa.event_timestamp_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_timestamp_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_timestamp_name CASCADE;
CREATE INDEX idx_event_timestamp_name ON eboa.event_timestamp_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_object_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_object_event_uuid CASCADE;
CREATE INDEX idx_event_object_event_uuid ON eboa.event_object_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_object_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_object_name CASCADE;
CREATE INDEX idx_event_object_name ON eboa.event_object_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geometry_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_geometry_event_uuid CASCADE;
CREATE INDEX idx_event_geometry_event_uuid ON eboa.event_geometry_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_geometry_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_geometry_name CASCADE;
CREATE INDEX idx_event_geometry_name ON eboa.event_geometry_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geometry_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_geometry_value CASCADE;
CREATE INDEX idx_event_geometry_value ON eboa.event_geometry_tb
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_boolean_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_boolean_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_boolean_annotation_uuid ON eboa.annotation_boolean_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_boolean_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_boolean_value CASCADE;
CREATE INDEX idx_annotation_boolean_value ON eboa.annotation_boolean_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_boolean_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_boolean_name CASCADE;
CREATE INDEX idx_annotation_boolean_name ON eboa.annotation_boolean_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_timestamp_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_timestamp_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_timestamp_annotation_uuid ON eboa.annotation_timestamp_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_timestamp_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_timestamp_value CASCADE;
CREATE INDEX idx_annotation_timestamp_value ON eboa.annotation_timestamp_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_timestamp_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_timestamp_name CASCADE;
CREATE INDEX idx_annotation_timestamp_name ON eboa.annotation_timestamp_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_text_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_text_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_text_annotation_uuid ON eboa.annotation_text_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_text_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_text_value CASCADE;
CREATE INDEX idx_annotation_text_value ON eboa.annotation_text_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_text_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_text_name CASCADE;
CREATE INDEX idx_annotation_text_name ON eboa.annotation_text_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_double_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_double_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_double_annotation_uuid ON eboa.annotation_double_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_double_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_double_value CASCADE;
CREATE INDEX idx_annotation_double_value ON eboa.annotation_double_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_double_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_double_name CASCADE;
CREATE INDEX idx_annotation_double_name ON eboa.annotation_double_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_object_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_object_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_object_annotation_uuid ON eboa.annotation_object_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_object_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_object_name CASCADE;
CREATE INDEX idx_annotation_object_name ON eboa.annotation_object_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_geometry_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_geometry_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_geometry_annotation_uuid ON eboa.annotation_geometry_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_geometry_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_geometry_name CASCADE;
CREATE INDEX idx_annotation_geometry_name ON eboa.annotation_geometry_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_geometry_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_geometry_value CASCADE;
CREATE INDEX idx_annotation_geometry_value ON eboa.annotation_geometry_tb
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_gauge_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_gauge_cnf_system CASCADE;
CREATE INDEX idx_gauge_cnf_system ON eboa.gauge_cnf_tb
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_gauge_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_gauge_cnf_name CASCADE;
CREATE INDEX idx_gauge_cnf_name ON eboa.gauge_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_gauge_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_gauge_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_gauge_cnf_dim_signature_id ON eboa.gauge_cnf_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_event_links_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_links_name CASCADE;
CREATE INDEX idx_event_links_name ON eboa.event_link_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_links_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_links_event_uuid CASCADE;
CREATE INDEX idx_event_links_event_uuid ON eboa.event_link_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_dim_signature_dim_signature | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_dim_signature_dim_signature CASCADE;
CREATE INDEX idx_dim_signature_dim_signature ON eboa.dim_signature_tb
	USING btree
	(
	  dim_signature
	);
-- ddl-end --

-- object: idx_dim_signature_dim_exec_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_dim_signature_dim_exec_name CASCADE;
CREATE INDEX idx_dim_signature_dim_exec_name ON eboa.dim_signature_tb
	USING btree
	(
	  dim_exec_name
	);
-- ddl-end --

-- object: idx_annotation_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_cnf_name CASCADE;
CREATE INDEX idx_annotation_cnf_name ON eboa.annotation_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_cnf_system CASCADE;
CREATE INDEX idx_annotation_cnf_system ON eboa.annotation_cnf_tb
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_annotation_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_annotation_cnf_dim_signature_id ON eboa.annotation_cnf_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_annotation_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_ingestion_time CASCADE;
CREATE INDEX idx_annotation_ingestion_time ON eboa.annotation_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_annotation_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_explicit_ref_id CASCADE;
CREATE INDEX idx_annotation_explicit_ref_id ON eboa.annotation_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_annotation_visible | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_visible CASCADE;
CREATE INDEX idx_annotation_visible ON eboa.annotation_tb
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_annotation_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_processing_uuid CASCADE;
CREATE INDEX idx_annotation_processing_uuid ON eboa.annotation_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_explicit_ref_links_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_links_name CASCADE;
CREATE INDEX idx_explicit_ref_links_name ON eboa.explicit_ref_link_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_explicit_ref_links_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_links_explicit_ref_id CASCADE;
CREATE INDEX idx_explicit_ref_links_explicit_ref_id ON eboa.explicit_ref_link_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_explicit_ref_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_ingestion_time CASCADE;
CREATE INDEX idx_explicit_ref_ingestion_time ON eboa.explicit_ref_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_explicit_ref_explicit_ref | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_explicit_ref CASCADE;
CREATE INDEX idx_explicit_ref_explicit_ref ON eboa.explicit_ref_tb
	USING btree
	(
	  explicit_ref
	);
-- ddl-end --

-- object: idx_explicit_ref_expl_ref_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_expl_ref_cnf_id CASCADE;
CREATE INDEX idx_explicit_ref_expl_ref_cnf_id ON eboa.explicit_ref_tb
	USING btree
	(
	  expl_ref_cnf_id
	);
-- ddl-end --

-- object: idx_explicit_ref_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_cnf_name CASCADE;
CREATE INDEX idx_explicit_ref_cnf_name ON eboa.explicit_ref_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_processing_filename | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_filename CASCADE;
CREATE INDEX idx_processing_filename ON eboa.dim_processing_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_processing_validity_start | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_validity_start CASCADE;
CREATE INDEX idx_processing_validity_start ON eboa.dim_processing_tb
	USING btree
	(
	  validity_start
	);
-- ddl-end --

-- object: idx_processing_validity_stop | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_validity_stop CASCADE;
CREATE INDEX idx_processing_validity_stop ON eboa.dim_processing_tb
	USING btree
	(
	  validity_stop
	);
-- ddl-end --

-- object: idx_processing_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_generation_time CASCADE;
CREATE INDEX idx_processing_generation_time ON eboa.dim_processing_tb
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_ingestion_time CASCADE;
CREATE INDEX idx_processing_ingestion_time ON eboa.dim_processing_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_duration | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_ingestion_duration CASCADE;
CREATE INDEX idx_processing_ingestion_duration ON eboa.dim_processing_tb
	USING btree
	(
	  ingestion_duration
	);
-- ddl-end --

-- object: idx_processing_dim_exec_version | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_dim_exec_version CASCADE;
CREATE INDEX idx_processing_dim_exec_version ON eboa.dim_processing_tb
	USING btree
	(
	  dim_exec_version
	);
-- ddl-end --

-- object: idx_processing_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_dim_signature_id CASCADE;
CREATE INDEX idx_processing_dim_signature_id ON eboa.dim_processing_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_dim_processing_status_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_dim_processing_status_time_stamp CASCADE;
CREATE INDEX idx_dim_processing_status_time_stamp ON eboa.dim_processing_status_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_dim_processing_status_proc_status | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_dim_processing_status_proc_status CASCADE;
CREATE INDEX idx_dim_processing_status_proc_status ON eboa.dim_processing_status_tb
	USING btree
	(
	  proc_status
	);
-- ddl-end --

-- object: idx_dim_processing_status_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_dim_processing_status_processing_uuid CASCADE;
CREATE INDEX idx_dim_processing_status_processing_uuid ON eboa.dim_processing_status_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_event_keys_event_key | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_event_key CASCADE;
CREATE INDEX idx_event_keys_event_key ON eboa.event_key_tb
	USING btree
	(
	  event_key
	);
-- ddl-end --

-- object: unique_dim_processing | type: CONSTRAINT --
-- ALTER TABLE eboa.dim_processing_tb DROP CONSTRAINT IF EXISTS unique_dim_processing CASCADE;
ALTER TABLE eboa.dim_processing_tb ADD CONSTRAINT unique_dim_processing UNIQUE (name,dim_signature_id,dim_exec_version);
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_key_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE eboa.event_key_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: unique_event_keys | type: CONSTRAINT --
-- ALTER TABLE eboa.event_key_tb DROP CONSTRAINT IF EXISTS unique_event_keys CASCADE;
ALTER TABLE eboa.event_key_tb ADD CONSTRAINT unique_event_keys UNIQUE (event_key,event_uuid);
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_key_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE eboa.event_key_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES eboa.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: idx_event_keys_visible | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_visible CASCADE;
CREATE INDEX idx_event_keys_visible ON eboa.event_key_tb
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_event_keys_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_event_uuid CASCADE;
CREATE INDEX idx_event_keys_event_uuid ON eboa.event_key_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_keys_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_dim_signature_id CASCADE;
CREATE INDEX idx_event_keys_dim_signature_id ON eboa.event_key_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_events_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_processing_uuid CASCADE;
CREATE INDEX idx_events_processing_uuid ON eboa.event_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_annotation_annotation_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_annotation_cnf_id CASCADE;
CREATE INDEX idx_annotation_annotation_cnf_id ON eboa.annotation_tb
	USING btree
	(
	  annotation_cnf_id
	);
-- ddl-end --

-- object: unique_event_links | type: CONSTRAINT --
-- ALTER TABLE eboa.event_link_tb DROP CONSTRAINT IF EXISTS unique_event_links CASCADE;
ALTER TABLE eboa.event_link_tb ADD CONSTRAINT unique_event_links UNIQUE (event_uuid_link,name,event_uuid);
-- ddl-end --

-- object: unique_explicit_ref_links | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_link_tb DROP CONSTRAINT IF EXISTS unique_explicit_ref_links CASCADE;
ALTER TABLE eboa.explicit_ref_link_tb ADD CONSTRAINT unique_explicit_ref_links UNIQUE (explicit_ref_id_link,name,explicit_ref_id);
-- ddl-end --

