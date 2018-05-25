-- Database generated with pgModeler (PostgreSQL Database Modeler).
-- pgModeler  version: 0.9.0
-- PostgreSQL version: 9.6
-- Project Site: pgmodeler.com.br
-- Model Author: Daniel Brosnan Blazquez

-- object: e2espm | type: ROLE --
-- DROP ROLE IF EXISTS e2espm;
CREATE ROLE e2espm WITH 
	INHERIT
	LOGIN;
-- ddl-end --


-- Database creation must be done outside an multicommand file.
-- These commands were put in this file only for convenience.
-- -- object: gsdmdb | type: DATABASE --
-- -- DROP DATABASE IF EXISTS gsdmdb;
-- CREATE DATABASE gsdmdb
-- ;
-- -- ddl-end --
-- 

-- object: e2espm | type: SCHEMA --
-- DROP SCHEMA IF EXISTS e2espm CASCADE;
CREATE SCHEMA e2espm;
-- ddl-end --
ALTER SCHEMA e2espm OWNER TO e2espm;
-- ddl-end --

SET search_path TO pg_catalog,public,e2espm;
-- ddl-end --

-- object: e2espm.event_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_tb CASCADE;
CREATE TABLE e2espm.event_tb(
	event_uuid uuid NOT NULL,
	start timestamp NOT NULL,
	stop timestamp NOT NULL,
	time_stamp timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	gauge_id_gauge_cnf_tb integer NOT NULL,
	explicit_ref_id_explicit_ref_tb integer,
	processing_uuid_dim_processing_tb uuid,
	CONSTRAINT event_tb_pk PRIMARY KEY (event_uuid)

);
-- ddl-end --
ALTER TABLE e2espm.event_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.gauge_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.gauge_cnf_tb CASCADE;
CREATE TABLE e2espm.gauge_cnf_tb(
	gauge_id integer NOT NULL,
	system text,
	name text NOT NULL,
	description text,
	dim_signature_id_dim_signature_tb integer NOT NULL,
	CONSTRAINT gauge_cnf_tb_pk PRIMARY KEY (gauge_id)

);
-- ddl-end --
ALTER TABLE e2espm.gauge_cnf_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.dim_processing_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.dim_processing_tb CASCADE;
CREATE TABLE e2espm.dim_processing_tb(
	processing_uuid uuid NOT NULL,
	filename text NOT NULL,
	validity_start timestamp NOT NULL,
	validity_stop timestamp NOT NULL,
	generation_time timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	dim_exec_version text NOT NULL,
	dim_signature_id_dim_signature_tb integer NOT NULL,
	CONSTRAINT dim_processing_tb_pk PRIMARY KEY (processing_uuid)

);
-- ddl-end --
ALTER TABLE e2espm.dim_processing_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.dim_signature_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.dim_signature_tb CASCADE;
CREATE TABLE e2espm.dim_signature_tb(
	dim_signature_id serial NOT NULL,
	dim_signature text NOT NULL,
	dim_exec_name text NOT NULL,
	CONSTRAINT dim_signature_tb_pk PRIMARY KEY (dim_signature_id)

);
-- ddl-end --
ALTER TABLE e2espm.dim_signature_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.explicit_ref_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.explicit_ref_tb CASCADE;
CREATE TABLE e2espm.explicit_ref_tb(
	explicit_ref_id serial NOT NULL,
	time_stamp timestamp NOT NULL,
	explicit_ref text NOT NULL,
	expl_ref_cnf_id_explicit_ref_cnf_tb integer,
	CONSTRAINT explicit_ref_tb_pk PRIMARY KEY (explicit_ref_id)

);
-- ddl-end --
ALTER TABLE e2espm.explicit_ref_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.event_text_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_text_tb CASCADE;
CREATE TABLE e2espm.event_text_tb(
	name text NOT NULL,
	value text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid_event_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.event_text_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.event_double_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_double_tb CASCADE;
CREATE TABLE e2espm.event_double_tb(
	name text NOT NULL,
	value double precision NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid_event_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.event_double_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.event_object_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_object_tb CASCADE;
CREATE TABLE e2espm.event_object_tb(
	name text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid_event_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.event_object_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.event_geosegment_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_geosegment_tb CASCADE;
CREATE TABLE e2espm.event_geosegment_tb(
	name text NOT NULL,
	value polygon NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid_event_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.event_geosegment_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_tb CASCADE;
CREATE TABLE e2espm.annot_tb(
	annotation_uuid uuid NOT NULL,
	time_stamp timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	explicit_ref_id_explicit_ref_tb integer NOT NULL,
	processing_uuid_dim_processing_tb uuid,
	annotation_cnf_id_annot_cnf_tb integer NOT NULL,
	CONSTRAINT annot_tb_pk PRIMARY KEY (annotation_uuid)

);
-- ddl-end --
ALTER TABLE e2espm.annot_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_text_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_text_tb CASCADE;
CREATE TABLE e2espm.annot_text_tb(
	value text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	name text NOT NULL,
	annotation_uuid_annot_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.annot_text_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_double_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_double_tb CASCADE;
CREATE TABLE e2espm.annot_double_tb(
	value double precision NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	name text NOT NULL,
	annotation_uuid_annot_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.annot_double_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_object_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_object_tb CASCADE;
CREATE TABLE e2espm.annot_object_tb(
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	name text NOT NULL,
	annotation_uuid_annot_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.annot_object_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_geosegment_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_geosegment_tb CASCADE;
CREATE TABLE e2espm.annot_geosegment_tb(
	name text NOT NULL,
	value polygon NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid_annot_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.annot_geosegment_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_cnf_tb CASCADE;
CREATE TABLE e2espm.annot_cnf_tb(
	annotation_cnf_id serial NOT NULL,
	name text NOT NULL,
	group_id_annot_group_cnf_tb integer NOT NULL,
	dim_signature_id_dim_signature_tb integer NOT NULL,
	CONSTRAINT annot_cnf_tb_pk PRIMARY KEY (annotation_cnf_id)

);
-- ddl-end --
ALTER TABLE e2espm.annot_cnf_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.annot_group_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.annot_group_cnf_tb CASCADE;
CREATE TABLE e2espm.annot_group_cnf_tb(
	group_id serial NOT NULL,
	name text NOT NULL,
	CONSTRAINT annot_group_cnf_tb_pk PRIMARY KEY (group_id)

);
-- ddl-end --
ALTER TABLE e2espm.annot_group_cnf_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.event_links_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_links_tb CASCADE;
CREATE TABLE e2espm.event_links_tb(
	event_uuid_link uuid NOT NULL,
	name text NOT NULL,
	event_uuid_event_tb uuid NOT NULL,
	CONSTRAINT event_links_tb_pk PRIMARY KEY (event_uuid_link)

);
-- ddl-end --
ALTER TABLE e2espm.event_links_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.explicit_ref_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.explicit_ref_cnf_tb CASCADE;
CREATE TABLE e2espm.explicit_ref_cnf_tb(
	expl_ref_cnf_id serial NOT NULL,
	name text NOT NULL,
	CONSTRAINT explicit_ref_cnf_tb_pk PRIMARY KEY (expl_ref_cnf_id)

);
-- ddl-end --
ALTER TABLE e2espm.explicit_ref_cnf_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.explicit_ref_links_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.explicit_ref_links_tb CASCADE;
CREATE TABLE e2espm.explicit_ref_links_tb(
	explicit_ref_id_link integer NOT NULL,
	name text NOT NULL,
	explicit_ref_id_explicit_ref_tb integer NOT NULL,
	CONSTRAINT explicit_ref_links_tb_pk PRIMARY KEY (explicit_ref_id_link)

);
-- ddl-end --
ALTER TABLE e2espm.explicit_ref_links_tb OWNER TO e2espm;
-- ddl-end --

-- object: e2espm.dim_processing_status_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.dim_processing_status_tb CASCADE;
CREATE TABLE e2espm.dim_processing_status_tb(
	time_stamp timestamp NOT NULL,
	proc_status integer NOT NULL,
	processing_uuid_dim_processing_tb uuid NOT NULL
);
-- ddl-end --
ALTER TABLE e2espm.dim_processing_status_tb OWNER TO e2espm;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.dim_processing_status_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE e2espm.dim_processing_status_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid_dim_processing_tb)
REFERENCES e2espm.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE e2espm.annot_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id_explicit_ref_tb)
REFERENCES e2espm.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: gauge_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_tb DROP CONSTRAINT IF EXISTS gauge_cnf_tb_fk CASCADE;
ALTER TABLE e2espm.event_tb ADD CONSTRAINT gauge_cnf_tb_fk FOREIGN KEY (gauge_id_gauge_cnf_tb)
REFERENCES e2espm.gauge_cnf_tb (gauge_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE e2espm.event_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid_dim_processing_tb)
REFERENCES e2espm.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: e2espm.event_keys_tb | type: TABLE --
-- DROP TABLE IF EXISTS e2espm.event_keys_tb CASCADE;
CREATE TABLE e2espm.event_keys_tb(
	event_key text NOT NULL,
	time_stamp timestamp NOT NULL,
	event_uuid_event_tb uuid NOT NULL,
	CONSTRAINT event_keys_tb_pk PRIMARY KEY (event_key)

);
-- ddl-end --
ALTER TABLE e2espm.event_keys_tb OWNER TO e2espm;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_double_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE e2espm.event_double_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid_event_tb)
REFERENCES e2espm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_text_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE e2espm.event_text_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid_event_tb)
REFERENCES e2espm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_object_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE e2espm.event_object_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid_event_tb)
REFERENCES e2espm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_geosegment_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE e2espm.event_geosegment_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid_event_tb)
REFERENCES e2espm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_text_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE e2espm.annot_text_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid_annot_tb)
REFERENCES e2espm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_object_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE e2espm.annot_object_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid_annot_tb)
REFERENCES e2espm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_double_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE e2espm.annot_double_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid_annot_tb)
REFERENCES e2espm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_geosegment_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE e2espm.annot_geosegment_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid_annot_tb)
REFERENCES e2espm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE e2espm.event_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id_explicit_ref_tb)
REFERENCES e2espm.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE e2espm.annot_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid_dim_processing_tb)
REFERENCES e2espm.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_tb DROP CONSTRAINT IF EXISTS annot_cnf_tb_fk CASCADE;
ALTER TABLE e2espm.annot_tb ADD CONSTRAINT annot_cnf_tb_fk FOREIGN KEY (annotation_cnf_id_annot_cnf_tb)
REFERENCES e2espm.annot_cnf_tb (annotation_cnf_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_group_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_cnf_tb DROP CONSTRAINT IF EXISTS annot_group_cnf_tb_fk CASCADE;
ALTER TABLE e2espm.annot_cnf_tb ADD CONSTRAINT annot_group_cnf_tb_fk FOREIGN KEY (group_id_annot_group_cnf_tb)
REFERENCES e2espm.annot_group_cnf_tb (group_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_links_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE e2espm.event_links_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid_event_tb)
REFERENCES e2espm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.explicit_ref_tb DROP CONSTRAINT IF EXISTS explicit_ref_cnf_tb_fk CASCADE;
ALTER TABLE e2espm.explicit_ref_tb ADD CONSTRAINT explicit_ref_cnf_tb_fk FOREIGN KEY (expl_ref_cnf_id_explicit_ref_cnf_tb)
REFERENCES e2espm.explicit_ref_cnf_tb (expl_ref_cnf_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.dim_processing_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE e2espm.dim_processing_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id_dim_signature_tb)
REFERENCES e2espm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.annot_cnf_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE e2espm.annot_cnf_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id_dim_signature_tb)
REFERENCES e2espm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.gauge_cnf_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE e2espm.gauge_cnf_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id_dim_signature_tb)
REFERENCES e2espm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.explicit_ref_links_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE e2espm.explicit_ref_links_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id_explicit_ref_tb)
REFERENCES e2espm.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: pk_events_tb | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.pk_events_tb CASCADE;
CREATE UNIQUE INDEX pk_events_tb ON e2espm.event_tb
	USING btree
	(
	  event_uuid ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_gauge_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_events_gauge_id CASCADE;
CREATE INDEX idx_events_gauge_id ON e2espm.event_tb
	USING btree
	(
	  gauge_id_gauge_cnf_tb
	);
-- ddl-end --

-- object: idx_events_start | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_events_start CASCADE;
CREATE INDEX idx_events_start ON e2espm.event_tb
	USING btree
	(
	  start ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_stop | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_events_stop CASCADE;
CREATE INDEX idx_events_stop ON e2espm.event_tb
	USING btree
	(
	  stop
	);
-- ddl-end --

-- object: idx_events_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_events_explicit_ref_id CASCADE;
CREATE INDEX idx_events_explicit_ref_id ON e2espm.event_tb
	USING btree
	(
	  explicit_ref_id_explicit_ref_tb
	);
-- ddl-end --

-- object: idx_events_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_events_time_stamp CASCADE;
CREATE INDEX idx_events_time_stamp ON e2espm.event_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_events_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_events_processing_uuid CASCADE;
CREATE INDEX idx_events_processing_uuid ON e2espm.event_tb
	USING btree
	(
	  processing_uuid_dim_processing_tb
	);
-- ddl-end --

-- object: idx_event_text_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_text_event_uuid CASCADE;
CREATE INDEX idx_event_text_event_uuid ON e2espm.event_text_tb
	USING btree
	(
	  event_uuid_event_tb
	);
-- ddl-end --

-- object: idx_event_text_value | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_text_value CASCADE;
CREATE INDEX idx_event_text_value ON e2espm.event_text_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_text_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_text_name CASCADE;
CREATE INDEX idx_event_text_name ON e2espm.event_text_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_double_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_double_event_uuid CASCADE;
CREATE INDEX idx_event_double_event_uuid ON e2espm.event_double_tb
	USING btree
	(
	  event_uuid_event_tb
	);
-- ddl-end --

-- object: idx_event_double_value | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_double_value CASCADE;
CREATE INDEX idx_event_double_value ON e2espm.event_double_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_double_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_double_name CASCADE;
CREATE INDEX idx_event_double_name ON e2espm.event_double_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_object_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_object_event_uuid CASCADE;
CREATE INDEX idx_event_object_event_uuid ON e2espm.event_object_tb
	USING btree
	(
	  event_uuid_event_tb
	);
-- ddl-end --

-- object: idx_event_object_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_object_name CASCADE;
CREATE INDEX idx_event_object_name ON e2espm.event_object_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geosegment_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_geosegment_event_uuid CASCADE;
CREATE INDEX idx_event_geosegment_event_uuid ON e2espm.event_geosegment_tb
	USING btree
	(
	  event_uuid_event_tb
	);
-- ddl-end --

-- object: idx_event_geosegment_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_geosegment_name CASCADE;
CREATE INDEX idx_event_geosegment_name ON e2espm.event_geosegment_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geosegment_value | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_geosegment_value CASCADE;
CREATE INDEX idx_event_geosegment_value ON e2espm.event_geosegment_tb
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_text_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_text_annotation_uuid CASCADE;
CREATE INDEX idx_annot_text_annotation_uuid ON e2espm.annot_text_tb
	USING btree
	(
	  annotation_uuid_annot_tb
	);
-- ddl-end --

-- object: idx_annot_text_value | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_text_value CASCADE;
CREATE INDEX idx_annot_text_value ON e2espm.annot_text_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_text_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_text_name CASCADE;
CREATE INDEX idx_annot_text_name ON e2espm.annot_text_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_double_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_double_annotation_uuid CASCADE;
CREATE INDEX idx_annot_double_annotation_uuid ON e2espm.annot_double_tb
	USING btree
	(
	  annotation_uuid_annot_tb
	);
-- ddl-end --

-- object: idx_annot_double_value | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_double_value CASCADE;
CREATE INDEX idx_annot_double_value ON e2espm.annot_double_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_double_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_double_name CASCADE;
CREATE INDEX idx_annot_double_name ON e2espm.annot_double_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_object_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_object_annotation_uuid CASCADE;
CREATE INDEX idx_annot_object_annotation_uuid ON e2espm.annot_object_tb
	USING btree
	(
	  annotation_uuid_annot_tb
	);
-- ddl-end --

-- object: idx_annot_object_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_object_name CASCADE;
CREATE INDEX idx_annot_object_name ON e2espm.annot_object_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_geosegment_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_geosegment_annotation_uuid CASCADE;
CREATE INDEX idx_annot_geosegment_annotation_uuid ON e2espm.annot_geosegment_tb
	USING btree
	(
	  annotation_uuid_annot_tb
	);
-- ddl-end --

-- object: idx_annot_geosegment_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_geosegment_name CASCADE;
CREATE INDEX idx_annot_geosegment_name ON e2espm.annot_geosegment_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_geosegment_value | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_geosegment_value CASCADE;
CREATE INDEX idx_annot_geosegment_value ON e2espm.annot_geosegment_tb
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_gauge_cnf_gauge_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_gauge_cnf_gauge_id CASCADE;
CREATE INDEX idx_gauge_cnf_gauge_id ON e2espm.gauge_cnf_tb
	USING btree
	(
	  gauge_id
	);
-- ddl-end --

-- object: idx_gauge_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_gauge_cnf_system CASCADE;
CREATE INDEX idx_gauge_cnf_system ON e2espm.gauge_cnf_tb
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_gauge_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_gauge_cnf_name CASCADE;
CREATE INDEX idx_gauge_cnf_name ON e2espm.gauge_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_gauge_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_gauge_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_gauge_cnf_dim_signature_id ON e2espm.gauge_cnf_tb
	USING btree
	(
	  dim_signature_id_dim_signature_tb
	);
-- ddl-end --

-- object: idx_event_links_event_uuid_link | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_links_event_uuid_link CASCADE;
CREATE INDEX idx_event_links_event_uuid_link ON e2espm.event_links_tb
	USING btree
	(
	  event_uuid_link
	);
-- ddl-end --

-- object: idx_event_links_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_links_name CASCADE;
CREATE INDEX idx_event_links_name ON e2espm.event_links_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_links_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_links_event_uuid CASCADE;
CREATE INDEX idx_event_links_event_uuid ON e2espm.event_links_tb
	USING btree
	(
	  event_uuid_event_tb
	);
-- ddl-end --

-- object: idx_dim_signature_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_dim_signature_dim_signature_id CASCADE;
CREATE INDEX idx_dim_signature_dim_signature_id ON e2espm.dim_signature_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_dim_signature_dim_signature | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_dim_signature_dim_signature CASCADE;
CREATE INDEX idx_dim_signature_dim_signature ON e2espm.dim_signature_tb
	USING btree
	(
	  dim_signature
	);
-- ddl-end --

-- object: idx_dim_signature_dim_exec_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_dim_signature_dim_exec_name CASCADE;
CREATE INDEX idx_dim_signature_dim_exec_name ON e2espm.dim_signature_tb
	USING btree
	(
	  dim_exec_name
	);
-- ddl-end --

-- object: idx_annot_group_group_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_group_group_id CASCADE;
CREATE INDEX idx_annot_group_group_id ON e2espm.annot_group_cnf_tb
	USING btree
	(
	  group_id
	);
-- ddl-end --

-- object: idx_annot_group_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_group_name CASCADE;
CREATE INDEX idx_annot_group_name ON e2espm.annot_group_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_cnf_annotation_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_cnf_annotation_cnf_id CASCADE;
CREATE INDEX idx_annot_cnf_annotation_cnf_id ON e2espm.annot_cnf_tb
	USING btree
	(
	  annotation_cnf_id
	);
-- ddl-end --

-- object: idx_annot_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_cnf_name CASCADE;
CREATE INDEX idx_annot_cnf_name ON e2espm.annot_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_cnf_group_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_cnf_group_id CASCADE;
CREATE INDEX idx_annot_cnf_group_id ON e2espm.annot_cnf_tb
	USING btree
	(
	  group_id_annot_group_cnf_tb
	);
-- ddl-end --

-- object: idx_annot_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_annot_cnf_dim_signature_id ON e2espm.annot_cnf_tb
	USING btree
	(
	  dim_signature_id_dim_signature_tb
	);
-- ddl-end --

-- object: idx_annot_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_annotation_uuid CASCADE;
CREATE INDEX idx_annot_annotation_uuid ON e2espm.annot_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_time_stamp CASCADE;
CREATE INDEX idx_annot_time_stamp ON e2espm.annot_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_annot_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_explicit_ref_id CASCADE;
CREATE INDEX idx_annot_explicit_ref_id ON e2espm.annot_tb
	USING btree
	(
	  explicit_ref_id_explicit_ref_tb
	);
-- ddl-end --

-- object: idx_annot_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_processing_uuid CASCADE;
CREATE INDEX idx_annot_processing_uuid ON e2espm.annot_tb
	USING btree
	(
	  processing_uuid_dim_processing_tb
	);
-- ddl-end --

-- object: idx_annot_annotation_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_annot_annotation_cnf_id CASCADE;
CREATE INDEX idx_annot_annotation_cnf_id ON e2espm.annot_tb
	USING btree
	(
	  processing_uuid_dim_processing_tb
	);
-- ddl-end --

-- object: idx_explicit_ref_links_explicit_ref_id_link | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_links_explicit_ref_id_link CASCADE;
CREATE INDEX idx_explicit_ref_links_explicit_ref_id_link ON e2espm.explicit_ref_links_tb
	USING btree
	(
	  explicit_ref_id_link
	);
-- ddl-end --

-- object: idx_explicit_ref_links_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_links_name CASCADE;
CREATE INDEX idx_explicit_ref_links_name ON e2espm.explicit_ref_links_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_explicit_ref_links_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_links_explicit_ref_id CASCADE;
CREATE INDEX idx_explicit_ref_links_explicit_ref_id ON e2espm.explicit_ref_links_tb
	USING btree
	(
	  explicit_ref_id_explicit_ref_tb
	);
-- ddl-end --

-- object: idx_explicit_ref_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_explicit_ref_id CASCADE;
CREATE INDEX idx_explicit_ref_explicit_ref_id ON e2espm.explicit_ref_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_explicit_ref_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_time_stamp CASCADE;
CREATE INDEX idx_explicit_ref_time_stamp ON e2espm.explicit_ref_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_explicit_ref_explicit_ref | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_explicit_ref CASCADE;
CREATE INDEX idx_explicit_ref_explicit_ref ON e2espm.explicit_ref_tb
	USING btree
	(
	  explicit_ref
	);
-- ddl-end --

-- object: idx_explicit_ref_explicit_ref_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_explicit_ref_cnf_id CASCADE;
CREATE INDEX idx_explicit_ref_explicit_ref_cnf_id ON e2espm.explicit_ref_tb
	USING btree
	(
	  expl_ref_cnf_id_explicit_ref_cnf_tb
	);
-- ddl-end --

-- object: idx_explicit_ref_cnf_expl_ref_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_cnf_expl_ref_cnf_id CASCADE;
CREATE INDEX idx_explicit_ref_cnf_expl_ref_cnf_id ON e2espm.explicit_ref_cnf_tb
	USING btree
	(
	  expl_ref_cnf_id
	);
-- ddl-end --

-- object: idx_explicit_ref_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_explicit_ref_cnf_name CASCADE;
CREATE INDEX idx_explicit_ref_cnf_name ON e2espm.explicit_ref_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_processing_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_processing_uuid CASCADE;
CREATE INDEX idx_processing_processing_uuid ON e2espm.dim_processing_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_processing_filename | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_filename CASCADE;
CREATE INDEX idx_processing_filename ON e2espm.dim_processing_tb
	USING btree
	(
	  filename
	);
-- ddl-end --

-- object: idx_processing_validity_start | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_validity_start CASCADE;
CREATE INDEX idx_processing_validity_start ON e2espm.dim_processing_tb
	USING btree
	(
	  validity_start
	);
-- ddl-end --

-- object: idx_processing_validity_stop | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_validity_stop CASCADE;
CREATE INDEX idx_processing_validity_stop ON e2espm.dim_processing_tb
	USING btree
	(
	  validity_stop
	);
-- ddl-end --

-- object: idx_processing_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_generation_time CASCADE;
CREATE INDEX idx_processing_generation_time ON e2espm.dim_processing_tb
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_ingestion_time CASCADE;
CREATE INDEX idx_processing_ingestion_time ON e2espm.dim_processing_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_processing_dim_exec_version | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_dim_exec_version CASCADE;
CREATE INDEX idx_processing_dim_exec_version ON e2espm.dim_processing_tb
	USING btree
	(
	  dim_exec_version
	);
-- ddl-end --

-- object: idx_processing_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_processing_dim_signature_id CASCADE;
CREATE INDEX idx_processing_dim_signature_id ON e2espm.dim_processing_tb
	USING btree
	(
	  dim_signature_id_dim_signature_tb
	);
-- ddl-end --

-- object: idx_dim_processing_status_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_dim_processing_status_time_stamp CASCADE;
CREATE INDEX idx_dim_processing_status_time_stamp ON e2espm.dim_processing_status_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_dim_processing_status_proc_status | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_dim_processing_status_proc_status CASCADE;
CREATE INDEX idx_dim_processing_status_proc_status ON e2espm.dim_processing_status_tb
	USING btree
	(
	  proc_status
	);
-- ddl-end --

-- object: idx_dim_processing_status_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_dim_processing_status_processing_uuid CASCADE;
CREATE INDEX idx_dim_processing_status_processing_uuid ON e2espm.dim_processing_status_tb
	USING btree
	(
	  processing_uuid_dim_processing_tb
	);
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_keys_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE e2espm.event_keys_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid_event_tb)
REFERENCES e2espm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_keys_tb_uq | type: CONSTRAINT --
-- ALTER TABLE e2espm.event_keys_tb DROP CONSTRAINT IF EXISTS event_keys_tb_uq CASCADE;
ALTER TABLE e2espm.event_keys_tb ADD CONSTRAINT event_keys_tb_uq UNIQUE (event_uuid_event_tb);
-- ddl-end --

-- object: idx_event_keys_event_key | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_keys_event_key CASCADE;
CREATE INDEX idx_event_keys_event_key ON e2espm.event_keys_tb
	USING btree
	(
	  event_key
	);
-- ddl-end --

-- object: idx_event_keys_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_keys_time_stamp CASCADE;
CREATE INDEX idx_event_keys_time_stamp ON e2espm.event_keys_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_event_keys_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS e2espm.idx_event_keys_event_uuid CASCADE;
CREATE INDEX idx_event_keys_event_uuid ON e2espm.event_keys_tb
	USING btree
	(
	  event_uuid_event_tb
	);
-- ddl-end --


