-- Database generated with pgModeler (PostgreSQL Database Modeler).
-- pgModeler  version: 0.9.0
-- PostgreSQL version: 9.6
-- Project Site: pgmodeler.com.br
-- Model Author: Daniel Brosnan Blazquez

-- object: gsdm | type: ROLE --
-- DROP ROLE IF EXISTS gsdm;
CREATE ROLE gsdm WITH 
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

-- object: gsdm | type: SCHEMA --
-- DROP SCHEMA IF EXISTS gsdm CASCADE;
CREATE SCHEMA gsdm;
-- ddl-end --
ALTER SCHEMA gsdm OWNER TO gsdm;
-- ddl-end --

SET search_path TO pg_catalog,public,gsdm;
-- ddl-end --

-- object: gsdm.event_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_tb CASCADE;
CREATE TABLE gsdm.event_tb(
	event_uuid uuid NOT NULL,
	start timestamp NOT NULL,
	stop timestamp NOT NULL,
	generation_time timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	visible boolean NOT NULL,
	gauge_id integer NOT NULL,
	explicit_ref_id integer,
	processing_uuid uuid,
	CONSTRAINT event_tb_pk PRIMARY KEY (event_uuid),
	CONSTRAINT unique_event UNIQUE (event_uuid)

);
-- ddl-end --
ALTER TABLE gsdm.event_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.gauge_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.gauge_cnf_tb CASCADE;
CREATE TABLE gsdm.gauge_cnf_tb(
	gauge_id serial NOT NULL,
	system text,
	name text NOT NULL,
	dim_signature_id integer NOT NULL,
	CONSTRAINT gauge_cnf_tb_pk PRIMARY KEY (gauge_id),
	CONSTRAINT unique_gauge_cnf UNIQUE (system,name)

);
-- ddl-end --
ALTER TABLE gsdm.gauge_cnf_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.dim_processing_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.dim_processing_tb CASCADE;
CREATE TABLE gsdm.dim_processing_tb(
	processing_uuid uuid NOT NULL,
	filename text NOT NULL,
	validity_start timestamp,
	validity_stop timestamp,
	generation_time timestamp NOT NULL,
	ingestion_time timestamp,
	ingestion_duration interval,
	dim_exec_version text NOT NULL,
	dim_signature_id integer NOT NULL,
	CONSTRAINT dim_processing_tb_pk PRIMARY KEY (processing_uuid)

);
-- ddl-end --
ALTER TABLE gsdm.dim_processing_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.dim_signature_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.dim_signature_tb CASCADE;
CREATE TABLE gsdm.dim_signature_tb(
	dim_signature_id serial NOT NULL,
	dim_signature text NOT NULL,
	dim_exec_name text NOT NULL,
	CONSTRAINT dim_signature_tb_pk PRIMARY KEY (dim_signature_id),
	CONSTRAINT unique_dim_signature UNIQUE (dim_signature,dim_exec_name)

);
-- ddl-end --
ALTER TABLE gsdm.dim_signature_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.explicit_ref_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.explicit_ref_tb CASCADE;
CREATE TABLE gsdm.explicit_ref_tb(
	explicit_ref_id serial NOT NULL,
	ingestion_time timestamp NOT NULL,
	explicit_ref text NOT NULL,
	expl_ref_cnf_id integer,
	CONSTRAINT explicit_ref_tb_pk PRIMARY KEY (explicit_ref_id),
	CONSTRAINT unique_explicit_ref UNIQUE (explicit_ref)

);
-- ddl-end --
ALTER TABLE gsdm.explicit_ref_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_text_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_text_tb CASCADE;
CREATE TABLE gsdm.event_text_tb(
	name text NOT NULL,
	value text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_text_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_double_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_double_tb CASCADE;
CREATE TABLE gsdm.event_double_tb(
	name text NOT NULL,
	value double precision NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_double_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_object_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_object_tb CASCADE;
CREATE TABLE gsdm.event_object_tb(
	name text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_object_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_geometry_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_geometry_tb CASCADE;
CREATE TABLE gsdm.event_geometry_tb(
	name text NOT NULL,
	value geometry NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_geometry_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_tb CASCADE;
CREATE TABLE gsdm.annot_tb(
	annotation_uuid uuid NOT NULL,
	generation_time timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	visible boolean NOT NULL,
	explicit_ref_id integer NOT NULL,
	processing_uuid uuid,
	annotation_cnf_id integer NOT NULL,
	CONSTRAINT annot_tb_pk PRIMARY KEY (annotation_uuid),
	CONSTRAINT unique_annotation UNIQUE (annotation_uuid)

);
-- ddl-end --
ALTER TABLE gsdm.annot_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_text_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_text_tb CASCADE;
CREATE TABLE gsdm.annot_text_tb(
	name text NOT NULL,
	value text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.annot_text_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_double_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_double_tb CASCADE;
CREATE TABLE gsdm.annot_double_tb(
	name text NOT NULL,
	value double precision NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.annot_double_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_object_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_object_tb CASCADE;
CREATE TABLE gsdm.annot_object_tb(
	name text NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.annot_object_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_geometry_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_geometry_tb CASCADE;
CREATE TABLE gsdm.annot_geometry_tb(
	name text NOT NULL,
	value geometry NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.annot_geometry_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_cnf_tb CASCADE;
CREATE TABLE gsdm.annot_cnf_tb(
	annotation_cnf_id serial NOT NULL,
	name text NOT NULL,
	system text,
	dim_signature_id integer NOT NULL,
	CONSTRAINT annot_cnf_tb_pk PRIMARY KEY (annotation_cnf_id),
	CONSTRAINT unique_annotation_cnf UNIQUE (name,system)

);
-- ddl-end --
ALTER TABLE gsdm.annot_cnf_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_links_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_links_tb CASCADE;
CREATE TABLE gsdm.event_links_tb(
	event_uuid_link uuid NOT NULL,
	name text NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_links_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.explicit_ref_cnf_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.explicit_ref_cnf_tb CASCADE;
CREATE TABLE gsdm.explicit_ref_cnf_tb(
	expl_ref_cnf_id serial NOT NULL,
	name text NOT NULL,
	CONSTRAINT explicit_ref_cnf_tb_pk PRIMARY KEY (expl_ref_cnf_id),
	CONSTRAINT unique_explicit_ref_group UNIQUE (name)

);
-- ddl-end --
ALTER TABLE gsdm.explicit_ref_cnf_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.explicit_ref_links_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.explicit_ref_links_tb CASCADE;
CREATE TABLE gsdm.explicit_ref_links_tb(
	explicit_ref_id_link integer NOT NULL,
	name text NOT NULL,
	explicit_ref_id integer NOT NULL,
	CONSTRAINT explicit_ref_links_tb_pk PRIMARY KEY (explicit_ref_id_link)

);
-- ddl-end --
ALTER TABLE gsdm.explicit_ref_links_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.dim_processing_status_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.dim_processing_status_tb CASCADE;
CREATE TABLE gsdm.dim_processing_status_tb(
	time_stamp timestamp NOT NULL,
	proc_status integer NOT NULL,
	processing_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.dim_processing_status_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_boolean_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_boolean_tb CASCADE;
CREATE TABLE gsdm.event_boolean_tb(
	name text NOT NULL,
	value boolean NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_boolean_tb OWNER TO gsdm;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_boolean_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_boolean_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: gsdm.annot_boolean_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_boolean_tb CASCADE;
CREATE TABLE gsdm.annot_boolean_tb(
	name text NOT NULL,
	value boolean NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.annot_boolean_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.annot_timestamp_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.annot_timestamp_tb CASCADE;
CREATE TABLE gsdm.annot_timestamp_tb(
	name text NOT NULL,
	value timestamp NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.annot_timestamp_tb OWNER TO gsdm;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_timestamp_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE gsdm.annot_timestamp_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES gsdm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_boolean_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE gsdm.annot_boolean_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES gsdm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.dim_processing_status_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE gsdm.dim_processing_status_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid)
REFERENCES gsdm.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE gsdm.annot_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id)
REFERENCES gsdm.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: gauge_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_tb DROP CONSTRAINT IF EXISTS gauge_cnf_tb_fk CASCADE;
ALTER TABLE gsdm.event_tb ADD CONSTRAINT gauge_cnf_tb_fk FOREIGN KEY (gauge_id)
REFERENCES gsdm.gauge_cnf_tb (gauge_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE gsdm.event_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid)
REFERENCES gsdm.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: gsdm.event_keys_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_keys_tb CASCADE;
CREATE TABLE gsdm.event_keys_tb(
	event_key text NOT NULL,
	generation_time timestamp NOT NULL,
	visible boolean NOT NULL,
	event_uuid uuid NOT NULL,
	dim_signature_id integer NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_keys_tb OWNER TO gsdm;
-- ddl-end --

-- object: gsdm.event_timestamp_tb | type: TABLE --
-- DROP TABLE IF EXISTS gsdm.event_timestamp_tb CASCADE;
CREATE TABLE gsdm.event_timestamp_tb(
	name text NOT NULL,
	value timestamp NOT NULL,
	level_position integer NOT NULL,
	child_position integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE gsdm.event_timestamp_tb OWNER TO gsdm;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_timestamp_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_timestamp_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_double_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_double_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_text_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_text_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_object_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_object_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_geometry_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_geometry_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_text_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE gsdm.annot_text_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES gsdm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_object_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE gsdm.annot_object_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES gsdm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_double_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE gsdm.annot_double_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES gsdm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_geometry_tb DROP CONSTRAINT IF EXISTS annot_tb_fk CASCADE;
ALTER TABLE gsdm.annot_geometry_tb ADD CONSTRAINT annot_tb_fk FOREIGN KEY (annotation_uuid)
REFERENCES gsdm.annot_tb (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE gsdm.event_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id)
REFERENCES gsdm.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_processing_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_tb DROP CONSTRAINT IF EXISTS dim_processing_tb_fk CASCADE;
ALTER TABLE gsdm.annot_tb ADD CONSTRAINT dim_processing_tb_fk FOREIGN KEY (processing_uuid)
REFERENCES gsdm.dim_processing_tb (processing_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annot_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_tb DROP CONSTRAINT IF EXISTS annot_cnf_tb_fk CASCADE;
ALTER TABLE gsdm.annot_tb ADD CONSTRAINT annot_cnf_tb_fk FOREIGN KEY (annotation_cnf_id)
REFERENCES gsdm.annot_cnf_tb (annotation_cnf_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_links_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_links_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_cnf_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.explicit_ref_tb DROP CONSTRAINT IF EXISTS explicit_ref_cnf_tb_fk CASCADE;
ALTER TABLE gsdm.explicit_ref_tb ADD CONSTRAINT explicit_ref_cnf_tb_fk FOREIGN KEY (expl_ref_cnf_id)
REFERENCES gsdm.explicit_ref_cnf_tb (expl_ref_cnf_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.dim_processing_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE gsdm.dim_processing_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES gsdm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.annot_cnf_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE gsdm.annot_cnf_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES gsdm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.gauge_cnf_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE gsdm.gauge_cnf_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES gsdm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.explicit_ref_links_tb DROP CONSTRAINT IF EXISTS explicit_ref_tb_fk CASCADE;
ALTER TABLE gsdm.explicit_ref_links_tb ADD CONSTRAINT explicit_ref_tb_fk FOREIGN KEY (explicit_ref_id)
REFERENCES gsdm.explicit_ref_tb (explicit_ref_id) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: pk_events_tb | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.pk_events_tb CASCADE;
CREATE UNIQUE INDEX pk_events_tb ON gsdm.event_tb
	USING btree
	(
	  event_uuid ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_visible | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_visible CASCADE;
CREATE INDEX idx_events_visible ON gsdm.event_tb
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_events_start | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_start CASCADE;
CREATE INDEX idx_events_start ON gsdm.event_tb
	USING btree
	(
	  start ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_stop | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_stop CASCADE;
CREATE INDEX idx_events_stop ON gsdm.event_tb
	USING btree
	(
	  stop
	);
-- ddl-end --

-- object: idx_events_gauge_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_gauge_id CASCADE;
CREATE INDEX idx_events_gauge_id ON gsdm.event_tb
	USING btree
	(
	  gauge_id
	);
-- ddl-end --

-- object: idx_events_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_generation_time CASCADE;
CREATE INDEX idx_events_generation_time ON gsdm.event_tb
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_events_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_ingestion_time CASCADE;
CREATE INDEX idx_events_ingestion_time ON gsdm.event_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_events_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_explicit_ref_id CASCADE;
CREATE INDEX idx_events_explicit_ref_id ON gsdm.event_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_event_boolean_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_boolean_event_uuid CASCADE;
CREATE INDEX idx_event_boolean_event_uuid ON gsdm.event_boolean_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_boolean_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_boolean_value CASCADE;
CREATE INDEX idx_event_boolean_value ON gsdm.event_boolean_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_boolean_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_boolean_name CASCADE;
CREATE INDEX idx_event_boolean_name ON gsdm.event_boolean_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_text_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_text_event_uuid CASCADE;
CREATE INDEX idx_event_text_event_uuid ON gsdm.event_text_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_text_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_text_value CASCADE;
CREATE INDEX idx_event_text_value ON gsdm.event_text_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_text_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_text_name CASCADE;
CREATE INDEX idx_event_text_name ON gsdm.event_text_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_double_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_double_event_uuid CASCADE;
CREATE INDEX idx_event_double_event_uuid ON gsdm.event_double_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_double_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_double_value CASCADE;
CREATE INDEX idx_event_double_value ON gsdm.event_double_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_double_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_double_name CASCADE;
CREATE INDEX idx_event_double_name ON gsdm.event_double_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_timestamp_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_timestamp_event_uuid CASCADE;
CREATE INDEX idx_event_timestamp_event_uuid ON gsdm.event_timestamp_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_timestamp_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_timestamp_value CASCADE;
CREATE INDEX idx_event_timestamp_value ON gsdm.event_timestamp_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_timestamp_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_timestamp_name CASCADE;
CREATE INDEX idx_event_timestamp_name ON gsdm.event_timestamp_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_object_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_object_event_uuid CASCADE;
CREATE INDEX idx_event_object_event_uuid ON gsdm.event_object_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_object_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_object_name CASCADE;
CREATE INDEX idx_event_object_name ON gsdm.event_object_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geometry_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_geometry_event_uuid CASCADE;
CREATE INDEX idx_event_geometry_event_uuid ON gsdm.event_geometry_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_geometry_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_geometry_name CASCADE;
CREATE INDEX idx_event_geometry_name ON gsdm.event_geometry_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geometry_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_geometry_value CASCADE;
CREATE INDEX idx_event_geometry_value ON gsdm.event_geometry_tb
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_boolean_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_boolean_annotation_uuid CASCADE;
CREATE INDEX idx_annot_boolean_annotation_uuid ON gsdm.annot_boolean_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_boolean_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_boolean_value CASCADE;
CREATE INDEX idx_annot_boolean_value ON gsdm.annot_boolean_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_boolean_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_boolean_name CASCADE;
CREATE INDEX idx_annot_boolean_name ON gsdm.annot_boolean_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_timestamp_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_timestamp_annotation_uuid CASCADE;
CREATE INDEX idx_annot_timestamp_annotation_uuid ON gsdm.annot_timestamp_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_timestamp_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_timestamp_value CASCADE;
CREATE INDEX idx_annot_timestamp_value ON gsdm.annot_timestamp_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_timestamp_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_timestamp_name CASCADE;
CREATE INDEX idx_annot_timestamp_name ON gsdm.annot_timestamp_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_text_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_text_annotation_uuid CASCADE;
CREATE INDEX idx_annot_text_annotation_uuid ON gsdm.annot_text_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_text_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_text_value CASCADE;
CREATE INDEX idx_annot_text_value ON gsdm.annot_text_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_text_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_text_name CASCADE;
CREATE INDEX idx_annot_text_name ON gsdm.annot_text_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_double_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_double_annotation_uuid CASCADE;
CREATE INDEX idx_annot_double_annotation_uuid ON gsdm.annot_double_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_double_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_double_value CASCADE;
CREATE INDEX idx_annot_double_value ON gsdm.annot_double_tb
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annot_double_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_double_name CASCADE;
CREATE INDEX idx_annot_double_name ON gsdm.annot_double_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_object_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_object_annotation_uuid CASCADE;
CREATE INDEX idx_annot_object_annotation_uuid ON gsdm.annot_object_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_object_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_object_name CASCADE;
CREATE INDEX idx_annot_object_name ON gsdm.annot_object_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_geometry_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_geometry_annotation_uuid CASCADE;
CREATE INDEX idx_annot_geometry_annotation_uuid ON gsdm.annot_geometry_tb
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annot_geometry_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_geometry_name CASCADE;
CREATE INDEX idx_annot_geometry_name ON gsdm.annot_geometry_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_geometry_value | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_geometry_value CASCADE;
CREATE INDEX idx_annot_geometry_value ON gsdm.annot_geometry_tb
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_gauge_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_gauge_cnf_system CASCADE;
CREATE INDEX idx_gauge_cnf_system ON gsdm.gauge_cnf_tb
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_gauge_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_gauge_cnf_name CASCADE;
CREATE INDEX idx_gauge_cnf_name ON gsdm.gauge_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_gauge_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_gauge_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_gauge_cnf_dim_signature_id ON gsdm.gauge_cnf_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_event_links_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_links_name CASCADE;
CREATE INDEX idx_event_links_name ON gsdm.event_links_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_links_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_links_event_uuid CASCADE;
CREATE INDEX idx_event_links_event_uuid ON gsdm.event_links_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_dim_signature_dim_signature | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_dim_signature_dim_signature CASCADE;
CREATE INDEX idx_dim_signature_dim_signature ON gsdm.dim_signature_tb
	USING btree
	(
	  dim_signature
	);
-- ddl-end --

-- object: idx_dim_signature_dim_exec_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_dim_signature_dim_exec_name CASCADE;
CREATE INDEX idx_dim_signature_dim_exec_name ON gsdm.dim_signature_tb
	USING btree
	(
	  dim_exec_name
	);
-- ddl-end --

-- object: idx_annot_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_cnf_name CASCADE;
CREATE INDEX idx_annot_cnf_name ON gsdm.annot_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annot_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_cnf_system CASCADE;
CREATE INDEX idx_annot_cnf_system ON gsdm.annot_cnf_tb
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_annot_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_annot_cnf_dim_signature_id ON gsdm.annot_cnf_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_annot_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_ingestion_time CASCADE;
CREATE INDEX idx_annot_ingestion_time ON gsdm.annot_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_annot_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_generation_time CASCADE;
CREATE INDEX idx_annot_generation_time ON gsdm.annot_tb
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_annot_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_explicit_ref_id CASCADE;
CREATE INDEX idx_annot_explicit_ref_id ON gsdm.annot_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_annot_visible | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_visible CASCADE;
CREATE INDEX idx_annot_visible ON gsdm.annot_tb
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_annot_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_processing_uuid CASCADE;
CREATE INDEX idx_annot_processing_uuid ON gsdm.annot_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_explicit_ref_links_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_explicit_ref_links_name CASCADE;
CREATE INDEX idx_explicit_ref_links_name ON gsdm.explicit_ref_links_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_explicit_ref_links_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_explicit_ref_links_explicit_ref_id CASCADE;
CREATE INDEX idx_explicit_ref_links_explicit_ref_id ON gsdm.explicit_ref_links_tb
	USING btree
	(
	  explicit_ref_id
	);
-- ddl-end --

-- object: idx_explicit_ref_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_explicit_ref_ingestion_time CASCADE;
CREATE INDEX idx_explicit_ref_ingestion_time ON gsdm.explicit_ref_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_explicit_ref_explicit_ref | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_explicit_ref_explicit_ref CASCADE;
CREATE INDEX idx_explicit_ref_explicit_ref ON gsdm.explicit_ref_tb
	USING btree
	(
	  explicit_ref
	);
-- ddl-end --

-- object: idx_explicit_ref_expl_ref_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_explicit_ref_expl_ref_cnf_id CASCADE;
CREATE INDEX idx_explicit_ref_expl_ref_cnf_id ON gsdm.explicit_ref_tb
	USING btree
	(
	  expl_ref_cnf_id
	);
-- ddl-end --

-- object: idx_explicit_ref_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_explicit_ref_cnf_name CASCADE;
CREATE INDEX idx_explicit_ref_cnf_name ON gsdm.explicit_ref_cnf_tb
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_processing_filename | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_filename CASCADE;
CREATE INDEX idx_processing_filename ON gsdm.dim_processing_tb
	USING btree
	(
	  filename
	);
-- ddl-end --

-- object: idx_processing_validity_start | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_validity_start CASCADE;
CREATE INDEX idx_processing_validity_start ON gsdm.dim_processing_tb
	USING btree
	(
	  validity_start
	);
-- ddl-end --

-- object: idx_processing_validity_stop | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_validity_stop CASCADE;
CREATE INDEX idx_processing_validity_stop ON gsdm.dim_processing_tb
	USING btree
	(
	  validity_stop
	);
-- ddl-end --

-- object: idx_processing_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_generation_time CASCADE;
CREATE INDEX idx_processing_generation_time ON gsdm.dim_processing_tb
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_ingestion_time CASCADE;
CREATE INDEX idx_processing_ingestion_time ON gsdm.dim_processing_tb
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_duration | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_ingestion_duration CASCADE;
CREATE INDEX idx_processing_ingestion_duration ON gsdm.dim_processing_tb
	USING btree
	(
	  ingestion_duration
	);
-- ddl-end --

-- object: idx_processing_dim_exec_version | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_dim_exec_version CASCADE;
CREATE INDEX idx_processing_dim_exec_version ON gsdm.dim_processing_tb
	USING btree
	(
	  dim_exec_version
	);
-- ddl-end --

-- object: idx_processing_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_processing_dim_signature_id CASCADE;
CREATE INDEX idx_processing_dim_signature_id ON gsdm.dim_processing_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_dim_processing_status_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_dim_processing_status_time_stamp CASCADE;
CREATE INDEX idx_dim_processing_status_time_stamp ON gsdm.dim_processing_status_tb
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_dim_processing_status_proc_status | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_dim_processing_status_proc_status CASCADE;
CREATE INDEX idx_dim_processing_status_proc_status ON gsdm.dim_processing_status_tb
	USING btree
	(
	  proc_status
	);
-- ddl-end --

-- object: idx_dim_processing_status_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_dim_processing_status_processing_uuid CASCADE;
CREATE INDEX idx_dim_processing_status_processing_uuid ON gsdm.dim_processing_status_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_event_keys_event_key | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_keys_event_key CASCADE;
CREATE INDEX idx_event_keys_event_key ON gsdm.event_keys_tb
	USING btree
	(
	  event_key
	);
-- ddl-end --

-- object: unique_dim_processing | type: CONSTRAINT --
-- ALTER TABLE gsdm.dim_processing_tb DROP CONSTRAINT IF EXISTS unique_dim_processing CASCADE;
ALTER TABLE gsdm.dim_processing_tb ADD CONSTRAINT unique_dim_processing UNIQUE (filename,dim_signature_id,dim_exec_version);
-- ddl-end --

-- object: event_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_keys_tb DROP CONSTRAINT IF EXISTS event_tb_fk CASCADE;
ALTER TABLE gsdm.event_keys_tb ADD CONSTRAINT event_tb_fk FOREIGN KEY (event_uuid)
REFERENCES gsdm.event_tb (event_uuid) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: unique_event_keys | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_keys_tb DROP CONSTRAINT IF EXISTS unique_event_keys CASCADE;
ALTER TABLE gsdm.event_keys_tb ADD CONSTRAINT unique_event_keys UNIQUE (event_key,event_uuid);
-- ddl-end --

-- object: dim_signature_tb_fk | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_keys_tb DROP CONSTRAINT IF EXISTS dim_signature_tb_fk CASCADE;
ALTER TABLE gsdm.event_keys_tb ADD CONSTRAINT dim_signature_tb_fk FOREIGN KEY (dim_signature_id)
REFERENCES gsdm.dim_signature_tb (dim_signature_id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: idx_event_keys_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_keys_generation_time CASCADE;
CREATE INDEX idx_event_keys_generation_time ON gsdm.event_keys_tb
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_event_keys_visible | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_keys_visible CASCADE;
CREATE INDEX idx_event_keys_visible ON gsdm.event_keys_tb
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_event_keys_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_keys_event_uuid CASCADE;
CREATE INDEX idx_event_keys_event_uuid ON gsdm.event_keys_tb
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_keys_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_event_keys_dim_signature_id CASCADE;
CREATE INDEX idx_event_keys_dim_signature_id ON gsdm.event_keys_tb
	USING btree
	(
	  dim_signature_id
	);
-- ddl-end --

-- object: idx_events_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_events_processing_uuid CASCADE;
CREATE INDEX idx_events_processing_uuid ON gsdm.event_tb
	USING btree
	(
	  processing_uuid
	);
-- ddl-end --

-- object: idx_annot_annotation_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS gsdm.idx_annot_annotation_cnf_id CASCADE;
CREATE INDEX idx_annot_annotation_cnf_id ON gsdm.annot_tb
	USING btree
	(
	  annotation_cnf_id
	);
-- ddl-end --

-- object: unique_event_links | type: CONSTRAINT --
-- ALTER TABLE gsdm.event_links_tb DROP CONSTRAINT IF EXISTS unique_event_links CASCADE;
ALTER TABLE gsdm.event_links_tb ADD CONSTRAINT unique_event_links UNIQUE (event_uuid_link,name,event_uuid);
-- ddl-end --

-- object: unique_explicit_ref_links | type: CONSTRAINT --
-- ALTER TABLE gsdm.explicit_ref_links_tb DROP CONSTRAINT IF EXISTS unique_explicit_ref_links CASCADE;
ALTER TABLE gsdm.explicit_ref_links_tb ADD CONSTRAINT unique_explicit_ref_links UNIQUE (explicit_ref_id_link,name,explicit_ref_id);
-- ddl-end --


