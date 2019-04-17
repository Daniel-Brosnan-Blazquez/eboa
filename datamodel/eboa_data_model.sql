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

-- object: postgis | type: EXTENSION --
-- DROP EXTENSION IF EXISTS postgis CASCADE;
CREATE EXTENSION postgis
      WITH SCHEMA eboa;
-- ddl-end --

-- object: eboa.events | type: TABLE --
-- DROP TABLE IF EXISTS eboa.events CASCADE;
CREATE TABLE eboa.events(
	event_uuid uuid NOT NULL,
	start timestamp NOT NULL,
	stop timestamp NOT NULL,
	ingestion_time timestamp NOT NULL,
	visible boolean NOT NULL,
	gauge_uuid uuid NOT NULL,
	explicit_ref_uuid uuid,
	source_uuid uuid NOT NULL,
	CONSTRAINT events_pk PRIMARY KEY (event_uuid),
	CONSTRAINT unique_event UNIQUE (event_uuid)

);
-- ddl-end --
ALTER TABLE eboa.events OWNER TO eboa;
-- ddl-end --

-- object: eboa.gauges | type: TABLE --
-- DROP TABLE IF EXISTS eboa.gauges CASCADE;
CREATE TABLE eboa.gauges(
	gauge_uuid uuid NOT NULL,
	system text,
	name text NOT NULL,
	description text,
	dim_signature_uuid uuid NOT NULL,
	CONSTRAINT gauge_cnfs_pk PRIMARY KEY (gauge_uuid)

);
-- ddl-end --
ALTER TABLE eboa.gauges OWNER TO eboa;
-- ddl-end --

-- object: eboa.sources | type: TABLE --
-- DROP TABLE IF EXISTS eboa.sources CASCADE;
CREATE TABLE eboa.sources(
	source_uuid uuid NOT NULL,
	name text NOT NULL,
	validity_start timestamp,
	validity_stop timestamp,
	generation_time timestamp,
	ingestion_time timestamp,
	ingestion_duration interval,
	processor text,
	processor_version text,
	content_json json,
	content_text text,
	parse_error text,
	processor_progress decimal(5,2),
	ingestion_progress decimal(5,2),
	ingestion_completeness integer,
	dim_signature_uuid uuid,
	CONSTRAINT sources_pk PRIMARY KEY (source_uuid)

);
-- ddl-end --
ALTER TABLE eboa.sources OWNER TO eboa;
-- ddl-end --

-- object: eboa.dim_signatures | type: TABLE --
-- DROP TABLE IF EXISTS eboa.dim_signatures CASCADE;
CREATE TABLE eboa.dim_signatures(
	dim_signature_uuid uuid NOT NULL,
	dim_signature text NOT NULL,
	CONSTRAINT dim_signatures_pk PRIMARY KEY (dim_signature_uuid),
	CONSTRAINT unique_dim_signature UNIQUE (dim_signature)

);
-- ddl-end --
ALTER TABLE eboa.dim_signatures OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_refs | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_refs CASCADE;
CREATE TABLE eboa.explicit_refs(
	explicit_ref_uuid uuid NOT NULL,
	ingestion_time timestamp NOT NULL,
	explicit_ref text NOT NULL,
	expl_ref_cnf_uuid uuid,
	CONSTRAINT explicit_refs_pk PRIMARY KEY (explicit_ref_uuid),
	CONSTRAINT unique_explicit_ref UNIQUE (explicit_ref)

);
-- ddl-end --
ALTER TABLE eboa.explicit_refs OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_texts | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_texts CASCADE;
CREATE TABLE eboa.event_texts(
	name text NOT NULL,
	value text NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_texts OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_doubles | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_doubles CASCADE;
CREATE TABLE eboa.event_doubles(
	name text NOT NULL,
	value double precision NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_doubles OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_objects | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_objects CASCADE;
CREATE TABLE eboa.event_objects(
	name text NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_objects OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_geometries | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_geometries CASCADE;
CREATE TABLE eboa.event_geometries(
	name text NOT NULL,
	value geometry NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_geometries OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotations | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotations CASCADE;
CREATE TABLE eboa.annotations(
	annotation_uuid uuid NOT NULL,
	ingestion_time timestamp NOT NULL,
	visible boolean NOT NULL,
	explicit_ref_uuid uuid NOT NULL,
	source_uuid uuid NOT NULL,
	annotation_cnf_uuid uuid NOT NULL,
	CONSTRAINT annotations_pk PRIMARY KEY (annotation_uuid),
	CONSTRAINT unique_annotation UNIQUE (annotation_uuid)

);
-- ddl-end --
ALTER TABLE eboa.annotations OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_texts | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_texts CASCADE;
CREATE TABLE eboa.annotation_texts(
	name text NOT NULL,
	value text NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_texts OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_doubles | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_doubles CASCADE;
CREATE TABLE eboa.annotation_doubles(
	name text NOT NULL,
	value double precision NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_doubles OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_objects | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_objects CASCADE;
CREATE TABLE eboa.annotation_objects(
	name text NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_objects OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_geometries | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_geometries CASCADE;
CREATE TABLE eboa.annotation_geometries(
	name text NOT NULL,
	value geometry NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_geometries OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_cnfs | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_cnfs CASCADE;
CREATE TABLE eboa.annotation_cnfs(
	annotation_cnf_uuid uuid NOT NULL,
	name text NOT NULL,
	system text,
	description text,
	dim_signature_uuid uuid NOT NULL,
	CONSTRAINT annotation_cnfs_pk PRIMARY KEY (annotation_cnf_uuid)

);
-- ddl-end --
ALTER TABLE eboa.annotation_cnfs OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_links | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_links CASCADE;
CREATE TABLE eboa.event_links(
	event_uuid_link uuid NOT NULL,
	name text NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_links OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_ref_cnfs | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_ref_cnfs CASCADE;
CREATE TABLE eboa.explicit_ref_cnfs(
	expl_ref_cnf_uuid uuid NOT NULL,
	name text NOT NULL,
	CONSTRAINT explicit_ref_cnfs_pk PRIMARY KEY (expl_ref_cnf_uuid),
	CONSTRAINT unique_explicit_ref_group UNIQUE (name)

);
-- ddl-end --
ALTER TABLE eboa.explicit_ref_cnfs OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_ref_links | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_ref_links CASCADE;
CREATE TABLE eboa.explicit_ref_links(
	explicit_ref_uuid_link uuid NOT NULL,
	name text NOT NULL,
	explicit_ref_uuid uuid NOT NULL,
	CONSTRAINT explicit_ref_linkss_pk PRIMARY KEY (explicit_ref_uuid_link)

);
-- ddl-end --
ALTER TABLE eboa.explicit_ref_links OWNER TO eboa;
-- ddl-end --

-- object: eboa.source_statuses | type: TABLE --
-- DROP TABLE IF EXISTS eboa.source_statuses CASCADE;
CREATE TABLE eboa.source_statuses(
	time_stamp timestamp NOT NULL,
	status integer NOT NULL,
	log text,
	source_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.source_statuses OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_booleans | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_booleans CASCADE;
CREATE TABLE eboa.event_booleans(
	name text NOT NULL,
	value boolean NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_booleans OWNER TO eboa;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_booleans DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_booleans ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: eboa.annotation_booleans | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_booleans CASCADE;
CREATE TABLE eboa.annotation_booleans(
	name text NOT NULL,
	value boolean NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_booleans OWNER TO eboa;
-- ddl-end --

-- object: eboa.annotation_timestamps | type: TABLE --
-- DROP TABLE IF EXISTS eboa.annotation_timestamps CASCADE;
CREATE TABLE eboa.annotation_timestamps(
	name text NOT NULL,
	value timestamp NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	annotation_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.annotation_timestamps OWNER TO eboa;
-- ddl-end --

-- object: annotations_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_timestamps DROP CONSTRAINT IF EXISTS annotations_fk CASCADE;
ALTER TABLE eboa.annotation_timestamps ADD CONSTRAINT annotations_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotations (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotations_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_booleans DROP CONSTRAINT IF EXISTS annotations_fk CASCADE;
ALTER TABLE eboa.annotation_booleans ADD CONSTRAINT annotations_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotations (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: sources_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.source_statuses DROP CONSTRAINT IF EXISTS sources_fk CASCADE;
ALTER TABLE eboa.source_statuses ADD CONSTRAINT sources_fk FOREIGN KEY (source_uuid)
REFERENCES eboa.sources (source_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_refs_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotations DROP CONSTRAINT IF EXISTS explicit_refs_fk CASCADE;
ALTER TABLE eboa.annotations ADD CONSTRAINT explicit_refs_fk FOREIGN KEY (explicit_ref_uuid)
REFERENCES eboa.explicit_refs (explicit_ref_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: gauges_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.events DROP CONSTRAINT IF EXISTS gauges_fk CASCADE;
ALTER TABLE eboa.events ADD CONSTRAINT gauges_fk FOREIGN KEY (gauge_uuid)
REFERENCES eboa.gauges (gauge_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: sources_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.events DROP CONSTRAINT IF EXISTS sources_fk CASCADE;
ALTER TABLE eboa.events ADD CONSTRAINT sources_fk FOREIGN KEY (source_uuid)
REFERENCES eboa.sources (source_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: eboa.event_keys | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_keys CASCADE;
CREATE TABLE eboa.event_keys(
	event_key text NOT NULL,
	visible boolean NOT NULL,
	event_uuid uuid NOT NULL,
	dim_signature_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_keys OWNER TO eboa;
-- ddl-end --

-- object: eboa.event_timestamps | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_timestamps CASCADE;
CREATE TABLE eboa.event_timestamps(
	name text NOT NULL,
	value timestamp NOT NULL,
	"position" integer NOT NULL,
	parent_level integer NOT NULL,
	parent_position integer NOT NULL,
	event_uuid uuid NOT NULL
);
-- ddl-end --
ALTER TABLE eboa.event_timestamps OWNER TO eboa;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_timestamps DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_timestamps ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_doubles DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_doubles ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_texts DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_texts ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_objects DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_objects ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_geometries DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_geometries ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotations_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_texts DROP CONSTRAINT IF EXISTS annotations_fk CASCADE;
ALTER TABLE eboa.annotation_texts ADD CONSTRAINT annotations_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotations (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotations_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_objects DROP CONSTRAINT IF EXISTS annotations_fk CASCADE;
ALTER TABLE eboa.annotation_objects ADD CONSTRAINT annotations_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotations (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotations_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_doubles DROP CONSTRAINT IF EXISTS annotations_fk CASCADE;
ALTER TABLE eboa.annotation_doubles ADD CONSTRAINT annotations_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotations (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotations_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_geometries DROP CONSTRAINT IF EXISTS annotations_fk CASCADE;
ALTER TABLE eboa.annotation_geometries ADD CONSTRAINT annotations_fk FOREIGN KEY (annotation_uuid)
REFERENCES eboa.annotations (annotation_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_refs_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.events DROP CONSTRAINT IF EXISTS explicit_refs_fk CASCADE;
ALTER TABLE eboa.events ADD CONSTRAINT explicit_refs_fk FOREIGN KEY (explicit_ref_uuid)
REFERENCES eboa.explicit_refs (explicit_ref_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: sources_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotations DROP CONSTRAINT IF EXISTS sources_fk CASCADE;
ALTER TABLE eboa.annotations ADD CONSTRAINT sources_fk FOREIGN KEY (source_uuid)
REFERENCES eboa.sources (source_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: annotation_cnfs_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotations DROP CONSTRAINT IF EXISTS annotation_cnfs_fk CASCADE;
ALTER TABLE eboa.annotations ADD CONSTRAINT annotation_cnfs_fk FOREIGN KEY (annotation_cnf_uuid)
REFERENCES eboa.annotation_cnfs (annotation_cnf_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_links DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_links ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_ref_cnfs_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_refs DROP CONSTRAINT IF EXISTS explicit_ref_cnfs_fk CASCADE;
ALTER TABLE eboa.explicit_refs ADD CONSTRAINT explicit_ref_cnfs_fk FOREIGN KEY (expl_ref_cnf_uuid)
REFERENCES eboa.explicit_ref_cnfs (expl_ref_cnf_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signatures_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.sources DROP CONSTRAINT IF EXISTS dim_signatures_fk CASCADE;
ALTER TABLE eboa.sources ADD CONSTRAINT dim_signatures_fk FOREIGN KEY (dim_signature_uuid)
REFERENCES eboa.dim_signatures (dim_signature_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signatures_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_cnfs DROP CONSTRAINT IF EXISTS dim_signatures_fk CASCADE;
ALTER TABLE eboa.annotation_cnfs ADD CONSTRAINT dim_signatures_fk FOREIGN KEY (dim_signature_uuid)
REFERENCES eboa.dim_signatures (dim_signature_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: dim_signatures_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.gauges DROP CONSTRAINT IF EXISTS dim_signatures_fk CASCADE;
ALTER TABLE eboa.gauges ADD CONSTRAINT dim_signatures_fk FOREIGN KEY (dim_signature_uuid)
REFERENCES eboa.dim_signatures (dim_signature_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_refs_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_links DROP CONSTRAINT IF EXISTS explicit_refs_fk CASCADE;
ALTER TABLE eboa.explicit_ref_links ADD CONSTRAINT explicit_refs_fk FOREIGN KEY (explicit_ref_uuid)
REFERENCES eboa.explicit_refs (explicit_ref_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: pk_eventss | type: INDEX --
-- DROP INDEX IF EXISTS eboa.pk_eventss CASCADE;
CREATE UNIQUE INDEX pk_eventss ON eboa.events
	USING btree
	(
	  event_uuid ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_visible | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_visible CASCADE;
CREATE INDEX idx_events_visible ON eboa.events
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_events_start | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_start CASCADE;
CREATE INDEX idx_events_start ON eboa.events
	USING btree
	(
	  start ASC NULLS LAST
	);
-- ddl-end --

-- object: idx_events_stop | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_stop CASCADE;
CREATE INDEX idx_events_stop ON eboa.events
	USING btree
	(
	  stop
	);
-- ddl-end --

-- object: idx_events_gauge_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_gauge_id CASCADE;
CREATE INDEX idx_events_gauge_id ON eboa.events
	USING btree
	(
	  gauge_uuid
	);
-- ddl-end --

-- object: idx_events_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_ingestion_time CASCADE;
CREATE INDEX idx_events_ingestion_time ON eboa.events
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_events_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_explicit_ref_id CASCADE;
CREATE INDEX idx_events_explicit_ref_id ON eboa.events
	USING btree
	(
	  explicit_ref_uuid
	);
-- ddl-end --

-- object: idx_event_boolean_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_boolean_event_uuid CASCADE;
CREATE INDEX idx_event_boolean_event_uuid ON eboa.event_booleans
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_boolean_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_boolean_value CASCADE;
CREATE INDEX idx_event_boolean_value ON eboa.event_booleans
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_boolean_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_boolean_name CASCADE;
CREATE INDEX idx_event_boolean_name ON eboa.event_booleans
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_text_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_text_event_uuid CASCADE;
CREATE INDEX idx_event_text_event_uuid ON eboa.event_texts
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_text_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_text_value CASCADE;
CREATE INDEX idx_event_text_value ON eboa.event_texts
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_text_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_text_name CASCADE;
CREATE INDEX idx_event_text_name ON eboa.event_texts
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_double_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_double_event_uuid CASCADE;
CREATE INDEX idx_event_double_event_uuid ON eboa.event_doubles
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_double_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_double_value CASCADE;
CREATE INDEX idx_event_double_value ON eboa.event_doubles
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_double_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_double_name CASCADE;
CREATE INDEX idx_event_double_name ON eboa.event_doubles
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_timestamp_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_timestamp_event_uuid CASCADE;
CREATE INDEX idx_event_timestamp_event_uuid ON eboa.event_timestamps
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_timestamp_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_timestamp_value CASCADE;
CREATE INDEX idx_event_timestamp_value ON eboa.event_timestamps
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_event_timestamp_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_timestamp_name CASCADE;
CREATE INDEX idx_event_timestamp_name ON eboa.event_timestamps
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_object_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_object_event_uuid CASCADE;
CREATE INDEX idx_event_object_event_uuid ON eboa.event_objects
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_object_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_object_name CASCADE;
CREATE INDEX idx_event_object_name ON eboa.event_objects
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geometry_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_geometry_event_uuid CASCADE;
CREATE INDEX idx_event_geometry_event_uuid ON eboa.event_geometries
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_geometry_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_geometry_name CASCADE;
CREATE INDEX idx_event_geometry_name ON eboa.event_geometries
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_geometry_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_geometry_value CASCADE;
CREATE INDEX idx_event_geometry_value ON eboa.event_geometries
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_boolean_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_boolean_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_boolean_annotation_uuid ON eboa.annotation_booleans
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_boolean_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_boolean_value CASCADE;
CREATE INDEX idx_annotation_boolean_value ON eboa.annotation_booleans
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_boolean_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_boolean_name CASCADE;
CREATE INDEX idx_annotation_boolean_name ON eboa.annotation_booleans
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_timestamp_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_timestamp_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_timestamp_annotation_uuid ON eboa.annotation_timestamps
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_timestamp_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_timestamp_value CASCADE;
CREATE INDEX idx_annotation_timestamp_value ON eboa.annotation_timestamps
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_timestamp_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_timestamp_name CASCADE;
CREATE INDEX idx_annotation_timestamp_name ON eboa.annotation_timestamps
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_text_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_text_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_text_annotation_uuid ON eboa.annotation_texts
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_text_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_text_value CASCADE;
CREATE INDEX idx_annotation_text_value ON eboa.annotation_texts
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_text_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_text_name CASCADE;
CREATE INDEX idx_annotation_text_name ON eboa.annotation_texts
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_double_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_double_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_double_annotation_uuid ON eboa.annotation_doubles
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_double_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_double_value CASCADE;
CREATE INDEX idx_annotation_double_value ON eboa.annotation_doubles
	USING btree
	(
	  value
	);
-- ddl-end --

-- object: idx_annotation_double_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_double_name CASCADE;
CREATE INDEX idx_annotation_double_name ON eboa.annotation_doubles
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_object_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_object_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_object_annotation_uuid ON eboa.annotation_objects
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_object_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_object_name CASCADE;
CREATE INDEX idx_annotation_object_name ON eboa.annotation_objects
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_geometry_annotation_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_geometry_annotation_uuid CASCADE;
CREATE INDEX idx_annotation_geometry_annotation_uuid ON eboa.annotation_geometries
	USING btree
	(
	  annotation_uuid
	);
-- ddl-end --

-- object: idx_annotation_geometry_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_geometry_name CASCADE;
CREATE INDEX idx_annotation_geometry_name ON eboa.annotation_geometries
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_geometry_value | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_geometry_value CASCADE;
CREATE INDEX idx_annotation_geometry_value ON eboa.annotation_geometries
	USING gist
	(
	  value
	);
-- ddl-end --

-- object: idx_gauge_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_gauge_cnf_system CASCADE;
CREATE INDEX idx_gauge_cnf_system ON eboa.gauges
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_gauge_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_gauge_cnf_name CASCADE;
CREATE INDEX idx_gauge_cnf_name ON eboa.gauges
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_gauge_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_gauge_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_gauge_cnf_dim_signature_id ON eboa.gauges
	USING btree
	(
	  dim_signature_uuid
	);
-- ddl-end --

-- object: idx_event_links_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_links_name CASCADE;
CREATE INDEX idx_event_links_name ON eboa.event_links
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_event_links_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_links_event_uuid CASCADE;
CREATE INDEX idx_event_links_event_uuid ON eboa.event_links
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_dim_signature_dim_signature | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_dim_signature_dim_signature CASCADE;
CREATE INDEX idx_dim_signature_dim_signature ON eboa.dim_signatures
	USING btree
	(
	  dim_signature
	);
-- ddl-end --

-- object: idx_annotation_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_cnf_name CASCADE;
CREATE INDEX idx_annotation_cnf_name ON eboa.annotation_cnfs
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_annotation_cnf_system | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_cnf_system CASCADE;
CREATE INDEX idx_annotation_cnf_system ON eboa.annotation_cnfs
	USING btree
	(
	  system
	);
-- ddl-end --

-- object: idx_annotation_cnf_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_cnf_dim_signature_id CASCADE;
CREATE INDEX idx_annotation_cnf_dim_signature_id ON eboa.annotation_cnfs
	USING btree
	(
	  dim_signature_uuid
	);
-- ddl-end --

-- object: idx_annotation_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_ingestion_time CASCADE;
CREATE INDEX idx_annotation_ingestion_time ON eboa.annotations
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_annotation_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_explicit_ref_id CASCADE;
CREATE INDEX idx_annotation_explicit_ref_id ON eboa.annotations
	USING btree
	(
	  explicit_ref_uuid
	);
-- ddl-end --

-- object: idx_annotation_visible | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_visible CASCADE;
CREATE INDEX idx_annotation_visible ON eboa.annotations
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_annotation_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_processing_uuid CASCADE;
CREATE INDEX idx_annotation_processing_uuid ON eboa.annotations
	USING btree
	(
	  source_uuid
	);
-- ddl-end --

-- object: idx_explicit_ref_links_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_links_name CASCADE;
CREATE INDEX idx_explicit_ref_links_name ON eboa.explicit_ref_links
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_explicit_ref_links_explicit_ref_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_links_explicit_ref_id CASCADE;
CREATE INDEX idx_explicit_ref_links_explicit_ref_id ON eboa.explicit_ref_links
	USING btree
	(
	  explicit_ref_uuid
	);
-- ddl-end --

-- object: idx_explicit_ref_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_ingestion_time CASCADE;
CREATE INDEX idx_explicit_ref_ingestion_time ON eboa.explicit_refs
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_explicit_ref_explicit_ref | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_explicit_ref CASCADE;
CREATE INDEX idx_explicit_ref_explicit_ref ON eboa.explicit_refs
	USING btree
	(
	  explicit_ref
	);
-- ddl-end --

-- object: idx_explicit_ref_expl_ref_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_expl_ref_cnf_id CASCADE;
CREATE INDEX idx_explicit_ref_expl_ref_cnf_id ON eboa.explicit_refs
	USING btree
	(
	  expl_ref_cnf_uuid
	);
-- ddl-end --

-- object: idx_explicit_ref_cnf_name | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_explicit_ref_cnf_name CASCADE;
CREATE INDEX idx_explicit_ref_cnf_name ON eboa.explicit_ref_cnfs
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_processing_filename | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_filename CASCADE;
CREATE INDEX idx_processing_filename ON eboa.sources
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_processing_validity_start | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_validity_start CASCADE;
CREATE INDEX idx_processing_validity_start ON eboa.sources
	USING btree
	(
	  validity_start
	);
-- ddl-end --

-- object: idx_processing_validity_stop | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_validity_stop CASCADE;
CREATE INDEX idx_processing_validity_stop ON eboa.sources
	USING btree
	(
	  validity_stop
	);
-- ddl-end --

-- object: idx_processing_generation_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_generation_time CASCADE;
CREATE INDEX idx_processing_generation_time ON eboa.sources
	USING btree
	(
	  generation_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_time | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_ingestion_time CASCADE;
CREATE INDEX idx_processing_ingestion_time ON eboa.sources
	USING btree
	(
	  ingestion_time
	);
-- ddl-end --

-- object: idx_processing_ingestion_duration | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_ingestion_duration CASCADE;
CREATE INDEX idx_processing_ingestion_duration ON eboa.sources
	USING btree
	(
	  ingestion_duration
	);
-- ddl-end --

-- object: idx_processing_dim_exec_version | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_dim_exec_version CASCADE;
CREATE INDEX idx_processing_dim_exec_version ON eboa.sources
	USING btree
	(
	  processor_version
	);
-- ddl-end --

-- object: idx_processing_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_processing_dim_signature_id CASCADE;
CREATE INDEX idx_processing_dim_signature_id ON eboa.sources
	USING btree
	(
	  dim_signature_uuid
	);
-- ddl-end --

-- object: idx_source_status_time_stamp | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_source_status_time_stamp CASCADE;
CREATE INDEX idx_source_status_time_stamp ON eboa.source_statuses
	USING btree
	(
	  time_stamp
	);
-- ddl-end --

-- object: idx_source_status_proc_status | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_source_status_proc_status CASCADE;
CREATE INDEX idx_source_status_proc_status ON eboa.source_statuses
	USING btree
	(
	  status
	);
-- ddl-end --

-- object: idx_source_status_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_source_status_processing_uuid CASCADE;
CREATE INDEX idx_source_status_processing_uuid ON eboa.source_statuses
	USING btree
	(
	  source_uuid
	);
-- ddl-end --

-- object: idx_event_keys_event_key | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_event_key CASCADE;
CREATE INDEX idx_event_keys_event_key ON eboa.event_keys
	USING btree
	(
	  event_key
	);
-- ddl-end --

-- object: unique_source | type: CONSTRAINT --
-- ALTER TABLE eboa.sources DROP CONSTRAINT IF EXISTS unique_source CASCADE;
ALTER TABLE eboa.sources ADD CONSTRAINT unique_source UNIQUE (name,processor_version,dim_signature_uuid,processor);
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_keys DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_keys ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: unique_event_keys | type: CONSTRAINT --
-- ALTER TABLE eboa.event_keys DROP CONSTRAINT IF EXISTS unique_event_keys CASCADE;
ALTER TABLE eboa.event_keys ADD CONSTRAINT unique_event_keys UNIQUE (event_key,event_uuid);
-- ddl-end --

-- object: dim_signatures_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_keys DROP CONSTRAINT IF EXISTS dim_signatures_fk CASCADE;
ALTER TABLE eboa.event_keys ADD CONSTRAINT dim_signatures_fk FOREIGN KEY (dim_signature_uuid)
REFERENCES eboa.dim_signatures (dim_signature_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: idx_event_keys_visible | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_visible CASCADE;
CREATE INDEX idx_event_keys_visible ON eboa.event_keys
	USING btree
	(
	  visible
	);
-- ddl-end --

-- object: idx_event_keys_event_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_event_uuid CASCADE;
CREATE INDEX idx_event_keys_event_uuid ON eboa.event_keys
	USING btree
	(
	  event_uuid
	);
-- ddl-end --

-- object: idx_event_keys_dim_signature_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_event_keys_dim_signature_id CASCADE;
CREATE INDEX idx_event_keys_dim_signature_id ON eboa.event_keys
	USING btree
	(
	  dim_signature_uuid
	);
-- ddl-end --

-- object: idx_events_processing_uuid | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_events_processing_uuid CASCADE;
CREATE INDEX idx_events_processing_uuid ON eboa.events
	USING btree
	(
	  source_uuid
	);
-- ddl-end --

-- object: idx_annotation_annotation_cnf_id | type: INDEX --
-- DROP INDEX IF EXISTS eboa.idx_annotation_annotation_cnf_id CASCADE;
CREATE INDEX idx_annotation_annotation_cnf_id ON eboa.annotations
	USING btree
	(
	  annotation_cnf_uuid
	);
-- ddl-end --

-- object: unique_event_links | type: CONSTRAINT --
-- ALTER TABLE eboa.event_links DROP CONSTRAINT IF EXISTS unique_event_links CASCADE;
ALTER TABLE eboa.event_links ADD CONSTRAINT unique_event_links UNIQUE (event_uuid_link,name,event_uuid);
-- ddl-end --

-- object: unique_explicit_ref_links | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_links DROP CONSTRAINT IF EXISTS unique_explicit_ref_links CASCADE;
ALTER TABLE eboa.explicit_ref_links ADD CONSTRAINT unique_explicit_ref_links UNIQUE (explicit_ref_uuid_link,name,explicit_ref_uuid);
-- ddl-end --

-- object: unique_gauge_cnf | type: CONSTRAINT --
-- ALTER TABLE eboa.gauges DROP CONSTRAINT IF EXISTS unique_gauge_cnf CASCADE;
ALTER TABLE eboa.gauges ADD CONSTRAINT unique_gauge_cnf UNIQUE (system,name,dim_signature_uuid);
-- ddl-end --

-- object: unique_annotation_cnf | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_cnfs DROP CONSTRAINT IF EXISTS unique_annotation_cnf CASCADE;
ALTER TABLE eboa.annotation_cnfs ADD CONSTRAINT unique_annotation_cnf UNIQUE (system,name,dim_signature_uuid);
-- ddl-end --

-- object: unique_value_event_booleans | type: CONSTRAINT --
-- ALTER TABLE eboa.event_booleans DROP CONSTRAINT IF EXISTS unique_value_event_booleans CASCADE;
ALTER TABLE eboa.event_booleans ADD CONSTRAINT unique_value_event_booleans UNIQUE (name,parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_event_texts | type: CONSTRAINT --
-- ALTER TABLE eboa.event_texts DROP CONSTRAINT IF EXISTS unique_value_event_texts CASCADE;
ALTER TABLE eboa.event_texts ADD CONSTRAINT unique_value_event_texts UNIQUE (name,parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_event_doubles | type: CONSTRAINT --
-- ALTER TABLE eboa.event_doubles DROP CONSTRAINT IF EXISTS unique_value_event_doubles CASCADE;
ALTER TABLE eboa.event_doubles ADD CONSTRAINT unique_value_event_doubles UNIQUE (name,parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_event_timestamps | type: CONSTRAINT --
-- ALTER TABLE eboa.event_timestamps DROP CONSTRAINT IF EXISTS unique_value_event_timestamps CASCADE;
ALTER TABLE eboa.event_timestamps ADD CONSTRAINT unique_value_event_timestamps UNIQUE (name,parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_event_objects | type: CONSTRAINT --
-- ALTER TABLE eboa.event_objects DROP CONSTRAINT IF EXISTS unique_value_event_objects CASCADE;
ALTER TABLE eboa.event_objects ADD CONSTRAINT unique_value_event_objects UNIQUE (name,parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_event_geometries | type: CONSTRAINT --
-- ALTER TABLE eboa.event_geometries DROP CONSTRAINT IF EXISTS unique_value_event_geometries CASCADE;
ALTER TABLE eboa.event_geometries ADD CONSTRAINT unique_value_event_geometries UNIQUE (name,parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_annotation_booleans | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_booleans DROP CONSTRAINT IF EXISTS unique_value_annotation_booleans CASCADE;
ALTER TABLE eboa.annotation_booleans ADD CONSTRAINT unique_value_annotation_booleans UNIQUE (name,parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_annotation_texts | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_texts DROP CONSTRAINT IF EXISTS unique_value_annotation_texts CASCADE;
ALTER TABLE eboa.annotation_texts ADD CONSTRAINT unique_value_annotation_texts UNIQUE (name,parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_annotation_doubles | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_doubles DROP CONSTRAINT IF EXISTS unique_value_annotation_doubles CASCADE;
ALTER TABLE eboa.annotation_doubles ADD CONSTRAINT unique_value_annotation_doubles UNIQUE (name,parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_annotation_timestamps | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_timestamps DROP CONSTRAINT IF EXISTS unique_value_annotation_timestamps CASCADE;
ALTER TABLE eboa.annotation_timestamps ADD CONSTRAINT unique_value_annotation_timestamps UNIQUE (name,parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_annotation_objects | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_objects DROP CONSTRAINT IF EXISTS unique_value_annotation_objects CASCADE;
ALTER TABLE eboa.annotation_objects ADD CONSTRAINT unique_value_annotation_objects UNIQUE (name,parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_annotation_geometries | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_geometries DROP CONSTRAINT IF EXISTS unique_value_annotation_geometries CASCADE;
ALTER TABLE eboa.annotation_geometries ADD CONSTRAINT unique_value_annotation_geometries UNIQUE (name,parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_position_event_booleans | type: CONSTRAINT --
-- ALTER TABLE eboa.event_booleans DROP CONSTRAINT IF EXISTS unique_value_position_event_booleans CASCADE;
ALTER TABLE eboa.event_booleans ADD CONSTRAINT unique_value_position_event_booleans UNIQUE ("position",parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_position_event_texts | type: CONSTRAINT --
-- ALTER TABLE eboa.event_texts DROP CONSTRAINT IF EXISTS unique_value_position_event_texts CASCADE;
ALTER TABLE eboa.event_texts ADD CONSTRAINT unique_value_position_event_texts UNIQUE ("position",parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_position_event_doubles | type: CONSTRAINT --
-- ALTER TABLE eboa.event_doubles DROP CONSTRAINT IF EXISTS unique_value_position_event_doubles CASCADE;
ALTER TABLE eboa.event_doubles ADD CONSTRAINT unique_value_position_event_doubles UNIQUE ("position",parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_position_event_timestamps | type: CONSTRAINT --
-- ALTER TABLE eboa.event_timestamps DROP CONSTRAINT IF EXISTS unique_value_position_event_timestamps CASCADE;
ALTER TABLE eboa.event_timestamps ADD CONSTRAINT unique_value_position_event_timestamps UNIQUE ("position",parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_position_event_objects | type: CONSTRAINT --
-- ALTER TABLE eboa.event_objects DROP CONSTRAINT IF EXISTS unique_value_position_event_objects CASCADE;
ALTER TABLE eboa.event_objects ADD CONSTRAINT unique_value_position_event_objects UNIQUE ("position",parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_position_event_geometries | type: CONSTRAINT --
-- ALTER TABLE eboa.event_geometries DROP CONSTRAINT IF EXISTS unique_value_position_event_geometries CASCADE;
ALTER TABLE eboa.event_geometries ADD CONSTRAINT unique_value_position_event_geometries UNIQUE ("position",parent_level,parent_position,event_uuid);
-- ddl-end --

-- object: unique_value_position_annotation_booleans | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_booleans DROP CONSTRAINT IF EXISTS unique_value_position_annotation_booleans CASCADE;
ALTER TABLE eboa.annotation_booleans ADD CONSTRAINT unique_value_position_annotation_booleans UNIQUE ("position",parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_position_annotation_texts | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_texts DROP CONSTRAINT IF EXISTS unique_value_position_annotation_texts CASCADE;
ALTER TABLE eboa.annotation_texts ADD CONSTRAINT unique_value_position_annotation_texts UNIQUE ("position",parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_position_annotation_doubles | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_doubles DROP CONSTRAINT IF EXISTS unique_value_position_annotation_doubles CASCADE;
ALTER TABLE eboa.annotation_doubles ADD CONSTRAINT unique_value_position_annotation_doubles UNIQUE ("position",parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_position_annotation_timestamps | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_timestamps DROP CONSTRAINT IF EXISTS unique_value_position_annotation_timestamps CASCADE;
ALTER TABLE eboa.annotation_timestamps ADD CONSTRAINT unique_value_position_annotation_timestamps UNIQUE ("position",parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_position_annotation_objects | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_objects DROP CONSTRAINT IF EXISTS unique_value_position_annotation_objects CASCADE;
ALTER TABLE eboa.annotation_objects ADD CONSTRAINT unique_value_position_annotation_objects UNIQUE ("position",parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: unique_value_position_annotation_geometries | type: CONSTRAINT --
-- ALTER TABLE eboa.annotation_geometries DROP CONSTRAINT IF EXISTS unique_value_position_annotation_geometries CASCADE;
ALTER TABLE eboa.annotation_geometries ADD CONSTRAINT unique_value_position_annotation_geometries UNIQUE ("position",parent_level,parent_position,annotation_uuid);
-- ddl-end --

-- object: eboa.event_alerts | type: TABLE --
-- DROP TABLE IF EXISTS eboa.event_alerts CASCADE;
CREATE TABLE eboa.event_alerts(
	message text NOT NULL,
	validated boolean,
	ingestion_time timestamp NOT NULL,
	generator text,
	notified boolean NOT NULL,
	alert_uuid uuid,
	event_uuid uuid
);
-- ddl-end --
ALTER TABLE eboa.event_alerts OWNER TO eboa;
-- ddl-end --

-- object: eboa.alerts | type: TABLE --
-- DROP TABLE IF EXISTS eboa.alerts CASCADE;
CREATE TABLE eboa.alerts(
	alert_uuid uuid NOT NULL,
	name text NOT NULL,
	severity integer NOT NULL,
	description text,
	CONSTRAINT unique_alerts UNIQUE (name),
	CONSTRAINT alerts_pk PRIMARY KEY (alert_uuid)

);
-- ddl-end --
ALTER TABLE eboa.alerts OWNER TO eboa;
-- ddl-end --

-- object: eboa.source_alerts | type: TABLE --
-- DROP TABLE IF EXISTS eboa.source_alerts CASCADE;
CREATE TABLE eboa.source_alerts(
	message text NOT NULL,
	validated boolean,
	ingestion_time timestamp NOT NULL,
	generator text,
	notified boolean NOT NULL,
	alert_uuid uuid,
	source_uuid uuid
);
-- ddl-end --
ALTER TABLE eboa.source_alerts OWNER TO eboa;
-- ddl-end --

-- object: eboa.explicit_ref_alerts | type: TABLE --
-- DROP TABLE IF EXISTS eboa.explicit_ref_alerts CASCADE;
CREATE TABLE eboa.explicit_ref_alerts(
	message text NOT NULL,
	validated boolean,
	ingestion_time timestamp NOT NULL,
	generator text,
	notified boolean NOT NULL,
	alert_uuid uuid,
	explicit_ref_uuid uuid
);
-- ddl-end --
ALTER TABLE eboa.explicit_ref_alerts OWNER TO eboa;
-- ddl-end --

-- object: alerts_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_alerts DROP CONSTRAINT IF EXISTS alerts_fk CASCADE;
ALTER TABLE eboa.explicit_ref_alerts ADD CONSTRAINT alerts_fk FOREIGN KEY (alert_uuid)
REFERENCES eboa.alerts (alert_uuid) MATCH FULL
ON DELETE SET NULL ON UPDATE CASCADE;
-- ddl-end --

-- object: alerts_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.source_alerts DROP CONSTRAINT IF EXISTS alerts_fk CASCADE;
ALTER TABLE eboa.source_alerts ADD CONSTRAINT alerts_fk FOREIGN KEY (alert_uuid)
REFERENCES eboa.alerts (alert_uuid) MATCH FULL
ON DELETE SET NULL ON UPDATE CASCADE;
-- ddl-end --

-- object: alerts_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_alerts DROP CONSTRAINT IF EXISTS alerts_fk CASCADE;
ALTER TABLE eboa.event_alerts ADD CONSTRAINT alerts_fk FOREIGN KEY (alert_uuid)
REFERENCES eboa.alerts (alert_uuid) MATCH FULL
ON DELETE SET NULL ON UPDATE CASCADE;
-- ddl-end --

-- object: events_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.event_alerts DROP CONSTRAINT IF EXISTS events_fk CASCADE;
ALTER TABLE eboa.event_alerts ADD CONSTRAINT events_fk FOREIGN KEY (event_uuid)
REFERENCES eboa.events (event_uuid) MATCH FULL
ON DELETE SET NULL ON UPDATE CASCADE;
-- ddl-end --

-- object: explicit_refs_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.explicit_ref_alerts DROP CONSTRAINT IF EXISTS explicit_refs_fk CASCADE;
ALTER TABLE eboa.explicit_ref_alerts ADD CONSTRAINT explicit_refs_fk FOREIGN KEY (explicit_ref_uuid)
REFERENCES eboa.explicit_refs (explicit_ref_uuid) MATCH FULL
ON DELETE SET NULL ON UPDATE CASCADE;
-- ddl-end --

-- object: sources_fk | type: CONSTRAINT --
-- ALTER TABLE eboa.source_alerts DROP CONSTRAINT IF EXISTS sources_fk CASCADE;
ALTER TABLE eboa.source_alerts ADD CONSTRAINT sources_fk FOREIGN KEY (source_uuid)
REFERENCES eboa.sources (source_uuid) MATCH FULL
ON DELETE SET NULL ON UPDATE CASCADE;
-- ddl-end --


