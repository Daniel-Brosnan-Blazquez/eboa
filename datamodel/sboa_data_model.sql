-- Database generated with pgModeler (PostgreSQL Database Modeler).
-- pgModeler  version: 0.9.2-beta1
-- PostgreSQL version: 11.0
-- Project Site: pgmodeler.io
-- Model Author: Daniel Brosnan Blázquez

-- object: sboa | type: ROLE --
-- DROP ROLE IF EXISTS sboa;
CREATE ROLE sboa WITH 
	INHERIT
	LOGIN;
-- ddl-end --


-- Database creation must be done outside a multicommand file.
-- These commands were put in this file only as a convenience.
-- -- object: sboadb | type: DATABASE --
-- -- DROP DATABASE IF EXISTS sboadb;
-- CREATE DATABASE sboadb;
-- -- ddl-end --
-- 

-- object: sboa | type: SCHEMA --
-- DROP SCHEMA IF EXISTS sboa CASCADE;
CREATE SCHEMA sboa;
-- ddl-end --
ALTER SCHEMA sboa OWNER TO sboa;
-- ddl-end --

SET search_path TO pg_catalog,public,sboa;
-- ddl-end --

-- object: sboa.rules | type: TABLE --
-- DROP TABLE IF EXISTS sboa.rules CASCADE;
CREATE TABLE sboa.rules (
	rule_uuid uuid NOT NULL,
	name text NOT NULL,
	periodicity double precision NOT NULL,
	window_delay double precision NOT NULL,
	window_size double precision NOT NULL,
	triggering_time timestamp NOT NULL,
	CONSTRAINT rules_pk PRIMARY KEY (rule_uuid),
	CONSTRAINT unique_rule UNIQUE (name)

);
-- ddl-end --
ALTER TABLE sboa.rules OWNER TO sboa;
-- ddl-end --

-- object: sboa.tasks | type: TABLE --
-- DROP TABLE IF EXISTS sboa.tasks CASCADE;
CREATE TABLE sboa.tasks (
	task_uuid uuid NOT NULL,
	name text NOT NULL,
	command text NOT NULL,
	rule_uuid uuid NOT NULL,
	CONSTRAINT tasks_pk PRIMARY KEY (task_uuid),
	CONSTRAINT unique_task UNIQUE (name)

);
-- ddl-end --
ALTER TABLE sboa.tasks OWNER TO sboa;
-- ddl-end --

-- object: rules_fk | type: CONSTRAINT --
-- ALTER TABLE sboa.tasks DROP CONSTRAINT IF EXISTS rules_fk CASCADE;
ALTER TABLE sboa.tasks ADD CONSTRAINT rules_fk FOREIGN KEY (rule_uuid)
REFERENCES sboa.rules (rule_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: sboa.triggerings | type: TABLE --
-- DROP TABLE IF EXISTS sboa.triggerings CASCADE;
CREATE TABLE sboa.triggerings (
	triggering_uuid uuid NOT NULL,
	date timestamp NOT NULL,
	task_uuid uuid NOT NULL,
	CONSTRAINT triggerings_pk PRIMARY KEY (triggering_uuid)

);
-- ddl-end --
ALTER TABLE sboa.triggerings OWNER TO sboa;
-- ddl-end --

-- object: tasks_fk | type: CONSTRAINT --
-- ALTER TABLE sboa.triggerings DROP CONSTRAINT IF EXISTS tasks_fk CASCADE;
ALTER TABLE sboa.triggerings ADD CONSTRAINT tasks_fk FOREIGN KEY (task_uuid)
REFERENCES sboa.tasks (task_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: idx_rule_name | type: INDEX --
-- DROP INDEX IF EXISTS sboa.idx_rule_name CASCADE;
CREATE INDEX idx_rule_name ON sboa.rules
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_rule_name_gin | type: INDEX --
-- DROP INDEX IF EXISTS sboa.idx_rule_name_gin CASCADE;
CREATE INDEX idx_rule_name_gin ON sboa.rules
	USING gin
	(
	  name
	);
-- ddl-end --

-- object: idx_triggering_time | type: INDEX --
-- DROP INDEX IF EXISTS sboa.idx_triggering_time CASCADE;
CREATE INDEX idx_triggering_time ON sboa.rules
	USING btree
	(
	  triggering_time
	);
-- ddl-end --

-- object: idx_task_name | type: INDEX --
-- DROP INDEX IF EXISTS sboa.idx_task_name CASCADE;
CREATE INDEX idx_task_name ON sboa.tasks
	USING btree
	(
	  name
	);
-- ddl-end --

-- object: idx_task_name_gin | type: INDEX --
-- DROP INDEX IF EXISTS sboa.idx_task_name_gin CASCADE;
CREATE INDEX idx_task_name_gin ON sboa.tasks
	USING gin
	(
	  name
	);
-- ddl-end --

-- object: idx_triggering_date | type: INDEX --
-- DROP INDEX IF EXISTS sboa.idx_triggering_date CASCADE;
CREATE INDEX idx_triggering_date ON sboa.triggerings
	USING btree
	(
	  date
	);
-- ddl-end --


