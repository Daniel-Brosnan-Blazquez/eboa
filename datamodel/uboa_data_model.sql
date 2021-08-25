-- Database generated with pgModeler (PostgreSQL Database Modeler).
-- pgModeler  version: 0.9.3
-- PostgreSQL version: 11.0
-- Project Site: pgmodeler.io
-- Model Author: John Bradley Valdenebro
-- object: uboa | type: ROLE --
-- DROP ROLE IF EXISTS uboa;
CREATE ROLE uboa WITH 
	INHERIT
	LOGIN;
-- ddl-end --


-- Database creation must be performed outside a multi lined SQL file. 
-- These commands were put in this file only as a convenience.
-- 
-- object: uboadb | type: DATABASE --
-- DROP DATABASE IF EXISTS uboadb;
CREATE DATABASE uboadb;
-- ddl-end --


-- object: uboa | type: SCHEMA --
-- DROP SCHEMA IF EXISTS uboa CASCADE;
CREATE SCHEMA uboa;
-- ddl-end --
ALTER SCHEMA uboa OWNER TO uboa;
-- ddl-end --

SET search_path TO pg_catalog,public,uboa;
-- ddl-end --

-- object: uboa.users | type: TABLE --
-- DROP TABLE IF EXISTS uboa.users CASCADE;
CREATE TABLE uboa.users (
	user_uuid uuid NOT NULL,
	email text NOT NULL,
	username text NOT NULL,
	"group" text NOT NULL,
	password text NOT NULL,
	last_login_at timestamp,
	current_login_at timestamp,
	last_login_ip text,
	current_login_ip text,
	login_count integer,
	active bool,
	fs_uniquifier text,
	confirmed_at timestamp,
	CONSTRAINT users_pk PRIMARY KEY (user_uuid),
	CONSTRAINT unique_user UNIQUE (email,username,fs_uniquifier)

);
-- ddl-end --
ALTER TABLE uboa.users OWNER TO uboa;
-- ddl-end --

-- object: uboa.roles | type: TABLE --
-- DROP TABLE IF EXISTS uboa.roles CASCADE;
CREATE TABLE uboa.roles (
	role_uuid uuid NOT NULL,
	name text NOT NULL,
	description text,
	CONSTRAINT unique_role UNIQUE (name),
	CONSTRAINT roles_pk PRIMARY KEY (role_uuid)

);
-- ddl-end --
ALTER TABLE uboa.roles OWNER TO uboa;
-- ddl-end --

-- object: uboa.roles_users | type: TABLE --
-- DROP TABLE IF EXISTS uboa.roles_users CASCADE;
CREATE TABLE uboa.roles_users (
	role_user_uuid uuid NOT NULL,
	role_uuid uuid NOT NULL,
	user_uuid uuid NOT NULL,
	CONSTRAINT roles_users_pk PRIMARY KEY (role_user_uuid)

);
-- ddl-end --
ALTER TABLE uboa.roles_users OWNER TO uboa;
-- ddl-end --

-- object: uboa.configurations | type: TABLE --
-- DROP TABLE IF EXISTS uboa.configurations CASCADE;
CREATE TABLE uboa.configurations (
	configuration_uuid uuid NOT NULL,
	name text NOT NULL,
	configuration json NOT NULL,
	permission integer NOT NULL,
	diff_previous_version text,
	active boolean NOT NULL,
	CONSTRAINT configurations_pk PRIMARY KEY (configuration_uuid)

);
-- ddl-end --
ALTER TABLE uboa.configurations OWNER TO uboa;
-- ddl-end --

-- object: roles_fk | type: CONSTRAINT --
-- ALTER TABLE uboa.roles_users DROP CONSTRAINT IF EXISTS roles_fk CASCADE;
ALTER TABLE uboa.roles_users ADD CONSTRAINT roles_fk FOREIGN KEY (role_uuid)
REFERENCES uboa.roles (role_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: users_fk | type: CONSTRAINT --
-- ALTER TABLE uboa.roles_users DROP CONSTRAINT IF EXISTS users_fk CASCADE;
ALTER TABLE uboa.roles_users ADD CONSTRAINT users_fk FOREIGN KEY (user_uuid)
REFERENCES uboa.users (user_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: uboa.configurations_users | type: TABLE --
-- DROP TABLE IF EXISTS uboa.configurations_users CASCADE;
CREATE TABLE uboa.configurations_users (
	configuration_user_uuid uuid NOT NULL,
	user_uuid uuid NOT NULL,
	configuration_uuid uuid NOT NULL,
	CONSTRAINT configurations_users_pk PRIMARY KEY (configuration_user_uuid)

);
-- ddl-end --
ALTER TABLE uboa.configurations_users OWNER TO uboa;
-- ddl-end --

-- object: users_fk | type: CONSTRAINT --
-- ALTER TABLE uboa.configurations_users DROP CONSTRAINT IF EXISTS users_fk CASCADE;
ALTER TABLE uboa.configurations_users ADD CONSTRAINT users_fk FOREIGN KEY (user_uuid)
REFERENCES uboa.users (user_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: configurations_fk | type: CONSTRAINT --
-- ALTER TABLE uboa.configurations_users DROP CONSTRAINT IF EXISTS configurations_fk CASCADE;
ALTER TABLE uboa.configurations_users ADD CONSTRAINT configurations_fk FOREIGN KEY (configuration_uuid)
REFERENCES uboa.configurations (configuration_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: uboa.configuration_changes | type: TABLE --
-- DROP TABLE IF EXISTS uboa.configuration_changes CASCADE;
CREATE TABLE uboa.configuration_changes (
	configuration_change_uuid uuid NOT NULL,
	"timestamp" timestamp NOT NULL,
	type integer NOT NULL,
	configuration_uuid uuid NOT NULL,
	user_uuid uuid NOT NULL,
	CONSTRAINT configuration_changes_pk PRIMARY KEY (configuration_change_uuid)

);
-- ddl-end --
ALTER TABLE uboa.configuration_changes OWNER TO uboa;
-- ddl-end --

-- object: configurations_fk | type: CONSTRAINT --
-- ALTER TABLE uboa.configuration_changes DROP CONSTRAINT IF EXISTS configurations_fk CASCADE;
ALTER TABLE uboa.configuration_changes ADD CONSTRAINT configurations_fk FOREIGN KEY (configuration_uuid)
REFERENCES uboa.configurations (configuration_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --

-- object: users_fk | type: CONSTRAINT --
-- ALTER TABLE uboa.configuration_changes DROP CONSTRAINT IF EXISTS users_fk CASCADE;
ALTER TABLE uboa.configuration_changes ADD CONSTRAINT users_fk FOREIGN KEY (user_uuid)
REFERENCES uboa.users (user_uuid) MATCH FULL
ON DELETE CASCADE ON UPDATE CASCADE;
-- ddl-end --


