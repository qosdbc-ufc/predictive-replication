--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

-- CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

-- COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: db_active; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE db_active (
    "time" time without time zone,
    vm_id character varying,
    db_name character varying,
    dbms_type integer,
    schema_definition text,
    dbms_user character varying,
    dbms_password character varying,
    dbms_port integer
);


ALTER TABLE public.db_active OWNER TO postgres;

--
-- Name: db_state; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE db_state (
    "time" time without time zone,
    vm_id character varying,
    db_name character varying,
    db_size numeric,
    dbms_connections numeric,
    dbms_type integer
);


ALTER TABLE public.db_state OWNER TO postgres;

--
-- Name: vm_active; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE vm_active (
    "time" time without time zone,
    vm_id character varying,
    mem_total numeric,
    disk_total numeric,
    agent_port integer
);


ALTER TABLE public.vm_active OWNER TO postgres;

--
-- Name: vm_state; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE vm_state (
    "time" time without time zone,
    vm_id character varying,
    mem_total numeric,
    disk_total numeric,
    cpu_free numeric,
    mem_free numeric,
    disk_free numeric
);


ALTER TABLE public.vm_state OWNER TO postgres;

--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

