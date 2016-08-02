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
-- Name: sql_log; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE sql_log (
    "time" bigint,
    vm_id character varying,
    db_name character varying,
    sql character varying,
    sql_type integer,
    response_time bigint,
    sla_response_time bigint,
    sla_violated boolean,
    connection_id bigint,
    transaction_id bigint,
    affected_rows bigint,
    time_local bigint,
    in_migration boolean
);


ALTER TABLE public.sql_log OWNER TO postgres;

--
-- Name: sla_log; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE sla_log (
    db_name character varying,
    response_time double precision
);


ALTER TABLE public.sla_log OWNER TO postgres;


CREATE TABLE replica_sync (
    "time" bigint,
    db_name character varying,
    sync_time bigint
);

ALTER TABLE public.replica_sync OWNER TO postgres;

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

