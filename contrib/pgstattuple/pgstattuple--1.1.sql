/* contrib/pgstattuple/pgstattuple--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgstattuple" to load this file. \quit

CREATE FUNCTION pgstattuple(IN relname text,
    OUT table_len BIGINT,		-- physical table length in bytes
    OUT tuple_count BIGINT,		-- number of live tuples
    OUT tuple_len BIGINT,		-- total tuples length in bytes
    OUT tuple_percent FLOAT8,		-- live tuples in %
    OUT dead_tuple_count BIGINT,	-- number of dead tuples
    OUT dead_tuple_len BIGINT,		-- total dead tuples length in bytes
    OUT dead_tuple_percent FLOAT8,	-- dead tuples in %
    OUT free_space BIGINT,		-- free space in bytes
    OUT free_percent FLOAT8)		-- free space in %
AS 'MODULE_PATHNAME', 'pgstattuple'
LANGUAGE C STRICT;

CREATE FUNCTION pgstattuple(IN reloid oid,
    OUT table_len BIGINT,		-- physical table length in bytes
    OUT tuple_count BIGINT,		-- number of live tuples
    OUT tuple_len BIGINT,		-- total tuples length in bytes
    OUT tuple_percent FLOAT8,		-- live tuples in %
    OUT dead_tuple_count BIGINT,	-- number of dead tuples
    OUT dead_tuple_len BIGINT,		-- total dead tuples length in bytes
    OUT dead_tuple_percent FLOAT8,	-- dead tuples in %
    OUT free_space BIGINT,		-- free space in bytes
    OUT free_percent FLOAT8)		-- free space in %
AS 'MODULE_PATHNAME', 'pgstattuplebyid'
LANGUAGE C STRICT;

CREATE FUNCTION pgstatindex(IN relname text,
    OUT version INT,
    OUT tree_level INT,
    OUT index_size BIGINT,
    OUT root_block_no BIGINT,
    OUT internal_pages BIGINT,
    OUT leaf_pages BIGINT,
    OUT empty_pages BIGINT,
    OUT deleted_pages BIGINT,
    OUT avg_leaf_density FLOAT8,
    OUT leaf_fragmentation FLOAT8)
AS 'MODULE_PATHNAME', 'pgstatindex'
LANGUAGE C STRICT;

CREATE FUNCTION pg_relpages(IN relname text)
RETURNS BIGINT
AS 'MODULE_PATHNAME', 'pg_relpages'
LANGUAGE C STRICT;

/* New stuff in 1.1 begins here */

CREATE FUNCTION pgstatginindex(IN relname regclass,
    OUT version INT4,
    OUT pending_pages INT4,
    OUT pending_tuples BIGINT)
AS 'MODULE_PATHNAME', 'pgstatginindex'
LANGUAGE C STRICT;

CREATE FUNCTION pg_hashtables(
	OUT len1 text,
	OUT len2 text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_hashtables'
LANGUAGE C STRICT;

CREATE FUNCTION pgstatdatabaseslot(
	OUT nname 		text,			-- datanode name
    OUT slot_id 	INT4,			-- slotid
    OUT located 	text,			-- slot located datanode name
    OUT space 		BIGINT,			-- physical table length in bytes
    OUT tp_count 	BIGINT,			-- number of live tuples
    OUT tp_len 		BIGINT,			-- total tuples length in bytes
    OUT tp_per 		FLOAT8,			-- live tuples in %
    OUT dtp_count 	BIGINT,			-- number of dead tuples
    OUT dtp_len 	BIGINT,			-- total dead tuples length in bytes
    OUT dtp_per 	FLOAT8,			-- dead tuples in %
    OUT free_space 	BIGINT,			-- free space in bytes
    OUT free_per 	FLOAT8)			-- free space in %
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgstatdatabaseslot'
LANGUAGE C STRICT;


CREATE FUNCTION pgstatslot(IN relname text,
    OUT nname 		text,			-- datanode name
    OUT slot_id 	INT4,			-- slotid
    OUT located 	text,			-- slot located datanode name
    OUT space 		BIGINT,			-- physical table length in bytes
    OUT tp_count 	BIGINT,			-- number of live tuples
    OUT tp_len 		BIGINT,			-- total tuples length in bytes
    OUT tp_per 		FLOAT8,			-- live tuples in %
    OUT dtp_count 	BIGINT,			-- number of dead tuples
    OUT dtp_len 	BIGINT,			-- total dead tuples length in bytes
    OUT dtp_per 	FLOAT8,			-- dead tuples in %
    OUT free_space 	BIGINT,			-- free space in bytes
    OUT free_per 	FLOAT8)			-- free space in %
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgstatslot'
LANGUAGE C STRICT;

CREATE FUNCTION pgvalueslot(IN	tablename text, IN hashkey text)
RETURNS INT4
AS 'MODULE_PATHNAME', 'pgvalueslot'
LANGUAGE C STRICT;
