/*
 * Oracle Functions
 *
 * Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Copyright (c) 2014-2016, ADB Development Group
 *
 * src/backend/oraschema/oracle_proc.sql
 */

/*
 * Function: bitand
 * Parameter Type: : (numeric, numeric)
 */
CREATE OR REPLACE FUNCTION oracle.bitand(bigint, bigint)
    RETURNS bigint
    AS $$select $1 & $2;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: nanvl
 * Parameter Type: (float8, float8)
 */
CREATE OR REPLACE FUNCTION oracle.nanvl(float8, float8)
    RETURNS float8
    AS $$SELECT CASE WHEN $1 = 'NaN' THEN $2 ELSE $1 END;$$
    LANGUAGE SQL
    IMMUTABLE
    STRICT;

/*
 * Function: sinh
 * sinh x = (e ^ x - e ^ (-x))/2
 * Parameter Type: : (numeric)
 */
CREATE OR REPLACE FUNCTION oracle.sinh(numeric)
    RETURNS numeric
    AS $$SELECT (exp($1) - exp(-$1)) / 2;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: sinh
 * sinh x = (e ^ x - e ^ (-x))/2
 * Parameter Type: : (float8)
 */
CREATE OR REPLACE FUNCTION oracle.sinh(float8)
    RETURNS float8
    AS $$SELECT (exp($1) - exp(-$1)) / 2;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: cosh
 * cosh x = (e ^ x + e ^ (-x))/2
 * Parameter Type: (numeric)
 */
CREATE OR REPLACE FUNCTION oracle.cosh(numeric)
    RETURNS numeric
    AS $$SELECT (exp($1) + exp(-$1)) / 2;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: cosh
 * cosh x = (e ^ x + e ^ (-x))/2
 * Parameter Type: (float8)
 */
CREATE OR REPLACE FUNCTION oracle.cosh(float8)
    RETURNS float8
    AS $$SELECT (exp($1) + exp(-$1)) / 2;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: tanh
 * tanh x = sinh x / cosh x = (e ^ x - e ^ (-x)) / (e ^ x + e ^ (-x))
 * Parameter Type: (numeric)
 */
CREATE OR REPLACE FUNCTION oracle.tanh(numeric)
    RETURNS numeric
    AS $$SELECT (exp($1) - exp(-$1)) / (exp($1) + exp(-$1));$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: tanh
 * tanh x = sinh x / cosh x = (e ^ x - e ^ (-x)) / (e ^ x + e ^ (-x))
 * Parameter Type: (float8)
 */
CREATE OR REPLACE FUNCTION oracle.tanh(float8)
    RETURNS float8
    AS $$SELECT (exp($1) - exp(-$1)) / (exp($1) + exp(-$1));$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: INSTR
 */
CREATE OR REPLACE FUNCTION oracle.instr(str text, patt text, start int default 1, nth int default 1)
    RETURNS int
    AS 'orastr_instr4'
    LANGUAGE INTERNAL
    IMMUTABLE STRICT;

/*
 * Function: ADD_MONTHS
 */
CREATE FUNCTION oracle.add_months(TIMESTAMP WITH TIME ZONE, INTEGER)
     RETURNS TIMESTAMP
     AS $$SELECT oracle.add_months($1::pg_catalog.date, $2) + $1::pg_catalog.time;$$
     LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: LAST_DAY
 */
CREATE OR REPLACE FUNCTION oracle.last_day(TIMESTAMP WITH TIME ZONE)
    RETURNS oracle.date
    AS $$SELECT (oracle.last_day($1::pg_catalog.date) + $1::time)::oracle.date;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: months_between
 */
CREATE FUNCTION oracle.months_between(TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH TIME ZONE)
    RETURNS NUMERIC
    AS $$SELECT oracle.months_between($1::pg_catalog.date, $2::pg_catalog.date);$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: new_time
 * Parameter Type: (timestamp, text, text)
 */
CREATE OR REPLACE FUNCTION oracle.new_time(tt timestamp with time zone, z1 text, z2 text)
    RETURNS timestamp
    AS $$
    DECLARE
    src_interval INTERVAL;
    dst_interval INTERVAL;
    BEGIN
        SELECT utc_offset INTO src_interval FROM pg_timezone_abbrevs WHERE abbrev = z1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Invalid time zone: %', z1;
        END IF;

        SELECT utc_offset INTO dst_interval FROM pg_timezone_abbrevs WHERE abbrev = z2;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Invalid time zone: %', z2;
        END IF;

        RETURN tt - src_interval + dst_interval;
    END;
    $$
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT;

/*
 * Function: next_day
 * Parameter Type: (oracle.date, text)
 * Parameter Type: (timestamptz, text)
 */
CREATE OR REPLACE FUNCTION oracle.next_day(oracle.date, text)
    RETURNS oracle.date
    AS $$SELECT (oracle.ora_next_day($1::pg_catalog.date, $2) + $1::time)::oracle.date;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.next_day(timestamptz, text)
    RETURNS oracle.date
    AS $$SELECT (oracle.ora_next_day($1::pg_catalog.date, $2) + $1::time)::oracle.date;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: round
 */
CREATE OR REPLACE FUNCTION oracle.round(pg_catalog.date, text default 'DDD')
    RETURNS pg_catalog.date
    AS 'ora_date_round'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.round(timestamptz, text default 'DDD')
    RETURNS oracle.date
    AS $$select oracle.ora_timestamptz_round($1, $2)::oracle.date;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: trunc
 * Parameter Type: (date, text)
 * Parameter Type: (date)
 * Parameter Type: (timestamp with time zone, text)
 * Parameter Type: (timestamp with time zone)
 */
CREATE OR REPLACE FUNCTION oracle.trunc(pg_catalog.date, text default 'DDD')
    RETURNS date
    AS 'ora_date_trunc'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.trunc(oracle.date, text default 'DDD')
    RETURNS oracle.date
    AS 'ora_timestamptz_trunc'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.trunc(timestamptz, text default 'DDD')
    RETURNS oracle.date
    AS $$select oracle.ora_timestamptz_trunc($1, $2)::oracle.date;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: first
 * Parameter Type: (text)
 */
CREATE OR REPLACE FUNCTION oracle.first(str text)
    RETURNS text
    AS 'orachr_first'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

/*
 * Function: last
 * Parameter Type: (text)
 */
CREATE OR REPLACE FUNCTION oracle.last(str text)
    RETURNS text
    AS 'orachr_last'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

/*
 * For date's operator +/- 
 */
CREATE OR REPLACE FUNCTION oracle.date_pl_numeric(oracle.date,numeric)
    RETURNS oracle.date
    AS $$SELECT ($1 + interval '1 day' * $2)::oracle.date;$$
    LANGUAGE SQL
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.numeric_pl_date(numeric, oracle.date)
    RETURNS oracle.date
    AS $$SELECT ($2 + interval '1 day' * $1)::oracle.date;$$
    LANGUAGE SQL
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.subtract (oracle.date, numeric)
    RETURNS oracle.date
    AS $$SELECT ($1 - interval '1 day' * $2)::oracle.date;$$
    LANGUAGE SQL
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.subtract(oracle.date, oracle.date)
    RETURNS double precision
    AS $$SELECT date_part('epoch', ($1 - $2)/3600/24);$$
    LANGUAGE SQL
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_date
 * Parameter Type: (text)
 * Parameter Type: (text, text)
 */
CREATE OR REPLACE FUNCTION oracle.to_date(text)
    RETURNS oracle.date
    AS 'text_todate'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_date(text, text)
    RETURNS oracle.date
    AS 'text_todate'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_timestamp
 * Parameter Type: (text)
 * Parameter Type: (text, text)
 */
CREATE OR REPLACE FUNCTION oracle.to_timestamp(text)
    RETURNS timestamp
    AS 'text_totimestamp'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_timestamp(text, text)
    RETURNS timestamp
    AS 'text_totimestamp'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_timestamp_tz
 * Parameter Type: (text)
 * Parameter Type: (text, text)
 */
CREATE OR REPLACE FUNCTION oracle.to_timestamp_tz(text)
    RETURNS timestamptz
    AS 'text_totimestamptz'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_timestamp_tz(text, text)
    RETURNS timestamptz
    AS 'text_totimestamptz'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_char
 * Parameter Type: (smallint)
 * Parameter Type: (smallint, text)
 * Parameter Type: (int)
 * Parameter Type: (int, text)
 * Parameter Type: (bigint)
 * Parameter Type: (bigint, text)
 * Parameter Type: (real)
 * Parameter Type: (real, text)
 * Parameter Type: (double precision)
 * Parameter Type: (double precision, text)
 * Parameter Type: (numeric)
 * Parameter Type: (numeric, text)
 * Parameter Type: (text)
 * Parameter Type: (timestamp)
 * Parameter Type: (timestamp, text)
 * Parameter Type: (timestamptz)
 * Parameter Type: (timestamptz, text)
 * Parameter Type: (interval)
 * Parameter Type: (interval, text)
 */
CREATE OR REPLACE FUNCTION oracle.to_char(smallint)
    RETURNS text
    AS 'int4_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(smallint, text)
    RETURNS text
    AS 'int4_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(int)
    RETURNS text
    AS 'int4_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(int, text)
    RETURNS text
    AS 'int4_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(bigint)
    RETURNS text
    AS 'int8_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(bigint, text)
    RETURNS text
    AS 'int8_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(real)
    RETURNS text
    AS 'float4_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(real, text)
    RETURNS text
    AS 'float4_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(double precision)
    RETURNS text
    AS 'float8_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(double precision, text)
    RETURNS text
    AS 'float8_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(numeric)
    RETURNS text
    AS 'numeric_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_char(numeric, text)
    RETURNS text
    AS 'numeric_tochar'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(text)
    RETURNS TEXT
    AS 'text_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(timestamp)
    RETURNS TEXT
    AS 'timestamp_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(timestamp, text)
    RETURNS TEXT
    AS 'timestamp_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(timestamptz)
    RETURNS TEXT
    AS 'timestamptz_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(timestamptz, text)
    RETURNS TEXT
    AS 'timestamptz_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(interval)
    RETURNS TEXT
    AS 'interval_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;
CREATE FUNCTION oracle.to_char(interval, text)
    RETURNS TEXT
    AS 'interval_tochar'
    LANGUAGE INTERNAL
    STABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_number
 * Parameter Type: (text)
 * Parameter Type: (text, text)
 * Parameter Type: (float4)
 * Parameter Type: (float4, text)
 * Parameter Type: (float8)
 * Parameter Type: (float8, text)
 * Parameter Type: (numeric)
 * Parameter Type: (numeric, text)
 */
CREATE OR REPLACE FUNCTION oracle.to_number(text)
    RETURNS numeric
    AS 'text_tonumber'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(text, text)
    RETURNS numeric
    AS 'text_tonumber'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(float4)
    RETURNS numeric
    AS 'float4_tonumber'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(float4, text)
    RETURNS numeric
    AS 'float4_tonumber'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(float8)
    RETURNS numeric
    AS 'float8_tonumber'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(float8, text)
    RETURNS numeric
    AS 'float8_tonumber'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(numeric)
    RETURNS numeric
    AS 'select $1'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
CREATE OR REPLACE FUNCTION oracle.to_number(numeric, text)
    RETURNS numeric
    AS 'select oracle.to_number($1::text, $2)'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_multi_byte
 * Parameter Type: (text)
 */
CREATE OR REPLACE FUNCTION oracle.to_multi_byte(text)
    RETURNS text
    AS 'ora_to_multi_byte'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: to_single_byte
 * Parameter Type: (text)
 */
CREATE OR REPLACE FUNCTION oracle.to_single_byte(text)
    RETURNS text
    AS 'ora_to_single_byte'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: substr
 * Parameter Type: (text, int)
 * Parameter Type: (text, int, int)
 * Parameter Type: (numeric, numeric)
 * Parameter Type: (numeric, numeric, numeric)
 * Parameter Type: (varchar, numeric)
 * Parameter Type: (varchar, numeric, numeric)
 */
CREATE OR REPLACE FUNCTION oracle.substr(text, integer)
    RETURNS text
    AS 'orastr_substr2'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION oracle.substr(text, integer, integer)
    RETURNS text
    AS 'orastr_substr3'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: dump
 * Parameter Type: (text, int)
 */
CREATE OR REPLACE FUNCTION oracle.dump(text, integer default 10)
    RETURNS varchar
    AS 'ora_dump'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

/*
 * Function: length
 * Parameter Type: (char)
 * Parameter Type: (varchar)
 * Parameter Type: (varchar2)
 * Parameter Type: (text)
 */
CREATE OR REPLACE FUNCTION oracle.length(char)
    RETURNS integer
    AS 'orastr_bpcharlen'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

/*
 * Function: lengthb
 * Parameter Type: (varchar2)
 * Parameter Type: (varchar)
 * Parameter Type: (text)
 */
CREATE OR REPLACE FUNCTION oracle.lengthb(varchar2)
    RETURNS integer
    AS 'byteaoctetlen'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

/* string functions for varchar2 type
 * these are 'byte' versions of corresponsing text/varchar functions
 */
CREATE OR REPLACE FUNCTION oracle.substrb(varchar2, integer)
    RETURNS varchar2
    AS 'bytea_substr_no_len'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

CREATE OR REPLACE FUNCTION oracle.substrb(varchar2, integer, integer)
    RETURNS varchar2
    AS 'bytea_substr'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

CREATE OR REPLACE FUNCTION oracle.strposb(varchar2, varchar2)
    RETURNS integer
    AS 'byteapos'
    LANGUAGE INTERNAL
    IMMUTABLE
    STRICT;

/*
 * Function: lpad
 * Parameter Type: (text, integer, text)
 */
CREATE OR REPLACE FUNCTION oracle.lpad(text, integer, text default ' '::text)
    RETURNS text
    AS 'lpad'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: rpad
 * Parameter Type: (text, integer, text)
 */
CREATE OR REPLACE FUNCTION oracle.rpad(text, integer, text default ' '::text)
    RETURNS text
    AS 'rpad'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: remainder
 * Parameter Type: (numeric, numeric)
 */
CREATE OR REPLACE FUNCTION oracle.remainder(n2 numeric, n1 numeric)
    RETURNS numeric
    AS $$select n2 - n1*round(n2/n1);$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
 * Function: sys_extract_utc
 */
CREATE OR REPLACE FUNCTION oracle.sys_extract_utc(timestamp with time zone)
    RETURNS timestamp
    AS $$select $1 at time zone 'UTC';$$
    LANGUAGE SQL
    IMMUTABLE
    STRICT;

/*
 * Function: mode
 * Parameter Type: (numeric, numeric)
 * Add oracle.mod(numeric, numeric) to make sure find oracle.mod if
 * current grammar is oracle;
 */
CREATE OR REPLACE FUNCTION oracle.mod(numeric, numeric)
    RETURNS numeric
    AS 'numeric_mod'
    LANGUAGE INTERNAL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

