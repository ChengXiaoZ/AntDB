/*
 * oracle_operator.sql
 *
 * Includes SQLs that create oracle operator.
 */

DROP OPERATOR IF EXISTS oracle.+ (oracle.date, numeric);
CREATE OPERATOR oracle.+ (
  LEFTARG   = oracle.date,
  RIGHTARG  = numeric,
  PROCEDURE = oracle.date_pl_numeric
);

DROP OPERATOR IF EXISTS oracle.+ (numeric, oracle.date);
CREATE OPERATOR oracle.+ (
  LEFTARG   = numeric,
  RIGHTARG  = oracle.date,
  PROCEDURE = oracle.numeric_pl_date
);

DROP OPERATOR IF EXISTS oracle.- (oracle.date, numeric);
CREATE OPERATOR oracle.- (
  LEFTARG   = oracle.date,
  RIGHTARG  = numeric,
  PROCEDURE = oracle.subtract
);

DROP OPERATOR IF EXISTS oracle.- (oracle.date, oracle.date);
CREATE OPERATOR oracle.- (
  LEFTARG   = oracle.date,
  RIGHTARG  = oracle.date,
  PROCEDURE = oracle.subtract
);

DROP OPERATOR IF EXISTS oracle.+ (oracle.date, interval);
CREATE OPERATOR oracle.+ (
  LEFTARG   = oracle.date,
  RIGHTARG  = interval,
  PROCEDURE = oracle.date_pl_interval
);

DROP OPERATOR IF EXISTS oracle.+ (interval, oracle.date);
CREATE OPERATOR oracle.+ (
  LEFTARG   = interval,
  RIGHTARG  = oracle.date,
  PROCEDURE = oracle.interval_pl_date
);

