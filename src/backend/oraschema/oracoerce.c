/*-------------------------------------------------------------------------
 *
 * oracoerce.c
 *	  try to coerce from source type to target type with oracle grammar.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2016, ADB Development Group of ASIA
 *
 * IDENTIFICATION
 *	  src/backend/oraschema/oracoerce.c
 *
 * TODO
 *	  Coercion of Oracle grammar can be used with Catalog system.
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "catalog/namespace.h"
#include "nodes/nodeFuncs.h"
#include "oraschema/oracoerce.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"

#define ORA_MIN(A, B) ((B)<(A)?(B):(A))
#define ORA_MAX(A, B) ((B)>(A)?(B):(A))
#define ORA_COERCE3(a, b, c) {a, b, c, c},
#define ORA_COERCE4(a, b, c, d) {a, b, c, d},
#define ORA_PRECEDENCE(a, b) {a, b},

static const OraCoercePath OraCoerceDefault[] =
{
	ORA_COERCE3(TEXTOID, FLOAT4OID, TEXTTOFLOAT4OID)							/* text -> float4 */
	ORA_COERCE3(TEXTOID, FLOAT8OID, TEXTTOFLOAT8OID)							/* text -> float8 */
	ORA_COERCE3(TEXTOID, NUMERICOID, TEXTTONUMERICOID)							/* text -> numeric */
	ORA_COERCE3(TEXTOID, ORADATEOID, TEXTTODATEOID)								/* text -> oracle.date */
	ORA_COERCE3(TEXTOID, TIMESTAMPOID, TEXTTOTSOID)								/* text -> timestamp */
	ORA_COERCE3(TEXTOID, TIMESTAMPTZOID, TEXTTOTSTZOID)							/* text -> timestamptz */
	ORA_COERCE3(TEXTOID, CSTRINGOID, TEXTTOCSTRINGOID)							/* text -> cstring */

	ORA_COERCE3(VARCHAROID, FLOAT4OID, TEXTTOFLOAT4OID)							/* varchar -> float4 */
	ORA_COERCE3(VARCHAROID, FLOAT8OID, TEXTTOFLOAT8OID)							/* varchar -> float8 */
	ORA_COERCE3(VARCHAROID, NUMERICOID, TEXTTONUMERICOID)						/* varchar -> numeric */
	ORA_COERCE3(VARCHAROID, ORADATEOID, TEXTTODATEOID)							/* varchar -> oracle.date */
	ORA_COERCE3(VARCHAROID, TIMESTAMPOID, TEXTTOTSOID)							/* varchar -> timestamp */
	ORA_COERCE3(VARCHAROID, TIMESTAMPTZOID, TEXTTOTSTZOID)						/* varchar -> timestamptz */
	ORA_COERCE3(VARCHAROID, CSTRINGOID, TEXTTOCSTRINGOID)						/* varchar -> cstring */

	ORA_COERCE3(VARCHAR2OID, FLOAT4OID, TEXTTOFLOAT4OID)						/* varchar2 -> float4 */
	ORA_COERCE3(VARCHAR2OID, FLOAT8OID, TEXTTOFLOAT8OID)						/* varchar2 -> float8 */
	ORA_COERCE3(VARCHAR2OID, NUMERICOID, TEXTTONUMERICOID)						/* varchar2 -> numeric */
	ORA_COERCE3(VARCHAR2OID, ORADATEOID, TEXTTODATEOID)							/* varchar2 -> oracle.date */
	ORA_COERCE3(VARCHAR2OID, TIMESTAMPOID, TEXTTOTSOID)							/* varchar2 -> timestamp */
	ORA_COERCE3(VARCHAR2OID, TIMESTAMPTZOID, TEXTTOTSTZOID)						/* varchar2 -> timestamptz */
	ORA_COERCE3(VARCHAR2OID, CSTRINGOID, TEXTTOCSTRINGOID)						/* varchar2 -> cstring */

	ORA_COERCE3(NVARCHAR2OID, FLOAT4OID, TEXTTOFLOAT4OID)						/* nvarchar2 -> float4 */
	ORA_COERCE3(NVARCHAR2OID, FLOAT8OID, TEXTTOFLOAT8OID)						/* nvarchar2 -> float8 */
	ORA_COERCE3(NVARCHAR2OID, NUMERICOID, TEXTTONUMERICOID)						/* nvarchar2 -> numeric */
	ORA_COERCE3(NVARCHAR2OID, ORADATEOID, TEXTTODATEOID)						/* nvarchar2 -> oracle.date */
	ORA_COERCE3(NVARCHAR2OID, TIMESTAMPOID, TEXTTOTSOID)						/* nvarchar2 -> timestamp */
	ORA_COERCE3(NVARCHAR2OID, TIMESTAMPTZOID, TEXTTOTSTZOID)					/* nvarchar2 -> timestamptz */
	ORA_COERCE3(NVARCHAR2OID, CSTRINGOID, TEXTTOCSTRINGOID)						/* nvarchar2 -> cstring */

	ORA_COERCE3(BPCHAROID, FLOAT4OID, TEXTTOFLOAT4OID)							/* bpchar -> float4 */
	ORA_COERCE3(BPCHAROID, FLOAT8OID, TEXTTOFLOAT8OID)							/* bpchar -> float8 */
	ORA_COERCE3(BPCHAROID, NUMERICOID, TEXTTONUMERICOID)						/* bpchar -> numeric */
	ORA_COERCE3(BPCHAROID, ORADATEOID, TEXTTODATEOID)							/* bpchar -> oracle.date */
	ORA_COERCE3(BPCHAROID, TIMESTAMPOID, TEXTTOTSOID)							/* bpchar -> timestamp */
	ORA_COERCE3(BPCHAROID, TIMESTAMPTZOID, TEXTTOTSTZOID)						/* bpchar -> timestamptz */
	ORA_COERCE3(BPCHAROID, CSTRINGOID, TEXTTOCSTRINGOID)						/* bpchar -> cstring */
};

static const int NumOraCoerceDefault = lengthof(OraCoerceDefault);

static const OraCoercePath OraCoerceOperator[] =
{
	ORA_COERCE3(TEXTOID, INT2OID, TEXTTONUMERICOID)								/* text -> int2 => numeric */
	ORA_COERCE3(TEXTOID, INT4OID, TEXTTONUMERICOID)								/* text -> int4 => numeric */
	ORA_COERCE3(TEXTOID, INT8OID, TEXTTONUMERICOID)								/* text -> int8 => numeric */

	ORA_COERCE3(VARCHAROID, INT2OID, TEXTTONUMERICOID)							/* varchar -> int2 => numeric */
	ORA_COERCE3(VARCHAROID, INT4OID, TEXTTONUMERICOID)							/* varchar -> int4 => numeric */
	ORA_COERCE3(VARCHAROID, INT8OID, TEXTTONUMERICOID)							/* varchar -> int8 => numeric */

	ORA_COERCE3(VARCHAR2OID, INT2OID, TEXTTONUMERICOID)							/* varchar2 -> int2 => numeric */
	ORA_COERCE3(VARCHAR2OID, INT4OID, TEXTTONUMERICOID)							/* varchar2 -> int4 => numeric */
	ORA_COERCE3(VARCHAR2OID, INT8OID, TEXTTONUMERICOID)							/* varchar2 -> int8 => numeric */

	ORA_COERCE3(NVARCHAR2OID, INT2OID, TEXTTONUMERICOID)						/* nvarchar2 -> int2 => numeric */
	ORA_COERCE3(NVARCHAR2OID, INT4OID, TEXTTONUMERICOID)						/* nvarchar2 -> int4 => numeric */
	ORA_COERCE3(NVARCHAR2OID, INT8OID, TEXTTONUMERICOID)						/* nvarchar2 -> int8 => numeric */

	ORA_COERCE3(BPCHAROID, INT2OID, TEXTTONUMERICOID)							/* bpchar -> int2 => numeric */
	ORA_COERCE3(BPCHAROID, INT4OID, TEXTTONUMERICOID)							/* bpchar -> int4 => numeric */
	ORA_COERCE3(BPCHAROID, INT8OID, TEXTTONUMERICOID)							/* bpchar -> int8 => numeric */
};

static const int NumOraCoerceOperator = lengthof(OraCoerceOperator);

static const OraCoercePath OraCoerceFunction[] =
{
	ORA_COERCE4(TEXTOID, INT2OID, TEXTTOINT2OID, TRUNC_TEXTTOINT2OID)			/* trunc(text::numeric) -> int2 */
	ORA_COERCE4(TEXTOID, INT4OID, TEXTTOINT4OID, TRUNC_TEXTTOINT4OID)			/* trunc(text::numeric) -> int4 */
	ORA_COERCE4(TEXTOID, INT8OID, TEXTTOINT8OID, TRUNC_TEXTTOINT8OID)			/* trunc(text::numeric) -> int8 */

	ORA_COERCE4(VARCHAROID, INT2OID, TEXTTOINT2OID, TRUNC_TEXTTOINT2OID)		/* trunc(varchar::numeric) -> int2 */
	ORA_COERCE4(VARCHAROID, INT4OID, TEXTTOINT4OID, TRUNC_TEXTTOINT4OID)		/* trunc(varchar::numeric) -> int4 */
	ORA_COERCE4(VARCHAROID, INT8OID, TEXTTOINT8OID, TRUNC_TEXTTOINT8OID)		/* trunc(varchar::numeric) -> int8 */

	ORA_COERCE4(VARCHAR2OID, INT2OID, TEXTTOINT2OID, TRUNC_TEXTTOINT2OID)		/* trunc(varchar2::numeric) -> int2 */
	ORA_COERCE4(VARCHAR2OID, INT4OID, TEXTTOINT4OID, TRUNC_TEXTTOINT4OID)		/* trunc(varchar2::numeric) -> int4 */
	ORA_COERCE4(VARCHAR2OID, INT8OID, TEXTTOINT8OID, TRUNC_TEXTTOINT8OID)		/* trunc(varchar2::numeric) -> int8 */

	ORA_COERCE4(NVARCHAR2OID, INT2OID, TEXTTOINT2OID, TRUNC_TEXTTOINT2OID)		/* trunc(nvarchar2::numeric) -> int2 */
	ORA_COERCE4(NVARCHAR2OID, INT4OID, TEXTTOINT4OID, TRUNC_TEXTTOINT4OID)		/* trunc(nvarchar2::numeric) -> int4 */
	ORA_COERCE4(NVARCHAR2OID, INT8OID, TEXTTOINT8OID, TRUNC_TEXTTOINT8OID)		/* trunc(nvarchar2::numeric) -> int8 */

	ORA_COERCE4(BPCHAROID, INT2OID, TEXTTOINT2OID, TRUNC_TEXTTOINT2OID)			/* trunc(bpchar::numeric) -> int2 */
	ORA_COERCE4(BPCHAROID, INT4OID, TEXTTOINT4OID, TRUNC_TEXTTOINT4OID)			/* trunc(bpchar::numeric) -> int4 */
	ORA_COERCE4(BPCHAROID, INT8OID, TEXTTOINT8OID, TRUNC_TEXTTOINT8OID)			/* trunc(bpchar::numeric) -> int8 */

	ORA_COERCE4(NUMERICOID, INT2OID, NUMERICTOINT2OID, TRUNC_NUMERICTOINT2OID)	/* trunc(numeric) -> int2 */
	ORA_COERCE4(NUMERICOID, INT4OID, NUMERICTOINT4OID, TRUNC_NUMERICTOINT4OID)	/* trunc(numeric) -> int4 */
	ORA_COERCE4(NUMERICOID, INT8OID, NUMERICTOINT8OID, TRUNC_NUMERICTOINT8OID)	/* trunc(numeric) -> int8 */

	ORA_COERCE4(FLOAT4OID, INT2OID, FLOAT4TOINT2OID, TRUNC_FLOAT4TOINT2OID)		/* trunc(float4) -> int2 */
	ORA_COERCE4(FLOAT4OID, INT4OID, FLOAT4TOINT4OID, TRUNC_FLOAT4TOINT4OID)		/* trunc(float4) -> int4 */
	ORA_COERCE4(FLOAT4OID, INT8OID, FLOAT4TOINT8OID, TRUNC_FLOAT4TOINT8OID)		/* trunc(float4) -> int8 */

	ORA_COERCE4(FLOAT8OID, INT2OID, FLOAT8TOINT2OID, TRUNC_FLOAT8TOINT2OID)		/* trunc(float8) -> int2 */
	ORA_COERCE4(FLOAT8OID, INT4OID, FLOAT8TOINT4OID, TRUNC_FLOAT8TOINT4OID)		/* trunc(float8) -> int4 */
	ORA_COERCE4(FLOAT8OID, INT8OID, FLOAT8TOINT8OID, TRUNC_FLOAT8TOINT8OID)		/* trunc(float8) -> int8 */

	ORA_COERCE3(INT2OID, TEXTOID, INT4TOCHAROID)								/* int2 -> text */
	ORA_COERCE3(INT4OID, TEXTOID, INT4TOCHAROID)								/* int4 -> text */
	ORA_COERCE3(INT8OID, TEXTOID, INT8TOCHAROID)								/* int8 -> text */
	ORA_COERCE3(FLOAT4OID, TEXTOID, FLOAT4TOCHAROID)							/* float4 -> text */
	ORA_COERCE3(FLOAT8OID, TEXTOID, FLOAT8TOCHAROID)							/* float8 -> text */
	ORA_COERCE3(TIMESTAMPOID, TEXTOID, TSTOCHAROID)								/* timestamp -> text */
	ORA_COERCE3(TIMESTAMPTZOID, TEXTOID, TSTZTOCHAROID)							/* timestamptz -> text */
	ORA_COERCE3(INTERVALOID, TEXTOID, INTERVALTOCHAROID)						/* interval -> text */
	ORA_COERCE3(NUMERICOID, TEXTOID, NUMERICTOCHAROID)							/* numeric -> text */

	ORA_COERCE3(INT2OID, VARCHAROID, INT4TOCHAROID)								/* int2 -> varchar */
	ORA_COERCE3(INT4OID, VARCHAROID, INT4TOCHAROID)								/* int4 -> varchar */
	ORA_COERCE3(INT8OID, VARCHAROID, INT8TOCHAROID)								/* int8 -> varchar */
	ORA_COERCE3(FLOAT4OID, VARCHAROID, FLOAT4TOCHAROID)							/* float4 -> varchar */
	ORA_COERCE3(FLOAT8OID, VARCHAROID, FLOAT8TOCHAROID)							/* float8 -> varchar */
	ORA_COERCE3(TIMESTAMPOID, VARCHAROID, TSTOCHAROID)							/* timestamp -> varchar */
	ORA_COERCE3(TIMESTAMPTZOID, VARCHAROID, TSTZTOCHAROID)						/* timestamptz -> varchar */
	ORA_COERCE3(INTERVALOID, VARCHAROID, INTERVALTOCHAROID)						/* interval -> varchar */
	ORA_COERCE3(NUMERICOID, VARCHAROID, NUMERICTOCHAROID)						/* numeric -> varchar */

	ORA_COERCE3(INT2OID, VARCHAR2OID, INT4TOCHAROID)							/* int2 -> varchar2 */
	ORA_COERCE3(INT4OID, VARCHAR2OID, INT4TOCHAROID)							/* int4 -> varchar2 */
	ORA_COERCE3(INT8OID, VARCHAR2OID, INT8TOCHAROID)							/* int8 -> varchar2 */
	ORA_COERCE3(FLOAT4OID, VARCHAR2OID, FLOAT4TOCHAROID)						/* float4 -> varchar2 */
	ORA_COERCE3(FLOAT8OID, VARCHAR2OID, FLOAT8TOCHAROID)						/* float8 -> varchar2 */
	ORA_COERCE3(TIMESTAMPOID, VARCHAR2OID, TSTOCHAROID)							/* timestamp -> varchar2 */
	ORA_COERCE3(TIMESTAMPTZOID, VARCHAR2OID, TSTZTOCHAROID)						/* timestamptz -> varchar2 */
	ORA_COERCE3(INTERVALOID, VARCHAR2OID, INTERVALTOCHAROID)					/* interval -> varchar2 */
	ORA_COERCE3(NUMERICOID, VARCHAR2OID, NUMERICTOCHAROID)						/* numeric -> varchar2 */

	ORA_COERCE3(INT2OID, NVARCHAR2OID, INT4TOCHAROID)							/* int2 -> nvarchar2 */
	ORA_COERCE3(INT4OID, NVARCHAR2OID, INT4TOCHAROID)							/* int4 -> nvarchar2 */
	ORA_COERCE3(INT8OID, NVARCHAR2OID, INT8TOCHAROID)							/* int8 -> nvarchar2 */
	ORA_COERCE3(FLOAT4OID, NVARCHAR2OID, FLOAT4TOCHAROID)						/* float4 -> nvarchar2 */
	ORA_COERCE3(FLOAT8OID, NVARCHAR2OID, FLOAT8TOCHAROID)						/* float8 -> nvarchar2 */
	ORA_COERCE3(TIMESTAMPOID, NVARCHAR2OID, TSTOCHAROID)						/* timestamp -> nvarchar2 */
	ORA_COERCE3(TIMESTAMPTZOID, NVARCHAR2OID, TSTZTOCHAROID)					/* timestamptz -> nvarchar2 */
	ORA_COERCE3(INTERVALOID, NVARCHAR2OID, INTERVALTOCHAROID)					/* interval -> nvarchar2 */
	ORA_COERCE3(NUMERICOID, NVARCHAR2OID, NUMERICTOCHAROID)						/* numeric -> nvarchar2 */

	ORA_COERCE3(INT2OID, BPCHAROID, INT4TOCHAROID)								/* int2 -> bpchar */
	ORA_COERCE3(INT4OID, BPCHAROID, INT4TOCHAROID)								/* int4 -> bpchar */
	ORA_COERCE3(INT8OID, BPCHAROID, INT8TOCHAROID)								/* int8 -> bpchar */
	ORA_COERCE3(FLOAT4OID, BPCHAROID, FLOAT4TOCHAROID)							/* float4 -> bpchar */
	ORA_COERCE3(FLOAT8OID, BPCHAROID, FLOAT8TOCHAROID)							/* float8 -> bpchar */
	ORA_COERCE3(TIMESTAMPOID, BPCHAROID, TSTOCHAROID)							/* timestamp -> bpchar */
	ORA_COERCE3(TIMESTAMPTZOID, BPCHAROID, TSTZTOCHAROID)						/* timestamptz -> bpchar */
	ORA_COERCE3(INTERVALOID, BPCHAROID, INTERVALTOCHAROID)						/* interval -> bpchar */
	ORA_COERCE3(NUMERICOID, BPCHAROID, NUMERICTOCHAROID)						/* numeric -> bpchar */

	ORA_COERCE3(INT2OID, NUMERICOID, INT2TONUMERICOID)							/* int2 -> numeric */
	ORA_COERCE3(INT4OID, NUMERICOID, INT4TONUMERICOID)							/* int4 -> numeric */
	ORA_COERCE3(INT8OID, NUMERICOID, INT8TONUMERICOID)							/* int8 -> numeric */
	ORA_COERCE3(FLOAT4OID, NUMERICOID, FLOAT4TONUMERICOID)						/* float4 -> numeric */
	ORA_COERCE3(FLOAT8OID, NUMERICOID, FLOAT8TONUMERICOID)						/* float8 -> numeric */

	ORA_COERCE3(NUMERICOID, FLOAT4OID, NUMERICTOFLOAT4OID)						/* numeric -> float4 */
	ORA_COERCE3(NUMERICOID, FLOAT8OID, NUMERICTOFLOAT8OID)						/* numeric -> float8 */
};

static const int NumOraCoerceFunction = lengthof(OraCoerceFunction);

static const OraPrecedence OraPrecedenceMap[] =
{
	ORA_PRECEDENCE(TIMESTAMPTZOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(TIMESTAMPOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(ORADATEOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(INTERNALOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(FLOAT8OID, LV_DOUBLE)
	ORA_PRECEDENCE(FLOAT4OID, LV_FLOAT)
	ORA_PRECEDENCE(NUMERICOID, LV_NUMBER)
	ORA_PRECEDENCE(TEXTOID, LV_CHARACTER)
	ORA_PRECEDENCE(VARCHAROID, LV_CHARACTER)
	ORA_PRECEDENCE(VARCHAR2OID, LV_CHARACTER)
	ORA_PRECEDENCE(NVARCHAR2OID, LV_CHARACTER)
	ORA_PRECEDENCE(BPCHAROID, LV_CHARACTER)
	ORA_PRECEDENCE(CHAROID, LV_CHARACTER)
};

static const int NumOraPrecedenceMap = lengthof(OraPrecedenceMap);

OraCoerceKind current_coerce_kind = ORA_COERCE_NOUSE;

static Oid ora_default_coerce(Oid sourceTypeId, Oid targetTypeId, bool trunc);
static Oid ora_operator_coerce(Oid sourceTypeId, Oid targetTypeId, bool trunc);
static Oid ora_function_coerce(Oid sourceTypeId, Oid targetTypeId, bool trunc);
static int get_precedence(Oid typoid);
static int ora_type_preferred(Oid typoid1, Oid typoid2);
static bool is_character_expr(Node *expr);
static bool is_arithmetic_operator(List *opname);

Oid
ora_find_coerce_func(Oid sourceTypeId, Oid targetTypeId)
{
	switch(current_coerce_kind)
	{
		case ORA_COERCE_DEFAULT:
			return ora_default_coerce(sourceTypeId, targetTypeId, false);
		case ORA_COERCE_OPERATOR:
			return ora_operator_coerce(sourceTypeId, targetTypeId, false);
		case ORA_COERCE_COMMON_FUNCTION:
			return ora_function_coerce(sourceTypeId, targetTypeId, true);
		case ORA_COERCE_SPECIAL_FUNCTION:
			return ora_function_coerce(sourceTypeId, targetTypeId, false);
		case ORA_COERCE_NOUSE:
		default:
			break;
	}
	return InvalidOid;
}

static Oid
ora_default_coerce(Oid sourceTypeId, Oid targetTypeId, bool trunc)
{
	int i;
	for (i = 0; i < NumOraCoerceDefault; i++)
	{
		if (sourceTypeId == OraCoerceDefault[i].coercesource &&
			targetTypeId == OraCoerceDefault[i].coercetarget)
		{
			if (trunc)
				return OraCoerceDefault[i].trunc_coercefunc;
			return OraCoerceDefault[i].coercefunc;
		}
	}

	return InvalidOid;
}

static Oid
ora_operator_coerce(Oid sourceTypeId, Oid targetTypeId, bool trunc)
{
	int i;
	for (i = 0; i < NumOraCoerceOperator; i++)
	{
		if (sourceTypeId == OraCoerceOperator[i].coercesource &&
			targetTypeId == OraCoerceOperator[i].coercetarget)
		{
			if (trunc)
				return OraCoerceOperator[i].trunc_coercefunc;
			return OraCoerceOperator[i].coercefunc;
		}
	}

	return ora_default_coerce(sourceTypeId, targetTypeId, trunc);
}


static Oid
ora_function_coerce(Oid sourceTypeId, Oid targetTypeId, bool trunc)
{
	int i;
	for (i = 0; i < NumOraCoerceFunction; i++)
	{
		if (sourceTypeId == OraCoerceFunction[i].coercesource &&
			targetTypeId == OraCoerceFunction[i].coercetarget)
		{
			if (trunc)
				return OraCoerceFunction[i].trunc_coercefunc;
			return OraCoerceFunction[i].coercefunc;
		}
	}

	return ora_default_coerce(sourceTypeId, targetTypeId, trunc);
}

static int
get_precedence(Oid typoid)
{
	int i;
	for (i = 0; i < NumOraPrecedenceMap; i++)
	{
		if (typoid == OraPrecedenceMap[i].typoid)
			return OraPrecedenceMap[i].level;
	}
	return LV_LEAST;
}

static int
ora_type_preferred(Oid typoid1, Oid typoid2)
{
	return get_precedence(typoid1) - get_precedence(typoid2);
}

static bool
is_character_expr(Node *expr)
{
	Oid typeId = exprType(expr);

	return IS_CHARACTER_TYPE(typeId);
}

static bool
is_arithmetic_operator(List *opname)
{
	char *nspname = NULL;
	char *objname = NULL;
	int objlen = 0;
	bool result = false;

	DeconstructQualifiedName(opname, &nspname, &objname);

	if (objname)
		objlen = strlen(objname);

	switch (objlen)
	{
		case 1:
			if (strncmp(objname, "+", objlen) == 0 ||
				strncmp(objname, "-", objlen) == 0 ||
				strncmp(objname, "*", objlen) == 0 ||
				strncmp(objname, "/", objlen) == 0)
				result = true;
			break;
		default:
			result = false;
			break;
	}

	return result;
}

void
try_coerce_in_operator(ParseState *pstate,	/* in */
					   List *opname,		/* in */
					   Node *lexpr,			/* in */
					   Node *rexpr,			/* in */
					   CoerceDrct drct,		/* in */
					   Node **ret_lexpr,	/* out */
					   Node **ret_rexpr		/* out */
					   )
{
	Oid		ltypeId = InvalidOid;
	Oid		rtypeId = InvalidOid;
	Oid		coerce_func_oid = InvalidOid;
	bool	arithmetic_op = false;

	Assert(ret_lexpr && ret_rexpr);
	if (!IsOracleParseGram(pstate) ||
		!lexpr ||
		!rexpr)
	{
		*ret_lexpr = lexpr;
		*ret_rexpr = rexpr;
		return ;
	}

	ltypeId = exprType(lexpr);
	rtypeId = exprType(rexpr);
	arithmetic_op = is_arithmetic_operator(opname);

	/*
	 * convert character data to numeric data
	 * during arithmetic operations.
	 */
	if (arithmetic_op && is_character_expr(lexpr) && is_character_expr(rexpr))
	{
		coerce_func_oid = ora_operator_coerce(ltypeId, NUMERICOID, false);
		if (OidIsValid(coerce_func_oid))
			*ret_lexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(lexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
		else
			*ret_lexpr = lexpr;

		coerce_func_oid = ora_operator_coerce(rtypeId, NUMERICOID, false);
		if (OidIsValid(coerce_func_oid))
			*ret_rexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(rexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
		else
			*ret_rexpr = rexpr;

		return ;
	}

	if (ltypeId == rtypeId)
	{
		*ret_lexpr = lexpr;
		*ret_rexpr = rexpr;
		return ;
	}

	/*
	 * try to coerce left to right.
	 */
	if (drct == ORA_COERCE_L2R ||
		drct == ORA_COERCE_S2S)
	{
		coerce_func_oid = ora_operator_coerce(ltypeId, rtypeId, false);
		if (OidIsValid(coerce_func_oid))
		{
			*ret_lexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(lexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
			*ret_rexpr = rexpr;
			return ;
		}
	}

	/*
	 * try to coerce right to left.
	 */
	if (drct == ORA_COERCE_R2L ||
		drct == ORA_COERCE_S2S)
	{
		coerce_func_oid = ora_operator_coerce(rtypeId, ltypeId, false);
		if (OidIsValid(coerce_func_oid))
		{
			*ret_lexpr = lexpr;
			*ret_rexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(rexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
			return ;
		}
	}

	/*
	 * can not coerce from any side to side
	 * return default.
	 */
	*ret_lexpr = lexpr;
	*ret_rexpr = rexpr;
	return ;
}

List *
transformNvlArgs(ParseState *pstate, List *args)
{
	List		*result = NIL;
	Node		*larg = NULL,
				*rarg = NULL,
				*cnode = NULL;
	Oid			ltypid = InvalidOid,
				rtypid = InvalidOid;
	int32		ltypmod = -1,
				rtypmod = -1,
				typmod = -1;

	Assert(pstate && args);
	Assert(list_length(args) == 2);

	larg = (Node *)linitial(args);
	rarg = (Node *)lsecond(args);
	if (exprType(larg) == UNKNOWNOID)
		larg = coerce_to_common_type(pstate, larg, TEXTOID, "NVL");
	if (exprType(rarg) == UNKNOWNOID)
		rarg = coerce_to_common_type(pstate, rarg, TEXTOID, "NVL");
	ltypid = exprType(larg);
	rtypid = exprType(rarg);
	ltypmod = exprTypmod(larg);
	rtypmod = exprTypmod(rarg);
	typmod = ORA_MAX(ltypmod, rtypmod);

	if (ltypid == rtypid)
	{
		if (ltypmod == rtypmod)
			return list_make2(larg, rarg);

		cnode = coerce_to_target_type(pstate,
									  larg,
									  ltypid,
									  ltypid,
									  typmod,
									  COERCION_IMPLICIT,
									  COERCE_IMPLICIT_CAST,
									  -1);
		if (!cnode)
			cnode = larg;
		result = lappend(result, cnode);

		cnode = coerce_to_target_type(pstate,
									  rarg,
									  rtypid,
									  ltypid,
									  typmod,
									  COERCION_IMPLICIT,
									  COERCE_IMPLICIT_CAST,
									  -1);
		if (!cnode)
			cnode = rarg;
		result = lappend(result, cnode);

		return result;
	}

	/*
	 * If expr1 is character data, then Oracle Database converts expr2 to the
	 * data type of expr1 before comparing them and returns VARCHAR2 in the
	 * character set of expr1.
	 *
	 * If expr1 is numeric, then Oracle Database determines which argument has
	 * the highest numeric precedence, implicitly converts the other argument
	 * to that data type, and returns that data type
	 */
	if (is_character_expr(larg) ||
		ora_type_preferred(ltypid, rtypid) > 0)
	{
		if (can_coerce_type(1, &rtypid, &ltypid, COERCION_IMPLICIT))
		{
			cnode = coerce_to_target_type(pstate,
										  rarg,
										  rtypid,
										  ltypid,
										  ltypmod,
										  COERCION_IMPLICIT,
										  COERCE_IMPLICIT_CAST,
										  -1);
			if (!cnode)
				cnode = rarg;
			result = lappend(result, larg);
			result = lappend(result, cnode);

			return result;
		}
		return list_make2(larg, rarg);
	}

	/*
	 * right expr has highest numeric precedence
	 */
	if (ora_type_preferred(ltypid, rtypid) < 0)
	{
		if (can_coerce_type(1, &ltypid, &rtypid, COERCION_IMPLICIT))
		{
			cnode = coerce_to_target_type(pstate,
										  larg,
										  ltypid,
										  rtypid,
										  rtypmod,
										  COERCION_IMPLICIT,
										  COERCE_IMPLICIT_CAST,
										  -1);
			if (!cnode)
				cnode = larg;
			result = lappend(result, cnode);
			result = lappend(result, rarg);

			return result;
		}
		return list_make2(larg, rarg);
	}

	/*
	 * left expr and right expr have the same numeric precedence
	 * try to implicitly converts to the data type of the right expr.
	 */
	if (can_coerce_type(1, &ltypid, &rtypid, COERCION_IMPLICIT))
	{
		cnode = coerce_to_target_type(pstate,
									  larg,
									  ltypid,
									  rtypid,
									  rtypmod,
									  COERCION_IMPLICIT,
									  COERCE_IMPLICIT_CAST,
									  -1);
		if (!cnode)
			cnode = larg;
		result = lappend(result, cnode);
		result = lappend(result, rarg);

		return result;
	}

	/*
	 * left expr and right expr have the same numeric precedence
	 * try to implicitly converts to the data type of the left expr.
	 */
	if (can_coerce_type(1, &rtypid, &ltypid, COERCION_EXPLICIT))
	{
		cnode = coerce_to_target_type(pstate,
									  rarg,
									  rtypid,
									  ltypid,
									  ltypmod,
									  COERCION_EXPLICIT,
									  COERCE_EXPLICIT_CALL,
									  -1);
		if (!cnode)
			cnode = rarg;
		result = lappend(result, larg);
		result = lappend(result, cnode);

		return result;
	}

	return list_make2(larg, rarg);
}

List *
transformNvl2Args(ParseState *pstate, List *args)
{
	List *result = NIL;
	void *arg1 = NULL,
		 *arg2 = NULL,
		 *arg3 = NULL;

	Assert(pstate && args);
	Assert(list_length(args) == 3);

	arg1 = linitial(args);
	arg2 = lsecond(args);
	arg3 = lthird(args);

	if (exprType(arg2) == UNKNOWNOID)
		arg2 = coerce_to_common_type(pstate, (Node *)arg2, TEXTOID, "NVL2");
	if (exprType(arg3) == UNKNOWNOID)
		arg3 = coerce_to_common_type(pstate, (Node *)arg3, TEXTOID, "NVL2");

	result = lcons(arg1, transformNvlArgs(pstate, list_make2(arg2, arg3)));

	return result;
}

Node *
transformOraAExprOp(ParseState * pstate,
					List *opname,
					Node *lexpr,
					Node *rexpr,
					int location)
{
	volatile bool	err = false;
	Node			*result = NULL;
	OraCoerceKind	sv_coerce_kind;

	Assert(pstate);
	Assert(lexpr && rexpr);
	Assert(IsOracleParseGram(pstate));

	sv_coerce_kind = current_coerce_kind;
	current_coerce_kind = ORA_COERCE_NOUSE;
	PG_TRY();
	{
		result = (Node *) make_op(pstate,
								  opname,
								  lexpr,
								  rexpr,
								  location);
	} PG_CATCH();
	{
		/*
		 * Here we make an error: e1
		 * but never throw it, remeber to dump or
		 * throw it before returning.
		 */
		err = true;
	} PG_END_TRY();

	if (!err)
	{
		current_coerce_kind = sv_coerce_kind;
		return result;
	}

	PG_TRY();
	{
		if (IsA(lexpr, CaseTestExpr))
			try_coerce_in_operator(pstate,
								   opname,
								   lexpr,
								   rexpr,
								   ORA_COERCE_L2R,
								   &lexpr,
								   &rexpr);
		else
			try_coerce_in_operator(pstate,
								   opname,
								   lexpr,
								   rexpr,
								   ORA_COERCE_S2S,
								   &lexpr,
								   &rexpr);

		result = (Node *) make_op(pstate,
								  opname,
								  lexpr,
								  rexpr,
								  location);
	} PG_CATCH();
	{
		current_coerce_kind = sv_coerce_kind;

		/*
		 * Here we make an error: e2
		 * dump error data "e2" then throw "e1"
		 */
		errdump();
		PG_RE_THROW();
	} PG_END_TRY();

	/*
	 * dump error data e1, see above
	 */
	errdump();
	current_coerce_kind = sv_coerce_kind;

	return result;
}

