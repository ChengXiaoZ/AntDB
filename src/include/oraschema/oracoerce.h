/*
 *
 * file:
 *		src/include/oraschema/oracoerce.h
 */
#ifndef _ORA_COERCE_H
#define _ORA_COERCE_H

#include "parser/parse_node.h"

typedef enum OraCoerceKind
{
	ORA_COERCE_DEFAULT,
	ORA_COERCE_OPERATOR,
	ORA_COERCE_COMMON_FUNCTION,
	ORA_COERCE_SPECIAL_FUNCTION,
	ORA_COERCE_NOUSE,
} OraCoerceKind;

typedef struct OraCoercePath
{
	Oid		coercesource;		/* coerce source type */
	Oid		coercetarget;		/* coerce target type */
	Oid		coercefunc;			/* coerce function */
	Oid		trunc_coercefunc;	/* trunc coerce function */
} OraCoercePath;

#define IS_CHARACTER_TYPE(typ) \
	((typ) == TEXTOID || \
	(typ) == VARCHAROID || \
	(typ) == VARCHAR2OID || \
	(typ) == BPCHAROID || \
	(typ) == NVARCHAR2OID || \
	(typ) == CHAROID)

#define IS_NUMERIC_TYPE(typ) \
	((typ) == INT2OID || \
	(typ) == INT4OID || \
	(typ) == INT8OID || \
	(typ) == FLOAT4OID || \
	(typ) == FLOAT8OID || \
	(typ) == NUMERICOID)

/* Data Type Precedence
 * Oracle uses data type precedence to determine implicit data type conversion,
 * which is discussed in the section that follows. Oracle data types take the
 * following precedence:
 *
 *		Datetime and interval data types
 *		BINARY_DOUBLE
 *		BINARY_FLOAT
 *		NUMBER
 *		Character data types
 *		All other built-in data types
 */
#define		LV_DATETIME_INTERVAL	5
#define		LV_DOUBLE 				4
#define		LV_FLOAT				3
#define		LV_NUMBER				2
#define		LV_CHARACTER			1
#define		LV_LEAST				0

typedef struct OraPrecedence
{
	Oid	typoid;
	int	level;
} OraPrecedence;

typedef enum CoerceDrct
{
	ORA_COERCE_L2R,				/* coerce from left to right */
	ORA_COERCE_R2L,				/* coerce from right to left */
	ORA_COERCE_S2S				/* coerce from side to side */
} CoerceDrct;

extern PGDLLEXPORT OraCoerceKind current_coerce_kind;

#define IsOracleCoerceFunc() \
	(current_coerce_kind == ORA_COERCE_COMMON_FUNCTION || \
	 current_coerce_kind == ORA_COERCE_SPECIAL_FUNCTION)

extern Oid ora_find_coerce_func(Oid sourceTypeId, Oid targetTypeId);

extern void try_coerce_in_operator(ParseState *pstate,	/* in */
								   List *opname,		/* in */
								   Node *lexpr,			/* in */
								   Node *rexpr,			/* in */
								   CoerceDrct drct,		/* in */ 
								   Node **ret_lexpr,	/* out */
								   Node **ret_rexpr		/* out */
								   );
extern List *transformNvlArgs(ParseState *pstate, List *args);
extern List *transformNvl2Args(ParseState *pstate, List *args);
extern Node *transformOraAExprOp(ParseState * pstate,
								 List *opname,
								 Node *lexpr,
								 Node *rexpr,
								 int location);

#endif   /* _ORA_COERCE_H */

