/*-------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#include "nodes/parsenodes.h"
#include "parser/scanner.h"

typedef enum
{
	BACKSLASH_QUOTE_OFF,
	BACKSLASH_QUOTE_ON,
	BACKSLASH_QUOTE_SAFE_ENCODING
}	BackslashQuoteType;

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern int	backslash_quote;
extern bool escape_string_warning;
extern PGDLLIMPORT bool standard_conforming_strings;

/* Primary entry point for the raw parsing functions */
extern List *raw_parser(const char *str);
#ifdef ADB
extern List *ora_raw_parser(const char *str);
#endif

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List *SystemFuncName(char *name);
extern TypeName *SystemTypeName(char *name);
extern TypeName *SystemTypeNameLocation(char *name, int location);
#ifdef ADB
extern List *OracleFuncName(char *name);
extern TypeName *OracleTypeName(char *name);
extern TypeName *OracleTypeNameLocation(char *name, int location);
extern void transformDistributeBy(DistributeBy *dbstmt);
#endif

/* function from gram.y */
extern Node *makeColumnRef(char *colname, List *indirection,
						   int location, core_yyscan_t yyscanner);
extern Node *makeTypeCast(Node *arg, TypeName *typename, int location);
extern Node *makeStringConst(char *str, int location);
extern Node *makeStringConstCast(char *str, int location, TypeName *typename);
extern Node *makeIntConst(int val, int location);
extern Node *makeFloatConst(char *str, int location);
extern Node *makeBitStringConst(char *str, int location);
extern Node *makeNullAConst(int location);
extern Node *makeAConst(Value *v, int location);
extern Node *makeBoolAConst(bool state, int location);
#ifdef ADB
extern List *check_sequence_name(List *names, core_yyscan_t yyscanner, int location);
#endif
extern void check_qualified_name(List *names, core_yyscan_t yyscanner);
extern List *check_func_name(List *names, core_yyscan_t yyscanner);
extern List *check_indirection(List *indirection, core_yyscan_t yyscanner);
extern List *extractArgTypes(List *parameters);
extern void insertSelectOptions(SelectStmt *stmt,
								List *sortClause, List *lockingClause,
								Node *limitOffset, Node *limitCount,
								WithClause *withClause,
								core_yyscan_t yyscanner);
extern Node *makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg);
extern Node *doNegate(Node *n, int location);
extern void doNegateFloat(Value *v);
extern Node *makeAArrayExpr(List *elements, int location);
extern Node *makeXmlExpr(XmlExprOp op, char *name, List *named_args,
						 List *args, int location);
extern List *mergeTableFuncParameters(List *func_args, List *columns);
extern TypeName *TableFuncTypeName(List *columns);
extern RangeVar *makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner);
extern void SplitColQualList(List *qualList,
							 List **constraintList, CollateClause **collClause,
							 core_yyscan_t yyscanner);
extern Node *makeRecursiveViewSelect(char *relname, List *aliases, Node *query);

#endif   /* PARSER_H */
