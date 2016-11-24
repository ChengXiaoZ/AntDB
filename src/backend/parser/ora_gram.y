%{

//#define YYDEBUG 1

/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/ora_gramparse.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/guc.h"

/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = YYRHSLOC(Rhs, 1); \
		else \
			(Current) = YYRHSLOC(Rhs, 0); \
	} while (0)

/*
 * The above macro assigns -1 (unknown) as the parse location of any
 * nonterminal that was reduced from an empty rule.  This is problematic
 * for nonterminals defined like
 *		OptFooList: / * EMPTY * / { ... } | OptFooList Foo { ... } ;
 * because we'll set -1 as the location during the first reduction and then
 * copy it during each subsequent reduction, leaving us with -1 for the
 * location even when the list is not empty.  To fix that, do this in the
 * action for the nonempty rule(s):
 *		if (@$ < 0) @$ = @2;
 * (Although we have many nonterminals that follow this pattern, we only
 * bother with fixing @$ like this when the nonterminal's parse location
 * is actually referenced in some rule.)
 */
 
/* ConstraintAttributeSpec yields an integer bitmask of these flags: */
#define CAS_NOT_DEFERRABLE			0x01
#define CAS_DEFERRABLE				0x02
#define CAS_INITIALLY_IMMEDIATE		0x04
#define CAS_INITIALLY_DEFERRED		0x08
#define CAS_NOT_VALID				0x10
#define CAS_NO_INHERIT				0x20

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)
#define pg_yyget_extra(yyscanner) (*((base_yy_extra_type **) (yyscanner)))

union YYSTYPE;					/* need forward reference for tok_is_keyword */
static void ora_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg);
static int ora_yylex(union YYSTYPE *lvalp, YYLTYPE *yylloc, core_yyscan_t yyscanner);
static Node *makeDeleteStmt(RangeVar *range, Alias *alias, WithClause *with, Node *where, List *returning);
static Node *reparse_decode_func(List *args, int location);
static void processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner);
static void add_alias_if_need(List *parseTree);
static Node* make_any_sublink(Node *testexpr, const char *operName, Node *subselect, int location);
#define MAKE_ANY_A_EXPR(name_, l_, r_, loc_) (Node*)makeA_Expr(AEXPR_OP_ANY, list_make1(makeString(pstrdup(name_))), l_, r_, loc_)
%}

%expect 0
%pure-parser
%name-prefix="ora_yy"
%locations

%parse-param {core_yyscan_t yyscanner}
%lex-param   {core_yyscan_t yyscanner}

%union
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	OnCommitAction		oncommit;
	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	FuncWithArgs		*funwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
/* PGXC_BEGIN */
	DistributeBy		*distby;
	PGXCSubCluster		*subclus;
/* PGXC_END */
}

/*
 * Non-keyword token types.  These are hard-wired into the "flex" lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  PL/pgsql depends on this so that it can share the
 * same lexer.  If you add/change tokens here, fix PL/pgsql to match!
 *
 * DOT_DOT is unused in the core SQL grammar, and so will always provoke
 * parse errors.  It is needed by PL/pgsql.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op

%token <ival>	ICONST PARAM

%type <ielem>	index_elem

%type <distby>	OptDistributeByInternal

%token			TYPECAST DOT_DOT COLON_EQUALS
%token			NULLS_FIRST NULLS_LAST

%type <list>	stmtblock stmtmulti opt_column_list columnList alter_table_cmds
				pgxcnodes pgxcnode_list OptRoleList

%type <list>	OptSeqOptList SeqOptList
/* %type <list>	NumericOnly_list */

%type <list>	ExclusionConstraintList ExclusionConstraintElem
%type <list>	/* generic_option_list*/ alter_generic_option_list

%type <str>		ExistingIndex 
%type <str>		generic_option_name
%type <str>		OptDistributeType
%type <node>	generic_option_arg

%type <node>	stmt ViewStmt alter_table_cmd

%type <node>	TableConstraint TableLikeClause

%type <node>	columnOptions

%type <alias>	alias_clause opt_alias_clause

%type <boolean>	opt_all opt_byte_char opt_restart_seqs opt_verbose opt_no_inherit
				opt_unique opt_concurrently opt_nowait opt_with_data

%type <dbehavior>	opt_drop_behavior

%type <defelt>	transaction_mode_item explain_option_elem def_elem reloption_elem
				SeqOptElem AlterOptRoleElem CreateOptRoleElem

%type <subclus> OptSubCluster OptSubClusterInternal
%type <distby>	OptDistributeBy

%type <istmt>	insert_rest

%type <into>  create_as_target /*into_clause*/

%type <ival>
	document_or_content
	Iconst
	opt_asc_desc
	OptTemp
	SignedIconst sub_type

%type <ival> ConstraintAttributeElem ConstraintAttributeSpec
	
%type <ival>	key_actions key_delete key_match key_update key_action
				for_locking_strength
				opt_nulls_order opt_column opt_set_data TableLikeOptionList
				TableLikeOption

%type <jexpr>	joined_table

%type <node>	join_qual
%type <jtype>	join_type

%type <objtype>	drop_type

%type <keyword>
	col_name_keyword
	reserved_keyword
	type_func_name_keyword
	unreserved_keyword

%type <list>
	any_operator any_name any_name_list attrs alter_generic_options
	case_when_list create_generic_options cte_list ctext_expr_list ctext_row
	ColQualList
	definition def_list
	explain_option_list expr_list extract_list
	from_clause from_list func_arg_list func_name for_locking_clause for_locking_items
	group_clause
	indirection insert_column_list interval_second index_params
	locked_rels_list
	multiple_set_clause
	name_list
	OptTableElementList opt_distinct opt_for_locking_clause
	opt_indirection opt_interval opt_name_list opt_sort_clause
	OptWith OptTypedTableElementList 
	opt_type_mod opt_type_modifiers opt_definition opt_collate opt_class opt_select_limit
	opt_reloptions OptInherit
	qual_Op qual_all_Op qualified_name_list
	relation_expr_list returning_clause returning_item reloption_list
	reloptions role_list
	select_limit set_clause_list set_clause set_expr_list set_expr_row
	set_target_list sortby_list sort_clause subquery_Op
	TableElementList target_list transaction_mode_list_or_empty TypedTableElementList
	transaction_mode_list /*transaction_mode_list_or_empty*/ trim_list
	var_list

%type <node>
	AexprConst a_expr AlterTableStmt alter_column_default AlterObjectSchemaStmt
	alter_using 
	b_expr
	common_table_expr columnDef columnref CreateStmt ctext_expr columnElem
	ColConstraint ColConstraintElem ConstraintAttr CreateRoleStmt
	case_default case_expr /*case_when*/ case_when_item c_expr
	ConstraintElem CreateSeqStmt CreateAsStmt
	DeleteStmt DropStmt def_arg
	ExplainStmt ExplainableStmt explain_option_arg ExclusionWhereClause
	func_arg_expr func_expr for_locking_item
	having_clause
	indirection_el InsertStmt IndexStmt
	join_outer
	limit_clause
	offset_clause opt_collate_clause
	SelectStmt select_clause select_no_parens select_with_parens set_expr
	simple_select select_limit_value select_offset_value
	TableElement TypedTableElement table_ref TruncateStmt
	TransactionStmt
	UpdateStmt
	RenameStmt
	values_clause VariableResetStmt VariableSetStmt var_value VariableShowStmt
	where_clause
	zone_value

%type <range> qualified_name relation_expr relation_expr_opt_alias

%type <sortby>	sortby

%type <defelt>	generic_option_elem alter_generic_option_elem

%type <str> all_Op attr_name access_method_clause access_method
	ColId ColLabel
	explain_option_name extract_arg
	iso_level index_name
	MathOp
	name NonReservedWord NonReservedWord_or_Sconst
	opt_boolean_or_string opt_encoding OptConsTableSpace opt_index_name
	OptTableSpace
	pgxcgroup_name pgxcnode_name
	RoleId
	Sconst
	type_function_name
	var_name

%type <target>	insert_column_item single_set_clause set_target target_item

%type <typnam>	Bit Character ConstDatetime ConstInterval ConstTypename 
				Numeric SimpleTypename Typename func_type

%type <value>	NumericOnly

%type <oncommit> OnCommitOption

%type <vsetstmt> set_rest set_rest_more

%type <with> with_clause opt_with_clause

%token <keyword> ACCESS ADD_P ALL ALTER ANALYZE ANALYSE AND ABORT_P
	ANY AS ASC AUDIT AUTHORIZATION ACTION ALWAYS
	ADMIN
	BEGIN_P BETWEEN BFILE BIGINT BINARY_FLOAT BINARY_DOUBLE BLOB BOOLEAN_P BOTH BY BYTE_P
	CASCADE CASE CAST CATALOG_P CHAR_P CHARACTERISTICS CHECK CLUSTER COLUMN COMMIT COMMENT 
	COLLATION CONVERSION_P CONNECTION
	COMMITTED COMPRESS COLLATE CONSTRAINT CYCLE NOCYCLE
	CONSTRAINTS CLOB COALESCE CONTENT_P CONTINUE_P CREATE CROSS CURRENT_DATE 
	CURRENT_P CURRENT_TIMESTAMP CURRVAL CURSOR CONCURRENTLY CONFIGURATION
	CACHE NOCACHE COMMENTS
	DATE_P DAY_P DBTIMEZONE_P DEC DECIMAL_P DEFAULT DEFERRABLE DELETE_P DESC DISTINCT 
	DO DOCUMENT_P DOUBLE_P DROP DEFERRED DATA_P DEFAULTS
	
	/* PGXC_BEGIN */
	DISABLE_P DISTRIBUTE PREPARE PREPARED DOMAIN_P DICTIONARY
	/* PGXC_END */
	
	ELSE END_P ESCAPE EXCLUSIVE EXISTS EXPLAIN EXTRACT
	ENABLE_P EXCLUDE EVENT EXTENSION EXCLUDING ENCRYPTED
	FALSE_P FILE_P FLOAT_P FOR FROM FOREIGN FULL
	GLOBAL GRANT GREATEST GROUP_P HAVING
	HOUR_P
	IDENTIFIED IF_P IMMEDIATE IN_P INCREMENT INDEX INITIAL_P INSERT INHERIT INITIALLY 
	INHERITS INCLUDING INDEXES INNER_P
	IDENTITY_P INTEGER INTERSECT INTO INTERVAL INT_P IS ISOLATION
	JOIN
	KEY
	LEADING LEAST LEFT LEVEL LIMIT LIKE LOCAL LOCALTIMESTAMP LOCK_P LOG_P LONG_P
	MATERIALIZED MAXEXTENTS MINUS MINUTE_P MLSLABEL MODE MODIFY MONTH_P
	MATCH MAXVALUE NOMAXVALUE  MINVALUE NOMINVALUE
	NAMES NCHAR NCLOB NEXT NEXTVAL NOAUDIT NOCOMPRESS NOT NOWAIT NULL_P NULLIF NUMBER_P 
	NUMERIC NVARCHAR2 NO
	/* PGXC add NODE token */
	NODE
	OF OFF OFFLINE OFFSET ON ONLINE ONLY OPERATOR OPTION OR ORDER OUTER_P
	OWNER OIDS OPTIONS OWNED
	PCTFREE PRECISION PRESERVE PRIOR PRIVILEGES PUBLIC PURGE
	PARTIAL PRIMARY PARSER PASSWORD
	RAW READ REAL RECURSIVE RENAME REPLACE REPEATABLE RESET RESOURCE RESTART RESTRICT
	RETURNING RETURN_P REVOKE REUSE RIGHT ROLE ROLLBACK ROW ROWID ROWNUM ROWS
	REFERENCES REPLICA RULE RELEASE
	SCHEMA SECOND_P SELECT SERIALIZABLE SESSION SESSIONTIMEZONE SET SHARE SHOW SIZE SEARCH
	SMALLINT SIMPLE SETOF STATISTICS SAVEPOINT SEQUENCE SYSID SOME
	SNAPSHOT START STORAGE SUCCESSFUL SYNONYM SYSDATE SYSTIMESTAMP
	TABLE TEMP TEMPLATE TEMPORARY THEN TIME TIMESTAMP TO TRAILING 
	TRANSACTION TREAT TRIM TRUNCATE TRIGGER TRUE_P TABLESPACE
	TYPE_P TEXT_P
	UID UNCOMMITTED UNION UNIQUE UPDATE USER USING UNLOGGED
	UNENCRYPTED UNTIL
	VALIDATE VALUES VARCHAR VARCHAR2 VERBOSE VIEW VALID
	WHEN WHENEVER WHERE WITH WRITE WITHOUT WORK
	XML_P
	YEAR_P
	ZONE

/*
 * same specific token
 */
%token	ORACLE_JOIN_OP

/* Precedence: lowest to highest */
%right	RETURN_P RETURNING PRIMARY
%right	HI_THEN_RETURN LO_THEN_LIMIT
%right	LIMIT OFFSET
%left	HI_THEN_LIMIT
%left		UNION /*EXCEPT*/
//%left		INTERSECT
%nonassoc	CASE SOME
%left		WHEN END_P
%left		OR
%left		AND
%right		NOT
%right		'='
%nonassoc	'<' '>'
%nonassoc	LIKE //ILIKE SIMILAR
%nonassoc	ESCAPE
//%nonassoc	OVERLAPS
%nonassoc	BETWEEN
%nonassoc	IN_P
%left		POSTFIXOP		/* dummy for postfix Op rules */
/*
 * To support target_el without AS, we must give IDENT an explicit priority
 * between POSTFIXOP and Op.  We can safely assign the same priority to
 * various unreserved keywords as needed to resolve ambiguities (this can't
 * have any bad effects since obviously the keywords will still behave the
 * same as if they weren't keywords).  We need to do this for PARTITION,
 * RANGE, ROWS to support opt_existing_window_name; and for RANGE, ROWS
 * so that they can follow a_expr without creating postfix-operator problems;
 * and for NULL so that it can follow b_expr in ColQualList without creating
 * postfix-operator problems.
 *
 * The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
 * are even messier: since UNBOUNDED is an unreserved keyword (per spec!),
 * there is no principled way to distinguish these from the productions
 * a_expr PRECEDING/FOLLOWING.  We hack this up by giving UNBOUNDED slightly
 * lower precedence than PRECEDING and FOLLOWING.  At present this doesn't
 * appear to cause UNBOUNDED to be treated differently from other unreserved
 * keywords anywhere else in the grammar, but it's definitely risky.  We can
 * blame any funny behavior of UNBOUNDED on the SQL standard, though.
 */
//%nonassoc	UNBOUNDED		/* ideally should have same precedence as IDENT */
%nonassoc	IDENT NULL_P /*PARTITION RANGE*/ ROWS //PRECEDING FOLLOWING
%left		Op OPERATOR		/* multi-character ops and user-defined operators */
//%nonassoc	NOTNULL
//%nonassoc	ISNULL
%nonassoc	IS				/* sets precedence for IS NULL, etc */
%left		'+' '-'
%left		'*' '/' '%'
%left		'^'
//%right SOME
/* Unary Operators */
%left		AT				/* sets precedence for AT TIME ZONE */
%left		COLLATE
%right		UMINUS
//%left		'[' ']'
%left		'(' ')'
%left		TYPECAST
%left		'.'
/*
 * These might seem to be low-precedence, but actually they are not part
 * of the arithmetic hierarchy at all in their use as JOIN operators.
 * We make them high-precedence to support their use as function names.
 * They wouldn't be given a precedence at all, were it not that we need
 * left-associativity among the JOIN rules themselves.
 */
%left		JOIN CROSS LEFT FULL RIGHT INNER_P //NATURAL
/* kluge to keep xml_whitespace_option from causing shift/reduce conflicts */
//%right		PRESERVE STRIP_P

%%
stmtblock: stmtmulti
		{
			ora_yyget_extra(yyscanner)->parsetree = $1;
			if(ora_yyget_extra(yyscanner)->has_no_alias_subquery)
				add_alias_if_need($1);
		}
	;

stmtmulti: stmtmulti ';' stmt
		{
#ifdef ADB
			BaseStmt *base;
			if ($1 != NIL)
			{
				base = (BaseStmt *)llast($1);
				base->endpos = @2;
			}
#endif
			if($3)
				$$ = lappend($1, $3);
			else
				$$ = $1;
		}
	| stmt
		{
			if($1)
				$$ = list_make1($1);
			else
				$$ = NIL;
		}
	;

stmt:
	  AlterTableStmt
	| AlterObjectSchemaStmt
	| CreateAsStmt
	| CreateStmt
	| CreateSeqStmt
	| CreateRoleStmt
	| DeleteStmt
	| DropStmt
	| ExplainStmt
	| InsertStmt
	| IndexStmt
	| RenameStmt
	| SelectStmt
	| TruncateStmt
	| TransactionStmt
	| UpdateStmt
	| VariableResetStmt
	| VariableShowStmt
	| VariableSetStmt
	| ViewStmt
	| /* empty */	{ $$ = NULL; }
	;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname AS SelectStmt [ WITH [NO] DATA ]
 *
 *
 * Note: SELECT ... INTO is a now-deprecated alternative for this.
 *
 *****************************************************************************/

CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
#ifdef ADB
					ctas->grammar = PARSE_GRAM_POSTGRES;
#endif /* ADB */
					ctas->query = $6;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
					$4->skipData = !($7);
					$$ = (Node *) ctas;
				}
		;

create_as_target:
			qualified_name opt_column_list OptWith OnCommitOption OptTableSpace
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;
					$$->tableSpaceName = $5;
					$$->viewQuery = NULL;
					$$->skipData = false;		/* might get changed later */
/* PGXC_BEGIN */
					$$->distributeby = $6;
					$$->subcluster = $7;
/* PGXC_END */
				}
		;

opt_with_data:
			 /*EMPTY*/								{ $$ = TRUE; }
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS role
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE ROLE RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_ROLE;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

OptRoleList:
			OptRoleList CreateOptRoleElem			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

CreateOptRoleElem:
			AlterOptRoleElem			{ $$ = $1; }
			/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (Node *)makeInteger($2));
				}
			| ADMIN role_list
				{
					$$ = makeDefElem("adminmembers", (Node *)$2);
				}
			| ROLE role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2);
				}
			| IN_P ROLE role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3);
				}
			| IN_P GROUP_P role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3);
				}
		;

role_list:	RoleId
					{ $$ = list_make1(makeString($1)); }
			| role_list ',' RoleId
					{ $$ = lappend($1, makeString($3)); }
		;

AlterOptRoleElem:
			PASSWORD Sconst
				{
					$$ = makeDefElem("password",
									 (Node *)makeString($2));
				}
			| PASSWORD NULL_P
				{
					$$ = makeDefElem("password", NULL);
				}
			| ENCRYPTED PASSWORD Sconst
				{
					$$ = makeDefElem("encryptedPassword",
									 (Node *)makeString($3));
				}
			| UNENCRYPTED PASSWORD Sconst
				{
					$$ = makeDefElem("unencryptedPassword",
									 (Node *)makeString($3));
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(TRUE));
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($3));
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (Node *)makeString($3));
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2);
				}
			| IDENT
				{
					/*
					 * We handle identifiers that aren't parser keywords with
					 * the following special-case codes, to avoid bloating the
					 * size of the main parser.
					 */
					if (strcmp($1, "superuser") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nosuperuser") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "createuser") == 0)
					{
						/* For backwards compatibility, synonym for SUPERUSER */
						$$ = makeDefElem("superuser", (Node *)makeInteger(TRUE));
					}
					else if (strcmp($1, "nocreateuser") == 0)
					{
						/* For backwards compatibility, synonym for SUPERUSER */
						$$ = makeDefElem("superuser", (Node *)makeInteger(FALSE));
					}
					else if (strcmp($1, "createrole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nocreaterole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "replication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "noreplication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "createdb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nocreatedb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "login") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nologin") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "noinherit") == 0)
					{
						/*
						 * Note that INHERIT is a keyword, so it's handled by main parser, but
						 * NOINHERIT is handled here.
						 */
						$$ = makeDefElem("inherit", (Node *)makeInteger(FALSE));
					}
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized role option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;

/*****************************************************************************
 *
 * ALTER THING name SET SCHEMA name
 *
 *****************************************************************************/

AlterObjectSchemaStmt:
			ALTER VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name RENAME TO newname
 *
 *****************************************************************************/

RenameStmt: ALTER INDEX qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}	
		;

/*****************************************************************************
 *
 *	ALTER [ TABLE | INDEX | SEQUENCE | VIEW | MATERIALIZED VIEW ] variations
 *
 * Note: we accept all subcommands for each of the five variants, and sort
 * out what's really legal at execution time.
 *****************************************************************************/

AlterTableStmt:
				ALTER INDEX qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER INDEX IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			|	ALTER VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			|	ALTER TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

alter_table_cmds:
			alter_table_cmd							{ $$ = list_make1($1); }
			| alter_table_cmds ',' alter_table_cmd	{ $$ = lappend($1, $3); }
		;

alter_table_cmd:
			/* ALTER TABLE <name> ADD <coldef> */
			ADD_P '(' columnDef ')'
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN <coldef> */
			| ADD_P COLUMN columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
			| ALTER opt_column ColId alter_column_default
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ColumnDefault;
					n->name = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL */
			| ALTER opt_column ColId DROP NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL */
			| ALTER opt_column ColId SET NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS <SignedIconst> */
			| ALTER opt_column ColId SET STATISTICS SignedIconst
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStatistics;
					n->name = $3;
					n->def = (Node *) makeInteger($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
			| ALTER opt_column ColId SET STORAGE ColId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStorage;
					n->name = $3;
					n->def = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE] */
			| DROP opt_column IF_P EXISTS ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
			| DROP opt_column ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/*
			 * ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
			 *		[ USING <expression> ]
			 */
			| ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					/* We only use these three fields of the ColumnDef node */
					def->typeName = $6;
					def->collClause = (CollateClause *) $7;
					def->raw_default = $8;
					$$ = (Node *)n;
				}
			| MODIFY opt_column ColId Typename opt_collate_clause alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					/* We only use these three fields of the ColumnDef node */
					def->typeName = $4;
					def->collClause = (CollateClause *) $5;
					def->raw_default = $6;
					$$ = (Node *)n;
				}	
			/* ALTER FOREIGN TABLE <name> ALTER [COLUMN] <colname> OPTIONS */
			| ALTER opt_column ColId alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AlterColumnGenericOptions;
					n->name = $3;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD CONSTRAINT ... */
			| ADD_P TableConstraint
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddConstraint;
					n->def = $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> VALIDATE CONSTRAINT ... */
			| VALIDATE CONSTRAINT name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ValidateConstraint;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITH OIDS  */
			| SET WITH OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT OIDS  */
			| SET WITHOUT OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> CLUSTER ON <indexname> */
			| CLUSTER ON name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ClusterOn;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT CLUSTER */
			| SET WITHOUT CLUSTER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropCluster;
					n->name = NULL;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER <trig> */
			| ENABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS TRIGGER <trig> */
			| ENABLE_P ALWAYS TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA TRIGGER <trig> */
			| ENABLE_P REPLICA TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER ALL */
			| ENABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER USER */
			| ENABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER <trig> */
			| DISABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER ALL */
			| DISABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER USER */
			| DISABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE RULE <rule> */
			| ENABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS RULE <rule> */
			| ENABLE_P ALWAYS RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA RULE <rule> */
			| ENABLE_P REPLICA RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE RULE <rule> */
			| DISABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> INHERIT <parent> */
			| INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddInherit;
					n->def = (Node *) $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO INHERIT <parent> */
			| NO INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropInherit;
					n->def = (Node *) $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OF <type_name> */
			| OF any_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					TypeName *def = makeTypeNameFromNameList($2);
					def->location = @2;
					n->subtype = AT_AddOf;
					n->def = (Node *) def;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NOT OF */
			| NOT OF
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOf;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OWNER TO RoleId */
			| OWNER TO RoleId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ChangeOwner;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET TABLESPACE <tablespacename> */
			| SET TABLESPACE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetTableSpace;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET (...) */
			| SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> RESET (...) */
			| RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			| alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_GenericOptions;
					n->def = (Node *)$1;
					$$ = (Node *) n;
				}
/* PGXC_BEGIN */
			/* ALTER TABLE <name> DISTRIBUTE BY ... */
			| OptDistributeByInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DistributeBy;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> TO [ NODE (nodelist) | GROUP groupname ] */
			| OptSubClusterInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SubCluster;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD NODE (nodelist) */
			| ADD_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DELETE NODE (nodelist) */
			| DELETE_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DeleteNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
/* PGXC_END */
		;

opt_set_data: SET DATA_P							{ $$ = 1; }
			| /*EMPTY*/								{ $$ = 0; }
		;

alter_using:
			USING a_expr				{ $$ = $2; }
			| /* EMPTY */				{ $$ = NULL; }
		;

/* Options definition for ALTER FDW, SERVER and USER MAPPING */
alter_generic_options:
			OPTIONS	'(' alter_generic_option_list ')'		{ $$ = $3; }
		;

alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = list_make1($1);
				}
			| alter_generic_option_list ',' alter_generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1;
				}
			| SET generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_SET;
				}
			| ADD_P generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_ADD;
				}
			| DROP generic_option_name
				{
					$$ = makeDefElemExtended(NULL, $2, NULL, DEFELEM_DROP);
				}
		;

generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = makeDefElem($1, $2);
				}
		;

generic_option_name:
				ColLabel			{ $$ = $1; }
		;

/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
				Sconst				{ $$ = (Node *) makeString($1); }
		;

opt_collate_clause:
			COLLATE any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| /* EMPTY */				{ $$ = NULL; }
		;

/*
 * For the distribution type, we use IDENT to limit the impact of keywords
 * related to distribution on other commands and to allow extensibility for
 * new distributions.
 */
OptDistributeType: IDENT							{ $$ = $1; }
		;

OptDistributeByInternal:  DISTRIBUTE BY OptDistributeType
				{
					DistributeBy *n = makeNode(DistributeBy);
					if (strcmp($3, "replication") == 0)
						n->disttype = DISTTYPE_REPLICATION;
					else if (strcmp($3, "roundrobin") == 0)
						n->disttype = DISTTYPE_ROUNDROBIN;
                    else
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("unrecognized distribution option \"%s\"", $3)));
					n->colname = NULL;
					$$ = n;
				}
			| DISTRIBUTE BY func_name '(' func_arg_list ')'
				{
					DistributeBy *n = makeNode(DistributeBy);
					n->disttype = DISTTYPE_USER_DEFINED;
					n->funcname = $3;
					n->funcargs = $5;
					transformDistributeBy(n);
					$$ = n;
				}
		;
		
OptSubCluster: OptSubClusterInternal				{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSubClusterInternal:
			TO NODE pgxcnodes
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_NODE;
					n->members = $3;
					$$ = n;
				}
			| TO GROUP_P pgxcgroup_name
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_GROUP;
					n->members = list_make1(makeString($3));
					$$ = n;
				}
		;

pgxcnode_name:
			ColId							{ $$ = $1; }
			;

pgxcgroup_name:
			ColId							{ $$ = $1; }
		;

pgxcnodes:
			'(' pgxcnode_list ')'			{ $$ = $2; }
		;

pgxcnode_list:
			pgxcnode_list ',' pgxcnode_name		{ $$ = lappend($1, makeString($3)); }
			| pgxcnode_name						{ $$ = list_make1(makeString($1)); }
		;

/* ConstraintElem specifies constraint syntax which is not embedded into
 *	a column definition. ColConstraintElem specifies the embedded form.
 * - thomas 1997-12-03
 */
TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					Constraint *n = (Constraint *) $3;
					Assert(IsA(n, Constraint));
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ConstraintElem						{ $$ = $1; }
		;

ConstraintElem:
			CHECK '(' a_expr ')' ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					processCASbits($5, @5, "CHECK",
								   NULL, NULL, &n->skip_validation,
								   &n->is_no_inherit, yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
			| UNIQUE '(' columnList ')' opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->options = $5;
					n->indexname = NULL;
					n->indexspace = $6;
					processCASbits($7, @7, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| UNIQUE ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $2;
					n->indexspace = NULL;
					processCASbits($3, @3, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY '(' columnList ')' opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $4;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = $7;
					processCASbits($8, @8, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $3;
					n->indexspace = NULL;
					processCASbits($4, @4, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| EXCLUDE access_method_clause '(' ExclusionConstraintList ')'
				opt_definition OptConsTableSpace ExclusionWhereClause
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_EXCLUSION;
					n->location = @1;
					n->access_method	= $2;
					n->exclusions		= $4;
					n->options			= $6;
					n->indexname		= NULL;
					n->indexspace		= $7;
					n->where_clause		= $8;
					processCASbits($9, @9, "EXCLUDE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					processCASbits($11, @11, "FOREIGN KEY",
								   &n->deferrable, &n->initdeferred,
								   &n->skip_validation, NULL,
								   yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
		;

ExistingIndex:   USING INDEX index_name				{ $$ = $3; }
		;

ExclusionWhereClause:
			WHERE '(' a_expr ')'					{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ExclusionConstraintList:
			ExclusionConstraintElem					{ $$ = list_make1($1); }
			| ExclusionConstraintList ',' ExclusionConstraintElem
													{ $$ = lappend($1, $3); }
		;

ExclusionConstraintElem: index_elem WITH any_operator
			{
				$$ = list_make2($1, $3);
			}
			/* allow OPERATOR() decoration for the benefit of ruleutils.c */
			| index_elem WITH OPERATOR '(' any_operator ')'
			{
				$$ = list_make2($1, $5);
			}
		;

ConstraintAttributeSpec:
			/*EMPTY*/
				{ $$ = 0; }
			| ConstraintAttributeSpec ConstraintAttributeElem
				{
					/*
					 * We must complain about conflicting options.
					 * We could, but choose not to, complain about redundant
					 * options (ie, where $2's bit is already set in $1).
					 */
					int		newspec = $1 | $2;

					/* special message for this case */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)) == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 parser_errposition(@2)));
					/* generic message for other conflicts */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE) ||
						(newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)) == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting constraint properties"),
								 parser_errposition(@2)));
					$$ = newspec;
				}
		;

ConstraintAttributeElem:
			NOT DEFERRABLE					{ $$ = CAS_NOT_DEFERRABLE; }
			| DEFERRABLE					{ $$ = CAS_DEFERRABLE; }
			| INITIALLY IMMEDIATE			{ $$ = CAS_INITIALLY_IMMEDIATE; }
			| INITIALLY DEFERRED			{ $$ = CAS_INITIALLY_DEFERRED; }
			| NOT VALID						{ $$ = CAS_NOT_VALID; }
			| NO INHERIT					{ $$ = CAS_NO_INHERIT; }
		;

RoleId:		NonReservedWord							{ $$ = $1; }
		;

opt_column: COLUMN									{ $$ = COLUMN; }
			| /*EMPTY*/								{ $$ = 0; }
		;

alter_column_default:
			SET DEFAULT a_expr			{ $$ = $3; }
			| DROP DEFAULT				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/

CreateSeqStmt:
			CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$4->relpersistence = $2;
					n->sequence = $4;
					n->options = $5;
					n->ownerId = InvalidOid;
/* PGXC_BEGIN */
					n->is_serial = false;
/* PGXC_END */
					$$ = (Node *)n;
				}
		;

		
		
OptSeqOptList: SeqOptList							{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;

SeqOptElem: CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2);
				}
			| NOCACHE
				{
					$$ = makeDefElem("cache", (Node *)makeInteger(20));
				}	
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(TRUE));
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE));
				}
			| NOCYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE));
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (Node *)$3);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (Node *)$2);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (Node *)$2);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL);
				}
			| NOMAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL);
				}
			| NOMINVALUE
				{
					$$ = makeDefElem("minvalue", NULL);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (Node *)$3);
				}
			| RESTART
				{
					$$ = makeDefElem("restart", NULL);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3);
				}
		;

opt_with:	WITH									{}
			| /*EMPTY*/								{}
		;
		
opt_by:		BY				{}
			| /* empty */	{}
	  ;

/* NumericOnly_list:	NumericOnly						{ $$ = list_make1($1); }
				| NumericOnly_list ',' NumericOnly	{ $$ = lappend($1, $3); }
		; */

/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 * Note: we cannot put TABLESPACE clause after WHERE clause unless we are
 * willing to make TABLESPACE a fully reserved word.
 *****************************************************************************/

IndexStmt:	CREATE opt_unique INDEX opt_concurrently opt_index_name
			ON qualified_name access_method_clause '(' index_params ')'
		    opt_reloptions OptTableSpace where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->unique = $2;
					n->concurrent = $4;
					n->idxname = $5;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
					n->options = $12;
					n->tableSpace = $13;
					n->whereClause = $14;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
		;
		
opt_unique:
			UNIQUE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;	
opt_concurrently:
			CONCURRENTLY							{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;
opt_index_name:
			index_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
index_name: ColId									{ $$ = $1; };
access_method_clause:
			USING access_method						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = DEFAULT_INDEX_TYPE; }
		;
access_method:
			ColId									{ $$ = $1; };

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }
			
/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem:	ColId opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| func_expr opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->indexcolname = NULL;
					$$->collation = $4;
					$$->opclass = $5;
					$$->ordering = $6;
					$$->nulls_ordering = $7;
				}
		;
		
opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_class:	any_name								{ $$ = $1; }
			| USING any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_nulls_order: NULLS_FIRST				{ $$ = SORTBY_NULLS_FIRST; }
			| NULLS_LAST					{ $$ = SORTBY_NULLS_LAST; }
			| /*EMPTY*/						{ $$ = SORTBY_NULLS_DEFAULT; }
		;

reloptions:
			'(' reloption_list ')'					{ $$ = $2; }
		;

opt_reloptions:		WITH reloptions					{ $$ = $2; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

reloption_list:
			reloption_elem							{ $$ = list_make1($1); }
			| reloption_list ',' reloption_elem		{ $$ = lappend($1, $3); }
		;

/* This should match def_elem and also allow qualified names */
reloption_elem:
			ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL);
				}
			| ColLabel '.' ColLabel '=' def_arg
				{
					$$ = makeDefElemExtended($1, $3, (Node *) $5,
											 DEFELEM_UNSPEC);
				}
			| ColLabel '.' ColLabel
				{
					$$ = makeDefElemExtended($1, $3, NULL, DEFELEM_UNSPEC);
				}
		;

/* Note: any simple identifier will be returned as a type name! */
def_arg:	func_type						{ $$ = (Node *)$1; }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
		;

/*
 * We would like to make the %TYPE productions here be ColId attrs etc,
 * but that causes reduce/reduce conflicts.  type_function_name
 * is next best choice.
 */
func_type:	Typename								{ $$ = $1; }
			| type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = true;
					$$->location = @1;
				}
			| SETOF type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($2), $3));
					$$->pct_type = true;
					$$->setof = TRUE;
					$$->location = @2;
				}
		;

qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

sub_type:	ANY										{ $$ = ANY_SUBLINK; }
			/*
			 * in oracle "some" is not keyword, we can not put here
			 * | SOME									{ $$ = ANY_SUBLINK; }*/
			| ALL									{ $$ = ALL_SUBLINK; }
		;

subquery_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
			| LIKE
					{ $$ = list_make1(makeString("~~")); }
			| NOT LIKE
					{ $$ = list_make1(makeString("!~~")); }
			/*| ILIKE
					{ $$ = list_make1(makeString("~~*")); }
			| NOT ILIKE
					{ $$ = list_make1(makeString("!~~*")); }*/
/* cannot put SIMILAR TO here, because SIMILAR TO is a hack.
 * the regular expression is preprocessed by a function (similar_escape),
 * and the ~ operator for posix regular expressions is used.
 *        x SIMILAR TO y     ->    x ~ similar_escape(y)
 * this transformation is made on the fly by the parser upwards.
 * however the SubLink structure which handles any/some/all stuff
 * is not ready for such a thing.
 */
			;

OptTableSpace:   TABLESPACE name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/**********************************************************/

alias_clause:
			AS ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					$$->colnames = $4;
				}
			| AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
				}
			| ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
					$$->colnames = $3;
				}
			| ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
				}
		;

all_Op: Op
	| MathOp
	;

any_operator: all_Op 			{ $$ = list_make1(makeString($1)); }
	| ColId '.' any_operator 	{ $$ = lcons(makeString($1), $3); }
	;

analyze_keyword:
		ANALYZE									{}
		| ANALYSE /* British */					{}
	;

a_expr:	c_expr
	| a_expr TYPECAST Typename
		{ $$ = makeTypeCast($1, $3, @2); }
	| a_expr AT TIME ZONE a_expr			%prec AT
		{
			FuncCall *n = makeNode(FuncCall);
			n->funcname = SystemFuncName("timezone");
			n->args = list_make2($5, $1);
			n->agg_order = NIL;
			n->agg_star = FALSE;
			n->agg_distinct = FALSE;
			n->func_variadic = FALSE;
			n->over = NULL;
			n->location = @2;
			$$ = (Node *) n;
		}
	| '+' a_expr						%prec UMINUS
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
	| '-' a_expr						%prec UMINUS
		{ $$ = doNegate($2, @1); }
	| a_expr '+' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
	| a_expr '-' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
	| a_expr '*' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
	| a_expr '/' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
	| a_expr '%' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
	| a_expr '^' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
	| a_expr '<' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
	| a_expr '>' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
	| a_expr '=' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }

	| a_expr qual_Op a_expr				%prec Op
		{
			char *nspname = NULL;
			char *objname = NULL;

			DeconstructQualifiedName($2, &nspname, &objname);
			if (objname &&
				strncmp(objname, "||", strlen(objname)) == 0)
			{	
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("concat");
				n->args = list_make2($1, $3);
				n->location = @1;
				$$ = (Node *)n;
			} else
			{
				$$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2);
			}
		}
	| qual_Op a_expr					%prec Op
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
	| a_expr qual_Op					%prec POSTFIXOP
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }

	| a_expr AND a_expr
		{ $$ = (Node *) makeA_Expr(AEXPR_AND, NIL, $1, $3, @2); }
	| a_expr OR a_expr
		{ $$ = (Node *) makeA_Expr(AEXPR_OR, NIL, $1, $3, @2); }
	| NOT a_expr
		{ $$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, $2, @1); }
	| a_expr LIKE a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, $3, @2); }
	| a_expr LIKE a_expr ESCAPE a_expr
		{
			FuncCall *n = makeNode(FuncCall);
			n->funcname = SystemFuncName("like_escape");
			n->args = list_make2($3, $5);
			n->agg_order = NIL;
			n->agg_star = FALSE;
			n->agg_distinct = FALSE;
			n->func_variadic = FALSE;
			n->over = NULL;
			n->location = @2;
			$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, (Node *) n, @2);
		}
	| a_expr NOT LIKE a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, $4, @2); }
	| a_expr NOT LIKE a_expr ESCAPE a_expr
		{
			FuncCall *n = makeNode(FuncCall);
			n->funcname = SystemFuncName("like_escape");
			n->args = list_make2($4, $6);
			n->agg_order = NIL;
			n->agg_star = FALSE;
			n->agg_distinct = FALSE;
			n->func_variadic = FALSE;
			n->over = NULL;
			n->location = @2;
			$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, (Node *) n, @2);
		}
	| a_expr IS NULL_P							%prec IS
		{
			NullTest *n = makeNode(NullTest);
			n->arg = (Expr *) $1;
			n->nulltesttype = IS_NULL;
			$$ = (Node *) n;
		}
	| a_expr IS NOT NULL_P						%prec IS
		{
			NullTest *n = makeNode(NullTest);
			n->arg = (Expr *) $1;
			n->nulltesttype = IS_NOT_NULL;
			$$ = (Node *) n;
		}
	| a_expr BETWEEN b_expr AND b_expr			%prec BETWEEN
		{
			$$ = (Node *) makeA_Expr(AEXPR_AND, NIL,
				(Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2),
				(Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $5, @2),
									 @2);
		}
	| a_expr NOT BETWEEN b_expr AND b_expr		%prec BETWEEN
		{
			$$ = (Node *) makeA_Expr(AEXPR_OR, NIL,
				(Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $4, @2),
				(Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $6, @2),
									 @2);
		}
	| a_expr IN_P select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subselect = $3;
			n->subLinkType = ANY_SUBLINK;
			n->testexpr = $1;
			n->operName = list_make1(makeString("="));
			n->location = @2;
			$$ = (Node*)n;
		}
	| a_expr IN_P '(' expr_list ')'
		{
			/*$$ = (Node*)makeSimpleA_Expr(AEXPR_IN, "=", $1, (Node*)$4, @2);*/
			ListCell *lc = NULL;
			Node *result = NULL;

			result = (Node *)makeSimpleA_Expr(AEXPR_OP, "=", $1, (Node *)linitial($4), @2);
			lc = lnext(list_head($4));
			for_each_cell (lc, lc)
			{
				result = (Node*)makeA_Expr(AEXPR_OR,
										   NIL,
										   result,
										   (Node *)makeSimpleA_Expr(AEXPR_OP, "=", $1, (Node *)lfirst(lc), @2),
										   @2);
			}
			$$ = result;
		}
	| a_expr NOT IN_P select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subselect = $4;
			n->subLinkType = ANY_SUBLINK;
			n->testexpr = $1;
			n->operName = list_make1(makeString("="));
			n->location = @3;
			$$ = (Node*)makeA_Expr(AEXPR_NOT, NIL, NULL, (Node*)n, @2);
		}
	| a_expr NOT IN_P '(' expr_list ')'
		{
			/*$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, (Node*)$5, @2);*/
			ListCell *lc = NULL;
			Node *result = NULL;

			result = (Node *)makeSimpleA_Expr(AEXPR_OP, "<>", $1, (Node *)linitial($5), @2);
			lc = lnext(list_head($5));
			for_each_cell (lc, lc)
			{
				result = (Node*)makeA_Expr(AEXPR_AND,
										   NIL,
										   result,
										   (Node *)makeSimpleA_Expr(AEXPR_OP, "<>", $1, (Node *)lfirst(lc), @2),
										   @2);
			}
			$$ = result;
		}
	| a_expr subquery_Op sub_type select_with_parens	%prec Op
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = $3;
			n->testexpr = $1;
			n->operName = $2;
			n->subselect = $4;
			n->location = @2;
			$$ = (Node *)n;
		}
	| a_expr qual_Op SOME select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = ANY_SUBLINK;
			n->testexpr = $1;
			n->operName = $2;
			n->subselect = $4;
			n->location = @2;
			$$ = (Node *)n;
		}
	| a_expr '+' SOME select_with_parens		{ $$ = make_any_sublink($1, "+", $4, @2); }
	| a_expr '-' SOME select_with_parens		{ $$ = make_any_sublink($1, "-", $4, @2); }
	| a_expr '*' SOME select_with_parens		{ $$ = make_any_sublink($1, "*", $4, @2); }
	| a_expr '/' SOME select_with_parens		{ $$ = make_any_sublink($1, "/", $4, @2); }
	| a_expr '%' SOME select_with_parens		{ $$ = make_any_sublink($1, "%", $4, @2); }
	| a_expr '^' SOME select_with_parens		{ $$ = make_any_sublink($1, "^", $4, @2); }
	| a_expr '<' SOME select_with_parens		{ $$ = make_any_sublink($1, "<", $4, @2); }
	| a_expr '>' SOME select_with_parens		{ $$ = make_any_sublink($1, ">", $4, @2); }
	| a_expr '=' SOME select_with_parens		{ $$ = make_any_sublink($1, "=", $4, @2); }
	| a_expr LIKE SOME select_with_parens		{ $$ = make_any_sublink($1, "~~", $4, @2); }
	| a_expr NOT LIKE SOME select_with_parens	{ $$ = make_any_sublink($1, "!~~", $5, @2); }
	| a_expr subquery_Op sub_type '(' a_expr ')'
		{
			if ($3 == ANY_SUBLINK)
				$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
			else
				$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, $5, @2);
		}
	| a_expr qual_Op SOME '(' a_expr ')'
		{
			$$ = (Node*) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
		}
	| a_expr '+' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("+", $1, $5, @2); }
	| a_expr '*' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("*", $1, $5, @2); }
	| a_expr '/' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("/", $1, $5, @2); }
	| a_expr '%' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("%", $1, $5, @2); }
	| a_expr '^' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("^", $1, $5, @2); }
	| a_expr '<' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("<", $1, $5, @2); }
	| a_expr '>' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR(">", $1, $5, @2); }
	| a_expr '=' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("=", $1, $5, @2); }
	| a_expr LIKE SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("~~", $1, $5, @2); }
	| a_expr NOT LIKE SOME '(' a_expr ')'		{ $$ = MAKE_ANY_A_EXPR("!~~", $1, $6, @2); }
	;

/*
 * Constants
 */
AexprConst: Iconst
				{
					$$ = makeIntConst($1, @1);
				}
			| FCONST
				{
					$$ = makeFloatConst($1, @1);
				}
			| Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
			| func_name Sconst
				{
					/* generic type 'literal' syntax */
					TypeName *t = makeTypeNameFromNameList($1);
					t->location = @1;
					$$ = makeStringConstCast($2, @2, t);
				}
			| func_name '(' func_arg_list ')' Sconst
				{
					/* generic syntax with a type modifier */
					TypeName *t = makeTypeNameFromNameList($1);
					ListCell *lc;

					/*
					 * We must use func_arg_list in the production to avoid
					 * reduce/reduce conflicts, but we don't actually wish
					 * to allow NamedArgExpr in this context.
					 */
					foreach(lc, $3)
					{
						NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

						if (IsA(arg, NamedArgExpr))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have parameter name"),
									 parser_errposition(arg->location)));
					}
					t->typmods = $3;
					t->location = @1;
					$$ = makeStringConstCast($5, @5, t);
				}
			| ConstTypename Sconst
				{
					$$ = makeStringConstCast($2, @2, $1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					TypeName *t = $1;
					if ($6 != NIL)
					{
						if (list_length($6) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						t->typmods = lappend($6, makeIntConst($3, @3));
					}
					else
						t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
		;

attr_name: ColLabel
	;

Bit:  BFILE					{ $$ = SystemTypeNameLocation("bytea", @1); }
	| BLOB					{ $$ = SystemTypeNameLocation("bytea", @1); }
	| LONG_P RAW			{ $$ = SystemTypeNameLocation("bytea", @1); }
	| RAW '(' Iconst ')'
		{
			$$ = SystemTypeNameLocation("bytea", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	;

b_expr: c_expr
	| b_expr TYPECAST Typename
		{ $$ = makeTypeCast($1, $3, @2); }
	| '+' b_expr
		{ $$ = (Node*)makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
	| '-' b_expr
		{ $$ = doNegate($2, @1); }
	| b_expr '+' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
	| b_expr '-' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
	| b_expr '*' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
	| b_expr '/' b_expr
	| b_expr '%' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
	| b_expr '^' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
	| b_expr '<' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
	| b_expr '>' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
	| b_expr '=' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
	| b_expr qual_Op b_expr				%prec Op
		{
			char *nspname = NULL;
			char *objname = NULL;

			DeconstructQualifiedName($2, &nspname, &objname);
			if (objname &&
				strncmp(objname, "||", strlen(objname)) == 0)
			{	
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("concat");
				n->args = list_make2($1, $3);
				n->location = @1;
				$$ = (Node *)n;
			} else
			{
				$$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2);
			}
		}
	| qual_Op b_expr					%prec Op
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
	| b_expr qual_Op					%prec POSTFIXOP
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }
	| b_expr IS DISTINCT FROM b_expr		%prec IS
		{
			$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
		}
	;

//case_arg: a_expr
//	| /* empty */ { $$ = NULL;}
//	;
//
case_default: ELSE a_expr		{ $$ = $2; }
	| /* empty */				{ $$ = NULL; }
	;

case_expr:
	CASE case_when_list case_default END_P
		{
			CaseExpr *c = makeNode(CaseExpr);
			c->casetype = InvalidOid;
			c->arg = NULL;
			c->args = $2;
			c->defresult = (Expr*)$3;
			c->location = @1;
			$$ = (Node*)c;
		}
	| CASE a_expr case_when_list case_default END_P
		{
			CaseExpr *c = makeNode(CaseExpr);
			c->casetype = InvalidOid;
			c->arg = (Expr*)$2;
			c->args = $3;
			c->defresult = (Expr*)$4;
			c->location = @1;
			$$ = (Node*)c;
		}
	;

case_when_list: case_when_item			{ $$ = list_make1($1); }
		| case_when_list case_when_item	{ $$ = lappend($1, $2); }
	;

case_when_item: WHEN a_expr THEN a_expr
		{
			CaseWhen *w = makeNode(CaseWhen);
			w->expr = (Expr *) $2;
			w->result = (Expr *) $4;
			w->location = @1;
			$$ = (Node *)w;
		}
	;

Character:
	  VARCHAR opt_type_mod
		{
			$$ = SystemTypeNameLocation("varchar", @1);
			$$->typmods = $2;
		}
	| VARCHAR2 '(' Iconst opt_byte_char ')'
		{
			$$ = SystemTypeNameLocation("varchar2", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| NVARCHAR2 '(' Iconst ')'
		{
			$$ = SystemTypeNameLocation("nvarchar2", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| CHAR_P '(' Iconst opt_byte_char ')'
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| CHAR_P
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			/* char defaults to char(1) */
			$$->typmods = list_make1(makeIntConst(1, -1));
		}
	| NCHAR '(' Iconst ')'
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| NCHAR
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			/* nchar defaults to char(1) */
			$$->typmods = list_make1(makeIntConst(1, -1));
		}
	| CLOB		{ $$ = SystemTypeNameLocation("text", @1); }
	| NCLOB		{ $$ = SystemTypeNameLocation("text", @1); }
	;

c_expr: columnref
	| AexprConst
	| func_expr
	| case_expr
	| '(' a_expr ')' { $$ = $2; }
	| PARAM opt_indirection
		{
			ParamRef *p = makeNode(ParamRef);
			p->number = $1;
			p->location = @1;
			if ($2)
			{
				A_Indirection *n = makeNode(A_Indirection);
				n->arg = (Node *) p;
				n->indirection = check_indirection($2, yyscanner);
				$$ = (Node *) n;
			}
			else
				$$ = (Node *) p;
		}
	| select_with_parens			%prec UMINUS
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = EXPR_SUBLINK;
			n->testexpr = NULL;
			n->operName = NIL;
			n->subselect = $1;
			n->location = @1;
			$$ = (Node *)n;
		}
	| EXISTS select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = EXISTS_SUBLINK;
			/*n->testexpr = NULL;
			n->operName = NIL;*/
			n->subselect = $2;
			n->location = @1;
			$$ = (Node *)n;
		}
	;

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:		IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
			| reserved_keyword						{ $$ = pstrdup($1); }
		;

common_table_expr: name opt_name_list AS '(' SelectStmt ')'
		{
			CommonTableExpr *n = makeNode(CommonTableExpr);
			n->ctename = $1;
			n->aliascolnames = $2;
			n->ctequery = $5;
			n->location = @1;
			$$ = (Node *) n;
		}
	;


columnDef: ColId Typename create_generic_options ColQualList
		{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $3;
					SplitColQualList($4, &n->constraints, &n->collClause,
									 yyscanner);
					$$ = (Node *)n;
		}
	;
	
ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;
		
ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					Constraint *n = (Constraint *) $3;
					Assert(IsA(n, Constraint));
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
			| COLLATE any_name
				{
					/*
					 * Note: the CollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
		;	

any_name_list: any_name						{ $$ = list_make1($1); }
			| any_name_list ',' any_name	{ $$ = lappend($1,$3); }
		;

any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
		;

attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;
ColConstraintElem:
			NOT NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					n->indexspace = $3;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					n->indexspace = $4;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')' opt_no_inherit
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| DEFAULT b_expr
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_DEFAULT;
					n->location = @1;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid  = true;
					$$ = (Node *)n;
				}
		;	
opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
definition: '(' def_list ')'						{ $$ = $2; }
		;

def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;

def_elem:	ColLabel
				{
					$$ = makeDefElem($1, NULL);
				}
		;
	
		
OptConsTableSpace:   USING INDEX TABLESPACE name	{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
		
opt_no_inherit:	NO INHERIT							{  $$ = TRUE; }
			| /* EMPTY */							{  $$ = FALSE; }
		;
opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;

columnElem: ColId
				{
					$$ = (Node *) makeString($1);
				}
		;
		
key_match:  MATCH FULL
			{
				$$ = FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented"),
						 parser_errposition(@1)));
				$$ = FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		| /*EMPTY*/
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		;
		
/*
 * We combine the update and delete actions into one value temporarily
 * for simplicity of parsing, and then break them down again in the
 * calling production.  update is in the left 8 bits, delete in the right.
 * Note that NOACTION is the default.
 */
key_actions:
			key_update
				{ $$ = ($1 << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
		;

key_update: ON UPDATE key_action		{ $$ = $3; }
		;

key_delete: ON DELETE_P key_action		{ $$ = $3; }
		;

key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }
		;
		
ConstraintAttr:
			DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NOT DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_NOT_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY DEFERRED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRED;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY IMMEDIATE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_IMMEDIATE;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

columnref:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| ROWID
				{
					$$ = makeColumnRef(pstrdup($1), NIL, @1, yyscanner);
				}
			| ColId indirection
				{
					$$ = makeColumnRef($1, $2, @1, yyscanner);
				}
			| columnref ORACLE_JOIN_OP
				{
					ColumnRefJoin *n = makeNode(ColumnRefJoin);
					n->column = (ColumnRef*)$1;
					n->location = @2;
					$$ = (Node*)n;
				}
		;

ConstInterval:
	  INTERVAL		{ $$ = SystemTypeNameLocation("interval", @1); }
	;

ConstDatetime:
	  DATE_P				
	  	{ 
	  		$$ = OracleTypeNameLocation("date", @1);
	  	}
	| TIMESTAMP opt_type_mod
		{
			$$ = SystemTypeNameLocation("timestamp", @1);
			$$->typmods = $2;
		}
	| TIMESTAMP opt_type_mod WITH TIME ZONE
		{
			$$ = SystemTypeNameLocation("timestamptz", @1);
			$$->typmods = $2;
		}
	| TIMESTAMP opt_type_mod WITH LOCAL TIME ZONE
		{
			$$ = SystemTypeNameLocation("timestamptz", @1);
			$$->typmods = $2;
		}
	;

/* We have a separate ConstTypename to allow defaulting fixed-length
 * types such as CHAR() and BIT() to an unspecified length.
 * SQL9x requires that these default to a length of one, but this
 * makes no sense for constructs like CHAR 'hi' and BIT '0101',
 * where there is an obvious better choice to make.
 * Note that ConstInterval is not included here since it must
 * be pushed up higher in the rules to accommodate the postfix
 * options (e.g. INTERVAL '1' YEAR). Likewise, we have to handle
 * the generic-type-name case in AExprConst to avoid premature
 * reduce/reduce conflicts against function names.
 */
ConstTypename:
			Numeric									{ $$ = $1; }
			/*| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }*/
			| ConstDatetime							{ $$ = $1; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *		PGXC-related extensions:
 *		1) Distribution type of a table:
 *			DISTRIBUTE BY ( HASH(column) | MODULO(column) |
 *							REPLICATION | ROUNDROBIN )
 *		2) Subcluster for table
 *			TO ( GROUP groupname | NODE nodename1,...,nodenameN )
 *
 *****************************************************************************/

CreateStmt:	CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptWith OnCommitOption OptTableSpace
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = $8;
					n->constraints = NIL;
					n->options = $9;
					n->oncommit = $10;
					n->tablespacename = $11;
					n->if_not_exists = false;
/* PGXC_BEGIN */
					n->distributeby = $12;
					n->subcluster = $13;
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('
			OptTableElementList ')' OptInherit OptWith OnCommitOption
			OptTableSpace
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->inhRelations = $11;
					n->constraints = NIL;
					n->options = $12;
					n->oncommit = $13;
					n->tablespacename = $14;
					n->if_not_exists = true;
/* PGXC_BEGIN */
					n->distributeby = $15;
					n->subcluster = $16;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE qualified_name OF any_name
			OptTypedTableElementList OptWith OnCommitOption OptTableSpace
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $7;
					n->ofTypename = makeTypeNameFromNameList($6);
					n->ofTypename->location = @6;
					n->constraints = NIL;
					n->options = $8;
					n->oncommit = $9;
					n->tablespacename = $10;
					n->if_not_exists = false;
/* PGXC_BEGIN */
					n->distributeby = $11;
					n->subcluster = $12;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name
			OptTypedTableElementList OptWith OnCommitOption OptTableSpace
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $10;
					n->ofTypename = makeTypeNameFromNameList($9);
					n->ofTypename->location = @9;
					n->constraints = NIL;
					n->options = $11;
					n->oncommit = $12;
					n->tablespacename = $13;
					n->if_not_exists = true;
/* PGXC_BEGIN */
					n->distributeby = $14;
					n->subcluster = $15;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* PGXC_END */
					$$ = (Node *)n;
				}
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior, so warn about that.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 */
OptTemp:	TEMPORARY					{ $$ = RELPERSISTENCE_TEMP; }
			| TEMP						{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMPORARY			{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMP				{ $$ = RELPERSISTENCE_TEMP; }
			| GLOBAL TEMPORARY
				{
					/*ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1))); */
					$$ = RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMP
				{
					/* ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1))); */
					$$ = RELPERSISTENCE_TEMP;
				}
			| UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;

OptInherit: INHERITS '(' qualified_name_list ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

create_generic_options:
	  /* empty */						{ $$ = NIL; }
	;

cte_list:
	  common_table_expr 				{ $$ = list_make1($1); }
	| cte_list ',' common_table_expr	{ $$ = lappend($1, $3); }
	;

/* WITH (options) is preferred, WITH OIDS and WITHOUT OIDS are legacy forms */
OptWith:
			WITH reloptions				{ $$ = $2; }
			| WITH OIDS					{ $$ = list_make1(defWithOids(true)); }
			| WITHOUT OIDS				{ $$ = list_make1(defWithOids(false)); }
			| /*EMPTY*/					{ $$ = NIL; }
		;

OnCommitOption:  ON COMMIT DROP				{ $$ = ONCOMMIT_DROP; }
			| ON COMMIT DELETE_P ROWS		{ $$ = ONCOMMIT_DELETE_ROWS; }
			| ON COMMIT PRESERVE ROWS		{ $$ = ONCOMMIT_PRESERVE_ROWS; }
			| /*EMPTY*/						{ $$ = ONCOMMIT_NOOP; }
		;

OptDistributeBy: OptDistributeByInternal			{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptTypedTableElementList:
			'(' TypedTableElementList ')'		{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TypedTableElementList:
			TypedTableElement
				{
					$$ = list_make1($1);
				}
			| TypedTableElementList ',' TypedTableElement
				{
					$$ = lappend($1, $3);
				}
		;

TypedTableElement:
			columnOptions						{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;

columnOptions:	ColId WITH OPTIONS ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					SplitColQualList($4, &n->constraints, &n->collClause,
									 yyscanner);
					$$ = (Node *)n;
				}
		;


/*
 * The SQL spec defines "contextually typed value expressions" and
 * "contextually typed row value constructors", which for our purposes
 * are the same as "a_expr" and "row" except that DEFAULT can appear at
 * the top level.
 */

ctext_expr:
		a_expr					{ $$ = (Node *) $1; }
		| DEFAULT
			{
				SetToDefault *n = makeNode(SetToDefault);
				n->location = @1;
				$$ = (Node *) n;
			}
	;

ctext_expr_list:
		ctext_expr								{ $$ = list_make1($1); }
		| ctext_expr_list ',' ctext_expr		{ $$ = lappend($1, $3); }
	;

ctext_row: '(' ctext_expr_list ')'					{ $$ = $2; }
	;

DeleteStmt:
	  opt_with_clause DELETE_P opt_from relation_expr
	  opt_alias_clause where_clause returning_clause
		{
			$$ = makeDeleteStmt($4, $5, $1, $6, $7);
		}
	/*| opt_with_clause  DELETE_P opt_from ONLY '(' relation_expr ')'
	  opt_alias_clause where_clause returning_clause
		{
			$$ = makeDeleteStmt($6, $8, $1, $9, $10);
		}*/
	/*| DELETE_P opt_from select_with_parens
	  opt_alias_clause where_clause returning_clause
		{
		}*/
	;

document_or_content: DOCUMENT_P						{ $$ = XMLOPTION_DOCUMENT; }
			| CONTENT_P								{ $$ = XMLOPTION_CONTENT; }
		;

DropStmt:
	DROP drop_type IF_P EXISTS any_name_list opt_drop_behavior DropStmt_opt_purge
	    {
	    	DropStmt *n = makeNode(DropStmt);
	    	n->removeType = $2;
	    	n->missing_ok = TRUE;
	    	n->objects = $5;
	    	n->arguments = NIL;
	    	n->behavior = $6;
	    	n->concurrent = false;
	    	$$ = (Node *)n;
		}
    | DROP drop_type any_name_list opt_drop_behavior DropStmt_opt_purge
    	{
    		DropStmt *n = makeNode(DropStmt);
    		n->removeType = $2;
    		n->missing_ok = FALSE;
    		n->objects = $3;
    		n->arguments = NIL;
    		n->behavior = $4;
    		n->concurrent = false;
    		$$ = (Node *)n;
    	}
    | DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
    	{
    		DropStmt *n = makeNode(DropStmt);
    		n->removeType = OBJECT_INDEX;
    		n->missing_ok = FALSE;
    		n->objects = $4;
    		n->arguments = NIL;
    		n->behavior = $5;
    		n->concurrent = true;
    		$$ = (Node *)n;
    	}
    | DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
    	{
    		DropStmt *n = makeNode(DropStmt);
    		n->removeType = OBJECT_INDEX;
    		n->missing_ok = TRUE;
    		n->objects = $6;
    		n->arguments = NIL;
    		n->behavior = $7;
    		n->concurrent = true;
    		$$ = (Node *)n;
    	}
		;	
	;

drop_type:	TABLE									{ $$ = OBJECT_TABLE; }
			| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = OBJECT_MATVIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGN_TABLE; }
			| EVENT TRIGGER 						{ $$ = OBJECT_EVENT_TRIGGER; }
			| TYPE_P								{ $$ = OBJECT_TYPE; }
			| DOMAIN_P								{ $$ = OBJECT_DOMAIN; }
			| COLLATION								{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| EXTENSION								{ $$ = OBJECT_EXTENSION; }
			| TEXT_P SEARCH PARSER					{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = OBJECT_TSCONFIGURATION; }
		;

DropStmt_opt_purge:
	  PURGE
	| /* empty */
	;

ExplainStmt:
	EXPLAIN ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $2;
			n->options = NIL;
			$$ = (Node *) n;
		}
	| EXPLAIN analyze_keyword opt_verbose ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $4;
			n->options = list_make1(makeDefElem("analyze", NULL));
			if ($3)
				n->options = lappend(n->options,
									 makeDefElem("verbose", NULL));
			$$ = (Node *) n;
		}
	| EXPLAIN VERBOSE ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $3;
			n->options = list_make1(makeDefElem("verbose", NULL));
			$$ = (Node *) n;
		}
	| EXPLAIN '(' explain_option_list ')' ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $5;
			n->options = $3;
			$$ = (Node *) n;
		}
	;

ExplainableStmt:
	SelectStmt
	| InsertStmt
	| UpdateStmt
	| DeleteStmt
	;

explain_option_list:
	explain_option_elem
		{
			$$ = list_make1($1);
		}
	| explain_option_list ',' explain_option_elem
		{
			$$ = lappend($1, $3);
		}
	;

explain_option_elem:
	explain_option_name explain_option_arg
		{
			$$ = makeDefElem($1, $2);
		}
	;

explain_option_name:
	NonReservedWord
	{
		if(strcmp($1, "analyse") == 0)
			$$ = "analyze";
		else
			$$ = $1;
	}
	;

explain_option_arg:
		opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
		| NumericOnly			{ $$ = (Node *) $1; }
		| /* EMPTY */			{ $$ = NULL; }
	;

expr_list: a_expr { $$ = list_make1($1); }
	| expr_list ',' a_expr
		{ $$ = lappend($1, $3); }
	;

trim_list:	a_expr FROM expr_list					{ $$ = lappend($3, $1); }
		| FROM expr_list						{ $$ = $2; }
		| expr_list								{ $$ = $1; }
	;

from_clause: FROM from_list			{ $$ = $2; }
			| /* empty */ { $$ = NIL; }
		;

from_list: table_ref				{ $$ = $1 ? list_make1($1):NIL; }
		| from_list ',' table_ref	{ $$ = $3 ? lappend($1, $3):$1; }
		;

func_arg_expr: a_expr				{ $$ = $1; }
		;

func_arg_list: func_arg_expr				{ $$ = list_make1($1); }
		| func_arg_list ',' func_arg_expr	{ $$ = lappend($1, $3); }
		;

extract_list:
			extract_arg FROM a_expr
				{
					$$ = list_make2(makeStringConst($1, @1), $3);
				}
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* Allow delimited string Sconst in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */
extract_arg:
			IDENT									{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| Sconst								{ $$ = $1; }
		;
/*
 * func_expr is split out from c_expr just so that we have a classification
 * for "everything that is a function call or looks like one".  This isn't
 * very important, but it saves us having to document which variants are
 * legal in the backwards-compatible functional-index syntax for CREATE INDEX.
 * (Note that many of the special SQL functions wouldn't actually make any
 * sense as functional index entries, but we ignore that consideration here.)
 */
func_expr:	func_name '(' ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = NIL;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' func_arg_list ')'
			{
				char *nspname = NULL;
				char *objname = NULL;

				DeconstructQualifiedName($1, &nspname, &objname);
				if (strcasecmp(objname, "decode") == 0 && 
					(nspname == NULL ||
					strcasecmp(nspname, "oracle") == 0))
				{
					if (list_length($3) < 3)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Not engouh parameters for \"decode\" function"),
								 parser_errposition(@1)));

					$$ = reparse_decode_func($3, @1);
				} else
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->location = @1;
					$$ = (Node *)n;
				}
			}
		| func_name '(' func_arg_list sort_clause ')' 
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = $3;
				n->agg_order = $4;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				/* n->over = $6; */
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' ALL func_arg_list opt_sort_clause ')' 
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = $4;
				n->agg_order = $5;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				/* Ideally we'd mark the FuncCall node to indicate
				 * "must be an aggregate", but there's no provision
				 * for that in FuncCall at the moment.
				 */
				n->func_variadic = FALSE;
				/* n->over = $7; */
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' DISTINCT func_arg_list opt_sort_clause ')' 
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = $4;
				n->agg_order = $5;
				n->agg_star = FALSE;
				n->agg_distinct = TRUE;
				n->func_variadic = FALSE;
				/* n->over = $7; */
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' '*' ')'
			{
				/*
				 * We consider AGGREGATE(*) to invoke a parameterless
				 * aggregate.  This does the right thing for COUNT(*),
				 * and there are no other aggregates in SQL that accept
				 * '*' as parameter.
				 *
				 * The FuncCall node is also marked agg_star = true,
				 * so that later processing can detect what the argument
				 * really was.
				 */
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = NIL;
				n->agg_order = NIL;
				n->agg_star = TRUE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->location = @1;
				$$ = (Node *)n;
			}
		| SYSDATE
			{
				/*
				 * Translate as "ora_sys_now()::timestamp(0)".
				 */
				TypeName *tn;
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_sys_now");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = FALSE;
				fc->agg_distinct = FALSE;
				fc->func_variadic = FALSE;
				fc->over = NULL;
				fc->location = -1;

				tn = OracleTypeName("date");
				$$ = makeTypeCast((Node *)fc, tn, @1);
			}
		| SYSTIMESTAMP
			{
				/*
				 * Translate as "ora_sys_now()::timestamp".
				 */
				TypeName *tn;
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_sys_now");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = FALSE;
				fc->agg_distinct = FALSE;
				fc->func_variadic = FALSE;
				fc->over = NULL;
				fc->location = -1;

				tn = SystemTypeName("timestamp");
				$$ = makeTypeCast((Node *)fc, tn, @1);
			}
		| CURRENT_DATE
			{
				/*
				 * Translate as "'now'::text::timestamp(0)".
				 */
				Node *n;
				TypeName *tn;

				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				tn = OracleTypeName("date");
				$$ = makeTypeCast(n, tn, @1);
			}
		| CURRENT_TIMESTAMP
			{
				/*
				 * Translate as "now()", since we have a function that
				 * does exactly what is needed.
				 */
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("now");
				n->args = NIL;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| CURRENT_TIMESTAMP '(' Iconst ')'
			{
				/*
				 * Translate as "'now'::text::timestamptz(n)".
				 * See comments for CURRENT_DATE.
				 */
				Node *n;
				TypeName *d;
				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				d = SystemTypeName("timestamptz");
				d->typmods = list_make1(makeIntConst($3, @3));
				$$ = makeTypeCast(n, d, @1);
			}
		| LOCALTIMESTAMP
			{
				/*
				 * Translate as "'now'::text::timestamp".
				 * See comments for CURRENT_DATE.
				 */
				Node *n;
				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				$$ = makeTypeCast(n, SystemTypeName("timestamp"), @1);
			}
		| LOCALTIMESTAMP '(' Iconst ')'
			{
				/*
				 * Translate as "'now'::text::timestamp(n)".
				 * See comments for CURRENT_DATE.
				 */
				Node *n;
				TypeName *d;
				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				d = SystemTypeName("timestamp");
				d->typmods = list_make1(makeIntConst($3, @3));
				$$ = makeTypeCast(n, d, @1);
			}
		| COALESCE '(' expr_list ')'
			{
				CoalesceExpr *c = makeNode(CoalesceExpr);
				c->args = $3;
				c->location = @1;
				$$ = (Node *)c;
			}
		| NULLIF '(' a_expr ',' a_expr ')'
			{
				$$ = (Node *) makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1);
			}
		| GREATEST '(' expr_list ')'
			{
				MinMaxExpr *v = makeNode(MinMaxExpr);
				v->args = $3;
				v->op = IS_GREATEST;
				v->location = @1;
				$$ = (Node *)v;
			}
		| LEAST '(' expr_list ')'
			{
				MinMaxExpr *v = makeNode(MinMaxExpr);
				v->args = $3;
				v->op = IS_LEAST;
				v->location = @1;
				$$ = (Node *)v;
			}
		| TREAT '(' a_expr AS Typename ')'
			{
				/* TREAT(expr AS target) converts expr of a particular type to target,
				 * which is defined to be a subtype of the original expression.
				 * In SQL99, this is intended for use with structured UDTs,
				 * but let's make this a generally useful form allowing stronger
				 * coercions than are handled by implicit casting.
				 */
				FuncCall *n = makeNode(FuncCall);
				/* Convert SystemTypeName() to SystemFuncName() even though
				 * at the moment they result in the same thing.
				 */
				n->funcname = SystemFuncName(((Value *)llast($5->names))->val.str);
				n->args = list_make1($3);
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' BOTH trim_list ')'
			{
				/* various trim expressions are defined in SQL
				 * - thomas 1997-07-19
				 */
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("btrim");
				n->args = $4;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' LEADING trim_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("ltrim");
				n->args = $4;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' TRAILING trim_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("rtrim");
				n->args = $4;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' trim_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("btrim");
				n->args = $3;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| DBTIMEZONE_P
			{
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_dbtimezone");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = FALSE;
				fc->agg_distinct = FALSE;
				fc->func_variadic = FALSE;
				fc->over = NULL;
				fc->location = -1;

				$$ = (Node *)fc;
			}
		| SESSIONTIMEZONE 
			{
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_session_timezone");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = FALSE;
				fc->agg_distinct = FALSE;
				fc->func_variadic = FALSE;
				fc->over = NULL;
				fc->location = -1;

				$$ = (Node *)fc;
			}
		| EXTRACT '(' extract_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("date_part");
				n->args = $3;
				n->agg_order = NIL;
				n->agg_star = FALSE;
				n->agg_distinct = FALSE;
				n->func_variadic = FALSE;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| ROWNUM
			{
				RownumExpr *n = makeNode(RownumExpr);
				n->location = @1;
				$$ = (Node*)n;
			}
		| NEXTVAL '(' func_arg_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("nextval");
				n->args = $3;
				n->location = @1;
				$$ = (Node *)n;
			}
		| CURRVAL '(' func_arg_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("currval");
				n->args = $3;
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId '.' NEXTVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("nextval");
				n->args = list_make1(makeStringConst($1, @1));
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId indirection '.' NEXTVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("nextval");
				n->args = check_sequence_name(lcons(makeString($1), $2), yyscanner, @1);
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId '.' CURRVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("currval");
				n->args = list_make1(makeStringConst($1, @1));
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId  indirection '.' CURRVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("currval");
				n->args = check_sequence_name(lcons(makeString($1), $2), yyscanner, @1);
				n->location = @1;
				$$ = (Node *)n;
			}
		;

/*
 * The production for a qualified func_name has to exactly match the
 * production for a qualified columnref, because we cannot tell which we
 * are parsing until we see what comes after it ('(' or Sconst for a func_name,
 * anything else for a columnref).  Therefore we allow 'indirection' which
 * may contain subscripts, and reject that case in the C code.  (If we
 * ever implement SQL99-like methods, such syntax may actually become legal!)
 */
func_name:	type_function_name
				{ $$ = list_make1(makeString($1)); }
			| ColId indirection
				{
					$$ = check_func_name(lcons(makeString($1), $2),
										 yyscanner);
				}
			| PUBLIC indirection
				{
					$$ = check_func_name(lcons(makeString(pstrdup($1)), $2),
										 yyscanner);
				}
		;

group_clause:
		GROUP_P BY expr_list					{ $$ = $3; }
		| /*EMPTY*/								{ $$ = NIL; }
	;

having_clause:
		HAVING a_expr							{ $$ = $2; }
		| /*EMPTY*/								{ $$ = NULL; }
	;

Iconst: ICONST		{ $$ = $1; };

indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
			| '.' '*'
				{
					$$ = (Node *) makeNode(A_Star);
				}
			| '[' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' a_expr ':' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

InsertStmt:
	opt_with_clause INSERT INTO qualified_name insert_rest returning_clause
		{
			$5->relation = $4;
			$5->returningList = $6;
			$5->withClause = $1;
			$$ = (Node *) $5;
		}
	;

insert_rest:
		SelectStmt
			{
				$$ = makeNode(InsertStmt);
				$$->cols = NIL;
				$$->selectStmt = $1;
			}
		| '(' insert_column_list ')' SelectStmt
			{
				$$ = makeNode(InsertStmt);
				$$->cols = $2;
				$$->selectStmt = $4;
			}
		| DEFAULT VALUES
			{
				$$ = makeNode(InsertStmt);
				$$->cols = NIL;
				$$->selectStmt = NULL;
			}
	;

insert_column_list:
		insert_column_item
				{ $$ = list_make1($1); }
		| insert_column_list ',' insert_column_item
				{ $$ = lappend($1, $3); }
	;

insert_column_item:
		ColId opt_indirection
			{
				$$ = makeNode(ResTarget);
				$$->name = $1;
				$$->indirection = check_indirection($2, yyscanner);
				$$->val = NULL;
				$$->location = @1;
			}
	;

interval_second:
			SECOND_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(SECOND), @1));
				}
			| SECOND_P '(' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(SECOND), @1),
									makeIntConst($3, @3));
				}
		;

iso_level:	READ UNCOMMITTED						{ $$ = "read uncommitted"; }
			| READ COMMITTED						{ $$ = "read committed"; }
			| REPEATABLE READ						{ $$ = "repeatable read"; }
			| SERIALIZABLE							{ $$ = "serializable"; }
		;

joined_table: '(' joined_table ')'			{ $$ = $2; }
			| table_ref CROSS JOIN table_ref
				{
					/* CROSS JOIN is same as unqualified 
					join */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL;
					n->quals = NULL;
					$$ = n;
				}
			/* | table_ref join_type JOIN table_ref ON a_expr
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					n->quals = $6; 
					$$ = n;
				} */
			| table_ref join_type JOIN table_ref join_qual
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					if ($5 != NULL && IsA($5, List))
						n->usingClause = (List *) $5; /* USING clause */
					else
						n->quals = $5; /* ON clause */
					$$ = n;
				}
			| table_ref JOIN table_ref ON a_expr
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $3;
					n->quals = $5; /* ON clause */
					$$ = n;
				}
		;

join_qual:	USING '(' name_list ')'					{ $$ = (Node *) $3; }
			| ON a_expr								{ $$ = $2; }
		;

/* OUTER is just noise... */
join_outer: OUTER_P									{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

join_type:	FULL join_outer							{ $$ = JOIN_FULL; }
			| LEFT join_outer						{ $$ = JOIN_LEFT; }
			| RIGHT join_outer						{ $$ = JOIN_RIGHT; }
			| INNER_P								{ $$ = JOIN_INNER; }
		;

MathOp: '+'									{ $$ = "+"; }
	| '-'									{ $$ = "-"; }
	| '*'									{ $$ = "*"; }
	| '/'									{ $$ = "/"; }
	| '%'									{ $$ = "%"; }
	| '^'									{ $$ = "^"; }
	| '<'									{ $$ = "<"; }
	| '>'									{ $$ = ">"; }
	| '='									{ $$ = "="; }
	;

multiple_set_clause:
			'(' set_target_list ')' '=' set_expr_row
				{
					ListCell *col_cell;
					ListCell *val_cell;

					/*
					 * Break the set_expr_row apart, merge individual expressions
					 * into the destination ResTargets.  XXX this approach
					 * cannot work for general row expressions as sources.
					 */
					if (list_length($2) != list_length($5))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("number of columns does not match number of values"),
								 parser_errposition(@1)));
					forboth(col_cell, $2, val_cell, $5)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						Node *res_val = (Node *) lfirst(val_cell);

						res_col->val = res_val;
					}

					$$ = $2;
				}
		;


name: ColId
	;

name_list: name				{ $$ = list_make1($1); }
	| name_list ',' name	{ $$ = lappend($1, $3); }
	;

/* Any not-fully-reserved word --- these names can be, eg, role names.
 */
NonReservedWord:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1; }
			| Sconst								{ $$ = $1; }
		;

Numeric:
	  INT_P						{ $$ = SystemTypeNameLocation("int4", @1); }
	| INTEGER					{ $$ = SystemTypeNameLocation("int4", @1); }
	| SMALLINT					{ $$ = SystemTypeNameLocation("int2", @1); }
	| BIGINT					{ $$ = SystemTypeNameLocation("int8", @1); }
	| LONG_P					{ $$ = SystemTypeNameLocation("text", @1); }
	| BINARY_FLOAT				{ $$ = SystemTypeNameLocation("float4", @1); }
	| REAL						{ $$ = SystemTypeNameLocation("float4", @1); }
	| BINARY_DOUBLE				{ $$ = SystemTypeNameLocation("float8", @1); }
	| FLOAT_P					{ $$ = SystemTypeNameLocation("float8", @1); }
	| FLOAT_P '(' Iconst ')'
		{
			/*
			 * Check FLOAT() precision limits assuming IEEE floating
			 * types - thomas 1997-09-18
			 */
			if ($3 < 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("precision for type float must be at least 1 bit"),
						 parser_errposition(@3)));
			else if ($3 <= 24)
				$$ = SystemTypeNameLocation("float4", @1);
			else if ($3 <= 53)
				$$ = SystemTypeNameLocation("float8", @2);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("precision for type float must be less than 54 bits"),
						 parser_errposition(@3)));
		}
	| DOUBLE_P PRECISION		{ $$ = SystemTypeNameLocation("float8", @1); }
	| DECIMAL_P opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| DEC opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| NUMERIC opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| NUMBER_P opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| BOOLEAN_P					{ $$ = SystemTypeNameLocation("bool", @1); }
	;

NumericOnly:
			FCONST								{ $$ = makeFloat($1); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;

OptTableElementList:
	  TableElementList	{ $$ = $1; }
	| /* empty */		{ $$ = NIL; }
	;

opt_alias_clause:
		alias_clause  %prec HI_THEN_RETURN
		| /* empty */ %prec RETURN_P		{ $$ = NULL; }
		;


opt_all:	ALL										{ $$ = TRUE; }
			| DISTINCT								{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_asc_desc:
	  ASC				{ $$ = SORTBY_ASC; }
	| DESC				{ $$ = SORTBY_DESC; }
	| /* empty */		{ $$ = SORTBY_DEFAULT; }
	;

opt_boolean_or_string:
			TRUE_P									{ $$ = "true"; }
			| FALSE_P								{ $$ = "false"; }
			| ON									{ $$ = "on"; }
			/*
			 * OFF is also accepted as a boolean value, but is handled by
			 * the NonReservedWord rule.  The action for booleans and strings
			 * is the same, so we don't need to distinguish them here.
			 */
			| NonReservedWord_or_Sconst				{ $$ = $1; }
		;

opt_byte_char:
	  BYTE_P				{ $$ = true; }
	| CHAR_P				{ $$ = true; }
	| /* empty */			{ $$ = false; }
	;

/* We use (NIL) as a placeholder to indicate that all target expressions
 * should be placed in the DISTINCT list during parsetree analysis.
 */
opt_distinct:
			DISTINCT								{ $$ = list_make1(NIL); }
			| ALL									{ $$ = NIL; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| CASCADE CONSTRAINTS		{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| /* EMPTY */				{ $$ = DROP_RESTRICT; /* default */ }
		;

opt_encoding:
			Sconst									{ $$ = $1; }
			| DEFAULT								{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_from:
	  FROM
	| /* empty */
	;

opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
		;

opt_interval:
			YEAR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR), @1)); }
			| MONTH_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MONTH), @1)); }
			| DAY_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY), @1)); }
			| HOUR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR), @1)); }
			| MINUTE_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MINUTE), @1)); }
			| interval_second
				{ $$ = $1; }
			| YEAR_P TO MONTH_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
												 INTERVAL_MASK(MONTH), @1));
				}
			| DAY_P TO HOUR_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR), @1));
				}
			| DAY_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| DAY_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| HOUR_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| HOUR_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| MINUTE_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;

opt_name_list:
	  name_list
	| /* empty */ { $$ = NULL; }
	;

opt_restart_seqs:
			CONTINUE_P IDENTITY_P		{ $$ = false; }
			| RESTART IDENTITY_P		{ $$ = true; }
			| /* EMPTY */				{ $$ = false; }
		;

opt_type_mod:
	  '(' Iconst ')'		{ $$ = list_make1(makeIntConst($2, @2)); }
	| /* empty */			{ $$ = NIL; }
	;

opt_type_modifiers:'(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
		;

opt_verbose:
			VERBOSE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_with_clause:
	  with_clause
	| /* empty */ { $$ = NULL; }
	;

qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

qualified_name:
		  ColId { $$ = makeRangeVar(NULL, $1, @1); }
		| ColId indirection
			{
				RangeVar *n;
				check_qualified_name($2, yyscanner);
				n = makeRangeVar(NULL, NULL, @1);
				switch (list_length($2))
				{
					case 1:
						n->catalogname = NULL;
						n->schemaname = $1;
						n->relname = strVal(linitial($2));
						break;
					case 2:
						n->catalogname = $1;
						n->schemaname = strVal(linitial($2));
						n->relname = strVal(lsecond($2));
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("improper qualified name (too many dotted names): %s",
										NameListToString(lcons(makeString($1), $2))),
								 parser_errposition(@1)));
						break;
				}
				$$ = n;
			}
		| PUBLIC indirection
			{
				RangeVar *n;
				check_qualified_name($2, yyscanner);
				n = makeRangeVar(NULL, NULL, @1);
				switch (list_length($2))
				{
					case 1:
						n->catalogname = NULL;
						n->schemaname = pstrdup($1);
						n->relname = strVal(linitial($2));
						break;
					case 2:
						n->catalogname = pstrdup($1);
						n->schemaname = strVal(linitial($2));
						n->relname = strVal(lsecond($2));
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("improper qualified name (too many dotted names): %s",
										NameListToString(lcons(makeString(pstrdup($1)), $2))),
								 parser_errposition(@1)));
						break;
				}
				$$ = n;
			}
		;

qual_Op: Op { $$ = list_make1(makeString($1)); }
	| OPERATOR '(' any_operator ')' { $$ = $3; }
	;

relation_expr:
		qualified_name
		{
				/* default inheritance */
				$$ = $1;
				$$->inhOpt = INH_DEFAULT;
				$$->alias = NULL;
		}
	|	qualified_name '*'
		{
			/* inheritance query */
			$$ = $1;
			$$->inhOpt = INH_YES;
			$$->alias = NULL;
		}
	|	ONLY qualified_name
		{
			/* no inheritance */
			$$ = $2;
			$$->inhOpt = INH_NO;
			$$->alias = NULL;
			}
	|	ONLY '(' qualified_name ')'
		{
			/* no inheritance, SQL99-style syntax */
			$$ = $3;
			$$->inhOpt = INH_NO;
			$$->alias = NULL;
		}
		;

relation_expr_list:
			relation_expr							{ $$ = list_make1($1); }
			| relation_expr_list ',' relation_expr	{ $$ = lappend($1, $3); }
		;

/*
 * Given "UPDATE foo set set ...", we have to decide without looking any
 * further ahead whether the first "set" is an alias or the UPDATE's SET
 * keyword.  Since "set" is allowed as a column name both interpretations
 * are feasible.  We resolve the shift/reduce conflict by giving the first
 * relation_expr_opt_alias production a higher precedence than the SET token
 * has, causing the parser to prefer to reduce, in effect assuming that the
 * SET is not an alias.
 */
relation_expr_opt_alias: relation_expr					%prec UMINUS
		{
			$$ = $1;
		}
	| relation_expr ColId
		{
			Alias *alias = makeNode(Alias);
			alias->aliasname = $2;
			$1->alias = alias;
			$$ = $1;
		}
	| relation_expr AS ColId
		{
			Alias *alias = makeNode(Alias);
			alias->aliasname = $3;
			$1->alias = alias;
			$$ = $1;
		}
	;

returning_clause:
	  RETURN_P returning_item	{ $$ = $2; }
	| RETURNING returning_item	{ $$ = $2; }
	| /* EMPTY */				{ $$ = NIL; }
	;

returning_item: target_list
	;

Sconst: SCONST		{ $$ = $1; };

select_clause:
		simple_select
		| select_with_parens		%prec LO_THEN_LIMIT
	;

SelectStmt: select_no_parens		%prec UMINUS
		| select_with_parens		%prec UMINUS
		;

select_with_parens:
		'(' select_no_parens ')'		{ $$ = $2; }
		| '(' select_with_parens ')'	{ $$ = $2; }
		;

select_no_parens:
		simple_select					{ $$ = $1; }
		| select_clause sort_clause
			{
				insertSelectOptions((SelectStmt *) $1, $2, NIL,
									NULL, NULL, NULL,
									yyscanner);
				$$ = $1;
			}
		| select_clause opt_sort_clause for_locking_clause opt_select_limit
			{
				insertSelectOptions((SelectStmt *) $1, $2, $3,
									list_nth($4, 0), list_nth($4, 1),
									NULL,
									yyscanner);
				$$ = $1;
			}
		| select_clause opt_sort_clause select_limit opt_for_locking_clause
			{
				insertSelectOptions((SelectStmt *) $1, $2, $4,
									list_nth($3, 0), list_nth($3, 1),
									NULL,
									yyscanner);
				$$ = $1;
			}
		| with_clause select_clause
			{
				insertSelectOptions((SelectStmt *) $2, NULL, NIL,
									NULL, NULL,
									$1,
									yyscanner);
				$$ = $2;
			}
		| with_clause select_clause sort_clause
			{
				insertSelectOptions((SelectStmt *) $2, $3, NIL,
									NULL, NULL,
									$1,
									yyscanner);
				$$ = $2;
			}
		| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
			{
				insertSelectOptions((SelectStmt *) $2, $3, $4,
									list_nth($5, 0), list_nth($5, 1),
									$1,
									yyscanner);
				$$ = $2;
			}
		| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
			{
				insertSelectOptions((SelectStmt *) $2, $3, $5,
									list_nth($4, 0), list_nth($4, 1),
									$1,
									yyscanner);
				$$ = $2;
			}
		;

set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause:
			single_set_clause						{ $$ = list_make1($1); }
			| multiple_set_clause					{ $$ = $1; }
		;

for_locking_clause:
			for_locking_items						{ $$ = $1; }
			| FOR READ ONLY							{ $$ = NIL; }
		;

opt_for_locking_clause:
			for_locking_clause						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

for_locking_items:
			for_locking_item						{ $$ = list_make1($1); }
			| for_locking_items for_locking_item	{ $$ = lappend($1, $2); }
		;

for_locking_item:
			for_locking_strength locked_rels_list opt_nowait
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $2;
					n->strength = $1;
					n->noWait = $3;
					$$ = (Node *) n;
				}
		;

for_locking_strength:
			FOR UPDATE 							{ $$ = LCS_FORUPDATE; }
			| FOR NO KEY UPDATE 				{ $$ = LCS_FORNOKEYUPDATE; }
			| FOR SHARE 						{ $$ = LCS_FORSHARE; }
			| FOR KEY SHARE 					{ $$ = LCS_FORKEYSHARE; }
		;

locked_rels_list:
			OF qualified_name_list					{ $$ = $2; }
			| /* EMPTY */							{ $$ = NIL; }
		;

opt_nowait:	NOWAIT							{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;


select_limit:
			limit_clause offset_clause			{ $$ = list_make2($2, $1); }
			| offset_clause limit_clause		{ $$ = list_make2($1, $2); }
			| limit_clause						{ $$ = list_make2(NULL, $1); }
			| offset_clause						{ $$ = list_make2($1, NULL); }
		;

opt_select_limit:
			select_limit						{ $$ = $1; }
			| /* EMPTY */						{ $$ = list_make2(NULL,NULL); }
		;

limit_clause:
			LIMIT select_limit_value
				{ $$ = $2; }
			| LIMIT select_limit_value ',' select_offset_value
				{
					/* Disabled because it was too confusing, bjm 2002-02-18 */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("LIMIT #,# syntax is not supported"),
							 errhint("Use separate LIMIT and OFFSET clauses."),
							 parser_errposition(@1)));
				}
			/* SQL:2008 syntax */
			/*| FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
				{ $$ = $3; }*/
		;

offset_clause:
			OFFSET select_offset_value
				{ $$ = $2; }
			/* SQL:2008 syntax */
			/*| OFFSET select_offset_value2 row_or_rows
				{ $$ = $2; }*/
			| OFFSET c_expr ROW		{ $$ = $2; }
			| OFFSET c_expr ROWS	{ $$ = $2; }
		;

select_limit_value:
			a_expr									{ $$ = $1; }
			| ALL
				{
					/* LIMIT ALL is represented as a NULL constant */
					$$ = makeNullAConst(@1);
				}
		;

select_offset_value:
			a_expr									{ $$ = $1; }
		;

set_expr:
	  a_expr
	| DEFAULT
		{
			SetToDefault *n = makeNode(SetToDefault);
			n->location = @1;
			$$ = (Node *) n;
		}
	;

set_expr_list: set_expr					{ $$ = list_make1($1); }
	| set_expr_list ',' set_expr		{ $$ = lappend($1, $3); }
	;

set_expr_row: '(' set_expr_list ')'		{ $$ = $2; }
	;

set_rest:
			TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION";
					n->args = $2;
					$$ = n;
				}
			| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "SESSION CHARACTERISTICS";
					n->args = $5;
					$$ = n;
				}
			| set_rest_more
			;

set_rest_more:	/* Generic SET syntaxes: */
			var_name TO var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name '=' var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name TO DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name '=' DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name FROM CURRENT_P
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_CURRENT;
					n->name = $1;
					$$ = n;
				}
			/* Special syntaxes mandated by SQL standard: */
			| TIME ZONE zone_value
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| CATALOG_P Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("current database cannot be changed"),
							 parser_errposition(@2)));
					$$ = NULL; /*not reached*/
				}
			| SCHEMA Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "search_path";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| NAMES opt_encoding
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "client_encoding";
					if ($2 != NULL)
						n->args = list_make1(makeStringConst($2, @2));
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| ROLE NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "role";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| SESSION AUTHORIZATION NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "session_authorization";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
			| SESSION AUTHORIZATION DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "session_authorization";
					$$ = n;
				}
			| XML_P OPTION document_or_content
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "xmloption";
					n->args = list_make1(makeStringConst($3 == XMLOPTION_DOCUMENT ? "DOCUMENT" : "CONTENT", @3));
					$$ = n;
				}
			/* Special syntaxes invented by PostgreSQL: */
			| TRANSACTION SNAPSHOT Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION SNAPSHOT";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
		;

set_target:
	ColId opt_indirection
		{
			$$ = makeNode(ResTarget);
			$$->name = $1;
			$$->indirection = check_indirection($2, yyscanner);
			$$->val = NULL;	/* upper production sets this */
			$$->location = @1;
		}
	;

set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;

SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;

single_set_clause:
			set_target '=' set_expr
				{
					$$ = $1;
					$$->val = (Node *) $3;
				}
		;

SimpleTypename:
	  Numeric
	| Bit
	| Character
	| ConstDatetime
	| ConstInterval
	| INTERVAL YEAR_P opt_type_mod TO MONTH_P
		{
			$$ = SystemTypeNameLocation("interval", @1);
			$$->typmods = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
											 INTERVAL_MASK(MONTH), @1));
			if($3)
				$$->typmods = lappend($$->typmods, $3);
		}
	| INTERVAL DAY_P opt_type_mod TO SECOND_P					%prec '+'
		{
			$$ = SystemTypeNameLocation("interval", @1);
			$$->typmods = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1));
			if($3)
				$$->typmods = lappend($$->typmods, $3);
		}
	| INTERVAL DAY_P opt_type_mod TO SECOND_P '(' Iconst ')'	%prec '/' /* height then no "( n )" operator */
		{
			$$ = SystemTypeNameLocation("interval", @1);
			$$->typmods = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1));
			if($3)
				$$->typmods = lappend($$->typmods, $3);
		}
	/*| type_function_name
		{
			$$ = makeTypeName($1);
			$$->location = @1;
		}
	| type_function_name '(' expr_list ')'
		{
			$$ = makeTypeName($1);
			$$->typmods = $3
			$$->location = @1;
		}*/
	;

simple_select:
		SELECT opt_distinct target_list from_clause where_clause
		group_clause having_clause
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = $2;
				n->targetList = $3;
				n->fromClause = $4;
				n->whereClause = $5;
				n->groupClause = $6;
				n->havingClause = $7;
				$$ = (Node*)n;
			}
		| select_clause UNION opt_all select_clause
			{
				$$ = makeSetOp(SETOP_UNION, $3, $1, $4);
			}
		| values_clause { $$ = $1; }
		;

sortby: a_expr opt_asc_desc
		{
			$$ = makeNode(SortBy);
			$$->node = $1;
			$$->sortby_dir = $2;
			$$->sortby_nulls = SORTBY_NULLS_DEFAULT;
			$$->useOp = NIL;
			$$->location = -1;		/* no operator */
		}
	;

sortby_list: sortby						{ $$ = list_make1($1); }
	| sortby_list ',' sortby			{ $$ = lappend($1, $3); }
	;

sort_clause: ORDER BY sortby_list		{ $$ = $3; }
	;

opt_sort_clause:
	  sort_clause						{ $$ = $1; }
	| /* empty */						{ $$ = NIL; }
	;

TableElement:
	  columnDef							{ $$ = $1; }
	| TableLikeClause					{ $$ = $1; }
	| TableConstraint					{ $$ = $1; }	  
	;

TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					n->options = $3;
					$$ = (Node *)n;
				}
		;

TableLikeOptionList:
				TableLikeOptionList INCLUDING TableLikeOption	{ $$ = $1 | $3; }
				| TableLikeOptionList EXCLUDING TableLikeOption	{ $$ = $1 & ~$3; }
				| /* EMPTY */						{ $$ = 0; }
		;

TableLikeOption:
				DEFAULTS			{ $$ = CREATE_TABLE_LIKE_DEFAULTS; }
				| CONSTRAINTS		{ $$ = CREATE_TABLE_LIKE_CONSTRAINTS; }
				| INDEXES			{ $$ = CREATE_TABLE_LIKE_INDEXES; }
				| STORAGE			{ $$ = CREATE_TABLE_LIKE_STORAGE; }
				| COMMENTS			{ $$ = CREATE_TABLE_LIKE_COMMENTS; }
				| ALL				{ $$ = CREATE_TABLE_LIKE_ALL; }
		;

TableElementList:
	  TableElement 						{ $$ = list_make1($1); }
	| TableElementList ',' TableElement { $$ = lappend($1, $3); }
	;

table_ref:
		  relation_expr opt_alias_clause
			{
				$1->alias = $2;
				$$ = (Node*) $1;
			}
		| select_with_parens opt_alias_clause
			{
				RangeSubselect *n = makeNode(RangeSubselect);
				n->lateral = false;
				n->subquery = $1;
				n->alias = $2;
				if(n->alias == NULL)
					ora_yyget_extra(yyscanner)->has_no_alias_subquery = true;
				$$ = (Node *) n;
			}
		| joined_table
			{
				$$ = (Node *) $1;
			}
		| '(' joined_table ')' alias_clause
			{
				$2->alias = $4;
				$$ = (Node *) $2;
			}
		;

target_item:
		a_expr AS ColLabel
			{
				$$ = makeNode(ResTarget);
				$$->name = $3;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}
		| a_expr IDENT
			{
				$$ = makeNode(ResTarget);
				$$->name = $2;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}
		| a_expr
			{
				$$ = makeNode(ResTarget);
				$$->name = NULL;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}
		| '*'
			{
				ColumnRef *n = makeNode(ColumnRef);
				n->fields = list_make1(makeNode(A_Star));
				n->location = @1;

				$$ = makeNode(ResTarget);
				$$->name = NULL;
				$$->indirection = NIL;
				$$->val = (Node *)n;
				$$->location = @1;
			}
		;

target_list:
		target_item 					{ $$ = list_make1($1); }
		| target_list ',' target_item	{ $$ = lappend($1, $3); }
		;

transaction_mode_item:
			ISOLATION LEVEL iso_level
					{ $$ = makeDefElem("transaction_isolation",
									   makeStringConst($3, @3)); }
			| READ ONLY
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(TRUE, @1)); }
			| READ WRITE
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(FALSE, @1)); }
			| DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(TRUE, @1)); }
			| NOT DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(FALSE, @1)); }
		;

/* Syntax with commas is SQL-spec, without commas is Postgres historical */
transaction_mode_list:
			transaction_mode_item
					{ $$ = list_make1($1); }
			| transaction_mode_list ',' transaction_mode_item
					{ $$ = lappend($1, $3); }
			| transaction_mode_list transaction_mode_item
					{ $$ = lappend($1, $2); }
		;

TruncateStmt:
	TRUNCATE TABLE relation_expr_list opt_restart_seqs opt_drop_behavior
	  TruncateStmt_log_opt TruncateStmt_storage_opt
		{
			TruncateStmt *n = makeNode(TruncateStmt);
			n->relations = $3;
			n->restart_seqs = $4;
			n->behavior = $5;
			/* ignore TruncateStmt_log_opt and TruncateStmt_storage_opt */
			$$ = (Node*)n;
		}
	;

TruncateStmt_log_opt:
	  PRESERVE MATERIALIZED VIEW LOG_P
	| PURGE MATERIALIZED VIEW LOG_P
	| /* empty */
	;

TruncateStmt_storage_opt:
	  DROP ALL STORAGE
	| DROP STORAGE
	| REUSE STORAGE
	| /* empty */
	;

Typename: SimpleTypename
	;

//transaction_mode_list_or_empty:
//			transaction_mode_list
//			| /* EMPTY */
//					{ $$ = NIL; }
//		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

UpdateStmt:
	  UPDATE relation_expr_opt_alias SET set_clause_list
	  where_clause returning_clause
		{
			UpdateStmt *n = makeNode(UpdateStmt);
			n->relation = $2;
			n->targetList = $4;
			n->whereClause = $5;
			n->returningList = $6;
			$$ = (Node*)n;
		}
	;

values_clause:
		VALUES ctext_row
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->valuesLists = list_make1($2);
				$$ = (Node *) n;
			}
		| values_clause ',' ctext_row
			{
				SelectStmt *n = (SelectStmt *) $1;
				n->valuesLists = lappend(n->valuesLists, $3);
				$$ = (Node *) n;
			}
	;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{
					$$ = palloc(strlen($1) + strlen($3) + 2);
					sprintf($$, "%s.%s", $1, $3);
				}
		;

VariableResetStmt:
			RESET var_name
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $2;
					$$ = (Node *) n;
				}
			| RESET TIME ZONE
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| RESET TRANSACTION ISOLATION LEVEL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| RESET SESSION AUTHORIZATION
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| RESET ALL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			| SET LOCAL set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (Node *) n;
				}
			| SET SESSION set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

VariableShowStmt:
	  SHOW var_name
		{
			VariableShowStmt *n = makeNode(VariableShowStmt);
			n->name = $2;
			$$ = (Node *) n; 
		}
         | SHOW TIME ZONE
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "timezone";
                         $$ = (Node *) n;
                 }
         | SHOW TRANSACTION ISOLATION LEVEL
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "transaction_isolation";
                         $$ = (Node *) n;
                 }
         | SHOW SESSION AUTHORIZATION
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "session_authorization";
                         $$ = (Node *) n;
                 }
         | SHOW ALL
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "all";
                         $$ = (Node *) n;
                 }
	;
	
/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query> 
 *
 *****************************************************************************/

ViewStmt: CREATE OptTemp VIEW qualified_name opt_column_list 
				AS SelectStmt 
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $4;
					n->view->relpersistence = $2;
					n->aliases = $5;
					n->query = $7;
					n->replace = false;
					/* n->options = $6; */
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt 
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $6;
					n->view->relpersistence = $4;
					n->aliases = $7;
					n->query = $10;
					n->replace = true;
					n->options = $8;
					$$ = (Node *) n;
				}
		| CREATE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' 
				AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $5;
					n->view->relpersistence = $2;
					n->aliases = $7;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $10);
					n->replace = false;
					/* n->options = $9;*/
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' 
				AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $7;
					n->view->relpersistence = $4;
					n->aliases = $9;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $12);
					n->replace = true;
					/* n->options = $11;*/
					$$ = (Node *) n;
				}
		;

//when_clause:
//	WHEN a_expr THEN a_expr
//		{
//			CaseWhen *w = makeNode(CaseWhen);
//			w->expr = (Expr *) $2;
//			w->result = (Expr *) $4;
//			w->location = @1;
//			$$ = (Node *)w;
//		}
//	;
//
//when_clause_list:
//	/* There must be at least one */
//	when_clause								{ $$ = list_make1($1); }
//	| when_clause_list when_clause			{ $$ = lappend($1, $2); }
//	;

where_clause: WHERE a_expr	{ $$ = $2; }
	| /* empty */ { $$ = NULL; }
	;

with_clause:
	  WITH cte_list
		{
			$$ = makeNode(WithClause);
			$$->ctes = $2;
			$$->recursive = false;
			$$->location = @1;
		}
	| WITH RECURSIVE cte_list
		{
			$$ = makeNode(WithClause);
			$$->ctes = $3;
			$$->recursive = true;
			$$->location = @1;
		}
	;

/*****************************************************************************
 *
 *		Transactions:
 *
 *		BEGIN / COMMIT / ROLLBACK
 *		(also older versions END / ABORT)
 *
 *****************************************************************************/

TransactionStmt:
			ABORT_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| BEGIN_P opt_transaction transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = $3;
					$$ = (Node *)n;
				}
			| START TRANSACTION transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = $3;
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| END_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| RELEASE SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($3)));
					$$ = (Node *)n;
				}
			| RELEASE ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($5)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($4)));
					$$ = (Node *)n;
				}
			| PREPARE TRANSACTION Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_PREPARE;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED IF_P EXISTS Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
#if defined(ADB)
					n->missing_ok = true;
#endif
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $5;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED IF_P EXISTS Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
#if defined(ADB)
					n->missing_ok = true;
#endif
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $5;
					$$ = (Node *)n;
				}
		;

opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

transaction_mode_list_or_empty:
			transaction_mode_list
			| /* EMPTY */
					{ $$ = NIL; }
		;

/* Timezone values can be:
 * - a string such as 'pst8pdt'
 * - an identifier such as "pst8pdt"
 * - an integer or floating point number
 * - a time interval per SQL99
 * ColId gives reduce/reduce errors against ConstInterval and LOCAL,
 * so use IDENT (meaning we reject anything that is a key word).
 */
zone_value:
			Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| IDENT
				{
					$$ = makeStringConst($1, @1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					if ($3 != NIL)
					{
						A_Const *n = (A_Const *) linitial($3);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@3)));
					}
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					TypeName *t = $1;
					if ($6 != NIL)
					{
						A_Const *n = (A_Const *) linitial($6);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@6)));
						if (list_length($6) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						t->typmods = lappend($6, makeIntConst($3, @3));
					}
					else
						t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;

/*********************************************************/
col_name_keyword:
	  BIGINT
	| BINARY_DOUBLE
	| BINARY_FLOAT
	| BOOLEAN_P
	| CASE
	| CHAR_P
	| DEC
	| DECIMAL_P
	| DATE_P
	| END_P
	| EXISTS
	| FLOAT_P
	| INT_P
	| INTEGER
	| LONG_P
	| NUMBER_P
	| SETOF
	| SMALLINT
	| NUMERIC
	| REAL
	| VARCHAR
	| TIME
	| TIMESTAMP
	| WHEN
	;

reserved_keyword:
	  ACCESS
	| ADD_P
	| ALL
	| AND
	| ANY
	| AS
	| ASC
	| AUDIT
	| BETWEEN
	| BY
	| CHECK
	| CLUSTER
	| COLUMN
	| COMMENT
	| CONSTRAINT
	| COLLATE
	| COMPRESS
	| CONTENT_P
	| CREATE
	| CURRENT_P
	| DEFAULT
	| DEFERRABLE
	| DELETE_P
	| DESC
	| DISTINCT
	| DROP
	| ELSE
	| FALSE_P
	| FILE_P
	| FOR
	| FROM
	| FOREIGN
	| GRANT
	| GROUP_P
	| HAVING
	| IDENTIFIED
	| IMMEDIATE
	| INITIALLY
	| IN_P
	| INCREMENT
	| INITIAL_P
	| INSERT
	| INTERSECT
	| INTO
	| IS
	| LEVEL
	| LIKE
	| LOCK_P
	| MAXEXTENTS
	| MINUS
	| MLSLABEL
	| MODE
	| MODIFY
	| NOAUDIT
	| NOCOMPRESS
	| NOT
	| NOWAIT
	| NULL_P
	| OF
	| OFFLINE
	| ON
	| ONLINE
	| OPTION
	| OR
	| ONLY
	| ORDER
	| PCTFREE
	| PRIOR
	| PUBLIC
	| RAW
	| REFERENCES
	| RESOURCE
	| REVOKE
	| ROW
	| ROWID
	| ROWNUM
	| ROWS
	| SELECT
	| SESSION
	| SET
	| SIZE
	| START
	| SUCCESSFUL
	| SYNONYM
	| SYSDATE
	| TABLE
	| THEN
	| TO
	| TRIGGER
	| TRUE_P
	| UID
	| UNION
	| UNIQUE
	| UPDATE
	| USER
	| USING
	| VALIDATE
	| VALUES
	| VARCHAR2
	| WHENEVER
	| WHERE
	| WITH
	;

type_func_name_keyword:
	  AUTHORIZATION
	| CROSS
	| COLLATION
	| CONCURRENTLY
	| FULL
	| INNER_P
	| JOIN
	| LEFT
	| OUTER_P
	| RIGHT
	;

unreserved_keyword:
	  ANALYSE
	| ABORT_P
	| ADMIN
	| ANALYZE
	| ALWAYS
	| ACTION
	| ALTER
	| BEGIN_P
	| BFILE
	| BLOB
	| BYTE_P
	| CASCADE
	| CACHE
	| NOCACHE
	| CATALOG_P
	| CONNECTION
	| CHARACTERISTICS
	| CLOB
	| COMMIT
	| CONFIGURATION
	| CONVERSION_P
	| COMMITTED
	| CONTINUE_P
	| COMMENTS
	| CONSTRAINTS
	/*| CURRVAL*/
	| CURSOR
	| CYCLE
	| NOCYCLE
	| DAY_P
	| DEFERRED
	| DICTIONARY
	| DOCUMENT_P
	| DOUBLE_P
	| DOMAIN_P
	| DATA_P
	| DEFAULTS
	| DISTRIBUTE
	| DISABLE_P
	| ESCAPE
	| ENABLE_P
	| EXCLUDE
	| ENCRYPTED
	| EXCLUDING
	| EVENT
	| EXTENSION
	| EXCLUSIVE
	| GLOBAL
	| IDENTITY_P
	| IF_P
	| INDEX
	| INHERIT
	| INDEXES
	| INCLUDING
	| INHERITS
	| ISOLATION
	| KEY
	| LIMIT
	| LOCAL
	| LOG_P
	| MATERIALIZED
	| MINUTE_P
	| MATCH
	| MAXVALUE
	| NOMAXVALUE
	| MINVALUE
	| NOMINVALUE
	| MONTH_P
	| NAMES
	| NCHAR
	| NODE
	| NCLOB
	| NO
	| NEXT
	/*| NEXTVAL*/
	| NVARCHAR2
	| OFF
	| OFFSET
	| OIDS
	| OPERATOR
	| OWNER
	| OWNED
	| OPTIONS
	| PASSWORD
	| PARSER
	| PRECISION
	| PREPARE
	| PREPARED
	| PRESERVE
	| PRIMARY
	| PARTIAL
	| PRIVILEGES
	| PURGE
	| READ
	| RELEASE
	| REPEATABLE
	| REPLACE
	| RESET
	| RULE
	| RESTART
	| RESTRICT
	| RETURNING
	| RETURN_P
	| REUSE
	| RENAME
	| REPLICA
	| ROLE
	| ROLLBACK
	| SCHEMA
	| SECOND_P
	| SEQUENCE
	| SHARE
	| STATISTICS
	| SERIALIZABLE
	| SHOW
	| SOME
	| SYSID
	| SEARCH
	| SAVEPOINT
	| SIMPLE
	| SNAPSHOT
	| STORAGE
	| TEMP
	| TEMPLATE
	| TEMPORARY
	| TRANSACTION
	| TRUNCATE
	| TYPE_P
	| TEXT_P
	| TABLESPACE
	| UNCOMMITTED
	| UNLOGGED
	| UNTIL
	| UNENCRYPTED
	| VERBOSE
	| VIEW
	| VALID
	| WRITE
	| WORK
	| WITHOUT
	| XML_P
	| YEAR_P
	| ZONE
	;

%%

#define INVALID_TOKEN -1

/*
 * Process result of ConstraintAttributeSpec, and set appropriate bool flags
 * in the output command node.  Pass NULL for any flags the particular
 * command doesn't support.
 */
static void
processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner)
{
	/* defaults */
	if (deferrable)
		*deferrable = false;
	if (initdeferred)
		*initdeferred = false;
	if (not_valid)
		*not_valid = false;

	if (cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED))
	{
		if (deferrable)
			*deferrable = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_INITIALLY_DEFERRED)
	{
		if (initdeferred)
			*initdeferred = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NOT_VALID)
	{
		if (not_valid)
			*not_valid = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NOT VALID",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NO_INHERIT)
	{
		if (no_inherit)
			*no_inherit = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NO INHERIT",
							constrType),
					 parser_errposition(location)));
	}
}

void ora_parser_init(ora_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;
	StaticAssertExpr(lengthof(yyext->lookahead)==2, "change init code");
	yyext->lookahead[0].token = yyext->lookahead[1].token = INVALID_TOKEN;
	yyext->has_no_alias_subquery = false;
}

static void ora_yyerror(YYLTYPE *yyloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static Node *makeDeleteStmt(RangeVar *range, Alias *alias, WithClause *with
	, Node *where, List *returning)
{
	DeleteStmt *stmt = makeNode(DeleteStmt);
	if(alias)
	{
		Assert(range->alias == NULL);
		range->alias = alias;
	}
	stmt->relation = range;
	stmt->withClause = with;
	stmt->whereClause = where;
	stmt->returningList = returning;
	return (Node*)stmt;
}

static int ora_yylex(YYSTYPE *lvalp, YYLTYPE *lloc, core_yyscan_t yyscanner)
{
	ora_yy_extra_type *yyextra = ora_yyget_extra(yyscanner);
	int			cur_token;

	if(yyextra->lookahead[0].token != INVALID_TOKEN)
	{
		cur_token = yyextra->lookahead[0].token;
		lvalp->core_yystype = yyextra->lookahead[0].lval;
		*lloc = yyextra->lookahead[0].loc;
		memcpy(yyextra->lookahead, &(yyextra->lookahead[1])
			, sizeof(yyextra->lookahead) - sizeof(yyextra->lookahead[0]));
		yyextra->lookahead[lengthof(yyextra->lookahead)-1].token = INVALID_TOKEN;
	}else
	{
		cur_token = core_yylex(&(lvalp->core_yystype), lloc, yyscanner);
	}

	switch(cur_token)
	{
	/* find "(+)" token */
	case '(':
		if(yyextra->lookahead[0].token == INVALID_TOKEN)
		{
			yyextra->lookahead[0].token = core_yylex(&(yyextra->lookahead[0].lval)
				, &(yyextra->lookahead[0].loc), yyscanner);
		}
		if(yyextra->lookahead[0].token != '+')
			break;

		if(yyextra->lookahead[1].token == INVALID_TOKEN)
		{
			yyextra->lookahead[1].token = core_yylex(&(yyextra->lookahead[1].lval)
				, &(yyextra->lookahead[1].loc), yyscanner);
		}
		if(yyextra->lookahead[1].token != ')')
			break;

		/* now we have "(+)" token */
		cur_token = ORACLE_JOIN_OP;
		StaticAssertExpr(lengthof(yyextra->lookahead)==2, "change invalid code");
		yyextra->lookahead[0].token = yyextra->lookahead[1].token = INVALID_TOKEN;
		break;
	default:
		break;
	}
	return cur_token;
}

static Node *reparse_decode_func(List *args, int location)
{
	List		*cargs = NIL;
	ListCell 	*lc = NULL;
	Expr		*expr = NULL;
	Node		*search = NULL;

	CaseExpr *c = makeNode(CaseExpr);
	c->casetype = InvalidOid; /* not analyzed yet */
	c->isdecode = true;
	expr = (Expr *)linitial(args);

	for_each_cell(lc, lnext(list_head(args)))
	{
		if (lnext(lc) == NULL)
		{
			break;
		} else
		{
			CaseWhen *w = makeNode(CaseWhen);
			search = (Node *)lfirst(lc);
			if (IsA(search, A_Const) &&
				((A_Const*)search)->val.type == T_Null)
			{
				NullTest *n = (NullTest *)makeNode(NullTest);
				n->arg = expr;
				n->nulltesttype = IS_NULL;
				w->expr = (Expr *)n;
			} else
			{
				w->expr = (Expr *) makeSimpleA_Expr(AEXPR_OP,
													"=",
													(Node *)expr,
													search,
													-1);
			}
			w->result = (Expr *) lfirst(lnext(lc));
			w->location = -1;
			cargs = lappend(cargs, w);
			lc = lnext(lc);
		}
	}
	c->args = cargs;
	c->defresult = lc ? (Expr *)lfirst(lc) : NULL;
	c->location = location;

	return (Node *)c;
}

static bool get_all_names(Node *n, List **pplist)
{
	AssertArg(pplist);
	if(n == NULL)
		return false;

	if(IsA(n, Alias))
	{
		//APPEND_ALIAS((Alias*)n);
		Assert(((Alias*)n)->aliasname);
		*pplist = lappend(*pplist, ((Alias*)n)->aliasname);
		return false;
	}/*if(IsA(n, JoinExpr))
	{
		APPEND_ALIAS(((JoinExpr*)n)->alias);
	}else if(IsA(n, RangeSubselect))
	{
		APPEND_ALIAS(((RangeSubselect*)n)->alias)
	}*/else if(IsA(n, RangeVar))
	{
		RangeVar *range = (RangeVar*)n;
		*pplist = lappend(*pplist, range->alias ? range->alias->aliasname : range->relname);
		return false;
	}else if(IsA(n, RangeFunction))
	{
		RangeFunction *range = (RangeFunction*)n;
		if(range->alias)
		{
			*pplist = lappend(*pplist, range->alias->aliasname);
			return false;
		}
	}else if(IsA(n, FuncCall))
	{
		FuncCall *func = (FuncCall*)n;
		Value *value;
		Assert(func->funcname);
		value = linitial(func->funcname);
		Assert(value && IsA(value,String));
		*pplist = lappend(*pplist, strVal(value));
		/* don't need function's argument(s) */
		return false;
	}else if(IsA(n, CommonTableExpr))
	{
		CommonTableExpr *cte = (CommonTableExpr*)n;
		*pplist = lappend(*pplist, cte->ctename);
	}else if(IsA(n, ColumnRef))
	{
		ColumnRef *column = (ColumnRef*)n;
		Value *value;
		int length = list_length(column->fields);
		if(length > 1)
		{
			value = list_nth(column->fields, length - 2);
			Assert(value && IsA(value, String));
			*pplist = lappend(*pplist, strVal(value));
		}
	}
	return node_tree_walker(n, get_all_names, pplist);
}

static unsigned int add_alias_name_idx;
static char add_alias_name[NAMEDATALEN];
static bool add_alias_internal(Node *n, List *names)
{
	if(n && IsA(n, RangeSubselect) && ((RangeSubselect*)n)->alias == NULL)
	{
		RangeSubselect *sub = (RangeSubselect*)n;
		Alias *alias = makeNode(Alias);
		ListCell *lc;
		bool used;
		do
		{
			used = false;
			++add_alias_name_idx;
			snprintf(add_alias_name, lengthof(add_alias_name)
				, "__AUTO_ADD_ALIAS_%d__", add_alias_name_idx);
			foreach(lc, names)
			{
				if(strcmp(add_alias_name, lfirst(lc)) == 0)
				{
					used = true;
					break;
				}
			}
		}while(used);
		alias->aliasname = pstrdup(add_alias_name);
		sub->alias = alias;
	}
	return node_tree_walker(n, add_alias_internal, names);
}

static void add_alias_if_need(List *parseTree)
{
	List *names = NIL;
	get_all_names((Node*)parseTree, &names);
	add_alias_name_idx = 0;
	add_alias_internal((Node*)parseTree, names);
	list_free(names);
}

static Node* make_any_sublink(Node *testexpr, const char *operName, Node *subselect, int location)
{
	SubLink *n = makeNode(SubLink);
	n->subLinkType = ANY_SUBLINK;
	n->testexpr = testexpr;
	n->operName = list_make1(makeString(pstrdup(operName)));
	n->subselect = subselect;
	n->location = location;
	return (Node*)n;
}
