%{

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "nodes/pg_list.h"
#include "parser/mgr_node.h"
#include "parser/parser.h"
#include "parser/scanner.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/mgr_parm.h"
/*
 * The YY_EXTRA data that a flex scanner allows us to pass around.  Private
 * state needed for raw parsing/lexing goes here.
 */
typedef struct mgr_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	core_yy_extra_type core_yy_extra;

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */
} mgr_yy_extra_type;

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define mgr_yyget_extra(yyscanner) (*((mgr_yy_extra_type **) (yyscanner)))

/*
 * Location tracking support --- simpler than bison's default, since we only
 * want to track the start position not the end position of each nonterminal.
 */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if ((N) > 0) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (-1); \
	} while (0)

#define YYMALLOC palloc
#define YYFREE   pfree

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)

union YYSTYPE;					/* need forward reference for tok_is_keyword */
static void mgr_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner,
						 const char *msg);
static int mgr_yylex(union YYSTYPE *lvalp, YYLTYPE *llocp,
		   core_yyscan_t yyscanner);
List *mgr_parse_query(const char *query_string);
static ResTarget* make_star_target(int location);
static Node* make_column_in(const char *col_name, List *values);
static Node* makeNode_RangeFunction(const char *func_name, List *func_args);
static Node* make_func_call(const char *func_name, List *func_args);
static List* make_start_agent_args(List *options);
extern char *defGetString(DefElem *def);
%}

%pure-parser
%expect 0
%name-prefix="mgr_yy"
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

	char				chr;
	bool				boolean;
	List				*list;
	Node				*node;
	VariableSetStmt		*vsetstmt;
	Value				*value;
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
%token			TYPECAST DOT_DOT COLON_EQUALS

%type <list>	stmtblock stmtmulti
%type <node>	stmt
%type <node>	AddHostStmt DropHostStmt ListHostStmt AlterHostStmt
				ListParmStmt StartAgentStmt AddNodeStmt StopAgentStmt
				DropNodeStmt AlterNodeStmt ListNodeStmt InitNodeStmt 
				VariableSetStmt StartNodeMasterStmt StopNodeMasterStmt
				MonitorStmt FailoverStmt ConfigAllStmt DeploryStmt
				Gethostparm ListMonitor Gettopologyparm Update_host_config_value
				Get_host_threshold Get_alarm_info AppendNodeStmt
				AddUpdataparmStmt CleanAllStmt ResetUpdataparmStmt

%type <list>	general_options opt_general_options general_option_list
				AConstList targetList ObjList var_list NodeConstList set_parm_general_options
%type <node>	general_option_item general_option_arg target_el
%type <node> 	var_value

%type <ival>	Iconst SignedIconst opt_gtm_inner_type opt_dn_inner_type
%type <vsetstmt> set_rest set_rest_more
%type <value>	NumericOnly

%type <keyword>	unreserved_keyword reserved_keyword
%type <str>		Ident SConst ColLabel var_name opt_boolean_or_string
				NonReservedWord NonReservedWord_or_Sconst set_ident
				opt_password opt_stop_mode_s opt_stop_mode_f opt_stop_mode_i

%token<keyword>	ADD_P DEPLOY DROP ALTER LIST
%token<keyword>	IF_P EXISTS NOT
%token<keyword>	FALSE_P TRUE_P
%token<keyword>	HOST MONITOR PARM
%token<keyword>	INIT GTM MASTER SLAVE EXTRA ALL NODE COORDINATOR DATANODE
%token<keyword> PASSWORD CLEAN RESET
%token<keyword> START AGENT STOP FAILOVER
%token<keyword> SET TO ON OFF
%token<keyword> APPEND CONFIG MODE FAST SMART IMMEDIATE S I F FORCE

/* for ADB monitor*/
%token<keyword> GET_HOST_LIST_ALL GET_HOST_LIST_SPEC
				GET_HOST_HISTORY_USAGE GET_ALL_NODENAME_IN_SPEC_HOST
				GET_AGTM_NODE_TOPOLOGY GET_COORDINATOR_NODE_TOPOLOGY GET_DATANODE_NODE_TOPOLOGY
				GET_CLUSTER_FOURITEM GET_CLUSTER_SUMMARY GET_DATABASE_TPS_QPS GET_CLUSTER_HEADPAGE_LINE
				GET_DATABASE_TPS_QPS_INTERVAL_TIME GET_DATABASE_SUMMARY GET_SLOWLOG GET_USER_INFO UPDATE_USER GET_SLOWLOG_COUNT
				UPDATE_THRESHOLD_VALUE UPDATE_PASSWORD CHECK_USER
				GET_THRESHOLD_TYPE GET_THRESHOLD_ALL_TYPE CHECK_PASSWORD GET_DB_THRESHOLD_ALL_TYPE
				GET_ALARM_INFO_ASC GET_ALARM_INFO_DESC RESOLVE_ALARM GET_ALARM_INFO_COUNT
%%
/*
 *	The target production for the whole parse.
 */
stmtblock:	stmtmulti
			{
				mgr_yyget_extra(yyscanner)->parsetree = $1;
			}
		;

/* the thrashing around here is to discard "empty" statements... */
stmtmulti:	stmtmulti ';' stmt
				{
					if ($3 != NULL)
						$$ = lappend($1, $3);
					else
						$$ = $1;
				}
			| stmt
				{
					if ($1 != NULL)
						$$ = list_make1($1);
					else
						$$ = NIL;
				}
		;

stmt :
	  AddHostStmt
	| DropHostStmt
	| ListHostStmt
	| AlterHostStmt
	| StartAgentStmt
	| StopAgentStmt
	| ListMonitor
	| ListParmStmt
	| AddNodeStmt
	| AlterNodeStmt
	| DropNodeStmt
	| ListNodeStmt
	| MonitorStmt
	| VariableSetStmt
	| InitNodeStmt
	| StartNodeMasterStmt
	| StopNodeMasterStmt
	| FailoverStmt
	| ConfigAllStmt
	| DeploryStmt
	| Gethostparm     /* for ADB monitor host page */
	| Gettopologyparm /* for ADB monitor home page */
	| Update_host_config_value
	| Get_host_threshold
	| Get_alarm_info
	| AppendNodeStmt
	| AddUpdataparmStmt
	|	ResetUpdataparmStmt
	| CleanAllStmt
	| /* empty */
		{ $$ = NULL; }
	;
AppendNodeStmt:
		APPEND DATANODE MASTER Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_dnmaster", args));
			$$ = (Node*)stmt;
		}
		| APPEND DATANODE SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_dnslave", args));
			$$ = (Node*)stmt;
		}
		| APPEND DATANODE EXTRA Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_dnextra", args));
			$$ = (Node*)stmt;
		}
		| APPEND COORDINATOR Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_coordmaster", args));
			$$ = (Node*)stmt;
		}
		| APPEND GTM SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_agtmslave", args));
			$$ = (Node*)stmt;
		}
		| APPEND GTM EXTRA Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_agtmextra", args));
			$$ = (Node*)stmt;
		};

Get_alarm_info:
		GET_ALARM_INFO_ASC '(' Ident ',' Ident ',' SConst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			args = lappend(args, makeIntConst($13, -1));
			args = lappend(args, makeIntConst($15, -1));
			args = lappend(args, makeIntConst($17, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_alarm_info_asc", args));
			$$ = (Node*)stmt;
		}
		| GET_ALARM_INFO_DESC '(' Ident ',' Ident ',' SConst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			args = lappend(args, makeIntConst($13, -1));
			args = lappend(args, makeIntConst($15, -1));
			args = lappend(args, makeIntConst($17, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_alarm_info_desc", args));
			$$ = (Node*)stmt;
		}
		| GET_ALARM_INFO_COUNT '(' Ident ',' Ident ',' SConst ',' SignedIconst ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			args = lappend(args, makeIntConst($13, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_alarm_info_count", args));
			$$ = (Node*)stmt;
		} 
		|RESOLVE_ALARM '(' SignedIconst ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args,makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("resolve_alarm", args));
			$$ = (Node*)stmt;
		};

Gettopologyparm:
        GET_AGTM_NODE_TOPOLOGY
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_agtm_node_topology"), -1));
            $$ = (Node*)stmt;
        }
        | GET_COORDINATOR_NODE_TOPOLOGY
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_coordinator_node_topology"), -1));
            $$ = (Node*)stmt;
        }
        | GET_DATANODE_NODE_TOPOLOGY
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_datanode_node_topology"), -1));
            $$ = (Node*)stmt;
        };

Gethostparm:
		GET_HOST_LIST_ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_all_host_parm"), -1));
			$$ = (Node*)stmt;
		}
		| GET_HOST_LIST_SPEC '(' AConstList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_spec_host_parm"), -1));
			stmt->whereClause = make_column_in("hostname", $3);
			$$ = (Node*)stmt;
		}
		| GET_HOST_HISTORY_USAGE '(' Ident ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeIntConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_host_history_usage", args));
			$$ = (Node*)stmt;
		}
        | GET_ALL_NODENAME_IN_SPEC_HOST '(' Ident ')'
        {
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_all_nodename_in_spec_host", args));
			$$ = (Node*)stmt;
        };

Update_host_config_value:
		UPDATE_THRESHOLD_VALUE '(' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args, makeIntConst($5, -1));
			args = lappend(args, makeIntConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("update_threshold_value", args));
			$$ = (Node*)stmt;
		};

Get_host_threshold:
		GET_THRESHOLD_TYPE '(' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_threshold_type", args));
			$$ = (Node*)stmt;
		}
		| GET_THRESHOLD_ALL_TYPE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_threshold_all_type"), -1));
			$$ = (Node*)stmt;
		}
		|	GET_DB_THRESHOLD_ALL_TYPE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_db_threshold_all_type"), -1));
			$$ = (Node*)stmt;
		}
		;

ConfigAllStmt:
		CONFIG ALL
		{
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_configure_nodes_all", NULL));
            $$ = (Node*)stmt;
		};

MonitorStmt:
		MONITOR ALL
		{
            SelectStmt *stmt = makeNode(SelectStmt);
            //List *arg = list_make1(makeNullAConst(-1));
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_all"), -1));
            $$ = (Node*)stmt;
		}
        | MONITOR COORDINATOR ALL
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            //List *arg = list_make1(makeNullAConst(-1));
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_coord_all", NULL));
            $$ = (Node*)stmt;
        }
		| MONITOR DATANODE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_datanode_all"), -1));
			$$ = (Node*)stmt;
		}
		| MONITOR DATANODE MASTER ALL
		{
            SelectStmt *stmt = makeNode(SelectStmt);
            //List *arg = list_make1(makeNullAConst(-1));
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_dnmaster_all", NULL));
            $$ = (Node*)stmt;
		}
		| MONITOR DATANODE SLAVE ALL
		{
            SelectStmt *stmt = makeNode(SelectStmt);
            //List *arg = list_make1(makeNullAConst(-1));
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_dnslave_all", NULL));
            $$ = (Node*)stmt;
		}
		| MONITOR DATANODE EXTRA ALL
		{
            SelectStmt *stmt = makeNode(SelectStmt);
            //List *arg = list_make1(makeNullAConst(-1));
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_dnextra_all", NULL));
            $$ = (Node*)stmt;
		}
        | MONITOR COORDINATOR NodeConstList
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_coord_namelist", $3));
            $$ = (Node*)stmt;
        }
        | MONITOR DATANODE MASTER NodeConstList
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_dnmaster_namelist", $4));
            $$ = (Node*)stmt;
        }
        | MONITOR DATANODE SLAVE NodeConstList
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_dnslave_namelist", $4));
            $$ = (Node*)stmt;
        }
		| MONITOR DATANODE EXTRA NodeConstList
		{
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_dnextra_namelist", $4));
            $$ = (Node*)stmt;
		}
        ;
		
VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			;
			
set_rest: set_rest_more { $$ = $1; };

set_rest_more:
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
			;

var_name:	Ident									{ $$ = $1; }
			| var_name '.' Ident
				{
					$$ = palloc(strlen($1) + strlen($3) + 2);
					sprintf($$, "%s.%s", $1, $3);
				}
			;
			
var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
			;

var_value:	opt_boolean_or_string  	{ $$ = makeStringConst($1, @1); }
			| NumericOnly    		{ $$ = makeAConst($1, @1); }
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
			
NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1; }
			| SConst								{ $$ = $1; }
			;
			
NonReservedWord:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
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

AddHostStmt:
	  ADD_P HOST Ident opt_general_options
		{
			MGRAddHost *node = makeNode(MGRAddHost);
			node->if_not_exists = false;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
		}
	| ADD_P HOST IF_P NOT EXISTS Ident opt_general_options
		{
			MGRAddHost *node = makeNode(MGRAddHost);
			node->if_not_exists = true;
			node->name = $6;
			node->options = $7;
			$$ = (Node*)node;
		}
	;

opt_general_options:
	  general_options	{ $$ = $1; }
	| /* empty */		{ $$ = NIL; }
	;

set_parm_general_options:
	  general_options	{ $$ = $1; }
	;
	
general_options: '(' general_option_list ')'
		{
			$$ = $2;
		}
	;

general_option_list:
	  general_option_item
		{
			$$ = list_make1($1);
		}
	| general_option_list ',' general_option_item
		{
			$$ = lappend($1, $3);
		}
	;

general_option_item:
	  ColLabel general_option_arg		{ $$ = (Node*)makeDefElem($1, $2); }
	| ColLabel '=' general_option_arg	{ $$ = (Node*)makeDefElem($1, $3); }
	| ColLabel 							{ $$ = (Node*)makeDefElem($1, NULL); }
	;

general_option_arg:
	  Ident								{ $$ = (Node*)makeString($1); }
	| SConst							{ $$ = (Node*)makeString($1); }
	| SignedIconst						{ $$ = (Node*)makeInteger($1); }
	| FCONST							{ $$ = (Node*)makeFloat($1); }
	| reserved_keyword					{ $$ = (Node*)makeString(pstrdup($1)); }
	;

DropHostStmt:
	  DROP HOST ObjList
		{
			MGRDropHost *node = makeNode(MGRDropHost);
			node->if_exists = false;
			node->hosts = $3;
			$$ = (Node*)node;
		}
	| DROP HOST IF_P EXISTS ObjList
		{
			MGRDropHost *node = makeNode(MGRDropHost);
			node->if_exists = true;
			node->hosts = $5;
			$$ = (Node*)node;
		}
	;

ObjList:
	  ObjList ',' Ident
		{
			$$ = lappend($1, makeString($3));
		}
	| Ident
		{
			$$ = list_make1(makeString($1));
		}
	;	

ListHostStmt:
	  LIST HOST
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			$$ = (Node*)stmt;
		}
	| LIST HOST '(' targetList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			$$ = (Node*)stmt;
		}
	| LIST HOST AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			stmt->whereClause = make_column_in("name", $3);
			$$ = (Node*)stmt;
		}
	| LIST HOST '(' targetList ')' AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			stmt->whereClause = make_column_in("name", $6);
			$$ = (Node*)stmt;
		}
	;

AConstList:
	  AConstList ',' Ident	{ $$ = lappend($1, makeAConst(makeString($3), @3)); }
	| Ident						{ $$ = list_make1(makeAConst(makeString($1), @1)); }
	;
NodeConstList:
	  NodeConstList	Ident	{ $$ = lappend($1, makeAConst(makeString($2), @2)); }
	| Ident						{ $$ = list_make1(makeAConst(makeString($1), @1)); }
	;
targetList:
	  targetList ',' target_el	{ $$ = lappend($1, $3); }
	| target_el					{ $$ = list_make1($1); }
	;

target_el:
	  Ident
		{
			ResTarget *target = makeNode(ResTarget);
			ColumnRef *col = makeNode(ColumnRef);
			col->fields = list_make1(makeString($1));
			col->location = @1;
			target->val = (Node*)col;
			target->location = @1;
			$$ = (Node*)target;
		}
	| '*'
		{
			$$ = (Node*)make_star_target(@1);
		}
	;

Ident:
	  IDENT					{ $$ = $1; }
	| unreserved_keyword	{ $$ = pstrdup($1); }
	;
set_ident:
	 Ident					{ $$ = $1; }
	|	ALL					{ $$ = pstrdup("*"); }
	;
SConst: SCONST				{ $$ = $1; }
Iconst: ICONST				{ $$ = $1; }

SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;

ColLabel:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| reserved_keyword						{ $$ = pstrdup($1); }

AlterHostStmt:
        ALTER HOST Ident opt_general_options
		{
			MGRAlterHost *node = makeNode(MGRAlterHost);
			node->if_not_exists = false;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
		}
	;

StartAgentStmt:
		START AGENT ALL opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = make_start_agent_args($4);
			args = lappend(args, makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_agent", args));
			$$ = (Node*)stmt;
		}
	 | START AGENT Ident opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = make_start_agent_args($4);
			args = lappend(args, makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_agent", args));
			$$ = (Node*)stmt;
		}
		;
StopAgentStmt:
		STOP AGENT ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_agent", args));
			$$ = (Node*)stmt;
		}
	|	STOP AGENT Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_agent", args));
			$$ = (Node*)stmt;
		}
		;

/* parm start*/
AddUpdataparmStmt:
		SET GTM opt_gtm_inner_type Ident set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTM;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	SET GTM opt_gtm_inner_type Ident set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTM;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force= true;
				$$ = (Node*)node;
		}
	| SET DATANODE opt_dn_inner_type set_ident set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| SET DATANODE opt_dn_inner_type set_ident set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| SET COORDINATOR MASTER set_ident set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| SET COORDINATOR MASTER set_ident set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
		;
ResetUpdataparmStmt:
		RESET GTM opt_gtm_inner_type Ident set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTM;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	RESET GTM opt_gtm_inner_type Ident set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTM;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET DATANODE opt_dn_inner_type set_ident set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET DATANODE opt_dn_inner_type set_ident set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET COORDINATOR MASTER set_ident set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET COORDINATOR MASTER set_ident set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
		;

ListParmStmt:
	  LIST PARM
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));
			$$ = (Node*)stmt;
		}
	| LIST PARM '(' targetList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));
			$$ = (Node*)stmt;
		}
	| LIST PARM AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));
			stmt->whereClause = make_column_in("nodename", $3);
			$$ = (Node*)stmt;
		}
	| LIST PARM '(' targetList ')' AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));
			stmt->whereClause = make_column_in("nodename", $6);
			$$ = (Node*)stmt;
		}
	;
/* parm end*/

CleanAllStmt:
		CLEAN ALL
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_clean_all", NULL));
			$$ = (Node*)stmt;
	}

/* gtm/coordinator/datanode 
*/
AddNodeStmt:
	  ADD_P GTM opt_gtm_inner_type Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->if_not_exists = false;
			node->nodetype = $3; 
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ADD_P GTM opt_gtm_inner_type IF_P NOT EXISTS Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->if_not_exists = true;
			node->nodetype = $3;
			node->name = $7;
			node->options = $8;
			$$ = (Node*)node;
		}
	| ADD_P COORDINATOR Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->if_not_exists = false;
			node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
		}
	| ADD_P COORDINATOR IF_P NOT EXISTS Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->if_not_exists = true;
			node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
			node->name = $6;
			node->options = $7;
			$$ = (Node*)node;
		}
	| ADD_P DATANODE opt_dn_inner_type Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->if_not_exists = false;
			node->nodetype = $3;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ADD_P DATANODE opt_dn_inner_type IF_P NOT EXISTS Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->if_not_exists = true;
			node->nodetype = $3;
			node->name = $7;
			node->options = $8;
			$$ = (Node*)node;
		}
	;
	

AlterNodeStmt:
		ALTER GTM opt_gtm_inner_type Ident opt_general_options
		{
			MGRAlterNode *node = makeNode(MGRAlterNode);
			node->if_not_exists = false;
			node->nodetype = $3;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ALTER COORDINATOR Ident opt_general_options
		{
			MGRAlterNode *node = makeNode(MGRAlterNode);
			node->if_not_exists = false;
			node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
		}
	| ALTER DATANODE opt_dn_inner_type Ident opt_general_options
		{
			MGRAlterNode *node = makeNode(MGRAlterNode);
			node->if_not_exists = false;
			node->nodetype = $3;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	;

DropNodeStmt:
	  DROP GTM opt_gtm_inner_type ObjList
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->if_exists = false;
			node->nodetype = $3;
			node->names = $4;
			$$ = (Node*)node;
		}
	|	DROP GTM opt_gtm_inner_type IF_P EXISTS ObjList
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->if_exists = true;
			node->nodetype = $3;
			node->names = $6;
			$$ = (Node*)node;
		}
	|	DROP COORDINATOR ObjList
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->if_exists = false;
			node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
			node->names = $3;
			$$ = (Node*)node;
		}
	|	DROP COORDINATOR IF_P EXISTS ObjList
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->if_exists = true;
			node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
			node->names = $5;
			$$ = (Node*)node;
		}
	|	DROP DATANODE opt_dn_inner_type ObjList
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->if_exists = false;
			node->nodetype = $3;
			node->names = $4;
			$$ = (Node*)node;
		}
	|	DROP DATANODE opt_dn_inner_type IF_P EXISTS ObjList
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->if_exists = true;
			node->nodetype = $3;
			node->names = $6;
			$$ = (Node*)node;
		}
	;


ListNodeStmt:
	  LIST NODE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			$$ = (Node*)stmt;
		}
	| LIST NODE '(' targetList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			$$ = (Node*)stmt;
		}
	| LIST NODE AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("name", $3);
			$$ = (Node*)stmt;
		}
	| LIST NODE '(' targetList ')' AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("name", $6);
			$$ = (Node*)stmt;
		}
	;
InitNodeStmt:
	  INIT GTM MASTER 
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_gtm_master", args));
			$$ = (Node*)stmt;
		}
	| INIT GTM SLAVE 
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_gtm_slave", args));
			$$ = (Node*)stmt;
		}
	| INIT GTM EXTRA 
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_gtm_extra", args));
			$$ = (Node*)stmt;
		}
	| INIT COORDINATOR NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_cn_master", $3));
			$$ = (Node*)stmt;
		}
	| INIT COORDINATOR  ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_cn_master", args));
			$$ = (Node*)stmt;
		}
	|	INIT DATANODE MASTER NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_dn_master", $4));
			$$ = (Node*)stmt;
		}
	|	INIT DATANODE MASTER ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_dn_master", args));
			$$ = (Node*)stmt;
		}
	| INIT DATANODE SLAVE AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_dn_slave", $4));
			$$ = (Node*)stmt;
		}
	| INIT DATANODE EXTRA AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_dn_extra", $4));
			$$ = (Node*)stmt;
		}
	|	INIT DATANODE SLAVE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_dn_slave_all", NULL));
			$$ = (Node*)stmt;
		}
	|	INIT DATANODE EXTRA ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_init_dn_extra_all", NULL));
			$$ = (Node*)stmt;
		}
	| INIT DATANODE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("initdatanodeall"), -1));
			$$ = (Node*)stmt;
		}
	| INIT ALL
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("initall"), -1));
			$$ = (Node*)stmt;
	}
	;
StartNodeMasterStmt:
		START GTM MASTER
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_gtm_master", args));
			$$ = (Node*)stmt;
		}
	|	START GTM SLAVE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_gtm_slave", args));
			$$ = (Node*)stmt;
		}
	| START GTM EXTRA
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_gtm_extra", args));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_cn_master", $3));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_cn_master", args));
			$$ = (Node*)stmt;
		}
	|	START DATANODE MASTER NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_master", $4));
			$$ = (Node*)stmt;
		}
	| START DATANODE MASTER ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_master", args));
			$$ = (Node*)stmt;
		}
	|	START DATANODE SLAVE NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_slave", $4));
			$$ = (Node*)stmt;
		}
	|	START DATANODE EXTRA NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_extra", $4));
			$$ = (Node*)stmt;
		}
	|	START DATANODE SLAVE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_slave", args));
			$$ = (Node*)stmt;
		}
	|	START DATANODE EXTRA ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_extra", args));
			$$ = (Node*)stmt;
		}
	|	START DATANODE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("start_datanode_all"), -1));
			$$ = (Node*)stmt;
		}
	|	START ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("startall"), -1));
			$$ = (Node*)stmt;
		}
	;
StopNodeMasterStmt:
		STOP GTM MASTER opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM MASTER opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_master_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM MASTER opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_master_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM SLAVE opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_slave", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM SLAVE opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_slave_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM SLAVE opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_slave_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM EXTRA opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_extra", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM EXTRA opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_extra_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTM EXTRA opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("gtm", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtm_extra_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR AConstList opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master", $3));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR AConstList opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master_i", $3));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR AConstList opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master_f", $3));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR ALL opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR ALL opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR ALL opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER AConstList opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER AConstList opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master_f", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER AConstList opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master_i", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER ALL opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER ALL opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER ALL opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE AConstList opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE AConstList opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave_f", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE AConstList opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave_i", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE EXTRA AConstList opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_extra", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE EXTRA AConstList opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_extra_f", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE EXTRA AConstList opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_extra_i", $4));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE EXTRA ALL opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_extra", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE EXTRA ALL opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_extra_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE EXTRA ALL opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_extra_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE ALL opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE ALL opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave_f", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE ALL opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave_i", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE ALL opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_datanode_all"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE ALL opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_datanode_all_f"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE ALL opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_datanode_all_i"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP ALL opt_stop_mode_s
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stopall"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP ALL opt_stop_mode_f
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stopall_f"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP ALL opt_stop_mode_i
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stopall_i"), -1));
			$$ = (Node*)stmt;
		}
	;
FailoverStmt:
		FAILOVER DATANODE SLAVE Ident
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("slave", -1));
			args = lappend(args,makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_one_dn", args));
			$$ = (Node*)stmt;
	}
	|	FAILOVER DATANODE EXTRA Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("extra", -1));
			args = lappend(args,makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_one_dn", args));
			$$ = (Node*)stmt;
		}
	| FAILOVER DATANODE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("either", -1));
			args = lappend(args,makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_one_dn", args));
			$$ = (Node*)stmt;
		}
	| FAILOVER GTM SLAVE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("slave", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_gtm", args));
			$$ = (Node*)stmt;
		}
	| FAILOVER GTM EXTRA
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("extra", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_gtm", args));
			$$ = (Node*)stmt;
		}
	| FAILOVER GTM
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("either", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_gtm", args));
			$$ = (Node*)stmt;
		}
	;
/* cndn end*/

DeploryStmt:
	  DEPLOY ALL opt_password
		{
			MGRDeplory *stmt = makeNode(MGRDeplory);
			stmt->hosts = NIL;
			stmt->password = $3;
			$$ = (Node*)stmt;
		}
	| DEPLOY ObjList opt_password
		{
			MGRDeplory *stmt = makeNode(MGRDeplory);
			stmt->hosts = $2;
			stmt->password = $3;
			$$ = (Node*)stmt;
		}
	;

opt_password:
	  PASSWORD SConst		{ $$ = $2; }
	| PASSWORD ColLabel		{ $$ = $2; }
	| /* empty */			{ $$ = NULL; }
	;
opt_stop_mode_s:
	MODE SMART			{ $$ = pstrdup("MODE SMART"); }
	| MODE S			{ $$ = pstrdup("MODE SMART"); }
	| /* empty */		{ $$ = pstrdup("MODE SMART"); }
	;
opt_stop_mode_f:
	MODE FAST	{ $$ = pstrdup("MODE FAST"); }
	| MODE F	{ $$ = pstrdup("MODE FAST"); }
	;
opt_stop_mode_i:
	MODE IMMEDIATE		{ $$ = pstrdup("MODE IMMEDIATE"); }
	| MODE I			{ $$ = pstrdup("MODE IMMEDIATE"); }
	;
opt_gtm_inner_type:
	  MASTER { $$ = GTM_TYPE_GTM_MASTER; }
	| SLAVE { $$ = GTM_TYPE_GTM_SLAVE; }
	| EXTRA { $$ = GTM_TYPE_GTM_EXTRA; }
	;
opt_dn_inner_type:
	 MASTER { $$ = CNDN_TYPE_DATANODE_MASTER; }
	|SLAVE { $$ = CNDN_TYPE_DATANODE_SLAVE; }
	| EXTRA { $$ = CNDN_TYPE_DATANODE_EXTRA; }
	;
ListMonitor:
	GET_CLUSTER_HEADPAGE_LINE
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_cluster_firstline_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_CLUSTER_FOURITEM  /*monitor first page, four item, the data in current 12hours*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_cluster_fouritem_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_CLUSTER_SUMMARY  /*monitor cluster summary, the data in current time*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_cluster_summary_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_DATABASE_TPS_QPS /*monitor all database tps,qps, runtime at current time*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_all_dbname_tps_qps_runtime_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_DATABASE_TPS_QPS_INTERVAL_TIME '(' Ident ',' Ident ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeIntConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_databasetps_func", args));
			$$ = (Node*)stmt;
		}
	| GET_DATABASE_SUMMARY '(' Ident')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_databasesummary_func", args));
			$$ = (Node*)stmt;
		}
	| GET_SLOWLOG '(' Ident ',' Ident ',' Ident ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_slowlog_func_page", args));
			$$ = (Node*)stmt;
		}
	| GET_SLOWLOG_COUNT '(' Ident ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_slowlog_count_func", args));
			$$ = (Node*)stmt;
		}
	| CHECK_USER '(' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_checkuser_func", args));
			$$ = (Node*)stmt;
		}
	| GET_USER_INFO  SignedIconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_getuserinfo_func", args));
			$$ = (Node*)stmt;
		}
	| UPDATE_USER SignedIconst '(' Ident ',' Ident ',' Ident ',' Ident ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, -1));
			args = lappend(args, makeStringConst($4, -1));
			args = lappend(args, makeStringConst($6, -1));
			args = lappend(args, makeStringConst($8, -1));
			args = lappend(args, makeStringConst($10, -1));
			args = lappend(args, makeStringConst($12, -1));
			args = lappend(args, makeStringConst($14, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_updateuserinfo_func", args));
			$$ = (Node*)stmt;
		}
	| CHECK_PASSWORD '(' SignedIconst ',' Ident ')'
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_checkuserpassword_func", args));
			$$ = (Node*)stmt;
	}
	| UPDATE_PASSWORD SignedIconst '(' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, -1));
			args = lappend(args, makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_updateuserpassword_func", args));
			$$ = (Node*)stmt;
		}
	;

unreserved_keyword:
	  ADD_P
	| AGENT
	| ALTER
	| APPEND
	| CHECK_PASSWORD
	| CHECK_USER
	| CLEAN
	| CONFIG
	| COORDINATOR
	| DATANODE
	| DEPLOY
	| DROP
	| EXISTS
	| EXTRA
	| F
	| FAILOVER
	| FAST
	| FORCE
	| GET_CLUSTER_FOURITEM
	| GET_CLUSTER_HEADPAGE_LINE
	| GET_CLUSTER_SUMMARY
	| GET_DATABASE_SUMMARY
	| GET_DATABASE_TPS_QPS
	| GET_DATABASE_TPS_QPS_INTERVAL_TIME
	| GET_HOST_LIST_ALL
	| GET_HOST_LIST_SPEC
	| GET_HOST_HISTORY_USAGE
	| GET_ALARM_INFO_ASC
	| GET_ALARM_INFO_COUNT
	| GET_ALARM_INFO_DESC
	| GET_ALL_NODENAME_IN_SPEC_HOST
	| GET_AGTM_NODE_TOPOLOGY
	| GET_COORDINATOR_NODE_TOPOLOGY
	| GET_DATANODE_NODE_TOPOLOGY
	| GET_DB_THRESHOLD_ALL_TYPE
	| GET_SLOWLOG
	| GET_SLOWLOG_COUNT
	| GET_THRESHOLD_TYPE
	| GET_THRESHOLD_ALL_TYPE
	| GET_USER_INFO
	| RESOLVE_ALARM
	| GTM
	| HOST
	| I
	| IF_P
	| IMMEDIATE
	| INIT
	| LIST
	| MASTER
	| MODE
	| MONITOR
	| NODE
	| OFF
	| PARM
	| PASSWORD
	| RESET
	| S
	| SET
	| SLAVE
	| SMART
	| START
	| STOP
	| TO
	| UPDATE_THRESHOLD_VALUE
	| UPDATE_USER
	| UPDATE_PASSWORD
	;

reserved_keyword:
	  FALSE_P
	| NOT
	| TRUE_P
	;

%%
/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
mgr_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static int mgr_yylex(union YYSTYPE *lvalp, YYLTYPE *llocp,
		   core_yyscan_t yyscanner)
{
	return core_yylex(&(lvalp->core_yystype), llocp, yyscanner);
}

List *mgr_parse_query(const char *query_string)
{
	core_yyscan_t yyscanner;
	mgr_yy_extra_type yyextra;
	int			yyresult;

	/* initialize the flex scanner */
	yyscanner = scanner_init(query_string, &yyextra.core_yy_extra,
							 ManagerKeywords, NumManagerKeywords);

	yyextra.parsetree = NIL;

	/* Parse! */
	yyresult = mgr_yyparse(yyscanner);

	/* Clean up (release memory) */
	scanner_finish(yyscanner);

	if (yyresult)				/* error */
		return NIL;

	return yyextra.parsetree;
}

static ResTarget* make_star_target(int location)
{
	ResTarget *target;
	ColumnRef *n = makeNode(ColumnRef);
	n->fields = list_make1(makeNode(A_Star));
	n->location = -1;

	target = makeNode(ResTarget);
	target->name = NULL;
	target->indirection = NIL;
	target->val = (Node *)n;
	target->location = -1;

	return target;
}

static Node* make_column_in(const char *col_name, List *values)
{
	A_Expr *expr;
	ColumnRef *col = makeNode(ColumnRef);
	col->fields = list_make1(makeString(pstrdup(col_name)));
	col->location = -1;
	expr = makeA_Expr(AEXPR_IN
			, list_make1(makeString(pstrdup("=")))
			, (Node*)col
			, (Node*)values
			, -1);
	return (Node*)expr;
}

static Node* makeNode_RangeFunction(const char *func_name, List *func_args)
{
	RangeFunction *n = makeNode(RangeFunction);
	n->lateral = false;
	n->funccallnode = make_func_call(func_name, func_args);
	n->alias = NULL;
	n->coldeflist = NIL;
	return (Node *) n;
}

static Node* make_func_call(const char *func_name, List *func_args)
{
	FuncCall *n = makeNode(FuncCall);
	n->funcname = list_make1(makeString(pstrdup(func_name)));
	n->args = func_args;
	n->agg_order = NIL;
	n->agg_star = FALSE;
	n->agg_distinct = FALSE;
	n->func_variadic = FALSE;
	n->over = NULL;
	n->location = -1;
	return (Node *)n;
}

static List* make_start_agent_args(List *options)
{
	List *result;
	char *password = NULL;
	ListCell *lc;
	DefElem *def;
	
	/* for(lc=list_head(options);lc;lc=lnext(lc)) */
	foreach(lc,options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "password") == 0)
			password = defGetString(def);
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is password.")));
		}
	}
	
	if(password == NULL)
		result = list_make1(makeNullAConst(-1));
	else
		result = list_make1(makeStringConst(password, -1));

	return result;
}
