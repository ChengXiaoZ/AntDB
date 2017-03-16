/*-------------------------------------------------------------------------
 *
 * parser.c
 *		Main entry point/driver for PostgreSQL grammar
 *
 * Note that the grammar is not allowed to perform any table access
 * (since we need to be able to do basic parsing even while inside an
 * aborted transaction).  Therefore, the data structures returned by
 * the grammar are "raw" parsetrees that still need to be analyzed by
 * analyze.c and related files.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "parser/gramparse.h"
#include "parser/ora_gramparse.h"
#include "parser/parser.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"

#ifdef ADB
#include "catalog/heap.h" /* SystemAttributeByName */
#include "lib/stringinfo.h"
#include "miscadmin.h" /* check_stack_depth */
#endif

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)

#ifdef ADB
typedef struct ConnectByMutatorContext
{
	core_yyscan_t yyscanner;
	char *base_rel_name;
	char *prior_rel_name;
	bool prior;
}ConnectByMutatorContext;

typedef struct SearchConnectByColumnContext
{
	core_yyscan_t yyscanner;
	List *list;
	const char *base_rel_name;
	const char *schema_name;
	const char *database_name;
	bool have_star;
}SearchConnectByColumnContext;

static bool search_columnref(ColumnRef *c, SearchConnectByColumnContext *context);
static List* make_target_list(SearchConnectByColumnContext *context);
static Node* mutator_column_table_name(Node *node, ConnectByMutatorContext *context);
static bool have_prior_expr(Node *node, void *context);
#endif /* ADB */
/*
 * raw_parser
 *		Given a query in string form, do lexical and grammatical analysis.
 *
 * Returns a list of raw (un-analyzed) parse trees.
 */
List *
raw_parser(const char *str)
{
	core_yyscan_t yyscanner;
	base_yy_extra_type yyextra;
	int			yyresult;

	/* initialize the flex scanner */
	yyscanner = scanner_init(str, &yyextra.core_yy_extra,
							 ScanKeywords, NumScanKeywords);

	/* base_yylex() only needs this much initialization */
	yyextra.have_lookahead = false;

	/* initialize the bison parser */
	parser_init(&yyextra);

	/* Parse! */
	yyresult = base_yyparse(yyscanner);

	/* Clean up (release memory) */
	scanner_finish(yyscanner);

	if (yyresult)				/* error */
		return NIL;

	return yyextra.parsetree;
}

#ifdef ADB
List* ora_raw_parser(const char *str)
{
	core_yyscan_t yyscanner;
	ora_yy_extra_type yyextra;
	int yyresult;

	/* initialize the flex scanner */
	yyscanner = scanner_init(str, &yyextra.core_yy_extra,
							 OraScanKeywords, OraNumScanKeywords);

	/* initialize the bison parser */
	ora_parser_init(&yyextra);

	/* Parse! */
	yyresult = ora_yyparse(yyscanner);

	/* Clean up (release memory) */
	scanner_finish(yyscanner);

	if (yyresult)				/* error */
		return NIL;

	return yyextra.parsetree;
}
#endif


/*
 * Intermediate filter between parser and core lexer (core_yylex in scan.l).
 *
 * The filter is needed because in some cases the standard SQL grammar
 * requires more than one token lookahead.  We reduce these cases to one-token
 * lookahead by combining tokens here, in order to keep the grammar LALR(1).
 *
 * Using a filter is simpler than trying to recognize multiword tokens
 * directly in scan.l, because we'd have to allow for comments between the
 * words.  Furthermore it's not clear how to do it without re-introducing
 * scanner backtrack, which would cost more performance than this filter
 * layer does.
 *
 * The filter also provides a convenient place to translate between
 * the core_YYSTYPE and YYSTYPE representations (which are really the
 * same thing anyway, but notationally they're different).
 */
int
base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner)
{
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	int			cur_token;
	int			next_token;
	core_YYSTYPE cur_yylval;
	YYLTYPE		cur_yylloc;

	/* Get next token --- we might already have it */
	if (yyextra->have_lookahead)
	{
		cur_token = yyextra->lookahead_token;
		lvalp->core_yystype = yyextra->lookahead_yylval;
		*llocp = yyextra->lookahead_yylloc;
		yyextra->have_lookahead = false;
	}
	else
		cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);

	/* Do we need to look ahead for a possible multiword token? */
	switch (cur_token)
	{
		case NULLS_P:

			/*
			 * NULLS FIRST and NULLS LAST must be reduced to one token
			 */
			cur_yylval = lvalp->core_yystype;
			cur_yylloc = *llocp;
			next_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);
			switch (next_token)
			{
				case FIRST_P:
					cur_token = NULLS_FIRST;
					break;
				case LAST_P:
					cur_token = NULLS_LAST;
					break;
				default:
					/* save the lookahead token for next time */
					yyextra->lookahead_token = next_token;
					yyextra->lookahead_yylval = lvalp->core_yystype;
					yyextra->lookahead_yylloc = *llocp;
					yyextra->have_lookahead = true;
					/* and back up the output info to cur_token */
					lvalp->core_yystype = cur_yylval;
					*llocp = cur_yylloc;
					break;
			}
			break;

		case WITH:

			/*
			 * WITH TIME must be reduced to one token
			 */
			cur_yylval = lvalp->core_yystype;
			cur_yylloc = *llocp;
			next_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);
			switch (next_token)
			{
				case TIME:
					cur_token = WITH_TIME;
					break;
				default:
					/* save the lookahead token for next time */
					yyextra->lookahead_token = next_token;
					yyextra->lookahead_yylval = lvalp->core_yystype;
					yyextra->lookahead_yylloc = *llocp;
					yyextra->have_lookahead = true;
					/* and back up the output info to cur_token */
					lvalp->core_yystype = cur_yylval;
					*llocp = cur_yylloc;
					break;
			}
			break;

		default:
			break;
	}

	return cur_token;
}

/* functions from gram.y */
Node *
makeColumnRef(char *colname, List *indirection,
			  int location, core_yyscan_t yyscanner)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection,
																  nfields),
												   yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		else if (IsA(lfirst(l), A_Star))
		{
			/* We only allow '*' at the end of a ColumnRef */
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}

Node *
makeTypeCast(Node *arg, TypeName *typename, int location)
{
	TypeCast *n = makeNode(TypeCast);
	n->arg = arg;
	n->typeName = typename;
	n->location = location;
	return (Node *) n;
}

Node *
makeStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeStringConstCast(char *str, int location, TypeName *typename)
{
	Node *s = makeStringConst(str, location);

	return makeTypeCast(s, typename, -1);
}

Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;

	return (Node *)n;
}

Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeBitStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_BitString;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeNullAConst(int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Null;
	n->location = location;

	return (Node *)n;
}

Node *
makeAConst(Value *v, int location)
{
	Node *n;

	switch (v->type)
	{
		case T_Float:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_Integer:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_String:
		default:
			n = makeStringConst(v->val.str, location);
			break;
	}

	return n;
}

/* makeBoolAConst()
 * Create an A_Const string node and put it inside a boolean cast.
 */
Node *
makeBoolAConst(bool state, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = (state ? "t" : "f");
	n->location = location;

	return makeTypeCast((Node *)n, SystemTypeName("bool"), -1);
}

#ifdef ADB
List *
check_sequence_name(List *names, core_yyscan_t yyscanner, int location)
{
	ListCell   *i;
	StringInfoData buf;

	initStringInfo(&buf);
	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");

		appendStringInfo(&buf, "%s.", strVal((Value*)lfirst(i)));
	}
	buf.data[buf.len - 1] = '\0';

	return list_make1(makeStringConst(buf.data, location));
}

Node *makeConnectByStmt(SelectStmt *stmt, Node *start, Node *connect_by,
								core_yyscan_t yyscanner)
{
	SelectStmt *new_select,
			   *union_all_left,
			   *union_all_right;
	CommonTableExpr *common_table;
	char *base_rel_name;
	RangeVar *range;
	List *base_tl;	/* base rel target list */
	ConnectByMutatorContext context;
	AssertArg(stmt && connect_by && yyscanner);

	/* have PriorExpr? */
	if(have_prior_expr(connect_by, NULL) == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("have no prior expression")));
	}

	if(stmt->distinctClause || stmt->groupClause || stmt->havingClause || stmt->windowClause)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("connect by not support distinct group window yet!")));
	}

	/* make new select and have recursive cte */
	new_select = makeNode(SelectStmt);
	new_select->withClause = makeNode(WithClause);
	new_select->withClause->recursive = true;
	new_select->withClause->location = -1;

	/* get base rel name */
	if(list_length(stmt->fromClause) != 1
		|| !IsA(linitial(stmt->fromClause), RangeVar))
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("connect by support one table yet")));
		base_rel_name = NULL;	/* never run, keep analyze quiet */
	}else
	{
		range = linitial(stmt->fromClause);
		base_rel_name = range->relname;
		if(range->alias)
			base_rel_name = range->alias->aliasname;
	}

	/* get using target list */
	{
		SearchConnectByColumnContext sc;
		memset(&sc, 0, sizeof(sc));
		sc.yyscanner = yyscanner;
		sc.base_rel_name = base_rel_name;
		search_columnref((ColumnRef*)(stmt->targetList), &sc);
		search_columnref((ColumnRef*)(stmt->distinctClause), &sc);
		search_columnref((ColumnRef*)(stmt->whereClause), &sc);
		search_columnref((ColumnRef*)(stmt->groupClause), &sc);
		search_columnref((ColumnRef*)(stmt->havingClause), &sc);
		base_tl = make_target_list(&sc);
	}

	/* when with a group select we let it as a CTE */
	if(stmt->distinctClause || stmt->groupClause)
	{
		common_table = makeNode(CommonTableExpr);
		common_table->ctename = pstrdup(base_rel_name);
		common_table->ctequery = (Node*)stmt;
		common_table->location = -1;
		new_select->withClause->ctes = list_make1(common_table);
	}

	/* make select union all left as "select * from base_rel where where_clause*/
	union_all_left = makeNode(SelectStmt);
	union_all_left->targetList = base_tl;
	union_all_left->fromClause = list_make1(makeRangeVar(NULL, base_rel_name, -1));
	/* add "start with clause" */
	union_all_left->whereClause = start;
	/* if base is not a CTE scan append where clause */
	if(stmt->whereClause && new_select->withClause->ctes == NIL)
	{
		if(union_all_left->whereClause)
		{
			/* make AND expr */
			union_all_left->whereClause = (Node*)
				makeA_Expr(AEXPR_AND, NIL, union_all_left->whereClause, stmt->whereClause, -1);
		}else
		{
			union_all_left->whereClause = stmt->whereClause;
		}
	}

	/* make CTE rel */
	common_table = makeNode(CommonTableExpr);
	if(strcmp(base_rel_name, "_CTE1") == 0)
		common_table->ctename = pstrdup("_CTE2");
	else
		common_table->ctename = pstrdup("_CTE1");
	common_table->location = -1;

	/* make union all right as
	 *   select base_rel.* from base_rel iner join "_CTEn" on "connect by"
	 */
	union_all_right = makeNode(SelectStmt);
	context.yyscanner = yyscanner;

	/* make base rel target list */
	context.base_rel_name = base_rel_name;
	context.prior_rel_name = base_rel_name;
	context.prior = false;
	union_all_right->targetList =
		(List*)mutator_column_table_name((Node*)base_tl, &context);

	/* make join */
	JoinExpr *join = makeNode(JoinExpr);
	join->jointype = JOIN_INNER;
	join->larg = (Node*)makeRangeVar(NULL, base_rel_name, -1);
	join->rarg = (Node*)makeRangeVar(NULL, common_table->ctename, -1);

	/* mutator qual */
	context.prior_rel_name = common_table->ctename;
	context.prior = false;
	join->quals = mutator_column_table_name(connect_by, &context);

	/* if base is not a CTE scan append where clause */
	if(stmt->whereClause && new_select->withClause->ctes == NIL)
	{
		/* use same rel name ot base rel name to modify column */
		context.prior_rel_name = base_rel_name;
		union_all_right->whereClause =
			mutator_column_table_name(stmt->whereClause, &context);
	}

	union_all_right->fromClause = list_make1(join);

	/* make union all select */
	common_table->ctequery = makeSetOp(SETOP_UNION, true, (Node*)union_all_left, (Node*)union_all_right);
	/* and append it to CTEs */
	new_select->withClause->ctes = lappend(new_select->withClause->ctes, common_table);

	/* make new target list */
	context.prior_rel_name = context.base_rel_name = common_table->ctename;
	new_select->targetList =
		(List*) mutator_column_table_name((Node*)stmt->targetList, &context);

	new_select->fromClause = list_make1(makeRangeVar(NULL, common_table->ctename, -1));

	return (Node*)new_select;
}

static bool have_prior_expr(Node *node, void *context)
{
	if(node == NULL)
		return false;
	if(IsA(node, PriorExpr))
		return true;
	return node_tree_walker(node, have_prior_expr, context);
}

static bool search_columnref(ColumnRef *c, SearchConnectByColumnContext *context)
{
	ListCell *lc;
	ColumnRef *c2;

	if(c == NULL)
		return false;

	check_stack_depth();
	if(!IsA(c, ColumnRef))
		return node_tree_walker((Node*)c, search_columnref, context);

	lc = list_head(c->fields);
	Assert(list_length(c->fields) <= 4);
	if(list_length(c->fields) > 3)
	{
		Assert(IsA(lfirst(lc), String));
		if(context->database_name == NULL)
		{
			context->database_name = strVal(lfirst(lc));
		}else if(strcmp(context->database_name, strVal(lfirst(lc))) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("cross-database references are not implemented"),
				scanner_errposition(c->location, context->yyscanner)));
		}
		lc = lnext(lc);
	}
	if(list_length(c->fields) > 2)
	{
		Assert(IsA(lfirst(lc), String));
		if(context->schema_name == NULL)
		{
			context->schema_name = strVal(lfirst(lc));
		}else if(strcmp(context->schema_name, strVal(lfirst(lc))) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("different schema name"),
				err_generic_string(PG_DIAG_SCHEMA_NAME, strVal(lfirst(lc))),
				scanner_errposition(c->location, context->yyscanner)));
		}
		lc = lnext(lc);
	}
	if(list_length(c->fields) > 1)
	{
		Assert(IsA(lfirst(lc), String));
		if(strcmp(context->base_rel_name, strVal(lfirst(lc))) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("different table name"),
				err_generic_string(PG_DIAG_TABLE_NAME, strVal(lfirst(lc))),
				scanner_errposition(c->location, context->yyscanner)));
		}
		lc = lnext(lc);
	}
	Assert(lnext(lc) == NULL);
	if(IsA(lfirst(lc), A_Star))
	{
		context->have_star = true;
	}else
	{
		const char *name;
		Assert(IsA(lfirst(lc), String));
		name = strVal(lfirst(lc));
		foreach(lc, context->list)
		{
			c2 = lfirst(lc);
			if(strcmp(strVal(llast(c2->fields)), name) == 0)
				break;
		}
		if(lc == NULL)
			context->list = lappend(context->list, copyObject(c));
	}
	return false;
}

static List* make_target_list(SearchConnectByColumnContext *context)
{
	List *new_list;
	ListCell *lc;
	ResTarget *rt;

	new_list = NIL;
	if(context->have_star == false)
	{
		/* have no "*" just use all column */
		foreach(lc, context->list)
		{
			rt = makeNode(ResTarget);
			rt->location = -1;
			rt->val = lfirst(lc);
			new_list = lappend(new_list, rt);
		}
		return new_list;
	}else
	{
		/* have "*", we select "*" and all system column */
		ColumnRef *cr;
		new_list = list_make1(make_star_target(-1));
		foreach(lc, context->list)
		{
			cr = lfirst(lc);
			Assert(cr);
			if(SystemAttributeByName(strVal(llast(cr->fields)), true) != NULL)
			{
				rt = makeNode(ResTarget);
				rt->location = -1;
				rt->val = (Node*)cr;
				new_list = lappend(new_list, rt);
			}
		}
	}

	if(new_list == NIL)
	{
		rt = makeNode(ResTarget);
		rt->location = -1;
		rt->val = makeNullAConst(-1);
		new_list = list_make1(rt);
	}

	return new_list;
}

static Node* mutator_column_table_name(Node *node, ConnectByMutatorContext *context)
{
	if(node == NULL)
		return NULL;

	switch(nodeTag(node))
	{
	case T_PriorExpr:
		{
			Node *new_node;
			bool prior_save = context->prior;
			context->prior = true;
			new_node = mutator_column_table_name(((PriorExpr*)node)->expr, context);
			context->prior = prior_save;
			return new_node;
		}
	case T_ColumnRef:
		{
			const char *name = (context->prior ? context->prior_rel_name : context->base_rel_name);
			ColumnRef *cr = makeNode(ColumnRef);
			ColumnRef *src = (ColumnRef*)node;
			int n = list_length(src->fields);
			cr->location = src->location;
			cr->fields = NIL;

			if(n > 1)
			{
				ListCell *lc;
				foreach(lc, src->fields)
				{
					--n;
					if(n != 1)
						cr->fields = lappend(cr->fields, copyObject(lfirst(lc)));
					else
						cr->fields = lappend(cr->fields, makeString(pstrdup(name)));
				}
			}else
			{
				cr->fields = list_make2(makeString(pstrdup(name)), copyObject(llast(src->fields)));
			}
			return (Node*)cr;
		}
	case T_RownumExpr:
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("connect by not support rownum yet"),
			scanner_errposition(((RownumExpr*)node)->location, context->yyscanner)));
		break;
	default:
		break;
	}
	return node_tree_mutator(node, mutator_column_table_name, context);
}

#endif

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
void
check_qualified_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
}

/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
List *
check_func_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
	return names;
}

/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
List *
check_indirection(List *indirection, core_yyscan_t yyscanner)
{
	ListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Star))
		{
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/* extractArgTypes()
 * Given a list of FunctionParameter nodes, extract a list of just the
 * argument types (TypeNames) for input parameters only.  This is what
 * is needed to look up an existing function, which is what is wanted by
 * the productions that use this call.
 */
List *
extractArgTypes(List *parameters)
{
	List	   *result = NIL;
	ListCell   *i;

	foreach(i, parameters)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(i);

		if (p->mode != FUNC_PARAM_OUT && p->mode != FUNC_PARAM_TABLE)
			result = lappend(result, p->argType);
	}
	return result;
}

/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in SelectStmt productions.
 */
void
insertSelectOptions(SelectStmt *stmt,
					List *sortClause, List *lockingClause,
					Node *limitOffset, Node *limitCount,
					WithClause *withClause,
					core_yyscan_t yyscanner)
{
	Assert(IsA(stmt, SelectStmt));

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple ORDER BY clauses not allowed"),
					 parser_errposition(exprLocation((Node *) sortClause))));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple OFFSET clauses not allowed"),
					 parser_errposition(exprLocation(limitOffset))));
		stmt->limitOffset = limitOffset;
	}
	if (limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple LIMIT clauses not allowed"),
					 parser_errposition(exprLocation(limitCount))));
		stmt->limitCount = limitCount;
	}
	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple WITH clauses not allowed"),
					 parser_errposition(exprLocation((Node *) withClause))));
		stmt->withClause = withClause;
	}
}

Node *
makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg)
{
	SelectStmt *n = makeNode(SelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (SelectStmt *) larg;
	n->rarg = (SelectStmt *) rarg;
	return (Node *) n;
}

/* SystemFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
List *
SystemFuncName(char *name)
{
	return list_make2(makeString("pg_catalog"), makeString(name));
}

/* SystemTypeName()
 * Build a properly-qualified reference to a built-in type.
 *
 * typmod is defaulted, but may be changed afterwards by caller.
 * Likewise for the location.
 */
TypeName *
SystemTypeName(char *name)
{
	return makeTypeNameFromNameList(list_make2(makeString("pg_catalog"),
											   makeString(name)));
}

TypeName *SystemTypeNameLocation(char *name, int location)
{
	TypeName *typ = makeTypeNameFromNameList(list_make2(makeString("pg_catalog"),
											   makeString(name)));
	typ->location = location;
	return typ;
}

#ifdef ADB
List *OracleFuncName(char *name)
{
	return list_make2(makeString("oracle"), makeString(name));
}

TypeName *OracleTypeName(char *name)
{
	return makeTypeNameFromNameList(list_make2(makeString("oracle"),
											   makeString(name)));
}

TypeName *OracleTypeNameLocation(char *name, int location)
{
	TypeName *typ = makeTypeNameFromNameList(list_make2(makeString("oracle"),
												makeString(name)));
	typ->location = location;
	return typ;
}

void transformDistributeBy(DistributeBy *dbstmt)
{
	List *funcname = NIL;
	List *funcargs = NIL;

	if (dbstmt == NULL ||
		/* must be replication or roundrobin */
		dbstmt->disttype != DISTTYPE_USER_DEFINED)
		return ;

	funcname = dbstmt->funcname;
	funcargs = dbstmt->funcargs;

	Assert(funcname && funcargs);

	/*
	 * try to judge distribution type
	 * HASH or MODULE or USER-DEFINED.
	 */
	if (list_length(funcname) == 1)
	{
		Node *argnode = linitial(funcargs);
		char *fname = strVal(linitial(funcname));
		if (strcasecmp(fname, "HASH") == 0)
		{
			if (list_length(funcargs) != 1 ||
				IsA(argnode, ColumnRef) == false ||
				list_length(((ColumnRef *)argnode)->fields) != 1)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("Invalid distribution column specified for \"HASH\""),
					errhint("Valid syntax input: HASH(column)")));

			dbstmt->disttype = DISTTYPE_HASH;
			dbstmt->colname = strVal(linitial(((ColumnRef *)argnode)->fields));
		}
		else
		if (strcasecmp(fname, "MODULO") == 0)
		{
			if (list_length(funcargs) != 1 ||
				IsA(argnode, ColumnRef) == false ||
				list_length(((ColumnRef *)argnode)->fields) != 1)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("Invalid distribution column specified for \"MODULO\""),
					errhint("Valid syntax input: MODULO(column)")));

			dbstmt->disttype = DISTTYPE_MODULO;
			dbstmt->colname = strVal(linitial(((ColumnRef *)argnode)->fields));
		}
		else
		{
			/*
			 * Nothing changed.
			 * Just keep compiler quiet.
			 */
		}
	} else
	{
		/*
		 * Nothing changed.
		 * Just keep compiler quiet.
		 */
	}
}
#endif

/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
Node *
doNegate(Node *n, int location)
{
	if (IsA(n, A_Const))
	{
		A_Const *con = (A_Const *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_Integer)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_Float)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (Node *) makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;

	Assert(IsA(v, Float));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
	{
		char   *newval = (char *) palloc(strlen(oldval) + 2);

		*newval = '-';
		strcpy(newval+1, oldval);
		v->val.str = newval;
	}
}

Node *
makeAArrayExpr(List *elements, int location)
{
	A_ArrayExpr *n = makeNode(A_ArrayExpr);

	n->elements = elements;
	n->location = location;
	return (Node *) n;
}

Node *
makeXmlExpr(XmlExprOp op, char *name, List *named_args, List *args,
			int location)
{
	XmlExpr		*x = makeNode(XmlExpr);

	x->op = op;
	x->name = name;
	/*
	 * named_args is a list of ResTarget; it'll be split apart into separate
	 * expression and name lists in transformXmlExpr().
	 */
	x->named_args = named_args;
	x->arg_names = NIL;
	x->args = args;
	/* xmloption, if relevant, must be filled in by caller */
	/* type and typmod will be filled in during parse analysis */
	x->type = InvalidOid;			/* marks the node as not analyzed */
	x->location = location;
	return (Node *) x;
}

/*
 * Merge the input and output parameters of a table function.
 */
List *
mergeTableFuncParameters(List *func_args, List *columns)
{
	ListCell   *lc;

	/* Explicit OUT and INOUT parameters shouldn't be used in this syntax */
	foreach(lc, func_args)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(lc);

		if (p->mode != FUNC_PARAM_IN && p->mode != FUNC_PARAM_VARIADIC)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("OUT and INOUT arguments aren't allowed in TABLE functions")));
	}

	return list_concat(func_args, columns);
}

/*
 * Determine return type of a TABLE function.  A single result column
 * returns setof that column's type; otherwise return setof record.
 */
TypeName *
TableFuncTypeName(List *columns)
{
	TypeName *result;

	if (list_length(columns) == 1)
	{
		FunctionParameter *p = (FunctionParameter *) linitial(columns);

		result = (TypeName *) copyObject(p->argType);
	}
	else
		result = SystemTypeName("record");

	result->setof = true;

	return result;
}

/*
 * Convert a list of (dotted) names to a RangeVar (like
 * makeRangeVarFromNameList, but with position support).  The
 * "AnyName" refers to the any_name production in the grammar.
 */
RangeVar *
makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner)
{
	RangeVar *r = makeNode(RangeVar);

	switch (list_length(names))
	{
		case 1:
			r->catalogname = NULL;
			r->schemaname = NULL;
			r->relname = strVal(linitial(names));
			break;
		case 2:
			r->catalogname = NULL;
			r->schemaname = strVal(linitial(names));
			r->relname = strVal(lsecond(names));
			break;
		case 3:
			r->catalogname = strVal(linitial(names));
			r->schemaname = strVal(lsecond(names));
			r->relname = strVal(lthird(names));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("improper qualified name (too many dotted names): %s",
							NameListToString(names)),
					 parser_errposition(position)));
			break;
	}

	r->relpersistence = RELPERSISTENCE_PERMANENT;
	r->location = position;

	return r;
}

/* Separate Constraint nodes from COLLATE clauses in a ColQualList */
void
SplitColQualList(List *qualList,
				 List **constraintList, CollateClause **collClause,
				 core_yyscan_t yyscanner)
{
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	*collClause = NULL;
	prev = NULL;
	for (cell = list_head(qualList); cell; cell = next)
	{
		Node   *n = (Node *) lfirst(cell);

		next = lnext(cell);
		if (IsA(n, Constraint))
		{
			/* keep it in list */
			prev = cell;
			continue;
		}
		if (IsA(n, CollateClause))
		{
			CollateClause *c = (CollateClause *) n;

			if (*collClause)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple COLLATE clauses not allowed"),
						 parser_errposition(c->location)));
			*collClause = c;
		}
		else
			elog(ERROR, "unexpected node type %d", (int) n->type);
		/* remove non-Constraint nodes from qualList */
		qualList = list_delete_cell(qualList, cell, prev);
	}
	*constraintList = qualList;
}


/*----------
 * Recursive view transformation
 *
 * Convert
 *
 *     CREATE RECURSIVE VIEW relname (aliases) AS query
 *
 * to
 *
 *     CREATE VIEW relname (aliases) AS
 *         WITH RECURSIVE relname (aliases) AS (query)
 *         SELECT aliases FROM relname
 *
 * Actually, just the WITH ... part, which is then inserted into the original
 * view definition as the query.
 * ----------
 */
Node *
makeRecursiveViewSelect(char *relname, List *aliases, Node *query)
{
	SelectStmt *s = makeNode(SelectStmt);
	WithClause *w = makeNode(WithClause);
	CommonTableExpr *cte = makeNode(CommonTableExpr);
	List	   *tl = NIL;
	ListCell   *lc;

	/* create common table expression */
	cte->ctename = relname;
	cte->aliascolnames = aliases;
	cte->ctequery = query;
	cte->location = -1;

	/* create WITH clause and attach CTE */
	w->recursive = true;
	w->ctes = list_make1(cte);
	w->location = -1;

	/* create target list for the new SELECT from the alias list of the
	 * recursive view specification */
	foreach (lc, aliases)
	{
		ResTarget *rt = makeNode(ResTarget);

		rt->name = NULL;
		rt->indirection = NIL;
		rt->val = makeColumnRef(strVal(lfirst(lc)), NIL, -1, 0);
		rt->location = -1;

		tl = lappend(tl, rt);
	}

	/* create new SELECT combining WITH clause, target list, and fake FROM
	 * clause */
	s->withClause = w;
	s->targetList = tl;
	s->fromClause = list_make1(makeRangeVar(NULL, relname, -1));

	return (Node *) s;
}

ResTarget* make_star_target(int location)
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