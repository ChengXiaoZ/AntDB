/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/pgxcplan.h"

/* not support Node(s) */
#define NO_NODE_PlannerInfo
#define NO_NODE_RelOptInfo

/* no sub Node Node(s) */
#define NO_NODE_Value
#define NO_NODE_Var
#define NO_NODE_A_Const
#define NO_NODE_Const
#define NO_NODE_Param
#define NO_NODE_CoerceToDomainValue
#define NO_NODE_CaseTestExpr
#define NO_NODE_SetToDefault
#define NO_NODE_CurrentOfExpr
#define NO_NODE_RangeTblRef
#define NO_NODE_SortGroupClause

/* declare static functions */
#define BEGIN_NODE(type)	\
	static bool walker_##type(type *node, bool (*walker)(), void *context);
#define NODE_SAME(t1, t2)
#include "nodes_define.h"
#include "nodes_undef.h"

/* walker function */
#define BEGIN_NODE(type)							\
static bool walker_##type(type *node				\
	, bool (*walker)(), void *context)				\
{
#define END_NODE(type)								\
	return false;									\
}
#define NODE_SAME(t1,t2) /* empty */
#define NODE_NODE(t,m)								\
	if((*walker)(node->m, context))					\
		return true;
#define NODE_BASE2(t,m)								\
	if(walker_##t(&(node->m), walker, context))		\
		return true;
#define NODE_NODE_MEB(t,m) NODE_BASE2(t,m)
#define NODE_NODE_ARRAY(t,m,l)	not support yet
#include "nodes_define.h"
#include "nodes_undef.h"

bool node_tree_walker(Node *n, bool (*walker)(), void *context)
{
	check_stack_depth();
	AssertArg(walker);
	if(n == NULL)
		return false;

	switch(nodeTag(n))
	{
#define CASE_TYPE(type, fun)							\
	case T_##type:										\
		return walker_##fun((fun*)n, walker, context)
#define BEGIN_NODE(type)	CASE_TYPE(type,type);
#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2);
#define NO_NODE_JoinPath 1
#include "nodes_define.h"
	case T_List:
		{
			ListCell *lc;
			foreach(lc, (List*)n)
			{
				if((*walker)(lfirst(lc), context))
					return true;
			}
			return false;
		}
	case T_Integer:
	case T_Float:
	case T_String:
	case T_BitString:
	case T_Null:
	case T_IntList:
	case T_OidList:
	case T_Var:
	case T_Const:
	case T_Param:
	case T_CoerceToDomainValue:
	case T_CaseTestExpr:
	case T_SetToDefault:
	case T_CurrentOfExpr:
	case T_RangeTblRef:
	case T_SortGroupClause:
	case T_Value:
	case T_A_Const:
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", (int)nodeTag(n))));
	}

	return false;
}