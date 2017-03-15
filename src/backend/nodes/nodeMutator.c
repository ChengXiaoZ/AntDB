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

#define NO_NODE_A_Const
#define NO_NODE_Value
#define NO_NODE_PlannerGlobal
#define NO_NODE_RestrictInfo
#define NO_STRUCT_ParamListInfoData
#define NO_STRUCT_MergeScanSelCache
#define NO_STRUCT_QualCost
#define NO_STRUCT_ParamExternData

static void *pmemdup(const void *src, Size size);

/* declare mutator functions */
#define BEGIN_NODE(type)	\
	static type* _mutator_##type(type *dest, const type *src, Node *(*mutator)(), void *context);
#define NODE_SAME(t1, t2)
#define BEGIN_STRUCT BEGIN_NODE
#include "nodes_define.h"
#include "nodes_undef.h"

/* mutator functions */
#define NODE_ARG_ src
#define BEGIN_NODE(type)										\
static type* _mutator_##type(type *dest, const type *src,		\
							Node *(*mutator)(), void *context)	\
{
#define END_NODE(type)										\
	return dest;											\
}

#define NODE_SAME(t1, t2)
#define BEGIN_STRUCT BEGIN_NODE
#define END_STRUCT END_NODE

#define NODE_NODE(t,m)	dest->m = (t*)(*mutator)(src->m, context);
#define NODE_BASE2(t,m)	_mutator_##t(&(dest->m), &(src->m), mutator, context);
#define NODE_NODE_MEB(t,m) NODE_BASE2(t,m)
#define NODE_NODE_ARRAY(t,m,l) not support yet
#define NODE_BITMAPSET(t,m) dest->m = bms_copy(src->m);
#define NODE_BITMAPSET_ARRAY(t,m,l)	not support yet
#define NODE_SCALAR_POINT(t,m,l) dest->m = pmemdup(src->m, sizeof(t)*(l));
#define NODE_STRING(m) dest->m = pstrdup(src->m);
#define NODE_STRUCT(t,m)									\
	do{														\
		dest->m = pmemdup(src->m, sizeof(t));				\
		_mutator_##t(dest->m, src->m, mutator, context);	\
	}while(false);
#define NODE_STRUCT_ARRAY(t,m,l) not support yet
#define NODE_STRUCT_LIST(t,m) dest->m = mutator_struct_list(src->m, sizeof(t), \
	_mutator_##t, mutator, context);
#define NODE_STRUCT_MEB(t,m) _mutator_##t(&(dest->m), &(src->m), mutator, context);
/* need copy datum ? */
#define NODE_DATUM(t,m,o,n)

#include "nodes_define.h"
#include "nodes_undef.h"

Node *node_tree_mutator(Node *node, Node *(*mutator)(), void *context)
{
	if(node == NULL)
		return NULL;
	check_stack_depth();

	switch(nodeTag(node))
	{
#define CASE_TYPE(type, fun)										\
	case T_##type:													\
		{															\
			type *dest = pmemdup(node, sizeof(type));				\
			_mutator_##fun(dest, (void*)node, mutator, context);	\
			return (Node*)dest;										\
		}
#define BEGIN_NODE(type) CASE_TYPE(type, type)
#define NODE_SAME(t1,t2) CASE_TYPE(t1, t2)
#define NO_NODE_JoinPath
#include "nodes_define.h"
	case T_List:
		{
			ListCell *lc;
			List *list = NIL;
			foreach(lc, (List*)node)
				list = lappend(list, (*mutator)(lfirst(lc), context));
			return (Node*)list;
		}
	case T_OidList:
	case T_IntList:
		return (Node*)list_copy((List*)node);
	case T_Integer:
		return (Node*)makeInteger(intVal(node));
	case T_Float:
		return (Node*)makeFloat(pstrdup(strVal(node)));
	case T_String:
		return (Node*)makeString(pstrdup(strVal(node)));
	case T_BitString:
		return (Node*)makeBitString(pstrdup(strVal(node)));
	case T_Null:
		return pmemdup(node, sizeof(Value));
	case T_A_Const:
		{
			A_Const *dest = pmemdup(node, sizeof(A_Const));
			switch(dest->val.type)
			{
			case T_Integer:
				break;
			case T_Float:
			case T_String:
			case T_BitString:
				dest->val.val.str = pstrdup(strVal(node));
				break;
			case T_Null:
				break;
			default:
				ereport(ERROR, (errmsg("unknown node type %d\n", (int)dest->val.type)));
			}
			return (Node*)dest;
		}
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", (int)nodeTag(node))));
	}
	return NULL;
}

static void *pmemdup(const void *src, Size size)
{
	void *dest = palloc(size);
	memcpy(dest, src, size);
	return dest;
}
