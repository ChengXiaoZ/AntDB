
/*-------------------------------------------------------------------------
 *
 * saveNode and loadNode
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq/pqformat.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/pgxcplan.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* not support Node */
#define NO_NODE_PlannerInfo
#define NO_NODE_RelOptInfo
#define NO_NODE_RestrictInfo
#define NO_STRUCT_ParamListInfoData
#define NO_STRUCT_ParamExternData
#define NO_STRUCT_MergeScanSelCache
/* not used struct */
#define NO_STRUCT_QualCost

/* declare static functions */
#define BEGIN_NODE(type) 										\
	static void save_##type(StringInfo buf, const type *node);	\
	static type* load_##type(StringInfo buf, type *node);
#define NODE_SAME(t1, t2)
#define BEGIN_STRUCT(type) BEGIN_NODE(type)
#include "nodes_define.h"
#include "nodes_undef.h"

/* save functions */
#define SAVE_IS_NULL()			pq_sendbyte(buf, true)
#define SAVE_IS_NOT_NULL()	pq_sendbyte(buf, false)

#define BEGIN_NODE(type) 											\
static void save_##type(StringInfo buf, const type *node)			\
{																	\
	AssertArg(node);
#define END_NODE(type)												\
}

#define BEGIN_STRUCT(type)											\
static void save_##type(StringInfo buf, const type *node)			\
{																	\
	if(node == NULL)												\
	{																\
		SAVE_IS_NULL();												\
		return;														\
	}
#define END_STRUCT(type)											\
}

#define NODE_SAME(t1,t2)
#define NODE_BASE(base)				save_##base(buf, (const base*)node);
#define NODE_NODE(t,m)				saveNode(buf, (Node*)node->m);
#define NODE_NODE_MEB(t,m)			not support yet//saveNode(buf, (Node*)&(node->m));
#define NODE_NODE_ARRAY(t,m,l)		not support yet//SAVE_ARRAY(t,m,l,saveNode,Node);
#define NODE_BITMAPSET(t,m)			save_node_bitmapset(buf, node->m);
#define NODE_BITMAPSET_ARRAY(t,m,l)	not support yet//SAVE_ARRAY(t,m,l,save_node_bitmapset,Bitmapset);
#define NODE_SCALAR(t,m)			pq_sendbytes(buf, (const char*)&(node->m), sizeof(node->m));
#define NODE_SCALAR_POINT(t,m,l)								\
	do{															\
		uint32 len = (l);										\
		if(len)													\
		{														\
			Assert(node->m);									\
			pq_sendbytes(buf, (const char*)node->m, len);		\
		}														\
	}while(0);
#define NODE_OTHER_POINT(t,m)		not support
#define NODE_STRING(m)											\
	do{															\
		if(node->m)												\
		{														\
			Assert(node->m != NULL);							\
			SAVE_IS_NOT_NULL();									\
			pq_sendstring(buf, node->m);						\
		}else													\
		{														\
			SAVE_IS_NULL();										\
		}														\
	}while(0);
#define NODE_STRUCT(t,m)				save_##t(buf, node->m);
#define NODE_STRUCT_ARRAY(t,m,l)		not support yet//SAVE_ARRAY(t,m,l,save_##t, t)
#define NODE_STRUCT_LIST(t,m)									\
	do{															\
		if(node->m != NIL)										\
		{														\
			ListCell *lc;										\
			SAVE_IS_NOT_NULL();									\
			pq_sendbytes(buf, (const char*)&(node->m->length)	\
				, sizeof(node->m->length));						\
			foreach(lc, node->m)								\
				save_##t(buf, lfirst(lc));						\
		}else													\
		{														\
			SAVE_IS_NULL();										\
		}														\
	}while(0);
#define NODE_STRUCT_MEB(t,m)			save_##t(buf, &node->m);
#define NODE_ENUM(t,m)					NODE_SCALAR(t,m)
#define NODE_DATUM(t,m,o,n)			not support

/*#define SAVE_ARRAY(t,m,l,f,t2)										\
	do{																	\
		uint32 i,len = (l);												\
		pq_sendbytes(buf, (const char*)&len, sizeof(len));	\
		Assert((len == 0 && node->m == NULL) || (len > 0 && node->m != NULL));\
		for(i=0;i<len;++i)												\
			f(buf, (const t2*)node->m[i]);									\
	}while(0);*/

static void save_node_bitmapset(StringInfo buf, const Bitmapset *node)
{
	if(node == NULL)
	{
		SAVE_IS_NULL();
	}else
	{
		SAVE_IS_NOT_NULL();
		pq_sendbytes(buf, (const char*)&node->nwords, sizeof(node->nwords));
		pq_sendbytes(buf, (const char*)node->words, sizeof(node->words[0])*(node->nwords));
	}
}

static void save_typeoid(StringInfo buf, Oid typid)
{
	HeapTuple tup;
	Type type;
	Form_pg_namespace nspForm;
	Form_pg_type typ;
	Oid namespaceId;

	if(!OidIsValid(typid))
	{
		SAVE_IS_NULL();
		return;
	}
	SAVE_IS_NOT_NULL();

	/* get pg_type cache */
	type = typeidType(typid);
	Assert(HeapTupleIsValid(type));
	typ = (Form_pg_type)GETSTRUCT(type);
	Assert(typ);

	/* get namespace ID */
	namespaceId = typ->typnamespace;

	/* get namespace cache */
	tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(namespaceId));
	Assert(HeapTupleIsValid(tup));
	nspForm = (Form_pg_namespace)GETSTRUCT(tup);
	Assert(nspForm);

	/* save namespace and type name */
	pq_sendstring(buf, NameStr(nspForm->nspname));
	ReleaseSysCache(tup);
	pq_sendstring(buf, NameStr(typ->typname));
	ReleaseSysCache(type);
}

static void save_datum(StringInfo buf, Oid typid, Datum datum)
{
	bytea *save_data;
	Oid typeSendId;
	bool typIsVarlena;

	getTypeBinaryOutputInfo(typid, &typeSendId, &typIsVarlena);
	save_data = OidSendFunctionCall(typeSendId, datum);
	Assert(save_data);
	//pq_sendint(buf, VARSIZE(save_data), 4);
	pq_sendbytes(buf, VARDATA(save_data), VARSIZE(save_data) - VARHDRSZ);
	pfree(save_data);
}

static void save_ParamExternData(StringInfo buf, const ParamExternData *node)
{
	AssertArg(node);
	pq_sendbytes(buf, (const char*)&(node->pflags), sizeof(node->pflags));
	save_typeoid(buf, node->ptype);
	if(node->isnull)
	{
		SAVE_IS_NULL();
	}else
	{
		SAVE_IS_NOT_NULL();
		save_datum(buf, node->ptype, node->value);
	}
}

BEGIN_STRUCT(ParamListInfoData)
	NODE_SCALAR(int, numParams)
	do{
		int i;
		for(i=0;i<node->numParams;++i)
			save_ParamExternData(buf, &(node->params[i]));
	}while(0);
END_STRUCT(ParamListInfoData)

static void save_Integer(StringInfo buf, const Value *node)
{
	AssertArg(node);
	pq_sendbytes(buf, (const char*)&(intVal(node)), sizeof(intVal(node)));
}

static void save_String(StringInfo buf, const Value *node)
{
	AssertArg(node);
	pq_sendstring(buf, strVal(node));
}
BEGIN_NODE(List)
	do{
		ListCell *lc;
		pq_sendbytes(buf, (const char *)&(node->length), sizeof(node->length));
		foreach(lc,node)
			saveNode(buf, lfirst(lc));
	}while(0);
END_NODE(List)

static void save_IntList(StringInfo buf, const List *node)
{
	ListCell *lc;
	AssertArg(node);
	pq_sendbytes(buf, (const char *)&(node->length), sizeof(node->length));
	foreach(lc,node)
		pq_sendbytes(buf, (const char*)&lfirst_int(lc), sizeof(lfirst_int(lc)));
}
static void save_OidList(StringInfo buf, const List *node)
{
	ListCell *lc;
	AssertArg(node);
	pq_sendbytes(buf, (const char *)&(node->length), sizeof(node->length));
	foreach(lc,node)
		pq_sendbytes(buf, (const char*)&lfirst_oid(lc), sizeof(lfirst_oid(lc)));
}
BEGIN_NODE(Const)
	save_typeoid(buf, node->consttype);
	NODE_SCALAR(int32,consttypmod)
	NODE_SCALAR(Oid,constcollid)
	NODE_SCALAR(int,constlen)
	if(node->constisnull)
	{
		SAVE_IS_NULL();
	}else
	{
		SAVE_IS_NOT_NULL();
		save_datum(buf, node->consttype, node->constvalue);
	}
	NODE_SCALAR(bool,constbyval)
	NODE_SCALAR(int,location)
END_NODE(Const)

BEGIN_NODE(A_Const)
	NODE_SCALAR(NodeTag, val.type);
	switch(node->val.type)
	{
	case T_Integer:
		save_Integer(buf, &(node->val));
		break;
	case T_Float:
	case T_String:
	case T_BitString:
		save_String(buf, &(node->val));
		break;
	case T_Null:
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", node->val.type)
			,errcode(ERRCODE_INTERNAL_ERROR)));
	}
	NODE_SCALAR(int, location);
END_NODE(A_Const)

#define NO_NODE_A_Const
#define NO_NODE_Const
#include "nodes_define.h"
#include "nodes_undef.h"
#undef NO_NODE_A_Const
#undef NO_NODE_Const

void saveNode(StringInfo buf, const Node *node)
{
	AssertArg(buf);
	if(node == NULL)
	{
		SAVE_IS_NULL();
		return;
	}

	SAVE_IS_NOT_NULL();
	pq_sendbytes(buf, (const char*)&(node->type), sizeof(node->type));
	switch(nodeTag(node))
	{
#define CASE_TYPE(type, fun)					\
	case T_##type:								\
		save_##fun(buf, (type *)node);			\
		break
#define BEGIN_NODE(type)	CASE_TYPE(type,type);
#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2);
#define NO_NODE_JoinPath
	case T_Integer:
		save_Integer(buf, (Value*)node);break;
	case T_Float:
	case T_String:
	case T_BitString:
		save_String(buf, (Value*)node);break;
	case T_Null:
		break;
	case T_List:
		save_List(buf, (const List*)node);break;
	case T_IntList:
		save_IntList(buf, (const List*)node);break;
	case T_OidList:
		save_OidList(buf, (const List*)node);break;
#include "nodes_define.h"
#include "nodes_undef.h"
#undef NO_NODE_JoinPath
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", (int)nodeTag(node))));
	}
}

/* load functions */
#define LOAD_IS_NULL() pq_getmsgbyte(buf)

#define BEGIN_NODE(type)									\
	static type* load_##type(StringInfo buf, type *node)	\
	{														\
		AssertArg(buf && node);
#define END_NODE(type)										\
		return node;										\
	}
#define BEGIN_STRUCT(type)			BEGIN_NODE(type)
#define END_STRUCT(type)			END_NODE(type)
#define NODE_SAME(t1,t2)
#define NODE_BASE(base)				load_##base(buf, (base*)node);
#define NODE_NODE(t,m)				node->m = (t*)loadNode(buf);
#define NODE_NODE_MEB(t,m)			not support yet
#define NODE_NODE_ARRAY(t,m,l)		not support yet
#define NODE_BITMAPSET(t,m)			node->m = load_Bitmapset(buf);
#define NODE_BITMAPSET_ARRAY(t,m,l)	not support yet
#define NODE_SCALAR(t,m)			pq_copymsgbytes(buf, (char*)&(node->m), sizeof(node->m));
#define NODE_SCALAR_POINT(t,m,l)	pq_copymsgbytes(buf, (char*)&(node->m), l);
#define NODE_OTHER_POINT(t,m)		not support
#define NODE_STRING(m)										\
	do{														\
		if(LOAD_IS_NULL())									\
			node->m = NULL;									\
		else												\
			node->m = pstrdup(pq_getmsgstring(buf));		\
	}while(0);
#define NODE_STRUCT(t,m)									\
	do{														\
		if(LOAD_IS_NULL())									\
			node->m = NULL;									\
		else												\
			node->m = load_##t(buf, palloc0(sizeof(node->m[0])));	\
	}while(0);
#define NODE_STRUCT_ARRAY(t,m,l)		not support yet//SAVE_ARRAY(t,m,l,save_##t, t)
#define NODE_STRUCT_LIST(t,m)								\
	do{														\
		node->m = NIL;										\
		if(!LOAD_IS_NULL())									\
		{													\
			int i,length;									\
			t *v;											\
			pq_copymsgbytes(buf, (char*)&length, sizeof(length));\
			for(i=0;i<length;i++)							\
			{												\
				if(LOAD_IS_NULL())							\
					v = NULL;								\
				else										\
					v = load_##t(buf, palloc0(sizeof(*v)));	\
				node->m = lappend(node->m, v);				\
			}												\
		}													\
	}while(0);
#define NODE_STRUCT_MEB(t,m)			(void)load_##t(buf,&(node->m));
#define NODE_ENUM(t,m)					NODE_SCALAR(t,m)
#define NODE_DATUM(t,m,o,n)				not support

static Bitmapset* load_Bitmapset(StringInfo buf)
{
	Bitmapset *node;
	int nwords;
	AssertArg(buf);
	if(LOAD_IS_NULL())
		return NULL;
	pq_copymsgbytes(buf, (char*)&nwords, sizeof(nwords));
	node = palloc(offsetof(Bitmapset, words) + nwords * sizeof(node->words[0]));
	pq_copymsgbytes(buf, (char*)(node->words), nwords * sizeof(node->words[0]));
	node->nwords = nwords;
	return node;
}

static Oid load_typeoid(StringInfo buf)
{
	const char *str_nsp,*str_type;
	HeapTuple tup;
	Oid typid,namespaceId;

	if(LOAD_IS_NULL())
		return InvalidOid;

	str_nsp = pq_getmsgstring(buf);
	namespaceId = LookupExplicitNamespace(str_nsp, false);
	Assert(OidIsValid(namespaceId));

	str_type = pq_getmsgstring(buf);
	tup = SearchSysCache2(TYPENAMENSP, CStringGetDatum(str_type)
		, ObjectIdGetDatum(namespaceId));
	if(!HeapTupleIsValid(tup))
	{
		ereport(ERROR, (errmsg("Can not found type \"%s\".\"%s\"", str_nsp, str_type)));
	}

	typid = HeapTupleGetOid(tup);
	ReleaseSysCache(tup);
	return typid;
}

static Datum load_datum(StringInfo buf, Oid typid)
{
	Datum datum;
	Oid typReceive,typIOParam;

	Assert(OidIsValid(typid));

	getTypeBinaryInputInfo(typid, &typReceive, &typIOParam);
	datum = OidReceiveFunctionCall(typReceive, buf, typIOParam, -1);

	return datum;
}

static ParamExternData* load_ParamExternData(StringInfo buf, ParamExternData *node)
{
	AssertArg(node);
	pq_copymsgbytes(buf, (char*)&(node->pflags), sizeof(node->pflags));
	node->ptype = load_typeoid(buf);
	if(LOAD_IS_NULL())
	{
		node->isnull = true;
		node->value = (Datum)0;
	}else
	{
		node->isnull = false;
		node->value = load_datum(buf, node->ptype);
	}
	return node;
}

BEGIN_STRUCT(ParamListInfoData)
	NODE_SCALAR(int, numParams);
	node = repalloc(node, offsetof(ParamListInfoData, params)
		+ node->numParams * (sizeof(node->params[0])));
	do{
		int i;
		for(i=0;i<node->numParams;++i)
			(void)load_ParamExternData(buf, &(node->params[i]));
	}while(0);
END_STRUCT(ParamListInfoData)

static Value* load_Integer(StringInfo buf, Value *node)
{
	AssertArg(node);
	pq_copymsgbytes(buf, (char*)&(intVal(node)), sizeof(intVal(node)));
	return node;
}

static Value* load_String(StringInfo buf, Value *node)
{
	AssertArg(node);
	strVal(node) = pstrdup(pq_getmsgstring(buf));
	return node;
}

static List* load_List(StringInfo buf)
{
	List *list = NIL;
	int i,length;
	pq_copymsgbytes(buf, (char*)&length, sizeof(length));
	for(i=0;i<length;++i)
		list = lappend(list, loadNode(buf));
	return list;
}

static List* load_OidList(StringInfo buf)
{
	List *list = NIL;
	int i,length;
	Oid oid;
	pq_copymsgbytes(buf, (char*)&length, sizeof(length));
	for(i=0;i<length;++i)
	{
		pq_copymsgbytes(buf, (char*)&oid, sizeof(oid));
		list = lappend_oid(list, oid);
	}
	return list;
}

static List* load_IntList(StringInfo buf)
{
	List *list = NIL;
	int i,length,val;
	pq_copymsgbytes(buf, (char*)&length, sizeof(length));
	for(i=0;i<length;++i)
	{
		pq_copymsgbytes(buf, (char*)&val, sizeof(val));
		list = lappend_int(list, val);
	}
	return list;
}

BEGIN_NODE(Const)
	node->consttype = load_typeoid(buf);
	NODE_SCALAR(int32,consttypmod)
	NODE_SCALAR(Oid,constcollid)
	NODE_SCALAR(int,constlen)
	if(LOAD_IS_NULL())
	{
		node->constisnull = true;
		node->constvalue = (Datum)0;
	}else
	{
		node->constisnull = false;
		node->constvalue = load_datum(buf, node->consttype);
	}
	NODE_SCALAR(bool,constbyval)
	NODE_SCALAR(int,location)
END_NODE(Const)

BEGIN_NODE(A_Const)
	NODE_SCALAR(NodeTag, val.type);
	switch(node->val.type)
	{
	case T_Integer:
		(void)load_Integer(buf, &(node->val));
		break;
	case T_Float:
	case T_String:
	case T_BitString:
		(void)load_String(buf, &(node->val));
		break;
	case T_Null:
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", node->val.type)
			,errcode(ERRCODE_INTERNAL_ERROR)));
	}
	NODE_SCALAR(int, location);
END_NODE(A_Const)

#define NO_NODE_Const
#define NO_NODE_A_Const
#include "nodes_define.h"
#include "nodes_undef.h"
#undef NO_NODE_Const
#undef NO_NODE_A_Const

Node* loadNode(StringInfo buf)
{
	NodeTag tag;
	AssertArg(buf);

	if(LOAD_IS_NULL())
		return NULL;

	pq_copymsgbytes(buf, (char*)&tag, sizeof(tag));
	switch(tag)
	{
	case T_Integer:
		return (Node*)load_Integer(buf, makeInteger(0));
	case T_Float:
	case T_String:
	case T_BitString:
	case T_Null:
		{
			Value *value = palloc0(sizeof(Value));
			NodeSetTag(value, tag);
			if(tag != T_Null)
				value = load_String(buf, value);
			return (Node*)value;
		}
	case T_List:
		return (Node*)load_List(buf);
	case T_IntList:
		return (Node*)load_IntList(buf);
	case T_OidList:
		return (Node*)load_OidList(buf);
#undef CASE_TYPE
#define CASE_TYPE(type, fun)							\
	case T_##type:										\
		return (Node*)load_##fun(buf, makeNode(type))
#define BEGIN_NODE(type)	CASE_TYPE(type,type);
#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2);
#define NO_NODE_JoinPath
#define NO_NODE_PlannerInfo
#include "nodes_define.h"
#include "nodes_undef.h"
#undef NO_NODE_JoinPath
#undef NO_NODE_PlannerInfo
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", (int)tag)));
	}
	return NULL;
}