/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "utils/lsyscache.h"

#ifdef PGXC
#include "optimizer/pgxcplan.h"
#endif

#ifdef ADBMGRD
#include "parser/mgr_node.h"
#endif /* ADBMGRD */

#define TABLE_STOP 2

#define MARK_INFO(m)	{#m,m}
typedef struct MarkInfo
{
	const char *mark;
	size_t		val;
}MarkInfo;

typedef void(*outputfunc)(const void *node, StringInfo str, int space);
typedef void(*outputscalar)(StringInfo str, const void *p);
static void printNode(const void *obj, StringInfo str, int space);

static void outputNodeBegin(StringInfo str, const char *type, int space)
{
	if(str->len && str->data[str->len-1] != '\n')
		appendStringInfoChar(str, '\n');
	appendStringInfoSpaces(str, space);
	appendStringInfo(str, "{%s\n", type);
}

static void outputNodeEnd(StringInfo str, const char *type, int space)
{
	appendStringInfoSpaces(str, space);
	appendStringInfoString(str, "}\n");
}

static void outputNode(const void *node, outputfunc func, const char *type, StringInfo str, int space)
{
	if(node)
	{
		outputNodeBegin(str, type, space);
		(*func)(node, str, space+TABLE_STOP);
		outputNodeEnd(str, type, space);
	}else
	{
		appendStringInfoString(str, "<>\n");
	}
}

static void outputNodeArray(void ** const node, size_t count, outputfunc func, const char *type, StringInfo str, int space)
{
	size_t i;
	int space2 = space+TABLE_STOP;
	appendStringInfoString(str, "{\n");
	for(i=0;i<count;++i)
		outputNode(node[i], func, type, str, space2);
	appendStringInfoSpaces(str, space);
	appendStringInfoString(str, "}\n");
}

static void outputBitmapset(const Bitmapset *node, StringInfo str, int space)
{
	Bitmapset *a = bms_copy(node);
	const char *tmp = "{";
	int n;
	while((n=bms_first_member(a)) >= 0)
	{
		appendStringInfoString(str, tmp);
		appendStringInfo(str, "%d", n);
		tmp = ",";
	}
	bms_free(a);
	if(tmp[0] == '{')
		appendStringInfoChar(str, '{');
	appendStringInfoString(str, "}\n");
}

static void appendObjectMebName(StringInfo str, const char *name, int space)
{
	appendStringInfoSpaces(str, space);
	appendStringInfoChar(str, ':');
	appendStringInfoString(str, name);
	appendStringInfoChar(str, ' ');
}

#define appendObjectMebEnd(str_, name_, space_)	appendStringInfoChar(str, '\n')

static void outputString(StringInfo str, const char *name, const char *value, int space)
{
	appendObjectMebName(str, name, space);
	if(value)
		appendStringInfo(str, "\"%s\"", value);
	else
		appendStringInfoString(str, "<>");
	appendObjectMebEnd(str, name, space);
}

static void outputValue(const Value *value, StringInfo str, int space)
{
	if(value == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}
	appendStringInfoChar(str, '{');
	switch(nodeTag(value))
	{
	case T_Value:
		appendStringInfoString(str, "Value");
		break;
	case T_Integer:
		appendStringInfo(str, "Integer %ld", intVal(value));
		break;
	case T_Float:
		appendStringInfo(str, "Float %s", strVal(value));
		break;
	case T_String:
		appendStringInfo(str, "String \"%s\"", strVal(value));
		break;
	case T_BitString:
		appendStringInfo(str, "BitString \"%s\"", strVal(value));
		break;
	case T_Null:
		appendStringInfoString(str, "Null");
		break;
	default:
		ereport(ERROR, (errmsg("unknown Value type %d", nodeTag(value))));
		break;
	}
	appendStringInfoString(str, "}\n");
}

static void outputListObject(const List *list, StringInfo str, int space, outputfunc func)
{
	const ListCell *lc;
	int space2;
	Assert(func);
	if(list == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}
	Assert(IsA(list, List));

	space2=space+TABLE_STOP;
	outputNodeBegin(str, "List", space);
	foreach(lc, list)
		(*func)(lfirst(lc), str, space2);
	outputNodeEnd(str, "List", space);
}

static void outputList(const List *list, StringInfo str, int space)
{
	const ListCell *lc;
	if(list == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}

	if(IsA(list, List))
	{
		outputListObject(list, str, space, printNode);
	}else if(IsA(list, OidList))
	{
		char tmp = '{';
		foreach(lc, list)
		{
			appendStringInfoChar(str, tmp);
			appendStringInfo(str, "%u", lfirst_oid(lc));
			tmp = ',';
		}
		if(tmp == '{')
			appendStringInfoChar(str, tmp);
		appendStringInfoString(str, "}\n");
	}else if(IsA(list, IntList))
	{
		char tmp = '{';
		foreach(lc, list)
		{
			appendStringInfoChar(str, tmp);
			appendStringInfo(str, "%d", lfirst_int(lc));
			tmp = ',';
		}
		if(tmp == '{')
			appendStringInfoChar(str, tmp);
		appendStringInfoString(str, "}\n");
	}else
	{
		ereport(ERROR, (errmsg("Unknown list type %d", nodeTag(list))));
	}
}

static void outputDatum(StringInfo str, Datum datum, Oid type, bool isnull)
{
	if(isnull)
	{
		appendStringInfoString(str, "NULL");
	}else
	{
		char *val;
		Oid			typoutput;
		bool		typIsVarlena;
		getTypeOutputInfo(type, &typoutput, &typIsVarlena);
		val = OidOutputFunctionCall(typoutput, datum);
		appendStringInfoString(str, val);
		pfree(val);
	}
	appendStringInfoChar(str, '\n');
}

static void outputAclMode(StringInfo str, const AclMode *value)
{
	static const MarkInfo acl[]=
	{
		MARK_INFO(ACL_INSERT),
		MARK_INFO(ACL_SELECT),
		MARK_INFO(ACL_UPDATE),
		MARK_INFO(ACL_DELETE),
		MARK_INFO(ACL_TRUNCATE),
		MARK_INFO(ACL_REFERENCES),
		MARK_INFO(ACL_TRIGGER),
		MARK_INFO(ACL_EXECUTE),
		MARK_INFO(ACL_USAGE),
		MARK_INFO(ACL_CREATE),
		MARK_INFO(ACL_CREATE_TEMP),
		MARK_INFO(ACL_CONNECT)
	};
	size_t i;
	AclMode v;
	char tmp;
	Assert(value);
	
	v = *value;
	if(v == 0)
	{
		appendStringInfoString(str, "0\n");
		return;
	}

	tmp = ' ';
	for(i=0;i<lengthof(acl);++i)
	{
		if(v & acl[i].val)
		{
			appendStringInfoChar(str, tmp);
			appendStringInfoString(str, acl[i].mark);
			tmp='|';
		}
	}
}

static void outputbool(StringInfo str, const bool *value)
{
	appendStringInfoString(str, *value ? "true":"false");
}

static void outputchar(StringInfo str, const char *value)
{
	Assert(value && str);
	if(isprint(*value))
		appendStringInfo(str, "'%c'", *value);
	else
		appendStringInfo(str, "%d", *value);
}

static void output_scalar_array(StringInfo str, const void *p
	, outputscalar func, size_t size, size_t count)
{
	size_t i;
	if(p == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}

	appendStringInfoChar(str, '[');
	for(i=0;i<count;++i)
	{
		(*func)(str, p);
		p = ((char*)p) + size;
		appendStringInfoChar(str, ' ');
	}
	appendStringInfoString(str, "]\n");
}

#define SIMPLE_OUTPUT_DECLARE(type, fmt)					\
static void output##type(StringInfo str, const type *value)	\
{															\
	appendStringInfo(str, fmt, *value);						\
}
SIMPLE_OUTPUT_DECLARE(int, "%d")
SIMPLE_OUTPUT_DECLARE(Cost, "%g")
SIMPLE_OUTPUT_DECLARE(double, "%g")
SIMPLE_OUTPUT_DECLARE(long, "%ld")
SIMPLE_OUTPUT_DECLARE(Index, "%u")
SIMPLE_OUTPUT_DECLARE(Oid, "%u")
SIMPLE_OUTPUT_DECLARE(AttrNumber, "%d")
SIMPLE_OUTPUT_DECLARE(int32, "%d")
SIMPLE_OUTPUT_DECLARE(int16, "%d")
SIMPLE_OUTPUT_DECLARE(uint16, "%u")
SIMPLE_OUTPUT_DECLARE(uint32, "%u")
SIMPLE_OUTPUT_DECLARE(BlockNumber, "%u")
SIMPLE_OUTPUT_DECLARE(RegProcedure, "%u")
SIMPLE_OUTPUT_DECLARE(Selectivity, "%g")
SIMPLE_OUTPUT_DECLARE(bits32, "%08x")

/* declare functions */
#define BEGIN_NODE(type)	\
	static void output##type(const type *node, StringInfo str, int space);
#define BEGIN_STRUCT(type)	BEGIN_NODE(type)
#define NODE_SAME(t1,t2)
#include "nodes_define.h"
#include "nodes_undef.h"

/* output enum functions */
#define BEGIN_ENUM(e)						\
static void output##e(StringInfo str, e v)	\
{											\
	const char *val = NULL;					\
	switch(v)								\
	{
#define END_ENUM(e)							\
	default:								\
		ereport(ERROR, (errmsg("unknown enum " #e " value %d", v)));	\
	}										\
	appendStringInfoString(str, val);		\
	appendStringInfoChar(str, '\n');		\
}
#define ENUM_VALUE(ev)						\
	case ev:								\
		val = #ev;							\
		break;
#include "enum_define.h"
#include "enum_undef.h"

/* functions */
#define BEGIN_NODE(type) 								\
	static void output##type(const type *node, StringInfo str, int space)	\
	{																		\
		Assert(node && str);												\

#define END_NODE(type)						\
	}

#define BEGIN_STRUCT	BEGIN_NODE
#define END_STRUCT		END_NODE
#define NODE_SAME(t1,t2)

#define NODE_BASE2(type, meb)	\
	outputNode(&(node->meb), (outputfunc)output##type, #type, str, space);

#define NODE_NODE(t,m)									\
		appendObjectMebName(str, #m, space);			\
		printNode(node->m, str, space+TABLE_STOP);

#define NODE_NODE_MEB(t,m)								\
		appendObjectMebName(str, #m, space);			\
		printNode(&(node->m), str, space+TABLE_STOP);

#define NODE_NODE_ARRAY(t,m,l)							\
		outputNodeArray((void **const )node->m, l, (outputfunc)printNode, #t, str, space);

#define NODE_BITMAPSET(t,m)								\
		appendObjectMebName(str, #m, space);		\
		outputBitmapset(node->m, str, space);

#define NODE_BITMAPSET_ARRAY(t,m,l)						\
		do												\
		{												\
			size_t count = (l);							\
			size_t i;									\
			appendObjectMebName(str, #m, space);		\
			appendStringInfoChar(str, '{');				\
			for(i=0;i<count;++i)						\
				outputBitmapset(node->m[i], str, space);\
			appendStringInfoString(str, "}\n");			\
		}while(0);

#define NODE_STRUCT(t,m)								\
		appendObjectMebName(str, #m, space);			\
		outputNode(node->m, (outputfunc)output##t, #t, str, space);

#define NODE_STRUCT_ARRAY(t,m,l)							\
		outputNodeArray((void **const )node->m, l, (outputfunc)output##t, #t, str, space);

#define NODE_STRUCT_LIST(t,m)							\
		appendObjectMebName(str, #m, space);			\
		outputListObject(node->m, str, space, (outputfunc)output##t);

#define NODE_STRUCT_MEB(t,m)								\
		appendObjectMebName(str, #m, space);				\
		outputNode(&(node->m), (outputfunc)output##t, #t, str, space);

#define NODE_STRING(m) outputString(str, #m, node->m, space);

#define NODE_SCALAR(t,m)						\
		appendObjectMebName(str, #m, space);	\
		output##t(str, &(node->m));				\
		appendObjectMebEnd(str, #m, space);

#define NODE_SCALAR_POINT(t,m,l)				\
		appendObjectMebName(str, #m, space);	\
		output_scalar_array(str, node->m, (outputscalar)output##t, sizeof(t), l);
#define NODE_LOCATION NODE_SCALAR

#define NODE_OTHER_POINT(t,m)					\
		appendObjectMebName(str, #m, space);	\
		appendStringInfo(str, "%p\n", node->m);

#define NODE_DATUM(t,m,o,n) 					\
		appendObjectMebName(str, #m, space);	\
		outputDatum(str, node->m, o, n);

#define NODE_ENUM(t, m)							\
		appendObjectMebName(str, #m, space);	\
		output##t(str, node->m);

#include "nodes_define.h"
#include "nodes_undef.h"

static void printNode(const void *obj, StringInfo str, int space)
{
	Assert(str);
	if(obj == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}
	switch(nodeTag(obj))
	{
	#define CASE_TYPE(type,fun)							\
		case T_##type:									\
			outputNode(obj, (outputfunc)output##fun, #type, str, space);\
			break;
	#define BEGIN_NODE(type)	CASE_TYPE(type,type)
	#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2)
	#define NO_NODE_JoinPath
	#include "nodes_define.h"
	case T_Value:
	case T_Integer:
	case T_Float:
	case T_String:
	case T_BitString:
	case T_Null:
		if(str->len > 0 && str->data[str->len-1]=='\n')
			appendStringInfoSpaces(str, space);
		outputValue(obj, str, space);
		break;
	case T_List:
	case T_OidList:
	case T_IntList:
		outputList(obj, str, space);
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", nodeTag(obj))));
	}
}

char *printObject(const void *obj)
{
	StringInfoData str;
	initStringInfo(&str);
	printNode(obj, &str, 0);
	return str.data;
}