/*
 * commands of hba
 */


#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/mgr_updateparm.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "funcapi.h"
#include "utils/lsyscache.h"
#include "../../interfaces/libpq/libpq-fe.h"
 
 
#include "catalog/mgr_hba.h"
#include "utils/formatting.h"
 
#define SPACE           " "
 
#define CONNECT_TYPE_INDEX     		0
#define DATABASE_INDEX     			1
#define ROLE_INDEX     				2
#define ADDRESS_INDEX     			3
#define ADDRESS_MASK_INDEX     		4
#define AUTH_METHOD_INDEX     		5
#define AUTH_OPTION_INDEX     		6

struct HbaParam
{
	char conntype;
	char *database;
	char *role;
	char *addr;
	int  addr_mask;
	char *auth_method;
	char *auth_option;	
};
 /*the values see agt_cmd.c, used for pg_hba.conf add content*/
typedef enum ConnectType
{
	CONNECT_ERR = 0,
	CONNECT_LOCAL=1,
	CONNECT_HOST,
	CONNECT_HOSTSSL,
	CONNECT_HOSTNOSSL
}ConnectType;

typedef struct TableInfo
{
	Relation rel;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}TableInfo;

#if (Natts_mgr_hba != 3)
#error "need change hba code"
#endif

extern void mgr_reload_conf(Oid hostoid, char *nodepath);

static TupleDesc common_command_tuple_desc = NULL;
static uint32 get_hba_max_id(void);
static Oid tuple_insert_table_hba(Relation rel, Datum *values, bool *isnull);
static ConnectType get_connect_type(char *str_type);
static HeapTuple tuple_form_table_hba(const uint32 row_id, const Name node_name, const char * values);
static TupleDesc get_tuple_desc_for_hba(void);

/*
Datum mgr_list_hba_by_name(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TableInfo *info;
	HeapTuple tuple;
	HeapTuple tup_result;
	Form_mgr_hba mgr_hba;
	ScanKeyData key[1];
	char *node_name;
	
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc(sizeof(*info));
		
		node_name = strVal(PG_GETARG_DATUM(0));
		ScanKeyInit(&key[0]
				,Anum_mgr_hba_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(node_name));

		info->rel= heap_open(HbaRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan(info->rel, SnapshotNow, 1, key);
		info->lcp =NULL;
*/
		/* save info */

/*		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);
	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(!PointerIsValid(tuple))
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}
	else
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		tup_result = tuple_form_table_hba(mgr_hba->row_id, &(mgr_hba->nodename), TextDatumGetCString(&(mgr_hba->hbavalue)));
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}
}*/
Datum mgr_list_hba_by_name(PG_FUNCTION_ARGS)
{
	List *nodenamelist;
	HeapTuple tup_result;
	HeapTuple tuple;
	Form_mgr_hba mgr_hba;
	ScanKeyData key[1];
	FuncCallContext *funcctx;
/*	ListCell **lcp; */
	TableInfo *info;
	char *nodestrname;
	
	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		nodenamelist = NIL;
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		/* allocate memory for user context */
		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		if(!PG_ARGISNULL(0))
		{
			#ifdef ADB
				nodenamelist = get_fcinfo_namelist("", 0, fcinfo, NULL);
			#else
				nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
			#endif
		}
		*(info->lcp) = list_head(nodenamelist);
		nodestrname = (char *) lfirst(*(info->lcp));
		ScanKeyInit(&key[0]
				,Anum_mgr_hba_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodestrname));
		info->rel = heap_open(HbaRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan(info->rel, SnapshotNow, 1, key);
		
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);
	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(!PointerIsValid(tuple))
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}
	else
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		tup_result = tuple_form_table_hba(mgr_hba->row_id, &(mgr_hba->nodename), TextDatumGetCString(&(mgr_hba->hbavalue)));
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}
}

void mgr_add_hba_one(MGRAddHba *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char *host_address;
	char *node_name;
	char * node_path;
	Datum datumPath;
	uint32 host_port;	
	ListCell *lc;
	List *list_hba = NIL, *list_elem = NIL;
	Value *value;
	char *str_elem, *str, *str_remain;
	StringInfoData infosendmsg;
	StringInfoData hbainfomsg;	
	bool isNull = false;
	GetAgentCmdRst getAgentCmdRst;
	uint32 hba_max_id = 0;
	ConnectType conntype = CONNECT_ERR;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	
	Assert(node && node->options && node->name);
	node_name = node->name;
	list_hba = node->options;
	
	/*step1: check the nodename is exist in the mgr_node and make sure it has been initialized*/
	rel = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(rel, node_name, CNDN_TYPE_COORDINATOR_MASTER);
	if(!(HeapTupleIsValid(tuple)))
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not exist", node_name)));
	}
    mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	host_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
	host_port = mgr_node->nodeport;
		
	/*step2: check the hba msg is valid*/
	initStringInfo(&infosendmsg);/*send to agent*/
	initStringInfo(&hbainfomsg);/*add to hba table*/
	foreach(lc, list_hba)
	{
		resetStringInfo(&hbainfomsg);
		value = lfirst(lc);
		Assert(value && IsA(value, String));
		str = strVal(value);
		str_elem = strtok_r(str, SPACE, &str_remain);
		conntype = get_connect_type(str_elem);
		if(conntype == CONNECT_ERR)
		{
			ereport(ERROR, (errmsg("%s", "the type of hba is wrong")));
		}
		appendStringInfo(&infosendmsg, "%c%c", conntype, '\0');	
		appendStringInfo(&hbainfomsg, "%s  ",str_elem);
		while(str_elem != NULL)
		{
			str_elem = strtok_r(NULL, SPACE, &str_remain);
			if(PointerIsValid(str_elem))
			{
				appendStringInfo(&infosendmsg,"%s%c", str_elem, '\0');	
				appendStringInfo(&hbainfomsg, "%s  ",str_elem);
			}						
		}	
		str_elem = palloc0(hbainfomsg.len + 1);
		memcpy(str_elem, hbainfomsg.data, hbainfomsg.len);
		list_elem = lappend(list_elem, str_elem);
	}

	/*step3: send msg to the specified coordinator to update datanode master's pg_hba.conf*/	
	initStringInfo(&(getAgentCmdRst.description));
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
	if(isNull)
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not exist nodepath", node_name)));
	}
	node_path = TextDatumGetCString(datumPath);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							node_path,
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
   		
	/*step4: execute pgxc_ctl reload to the specified host */
	mgr_reload_conf(mgr_node->nodehost, node_path);	
	heap_close(rel, AccessShareLock);

	/*step5: add a new tuple to hba table */
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	hba_max_id = get_hba_max_id();
	foreach(lc, list_elem)
	{
		str_elem = lfirst(lc);
		hba_max_id = hba_max_id + 1;
		datum[Anum_mgr_hba_id - 1] = Int32GetDatum(hba_max_id);
		datum[Anum_mgr_hba_nodename - 1] = NameGetDatum(node_name);
		datum[Anum_mgr_hba_value - 1] = CStringGetTextDatum(str_elem);
		tuple_insert_table_hba(rel, datum, isnull);
	}
	heap_close(rel, RowExclusiveLock);
	
	/*step6: Release an allocated chunk*/
	foreach(lc, list_elem)
	{
		pfree(lfirst(lc));
	}
	list_free(list_elem);
	pfree(hbainfomsg.data);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);	
	heap_freetuple(tuple);
}

static uint32 get_hba_max_id(void)
{
	Relation rel;
	HeapTuple tuple;
	HeapScanDesc rel_scan;
	Form_mgr_hba mgr_hba;
	uint32 max_id = 0;
	rel = heap_open(HbaRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		if(max_id < mgr_hba->row_id)
			max_id = mgr_hba->row_id;
	}
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	return max_id;
}

static Oid tuple_insert_table_hba(Relation rel, Datum *values, bool *isnull)
{
	HeapTuple newtuple;
	Oid hba_oid;
	Assert(values && isnull);
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	/* now, we can insert record */
	newtuple = heap_form_tuple(RelationGetDescr(rel), values, isnull);
	hba_oid = simple_heap_insert(rel, newtuple);
	CatalogUpdateIndexes(rel, newtuple);
	heap_freetuple(newtuple);

	/*close relation */
	heap_close(rel, RowExclusiveLock);
	return hba_oid;
}
static ConnectType get_connect_type(char *str_type)
{
	ConnectType conntype = CONNECT_ERR;
	char *str_lwr ;
	Assert(str_type);
	str_lwr = str_tolower(str_type, strlen(str_type), DEFAULT_COLLATION_OID);
	if(strcmp(str_lwr, "local") == 0)
		conntype = CONNECT_LOCAL;
	else if(strcmp(str_lwr, "host") == 0)
		conntype = CONNECT_HOST;
	else if(strcmp(str_lwr, "hostssl") == 0)
		conntype = CONNECT_HOSTSSL;
	else if(strcmp(str_lwr, "hostnossl") == 0)
		conntype = CONNECT_HOSTNOSSL;
	return conntype;
}

static TupleDesc get_tuple_desc_for_hba(void)
{
    if(common_command_tuple_desc == NULL)
    {
        MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
        TupleDesc volatile desc = NULL;
        PG_TRY();
        {
            desc = CreateTemplateTupleDesc(3, false);
            TupleDescInitEntry(desc, (AttrNumber) 1, "row_id",
                               INT4OID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 2, "nodename",
                               NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 3, "values",
                               TEXTOID, -1, 0);
            common_command_tuple_desc = BlessTupleDesc(desc);
        }PG_CATCH();
        {
            if(desc)
                FreeTupleDesc(desc);
            PG_RE_THROW();
        }PG_END_TRY();
        (void)MemoryContextSwitchTo(old_context);
    }
    Assert(common_command_tuple_desc);
    return common_command_tuple_desc;
}

static HeapTuple tuple_form_table_hba(const uint32 row_id, const Name node_name, const char * values)
{
    Datum datums[3];
    bool nulls[3];
    TupleDesc desc;
    AssertArg(node_name && values);
    desc = get_tuple_desc_for_hba();

    AssertArg(desc && desc->natts == 3
        && desc->attrs[0]->atttypid == INT4OID
        && desc->attrs[1]->atttypid == NAMEOID
        && desc->attrs[2]->atttypid == TEXTOID);
	datums[0] = Int32GetDatum(row_id);   
    datums[1] = NameGetDatum(node_name);
    datums[2] = CStringGetTextDatum(values);
    nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
    return heap_form_tuple(desc, datums, nulls);
}

