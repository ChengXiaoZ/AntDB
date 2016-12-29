/*
 * commands of hba
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
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
#include "../../bin/agent/hba_scan.h"
 
#include "catalog/mgr_hba.h"
#include "utils/formatting.h"


#define SPACE          				" "
#define INVALID_ID					0x7fffffff
#define HBA_RESULT_COLUMN  			3
#define HBA_ELEM_NUM				6
#define is_digit(c) ((unsigned)(c) - '0' <= 9)
 
typedef enum OperateHbaType
{
	HANDLE_NO = 0,	/*don't do any handle for drop table hba*/
	HBA_ALL=1,		/*drop table hba all*/
	HBA_NODENAME_ALL,/*drop table hba all base on nodename*/
	HBA_NODENAME_ID  /*drop table hba all base on row_id and nodename*/
}OperateHbaType;

typedef struct TableInfo
{
	Relation rel;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}TableInfo;

#if (Natts_mgr_hba != 3)
#error "need change hba code"
#endif
static TupleDesc common_command_tuple_desc = NULL;

/*--------------------------------------------------------------------*/
void mgr_clean_hba_table(void);
/*--------------------------------------------------------------------*/
	
extern void mgr_reload_conf(Oid hostoid, char *nodepath);
//extern HbaInfo* parse_hba_file(const char *filename);
/*--------------------------------------------------------------------*/
static void mgr_add_hba_all(uint32 *hba_max_id, List *args_list, GetAgentCmdRst *err_msg);
static void mgr_add_hba_one(uint32 *hba_max_id, char *coord_name, List *args_list, GetAgentCmdRst *err_msg, bool record_err_msg);
static void drop_hba_all(GetAgentCmdRst *err_msg);
static void drop_hba_nodename_all(char *coord_name, GetAgentCmdRst *err_msg);
static void drop_hba_nodename_id(char *coord_name, uint32 row_id, GetAgentCmdRst *err_msg);

static uint32 get_hba_max_id(void);
static Oid tuple_insert_table_hba(Datum *values, bool *isnull);
static HbaType get_connect_type(char *str_type);
static TupleDesc get_tuple_desc_for_hba(void);
static HeapTuple tuple_form_table_hba(const uint32 row_id, const Name node_name, const char * values);
static void delete_table_hba(uint32 row_id, char *coord_name);
static bool check_hba_tuple_exist(uint32 row_id, char *coord_name, char *values);
static bool IDIsValid(uint32 row_id);
static bool check_pghbainfo_vaild(StringInfo hba_info, StringInfo err_msg, bool record_err_msg);
static bool is_auth_method_valid(char *method);
/*--------------------------------------------------------------------*/
Datum mgr_alter_hba(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	char *coord_name = "coord1";
	tup_result = tuple_form_table_hba(21, (Name)coord_name, "success");
	return HeapTupleGetDatum(tup_result);
}

	
Datum mgr_list_hba_by_name(PG_FUNCTION_ARGS)
{
	List *nodenamelist;
	HeapTuple tup_result;
	HeapTuple tuple;
	Form_mgr_hba mgr_hba;
	ScanKeyData key[1];
	FuncCallContext *funcctx;
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
Datum mgr_add_hba(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst err_msg;
	OperateHbaType handle_type = HANDLE_NO;
	HeapTuple tup_result;
	List *args_list;
	char *coord_name;
	uint32 hba_max_id = 0;
	err_msg.ret = true;
	initStringInfo(&err_msg.description);
	/*step 1: parase args,and get nodename,hba values;
	if nodename equal '*', add to all the coordinator*/
	if(PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("args is null")));
	}
	#ifdef ADB
		args_list = get_fcinfo_namelist("", 0, fcinfo, NULL);
	#else
		args_list = get_fcinfo_namelist("", 0, fcinfo);
	#endif
	if(args_list->length < 2)
	{
		ereport(ERROR, (errmsg("args is not enough")));
	}	
	coord_name = llast(args_list);
	if(strcmp(coord_name,"*") == 0) 
		handle_type = HBA_ALL;         
	else
		handle_type = HBA_NODENAME_ALL;
	/*step 2: operate add hba comamnd*/	
	args_list = list_delete(args_list, llast(args_list)); /*remove nodename from list*/
	hba_max_id = get_hba_max_id();
	if(HBA_ALL == handle_type)
	{
		mgr_add_hba_all(&hba_max_id, args_list, &err_msg);
	}	
	else if(HBA_NODENAME_ALL == handle_type)
	{
		mgr_add_hba_one(&hba_max_id, coord_name, args_list, &err_msg, true);
	}	
	/*step 3: show the state of operating drop hba commands */
	tup_result = tuple_form_table_hba(INVALID_ID
									,(Name)coord_name
									,true == err_msg.ret ? "success" : err_msg.description.data);
	
	pfree(err_msg.description.data);		
	return HeapTupleGetDatum(tup_result);
}
static void mgr_add_hba_all(uint32 *hba_max_id, List *args_list, GetAgentCmdRst *err_msg)
{
	Relation rel;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	ScanKeyData key[1];
	char *coord_name;
	bool record_err_msg = true;
	ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
		
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		coord_name = NameStr(mgr_node->nodename);
		mgr_add_hba_one(hba_max_id, coord_name, args_list, err_msg, record_err_msg);
		record_err_msg = false;
	}
	
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}
static void mgr_add_hba_one(uint32 *hba_max_id, char *coord_name, List *args_list, GetAgentCmdRst *err_msg, bool record_err_msg)
{
	Relation rel;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char *host_address;
	char * node_path;
	Datum datumPath;
	Oid hostoid;
	ListCell *lc, *lc_elem;
	List *list_elem = NIL;
	char *str_elem, *str, *str_remain;
	char split_str[100];
	StringInfoData infosendmsg;
	StringInfoData hbainfomsg;
	StringInfoData hbasendmsg;
	bool isNull = false;
	HbaType conntype = HBA_TYPE_EMPTY;
	GetAgentCmdRst getAgentCmdRst;
	bool is_exist = false;
	bool is_valid = false;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	
	Assert(coord_name);
	Assert(args_list);
	initStringInfo(&getAgentCmdRst.description);
	/*step1: check the nodename is exist in the mgr_node table and make sure it has been initialized*/
	rel = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(rel, coord_name, CNDN_TYPE_COORDINATOR_MASTER);
	if(!(HeapTupleIsValid(tuple)))
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not exist", coord_name)));
	}
    mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);	
	if(false == mgr_node->nodeinited)	
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not init", coord_name)));
	}
	hostoid = mgr_node->nodehost;
	host_address = get_hostaddress_from_hostoid(hostoid);
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
	if(isNull)
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not exist nodepath", coord_name)));
	}
	node_path = TextDatumGetCString(datumPath);
	heap_close(rel, AccessShareLock);
	heap_freetuple(tuple);
	
	/*step2: parser the hba values and check whether it's valid*/
	initStringInfo(&infosendmsg);/*send to agent*/
	initStringInfo(&hbasendmsg);/*store one hba contxt*/
	initStringInfo(&hbainfomsg);/*add to hba table*/
	foreach(lc, args_list)
	{
		resetStringInfo(&hbainfomsg);
		resetStringInfo(&hbasendmsg);
		str = lfirst(lc);
		memcpy(split_str,str,strlen(str)+1);
		str_elem = strtok_r(split_str, SPACE, &str_remain);
		conntype = get_connect_type(str_elem);
		appendStringInfo(&hbasendmsg, "%c%c", conntype, '\0');	
		appendStringInfo(&hbainfomsg, "%s", str_elem);
		while(str_elem != NULL)
		{
			str_elem = strtok_r(NULL, SPACE, &str_remain);
			if(PointerIsValid(str_elem))
			{
				appendStringInfo(&hbasendmsg,"%s%c", str_elem, '\0');	
				appendStringInfo(&hbainfomsg, "  %s",str_elem);
			}						
		}	
		/*check the hba value is valid*/
		is_valid = check_pghbainfo_vaild(&hbasendmsg, &err_msg->description, record_err_msg);
		if(false == is_valid)
		{
			if(true == record_err_msg)
			{
				err_msg->ret = false;
				appendStringInfo(&err_msg->description, "in the item \"%s\".\n",str);
			}
			continue;
		}
		/*check the value whether exist in the hba table*/
		is_exist = check_hba_tuple_exist(INVALID_ID, coord_name, hbainfomsg.data);
		if(is_exist)
		{
			appendStringInfo(&err_msg->description, "nodename %s with values \"%s\" has existed.\n", coord_name, hbainfomsg.data);
			err_msg->ret = false;
			continue;
		}
		/*add to list and remove the same*/
		str_elem = palloc0(hbainfomsg.len + 1);
		memcpy(str_elem, hbainfomsg.data, hbainfomsg.len);
		foreach(lc_elem, list_elem)
		{
			if(strcmp(lfirst(lc_elem), str_elem) == 0)
				break;		
		}
		if(PointerIsValid(lc_elem))
		{
			pfree(str_elem);
		}
		else
		{
			appendBinaryStringInfo(&infosendmsg, hbasendmsg.data, hbasendmsg.len);
			list_elem = lappend(list_elem, str_elem);
		}						
	}
	if(list_length(list_elem) > 0)
	{	
		/*step3: send msg to the specified coordinator to update datanode master's pg_hba.conf*/	
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF
								,node_path
								,&infosendmsg
								,hostoid
								,&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			appendStringInfo(&err_msg->description,"add hba %s execute in agent failure\n",coord_name);
			appendStringInfo(&err_msg->description, "%s\n",getAgentCmdRst.description.data);
			err_msg->ret = false;
		}
		else   
		{
			/*step4: execute pgxc_ctl reload to the specified host, 
			it's to take effect for the new value in the pg_hba.conf  */
			mgr_reload_conf(hostoid, node_path);	
			/*step5: add a new tuple to hba table */		
			foreach(lc, list_elem)
			{
				str_elem = lfirst(lc);
				*hba_max_id = *hba_max_id + 1;
				datum[Anum_mgr_hba_id - 1] = Int32GetDatum(*hba_max_id);
				datum[Anum_mgr_hba_nodename - 1] = NameGetDatum(coord_name);
				datum[Anum_mgr_hba_value - 1] = CStringGetTextDatum(str_elem);
				tuple_insert_table_hba(datum, isnull);
			}
		}
	}
	/*step7: Release an allocated chunk*/
	foreach(lc, list_elem)
	{
		pfree(lfirst(lc));
	}
	list_free(list_elem);
	pfree(hbainfomsg.data);
	pfree(infosendmsg.data);
	pfree(hbasendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

static uint32 get_hba_max_id(void)
{
	Relation rel;
	HeapTuple tuple;
	HeapScanDesc rel_scan;
	Form_mgr_hba mgr_hba;
	uint32 max_id = 0;
	uint32 temp_num = 0;
	rel = heap_open(HbaRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		temp_num = mgr_hba->row_id;
		if(max_id < temp_num)
			max_id = temp_num;
	}
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	if(max_id >= INVALID_ID)
		ereport(ERROR, (errmsg("the row_id of hba has been used up")));
	
	return max_id;
}

static Oid tuple_insert_table_hba(Datum *values, bool *isnull)
{
	Relation rel;
	HeapTuple newtuple;
	Oid hba_oid;
	Assert(values && isnull);
	/* now, we can insert record */
	rel = heap_open(HbaRelationId, RowExclusiveLock);	

	newtuple = heap_form_tuple(RelationGetDescr(rel), values, isnull);
	hba_oid = simple_heap_insert(rel, newtuple);
	CatalogUpdateIndexes(rel, newtuple);
	heap_close(rel, RowExclusiveLock);
	heap_freetuple(newtuple);
	return hba_oid;
}

static TupleDesc get_tuple_desc_for_hba(void)
{
    if(common_command_tuple_desc == NULL)
    {
        MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
        TupleDesc volatile desc = NULL;
        PG_TRY();
        {
            desc = CreateTemplateTupleDesc(HBA_RESULT_COLUMN, false);
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
    Datum datums[HBA_RESULT_COLUMN];
    bool nulls[HBA_RESULT_COLUMN];
    TupleDesc desc;
    AssertArg(node_name && values);
	memset(nulls, false, HBA_RESULT_COLUMN);
    desc = get_tuple_desc_for_hba();

    AssertArg(desc && desc->natts == HBA_RESULT_COLUMN
        && desc->attrs[0]->atttypid == INT4OID
        && desc->attrs[1]->atttypid == NAMEOID
        && desc->attrs[2]->atttypid == TEXTOID);
	datums[0] = Int32GetDatum(row_id);   
    datums[1] = NameGetDatum(node_name);
    datums[2] = CStringGetTextDatum(values);
    return heap_form_tuple(desc, datums, nulls);
}

Datum mgr_drop_hba(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst err_msg;
	OperateHbaType handle_type = HANDLE_NO;
	HeapTuple tup_result;
	uint32 row_id = INVALID_ID;
	List *args_list;
	char *coord_name;
	char *num_str;
	bool is_exist = true;
	err_msg.ret = true;
	/*step 1: parase args,and get nodename, row_id;if nodename equal '*', delete all*/
	if(PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("args is null")));
	}
	#ifdef ADB
		args_list = get_fcinfo_namelist("", 0, fcinfo, NULL);
	#else
		args_list = get_fcinfo_namelist("", 0, fcinfo);
	#endif

	coord_name = linitial(args_list);
	if(args_list->length == 1)
	{
		if(strcmp(coord_name,"*") == 0)
			handle_type = HBA_ALL;
		else
			handle_type = HBA_NODENAME_ALL;
	}
	else
	{
		num_str = lsecond(args_list);
		row_id = pg_atoi(num_str,sizeof(uint32),0);
		handle_type = HBA_NODENAME_ID;
	}
	/*step 2: check the tuple is exist*/
	if(HBA_NODENAME_ALL == handle_type)
	{
		is_exist = check_hba_tuple_exist(INVALID_ID, coord_name, NULL);
		if(false == is_exist)
		{
			 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					 ,errmsg("hba items on \"%s\" does not exist", coord_name))); 
		}
	}	
	else if(HBA_NODENAME_ID == handle_type)
	{
		is_exist = check_hba_tuple_exist(row_id, coord_name, NULL);
		if(false == is_exist)
		{
			 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					 ,errmsg("hba items on \"%s\" with row_id %d does not exist", coord_name, row_id))); 
		}
	}
			
	/*step 3: operating drop table hba  according to the handle_type*/
		/*send drop msg to the agent to delete content of pg_hba.conf */
	initStringInfo(&(err_msg.description));
	switch(handle_type)
	{
		case HBA_ALL: drop_hba_all(&err_msg);
			break;
		case HBA_NODENAME_ALL: drop_hba_nodename_all(coord_name, &err_msg);
			break;
		case HBA_NODENAME_ID: drop_hba_nodename_id(coord_name, row_id, &err_msg);
			break;
		default:ereport(ERROR, (errmsg("operating drop table hba")));
			break;		
	}
	
	/*step 4: show the state of operating drop hba commands */
	tup_result = tuple_form_table_hba(INVALID_ID
									,(Name)coord_name
									,true == err_msg.ret ? "success" : err_msg.description.data);
	return HeapTupleGetDatum(tup_result);
}

static void drop_hba_all(GetAgentCmdRst *err_msg)
{	
	Relation rel;
	HeapTuple tuple;
	HeapScanDesc  rel_scan;
	Form_mgr_hba mgr_hba;	
	char *coord_name;
	uint32 row_id;
	/*Traverse all the coordinator in the node table*/
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		row_id = mgr_hba->row_id;
		coord_name = NameStr(mgr_hba->nodename);
		drop_hba_nodename_id(coord_name, row_id, err_msg);
/*		if(!err_msg->ret)
			break;
*/
	}
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}
/*
	delete one row form hba talbe base on nodename,
	to delete the nodename which you want.
*/	
static void drop_hba_nodename_all(char *coord_name, GetAgentCmdRst *err_msg)
{	
	Relation rel;
	ScanKeyData key[1];
	HeapTuple tuple;
	HeapScanDesc  rel_scan;
	Form_mgr_hba mgr_hba;	
	uint32 row_id;
	Assert(coord_name);
	
	ScanKeyInit(&key[0]
				,Anum_mgr_hba_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(coord_name));
	
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		row_id = mgr_hba->row_id;
		drop_hba_nodename_id(coord_name, row_id, err_msg);
/*		if(!err_msg->ret)
			break;
*/
	}
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}
/*
	delete one row form hba talbe base on nodename and row_id,
	if success return true,else ruturn false;
*/	
static void drop_hba_nodename_id(char *coord_name, uint32 row_id, GetAgentCmdRst *err_msg)
{	
	Relation rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Form_mgr_hba mgr_hba;
	char *host_address;
	char * coord_path;
	char * hba_values = NULL;
	Datum datumPath;
	Oid hostoid;
	char *str_elem, *str_remain;
	StringInfoData infosendmsg;
	bool isNull = false;
	HbaType conntype = HBA_TYPE_EMPTY;
	ScanKeyData key[2];
	GetAgentCmdRst getAgentCmdRst;
	Assert(coord_name);
	
	
	/*step1: get hba value to delete*/
	ScanKeyInit(&key[0]
				,Anum_mgr_hba_id
				,BTEqualStrategyNumber
				,F_INT4EQ
				,Int32GetDatum(row_id));
	ScanKeyInit(&key[1]
				,Anum_mgr_hba_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(coord_name));
	rel = heap_open(HbaRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);		
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		hba_values = TextDatumGetCString(&(mgr_hba->hbavalue));
		break;
	}
	if(!PointerIsValid(hba_values))
	{
		appendStringInfo(&err_msg->description, "nodename \"%s\" with row_id %d does not exist", coord_name, row_id);
		err_msg->ret = false;
		heap_endscan(rel_scan);
		heap_close(rel, AccessShareLock);
		return;
	}	
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);	
	
	/*step2: check the nodename is exist in the mgr_node and check it has been initialized*/
	rel = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(rel, coord_name, CNDN_TYPE_COORDINATOR_MASTER);
	if(!(HeapTupleIsValid(tuple)))
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not exist", coord_name)));
	}
    mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);	
	if(false == mgr_node->nodeinited)	
	{
		/*delete tuple of hba table*/
		delete_table_hba(row_id, coord_name);
		heap_close(rel, AccessShareLock);
		heap_freetuple(tuple);
		return;
	}
	hostoid = mgr_node->nodehost;
	host_address = get_hostaddress_from_hostoid(hostoid);
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
	if(isNull)
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("coordinator\"%s\" does not exist nodepath", coord_name)));
	}
	coord_path = TextDatumGetCString(datumPath);
	heap_close(rel, AccessShareLock);
	heap_freetuple(tuple);
	
	/*step3: parase the discovered hba values and encode it*/
	initStringInfo(&infosendmsg);
	str_elem = strtok_r(hba_values, SPACE, &str_remain);
	conntype = get_connect_type(str_elem);
	if(conntype == HBA_TYPE_EMPTY)
	{
		ereport(ERROR, (errmsg("%s", "the type of hba is wrong")));
	}
	appendStringInfo(&infosendmsg, "%c%c", conntype, '\0');	
	while(str_elem != NULL)
	{
		str_elem = strtok_r(NULL, SPACE, &str_remain);
		if(PointerIsValid(str_elem))
		{
			appendStringInfo(&infosendmsg,"%s%c", str_elem, '\0');	
		}						
	}	
	/*step4: send msg to the specified coordinator to update datanode master's pg_hba.conf*/
	initStringInfo(&getAgentCmdRst.description);
	mgr_send_conf_parameters(AGT_CMD_CNDN_DELETE_PGHBACONF,
							coord_path,
							&infosendmsg,
							hostoid,
							&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		appendStringInfo(&err_msg->description,"drop hba %s where row_id = %d failure\n",coord_name, row_id);
		appendStringInfo(&err_msg->description,"%s\n",getAgentCmdRst.description.data);
		err_msg->ret = false;
	}
   	else	
	{
		/*step4: execute pgxc_ctl reload to the specified host */
		mgr_reload_conf(hostoid, coord_path);	
		/*step5:delete tuple of hba table*/
		delete_table_hba(row_id, coord_name);
	}

	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

/*
if row_id is 0 or 0x7fffffff ,the param will be as invalid,
we will to check coord_name,
coord_name = "*" we delete all the hba table;
else if coord_name is valid ,we will delete the table according to it;
*/
static void delete_table_hba(uint32 row_id, char *coord_name)
{
	Relation rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ScanKeyData scankey[2];	
	Assert(coord_name);
	
	ScanKeyInit(&scankey[0]
				,Anum_mgr_hba_id
				,BTEqualStrategyNumber
				,F_INT4EQ
				,Int32GetDatum(row_id));
	ScanKeyInit(&scankey[1]
				,Anum_mgr_hba_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(coord_name));
			
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	if(strcmp(coord_name, "*") == 0)
		rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	else /*if(PointerIsValid(coord_name))*/
	{
		if(IDIsValid(row_id))
			rel_scan = heap_beginscan(rel, SnapshotNow, 2, scankey);
		else
			rel_scan = heap_beginscan(rel, SnapshotNow, 1, &scankey[1]);
	}	
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		simple_heap_delete(rel, &tuple->t_self);
		CatalogUpdateIndexes(rel, tuple);
	}
	
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}

void mgr_clean_hba_table(void)
{
	Relation rel;
	HeapTuple tuple;
	HeapScanDesc  rel_scan;
	/*Traverse all the coordinator in the node table*/
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		simple_heap_delete(rel, &tuple->t_self);
		CatalogUpdateIndexes(rel, tuple);
	}
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}

/*
	1,if row_id == INVALID_ID ,row_id won't be query;
	2,if coord_name == "*",it will check all the table,
	else only select the nodname is coord_name in the table;
	3,if values is NULL,it won't be query
*/
static bool check_hba_tuple_exist(uint32 row_id, char *coord_name, char *values)
{
	Relation rel;		
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ScanKeyData scankey[2];	
	Form_mgr_hba mgr_hba;
	bool ret = false;
	bool is_check_value = true;
	char *hba_values;
	Assert(coord_name);
	
	if(values == NULL)
		is_check_value = false;
	ScanKeyInit(&scankey[0]
			,Anum_mgr_hba_id
			,BTEqualStrategyNumber
			,F_INT4EQ
			,Int32GetDatum(row_id));
	ScanKeyInit(&scankey[1]
			,Anum_mgr_hba_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(coord_name));
		
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	if(strcmp(coord_name, "*") == 0)
		rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	else
	{
		if(IDIsValid(row_id))
			rel_scan = heap_beginscan(rel, SnapshotNow, 2, scankey);
		else
			rel_scan = heap_beginscan(rel, SnapshotNow, 1, &scankey[1]);
	}	
	
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		if(true == is_check_value)
		{
			hba_values = TextDatumGetCString(&(mgr_hba->hbavalue));			
			if(strcmp(hba_values, values) == 0)
			{
				ret = true;
				break;
			}		
		}
		else
		{
			ret = true;
			break;
		}
	}
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
	return ret;
}

static HbaType get_connect_type(char *str_type)
{
	HbaType conntype = HBA_TYPE_EMPTY;
	char *str_lwr ;
	Assert(str_type);
	str_lwr = str_tolower(str_type, strlen(str_type), DEFAULT_COLLATION_OID);
	if(strcmp(str_lwr, "local") == 0)
		conntype = HBA_TYPE_LOCAL;
	else if(strcmp(str_lwr, "host") == 0)
		conntype = HBA_TYPE_HOST;
	else if(strcmp(str_lwr, "hostssl") == 0)
		conntype = HBA_TYPE_HOSTSSL;
	else if(strcmp(str_lwr, "hostnossl") == 0)
		conntype = HBA_TYPE_HOSTNOSSL;
	return conntype;
}

static bool IDIsValid(uint32 row_id)
{
	if(0 < row_id && row_id < INVALID_ID)
		return true;
	else
		return false;
}

static bool check_pghbainfo_vaild(StringInfo hba_info, StringInfo err_msg, bool record_err_msg)
{
	bool is_valid = true;
	HbaInfo *newinfo;
	char *str_elem;
	StringInfoData str_hbainfo;
	int count_elem;
	int ipaddr;
	MemoryContext pgconf_context;
	MemoryContext oldcontext;
	pgconf_context = AllocSetContextCreate(CurrentMemoryContext,
									"pghbaadd",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(pgconf_context);
	newinfo = palloc(sizeof(HbaInfo));
	initStringInfo(&str_hbainfo);
	count_elem = 0;
	str_elem = hba_info->data;
	while(str_elem)
	{
		count_elem = count_elem + 1;
		hba_info->cursor = hba_info->cursor + strlen(str_elem) + 1;
		str_elem = &(hba_info->data[hba_info->cursor]);
		if(hba_info->cursor >= hba_info->len)
			break;
	}
	if(count_elem != HBA_ELEM_NUM)
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"number of hba item fields is incorrect\"\n");
		goto func_end;
	}
	hba_info->cursor = 0;
	
	/*type*/
	newinfo->type = hba_info->data[hba_info->cursor];
	hba_info->cursor = hba_info->cursor + sizeof(char) + 1;
	/*database*/
	newinfo->database = &(hba_info->data[hba_info->cursor]);
	hba_info->cursor = hba_info->cursor + strlen(newinfo->database) + 1;
	/*user*/
	newinfo->user = &(hba_info->data[hba_info->cursor]);
	hba_info->cursor = hba_info->cursor + strlen(newinfo->user) + 1;
	/*ip*/
	newinfo->addr = &(hba_info->data[hba_info->cursor]);
	hba_info->cursor = hba_info->cursor + strlen(newinfo->addr) + 1;
	/*mask*/
	newinfo->addr_mark = atoi(&(hba_info->data[hba_info->cursor]));
	hba_info->cursor = hba_info->cursor + strlen(&(hba_info->data[hba_info->cursor])) + 1;
	/*method*/
	newinfo->auth_method = &(hba_info->data[hba_info->cursor]);
	if(HBA_TYPE_EMPTY == newinfo->type)
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the conntype is invalid\"\n");
		goto func_end;
	}
	if(is_digit(newinfo->database[0]))
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the database name cannot start with a number\"\n");
		goto func_end;
	}
	if(newinfo->database[0] == '\'')
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the database name cannot start with a single quotes\"\n");
		goto func_end;
	}
	
	if(is_digit(newinfo->user[0]))
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the user name cannot start with a number\"\n");
		goto func_end;
	}	
	if(newinfo->user[0] == '\'')
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the user name cannot start with a single quotes\"\n");
		goto func_end;
	}
	
	if(inet_pton(AF_INET, newinfo->addr, &ipaddr) == 0)
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the address is invaild\"\n");
		goto func_end;
	}
	if(newinfo->addr_mark < 0 || newinfo->addr_mark > 32)
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the ip mask is invaild\"\n");
		goto func_end;
	}
	
	if(!is_auth_method_valid(newinfo->auth_method))
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the auth_method is invaild\"\n");
		goto func_end;
	}
func_end:
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(pgconf_context);
	return is_valid;
}

static bool is_auth_method_valid(char *method)
{	
	char *AuthMethod[11] = {"trust", "reject", "md5", "password", "gss", "sspi"
						  , "krb5", "ident", "ldap", "cert", "pam"};
	int i = 0;	
	char *method_lwr;
	Assert(method);
	method_lwr = str_tolower(method, strlen(method), DEFAULT_COLLATION_OID);
	for(i = 0; i < 11; ++i)
	{
		if(strcmp(method_lwr, AuthMethod[i]) == 0)
			return true;
	}
	return false;
}



