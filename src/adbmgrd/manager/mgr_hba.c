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
#define SPACE_INTERVAL				1					
#define HBA_RESULT_COLUMN  			2
#define HBA_ELEM_NUM				6
#define is_digit(c) ((unsigned)(c) - '0' <= 9)
 
typedef enum OperateHbaType
{
	HANDLE_NO = 0,	/*don't do any handle for drop table hba*/
	HBA_ALL=1,		/*drop table hba all*/
	HBA_NODENAME_ALL,/*drop table hba all base on nodename*/
	HBA_ALL_VALUE,
	HBA_NODENAME_VALUE  /*drop table hba all base on hbavalue and nodename*/
}OperateHbaType;

typedef struct TableInfo
{
	Relation rel;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}TableInfo;

#if (Natts_mgr_hba != 2)
#error "need change hba code"
#endif
static TupleDesc common_command_tuple_desc = NULL;

/*--------------------------------------------------------------------*/
void mgr_clean_hba_table(char *coord_name, char *values);
/*--------------------------------------------------------------------*/
	
extern void mgr_reload_conf(Oid hostoid, char *nodepath);
//extern HbaInfo* parse_hba_file(const char *filename);
/*--------------------------------------------------------------------*/
static void mgr_add_hba_all(List *args_list, GetAgentCmdRst *err_msg);
static void mgr_add_hba_one(char *coord_name, List *args_list, GetAgentCmdRst *err_msg, bool record_err_msg);
static void drop_hba_all(GetAgentCmdRst *err_msg);
static void drop_hba_nodename_all(char *coord_name, GetAgentCmdRst *err_msg);
static void drop_hba_all_value(List *args_list, GetAgentCmdRst *err_msg);
static void drop_hba_nodename_valuelist(char *coord_name, List *args_list, GetAgentCmdRst *err_msg);
static void drop_hba_nodename_value(char *coord_name, char *hbavalue, GetAgentCmdRst *err_msg);
static Oid tuple_insert_table_hba(Datum *values, bool *isnull);
static HbaType get_connect_type(char *str_type);
static TupleDesc get_tuple_desc_for_hba(void);
static HeapTuple tuple_form_table_hba(const Name node_name, const char * values);
static void delete_table_hba(char *coord_name, char *values);
static bool check_hba_tuple_exist(char *coord_name, char *values);
static bool check_pghbainfo_vaild(StringInfo hba_info, StringInfo err_msg, bool record_err_msg);
static bool is_auth_method_valid(char *method);
static List *parse_hba_list(List *args_list);
static void encode_hba_str(char *hbavalue, StringInfo infosendmsg);
static bool is_digit_str(char *s_digit);
/*--------------------------------------------------------------------*/

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
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
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
		tup_result = tuple_form_table_hba(&(mgr_hba->nodename), TextDatumGetCString(&(mgr_hba->hbavalue)));
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}
}
Datum mgr_add_hba(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst err_msg;
	OperateHbaType handle_type = HANDLE_NO;
	HeapTuple tup_result;
	List *args_list = NIL;
	char *coord_name;
	err_msg.ret = true;
	initStringInfo(&err_msg.description);
	/*step 1: parase args,and get nodename,hba values;
	if nodename equal '*', add to all the coordinator*/
	if(PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("args is null")));
	}
	args_list = get_fcinfo_namelist("", 0, fcinfo);
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
	if(HBA_ALL == handle_type)
	{
		mgr_add_hba_all(args_list, &err_msg);
	}	
	else if(HBA_NODENAME_ALL == handle_type)
	{
		mgr_add_hba_one(coord_name, args_list, &err_msg, true);
	}	
	/*step 3: show the state of operating drop hba commands */
	tup_result = tuple_form_table_hba((Name)coord_name
									,true == err_msg.ret ? "success" : err_msg.description.data);
	
	pfree(err_msg.description.data);		
	return HeapTupleGetDatum(tup_result);
}

static void mgr_add_hba_all(List *args_list, GetAgentCmdRst *err_msg)
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
		mgr_add_hba_one(coord_name, args_list, err_msg, record_err_msg);
		record_err_msg = false;
	}
	
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}

static void mgr_add_hba_one(char *coord_name, List *args_list, GetAgentCmdRst *err_msg, bool record_err_msg)
{
	Relation rel;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
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
				appendStringInfoSpaces(&hbainfomsg, SPACE_INTERVAL);
				appendStringInfo(&hbainfomsg, "%s",str_elem);
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
		is_exist = check_hba_tuple_exist(coord_name, hbainfomsg.data);
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
			appendStringInfo(&err_msg->description,"hba info sync error\n");
//			appendStringInfo(&err_msg->description, "%s\n",getAgentCmdRst.description.data);
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
		MemoryContext volatile old_context = MemoryContextSwitchTo(	TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(HBA_RESULT_COLUMN, false);
	
			TupleDescInitEntry(desc, (AttrNumber) 1, "nodename",
							NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "values",
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

static HeapTuple tuple_form_table_hba(const Name node_name, const char * values)
{
	Datum datums[HBA_RESULT_COLUMN];
	bool nulls[HBA_RESULT_COLUMN];
	TupleDesc desc;
	AssertArg(node_name && values);
	memset(nulls, false, HBA_RESULT_COLUMN);
	desc = get_tuple_desc_for_hba();

	AssertArg(desc && desc->natts == HBA_RESULT_COLUMN
		&& desc->attrs[0]->atttypid == NAMEOID
		&& desc->attrs[1]->atttypid == TEXTOID);
	datums[0] = NameGetDatum(node_name);
	datums[1] = CStringGetTextDatum(values);
	return heap_form_tuple(desc, datums, nulls);
}

Datum mgr_drop_hba(PG_FUNCTION_ARGS)
{	
	GetAgentCmdRst err_msg;
	OperateHbaType handle_type = HANDLE_NO;
	HeapTuple tup_result;
	List *args_list = NIL;
	char *coord_name;
	err_msg.ret = true;
	initStringInfo(&err_msg.description);
	/*step 1: parase args,and get nodename,hba values;
	if nodename equal '*', add to all the coordinator*/
	if(PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("args is null")));
	}
	args_list = get_fcinfo_namelist("", 0, fcinfo);
	
	if(args_list->length > 0)
	{
		coord_name = llast(args_list);
	}
	else
	{
		ereport(ERROR, (errmsg("args is not enough")));
	}
	if(args_list->length == 1)
	{	
		if(strcmp(coord_name,"*") == 0) 
			handle_type = HBA_ALL;    /*delete all the hba table*/     
		else
			handle_type = HBA_NODENAME_ALL;/*delete the hba table that is own to nodename*/  
	}else if(args_list->length > 1)
	{
		if(strcmp(coord_name,"*") == 0) 
			handle_type = HBA_ALL_VALUE;         /*delete the hba table which the conext is values*/  
		else
			handle_type = HBA_NODENAME_VALUE;    /*delete the hba table which the conext is values and own to nodename*/  
	}	
	
	args_list = list_delete(args_list, llast(args_list)); /*remove nodename from list*/
	
	/*step 2: operating drop table hba  according to the handle_type*/
		/*send drop msg to the agent to delete content of pg_hba.conf */
	initStringInfo(&(err_msg.description));
	switch(handle_type)
	{
		case HBA_ALL: drop_hba_all(&err_msg);
			break;
		case HBA_NODENAME_ALL: drop_hba_nodename_all(coord_name, &err_msg);
			break;
		case HBA_ALL_VALUE: drop_hba_all_value(args_list, &err_msg);
			break;
		case HBA_NODENAME_VALUE: drop_hba_nodename_valuelist(coord_name, args_list, &err_msg);
			break;
		default:ereport(ERROR, (errmsg("operating drop table hba")));
			break;		
	}
	/*step 3: show the state of operating drop hba commands */
	tup_result = tuple_form_table_hba((Name)coord_name
									,true == err_msg.ret ? "success" : err_msg.description.data);
	
	pfree(err_msg.description.data);		
	return HeapTupleGetDatum(tup_result);
}

static void drop_hba_all(GetAgentCmdRst *err_msg)
{	
	Relation rel;
	HeapTuple tuple;
	HeapScanDesc  rel_scan;
	Form_mgr_hba mgr_hba;	
	char *coord_name;
	char *hbavalue;
	/*Traverse all the coordinator in the node table*/
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);		
		coord_name = NameStr(mgr_hba->nodename);
		hbavalue = TextDatumGetCString(&(mgr_hba->hbavalue));
		drop_hba_nodename_value(coord_name, hbavalue, err_msg);
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
	char *hbavalue;
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
		hbavalue = TextDatumGetCString(&(mgr_hba->hbavalue));
		drop_hba_nodename_value(coord_name, hbavalue, err_msg);
	}
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}

static void drop_hba_all_value(List *args_list, GetAgentCmdRst *err_msg)
{
	Relation rel;
	HeapTuple tuple;
	HeapScanDesc  rel_scan;
	Form_mgr_hba mgr_hba;	
	char *coord_name;
	bool is_exist = false;
	bool tuple_exist = false;
	List *name_list = NIL;
	ListCell *lc_value, *lc_name;
	/*Traverse all the coordinator in the node table*/
	rel = heap_open(HbaRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_hba = (Form_mgr_hba)GETSTRUCT(tuple);
		Assert(mgr_hba);
		tuple_exist = true;
		coord_name = NameStr(mgr_hba->nodename);
		is_exist = false;
		foreach(lc_name, name_list)
		{	
			if(strcmp(lfirst(lc_name), coord_name) == 0)
			{
				is_exist = true;
				break;
			}
		}
		if(false == is_exist)
			name_list = lappend(name_list, coord_name);	
	}
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	if(false == tuple_exist)
	{
		appendStringInfo(&(err_msg->description), "%s", "Error: the hba talbe is empty.\n");
		err_msg->ret = false;
		return;
	}
	foreach(lc_name, name_list)
	{
		coord_name = lfirst(lc_name);
		foreach(lc_value, args_list)
		{
			drop_hba_nodename_value(coord_name, lfirst(lc_value), err_msg);
		}	
	}
}
static void drop_hba_nodename_valuelist(char *coord_name, List *args_list, GetAgentCmdRst *err_msg)
{
	ListCell *lc;
	List *hba_list = NIL;
	char *hbavalue;
	hba_list = parse_hba_list(args_list);
	foreach(lc, hba_list)
	{
		hbavalue = lfirst(lc);
		drop_hba_nodename_value(coord_name, hbavalue, err_msg);
	}	
	/* Release an allocated chunk*/
	foreach(lc, hba_list)
	{
		pfree(lfirst(lc));
	}
	list_free(hba_list);
}
/*
	delete one row form hba talbe base on nodename and value,
	if success return true,else ruturn false;
*/	
static void drop_hba_nodename_value(char *coord_name, char *hbavalue, GetAgentCmdRst *err_msg)
{	
	Relation rel;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * node_path;
	Datum datumPath;
	Oid hostoid;
	StringInfoData infosendmsg;
	bool isNull = false;
	GetAgentCmdRst getAgentCmdRst;

	Assert(coord_name);
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
		delete_table_hba(coord_name, hbavalue);
		return;
	}
	hostoid = mgr_node->nodehost;
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
	encode_hba_str(hbavalue, &infosendmsg);
	if(check_hba_tuple_exist(coord_name, hbavalue) == false)
	{
		pfree(infosendmsg.data);
		appendStringInfo(&err_msg->description, "coordinator \"%s\" with \"%s\" does not exist\n",coord_name, hbavalue);
		err_msg->ret = false;
		return;
	}
	if(check_pghbainfo_vaild(&infosendmsg, &(err_msg->description), true) == false)
	{
		err_msg->ret = false;
		appendStringInfo(&err_msg->description, "in the item \"%s\".\n",hbavalue);
		pfree(infosendmsg.data);
		return;
	}	
	/*step3: send msg to the specified coordinator to update datanode master's pg_hba.conf*/	
	mgr_send_conf_parameters(AGT_CMD_CNDN_DELETE_PGHBACONF
							,node_path
							,&infosendmsg
							,hostoid
							,&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		appendStringInfo(&err_msg->description,"drop hba %s failure\n",coord_name);
		appendStringInfo(&err_msg->description, "%s\n",getAgentCmdRst.description.data);
		err_msg->ret = false;
	}
	else   
	{
		/*step4: execute pgxc_ctl reload to the specified host */
		mgr_reload_conf(hostoid, node_path);	
		/*step5:delete tuple of hba table*/
		delete_table_hba(coord_name, hbavalue);
	}	
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

/*
if coord_name = "*"
	we delete all the hba table;
else if coord_name is valid and the values is NULL
	we will delete the table where coord_name is equal to coordinator name
else 
	we will delete the table by coord_name and values
*/
static void delete_table_hba(char *coord_name, char *values)
{
	Relation rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ScanKeyData scankey[2];	
	bool is_check_value = false;
	Assert(coord_name);
	
	ScanKeyInit(&scankey[0]
			,Anum_mgr_hba_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(coord_name));
	if(values != NULL)
	{
		is_check_value = true;
		ScanKeyInit(&scankey[1]
				,Anum_mgr_hba_value
				,BTEqualStrategyNumber
				,F_TEXTEQ
				,CStringGetTextDatum(values));
	}
	rel = heap_open(HbaRelationId, RowExclusiveLock);
	if(strcmp(coord_name, "*") == 0)
	{
		if(true == is_check_value)
			rel_scan = heap_beginscan(rel, SnapshotNow, 1, &scankey[1]);
		else
			rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	}			
	else
	{
		if(true == is_check_value)
			rel_scan = heap_beginscan(rel, SnapshotNow, 2, scankey);
		else
			rel_scan = heap_beginscan(rel, SnapshotNow, 1, &scankey[0]);
			
	}
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		simple_heap_delete(rel, &tuple->t_self);
		CatalogUpdateIndexes(rel, tuple);
	}
	
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
}

void mgr_clean_hba_table(char *coord_name, char *values)
{
	delete_table_hba(coord_name, values);
}

/*
	2,if coord_name == "*",it will check all the table,
	else only select the nodname is coord_name in the table;
	3,if values is NULL,it won't be query
*/
static bool check_hba_tuple_exist(char *coord_name, char *values)
{
	Relation rel;		
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ScanKeyData scankey[2];
	bool ret = false;
	bool is_check_value = true;
	Assert(coord_name);
	
	if(values == NULL)
		is_check_value = false;
	ScanKeyInit(&scankey[0]
			,Anum_mgr_hba_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(coord_name));
	ScanKeyInit(&scankey[1]
			,Anum_mgr_hba_value
			,BTEqualStrategyNumber
			,F_TEXTEQ
			,CStringGetTextDatum(values));
	rel = heap_open(HbaRelationId, AccessShareLock);
	if(strcmp(coord_name, "*") == 0)
	{
		if(true == is_check_value)
			rel_scan = heap_beginscan(rel, SnapshotNow, 1, &scankey[1]);
		else
			rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	}			
	else
	{
		if(true == is_check_value)
			rel_scan = heap_beginscan(rel, SnapshotNow, 2, scankey);
		else
			rel_scan = heap_beginscan(rel, SnapshotNow, 1, &scankey[0]);
			
	}	
	
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		ret = true;
	}
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	return ret;
}

static HbaType get_connect_type(char *str_type)
{
	HbaType conntype = HBA_TYPE_EMPTY;
	char *str_lwr ;
//	Assert(str_type);
	if(!PointerIsValid(str_type))
		return conntype;
	str_lwr = str_tolower(str_type, strlen(str_type), DEFAULT_COLLATION_OID);
	if(strcmp(str_lwr, "host") == 0)
		conntype = HBA_TYPE_HOST;
	else if(strcmp(str_lwr, "hostssl") == 0)
		conntype = HBA_TYPE_HOSTSSL;
	else if(strcmp(str_lwr, "hostnossl") == 0)
		conntype = HBA_TYPE_HOSTNOSSL;
	
	return conntype;
}

static bool check_pghbainfo_vaild(StringInfo hba_info, StringInfo err_msg, bool record_err_msg)
{
	bool is_valid = true;
	HbaInfo *newinfo;
	char *str_elem;
	StringInfoData str_hbainfo;
	int count_elem;
	int ipaddr;
	int str_len = 0;
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
	while(1)
	{
		count_elem = count_elem + 1;
		str_len = strlen(str_elem);
		if(0 == str_len)
			str_len = 1;
		hba_info->cursor = hba_info->cursor + str_len + 1;
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
	if(is_digit_str(&(hba_info->data[hba_info->cursor])))
		newinfo->addr_mark = atoi(&(hba_info->data[hba_info->cursor]));
	else
	{
		is_valid = false;
		if(true == record_err_msg)
			appendStringInfoString(err_msg, "Error:\"the mask is invalid\"\n");
		goto func_end;
	}
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
	char *AuthMethod[13] = {"trust", "reject", "md5", "password", "gss", "sspi"
						  , "krb5", "ident", "ldap", "cert", "pam", "peer", "radius"};
	int i = 0;	
	char *method_lwr;
	Assert(method);
	method_lwr = str_tolower(method, strlen(method), DEFAULT_COLLATION_OID);
	for(i = 0; i < 13; ++i)
	{
		if(strcmp(method_lwr, AuthMethod[i]) == 0)
			return true;
	}
	return false;
}


static List *parse_hba_list(List *args_list)
{
	StringInfoData hbainfomsg;
	StringInfoData split_str;
	List *list_elem = NIL;
	ListCell *lc, *lc_elem;
	char *str, *str_elem;
	char *str_remain;
	bool is_exist = true;
	/*step2: parser the hba values and check whether it's valid*/
	initStringInfo(&split_str);
	initStringInfo(&hbainfomsg);/*add to hba table*/
	foreach(lc, args_list)
	{
		resetStringInfo(&hbainfomsg);
		resetStringInfo(&split_str);
		str = lfirst(lc);
		appendStringInfo(&split_str, "%s", str);
		str_elem = strtok_r(split_str.data, SPACE, &str_remain);	
		appendStringInfo(&hbainfomsg, "%s", str_elem);
		while(str_elem != NULL)
		{
			str_elem = strtok_r(NULL, SPACE, &str_remain);
			if(PointerIsValid(str_elem))
			{
				appendStringInfoSpaces(&hbainfomsg, SPACE_INTERVAL);
				appendStringInfo(&hbainfomsg, "%s",str_elem);
			}						
		}	
		/*add to list and remove the same*/
		str_elem = palloc0(hbainfomsg.len + 1);
		memcpy(str_elem, hbainfomsg.data, hbainfomsg.len);
		is_exist = false;
		foreach(lc_elem, list_elem)
		{
			str = lfirst(lc_elem);
			if(strcmp(str, str_elem) == 0)
			{
				is_exist = true;
				break;
			}	
		}
		if(false == is_exist)
		{
			list_elem = lappend(list_elem, str_elem); 
		}
		else
		{
			pfree(str_elem);
		}
	}
	pfree(hbainfomsg.data);
	pfree(split_str.data);
	return list_elem;
}

static void encode_hba_str(char *hbavalue, StringInfo infosendmsg)
{
	StringInfoData split_str;
	HbaType conntype = HBA_TYPE_EMPTY;
	char *str_remain;
	char *str_elem;
	initStringInfo(&split_str);
	appendStringInfo(&split_str, "%s", hbavalue);
	str_elem = strtok_r(split_str.data, SPACE, &str_remain);
	conntype = get_connect_type(str_elem);	
	appendStringInfo(infosendmsg, "%c%c", conntype, '\0');
	while(str_elem != NULL)
	{
		str_elem = strtok_r(NULL, SPACE, &str_remain);
		if(PointerIsValid(str_elem))
		{
			appendStringInfo(infosendmsg,"%s%c", str_elem, '\0');
		}						
	}	
}

static bool is_digit_str(char *s_digit)
{
	int length = 0;
	int i = 0;
	length = strlen(s_digit);
	for(i = 0; i < length; ++i)
	{
		if(!isdigit(s_digit[i]))
			break;
	}
	if(i < length)
		return false;
	else
		return true;		
}