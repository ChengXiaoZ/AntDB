/*
 * commands of node
 */

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_gtm.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/pg_type.h"
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
#include "fmgr.h"
#include "utils/lsyscache.h"

#define DEFAULT_DB "postgres"
#define shutdown_s  "smart"
#define shutdown_f  "fast"
#define shutdown_i  "immediate"
#define takeplaparm_n  "none"
#define MAX_PREPARED_TRANSACTIONS_DEFAULT	100

static TupleDesc common_command_tuple_desc = NULL;
static TupleDesc get_common_command_tuple_desc_for_monitor(void);
static HeapTuple build_common_command_tuple_for_monitor(const Name name
                                                        ,char type             
                                                        ,bool status               
                                                        ,const char *description);
static void mgr_append_init_dnmaster(const char cmdtype
									,Relation noderel
									,HeapTuple	tuple
									,GetAgentCmdRst *getAgentCmdRst);


#if (Natts_mgr_node != 9)
#error "need change code"
#endif

typedef struct InitNodeInfo
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitNodeInfo;

void mgr_add_node(MGRAddNode *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	HeapTuple mastertuple;
	HeapTuple newtuple;
	HeapTuple checktuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	char *nodestring;
	NameData name;
	NameData mastername;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	bool primary;
	ObjectAddress myself;
	ObjectAddress host;
	Oid cndn_oid;
	char nodetype;			/*coordinator or datanode master/slave*/
	Assert(node && node->name);
	
	/*get node type*/
	if(node->is_coordinator && node->is_master)
	{
		nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		nodestring = "coordinator master";
	}
	else if(node->is_coordinator && (!node->is_master))
	{
		nodetype = CNDN_TYPE_COORDINATOR_SLAVE;
		Assert(node->mastername);
		namestrcpy(&mastername, node->mastername);
		nodestring = "coordinator slave";
	}
	else if((!node->is_coordinator) && node->is_master)
	{
		nodetype = CNDN_TYPE_DATANODE_MASTER;
		nodestring = "datanode master";
	}
	else if((!node->is_coordinator) && (!node->is_master))
	{
		nodetype = CNDN_TYPE_DATANODE_SLAVE;
		Assert(node->mastername);
		namestrcpy(&mastername, node->mastername);
		nodestring = "datanode slave";
	}
	else
	{
		/*never come here*/
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("node is not recognized")
			, errhint("option type is coordinator or datanode master/slave")));
	}
	
	
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	namestrcpy(&name, node->name);
	/* check exists */
	checktuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
	if (HeapTupleIsValid(checktuple))
	{
		heap_freetuple(checktuple);
		if(node->if_not_exists)
		{
			heap_close(rel, RowExclusiveLock);
			return;
		}
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("%s \"%s\" already exists", nodestring, NameStr(name))));
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_node_nodename-1] = NameGetDatum(&name);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));

		if(strcmp(def->defname, "host") == 0)
		{
			NameData hostname;
			if(got[Anum_mgr_node_nodehost-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			/* find host oid */
			namestrcpy(&hostname, defGetString(def));
			tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&hostname));
			if(!HeapTupleIsValid(tuple))
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("host \"%s\" not exists", defGetString(def))));
			}
			datum[Anum_mgr_node_nodehost-1] = ObjectIdGetDatum(HeapTupleGetOid(tuple));
			got[Anum_mgr_node_nodehost-1] = true;
			ReleaseSysCache(tuple);
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_node_nodeport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_node_nodeport-1] = Int32GetDatum(port);
			got[Anum_mgr_node_nodeport-1] = true;
		}else if(strcmp(def->defname, "path") == 0)
		{
			if(got[Anum_mgr_node_nodepath-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_node_nodepath-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_node_nodepath-1] = true;
		}else if(strcmp(def->defname, "primary") == 0)
		{
			if(got[Anum_mgr_node_nodeprimary-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			primary = defGetBoolean(def);
			datum[Anum_mgr_node_nodeprimary-1] = BoolGetDatum(primary);
			got[Anum_mgr_node_nodeprimary-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is host, port, path and primary")));
		}
		
	}

	/* if not give, set to default */
	if(got[Anum_mgr_node_nodetype-1] == false)
	{
		datum[Anum_mgr_node_nodetype-1] = CharGetDatum(nodetype);
	}
	if(got[Anum_mgr_node_nodepath-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"path\" must give")));
	}
	if(got[Anum_mgr_node_nodehost-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"host\" must give")));
	}
	if(got[Anum_mgr_node_nodeport-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"port\" must give")));
	}
	if(got[Anum_mgr_node_nodeprimary-1] == false)
	{
		datum[Anum_mgr_node_nodeprimary-1] = BoolGetDatum(false);
	}
	if(got[Anum_mgr_node_nodemasternameOid-1] == false)
	{
		if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_COORDINATOR_MASTER == nodetype)
			datum[Anum_mgr_node_nodemasternameOid-1] = UInt32GetDatum(0);
		else
		{
			mastertuple = mgr_get_tuple_node_from_name_type(rel, NameStr(mastername), CNDN_TYPE_DATANODE_MASTER);
			if(!HeapTupleIsValid(mastertuple))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("datanode master \"%s\" not exists", NameStr(mastername))));
			}
			datum[Anum_mgr_node_nodemasternameOid-1] = ObjectIdGetDatum(HeapTupleGetOid(mastertuple));
			heap_freetuple(mastertuple);
			
		}
	}
	/*the node is not in cluster until config all*/
	datum[Anum_mgr_node_nodeincluster-1] = BoolGetDatum(false);
	/* now, node is not initialized*/
	datum[Anum_mgr_node_nodeinited-1] = BoolGetDatum(false);

	/* now, we can insert record */
	newtuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	cndn_oid = simple_heap_insert(rel, newtuple);
	CatalogUpdateIndexes(rel, newtuple);
	heap_freetuple(newtuple);

	/*close relation */
	heap_close(rel, RowExclusiveLock);

	/* Record dependencies on host */
	myself.classId = NodeRelationId;
	myself.objectId = cndn_oid;
	myself.objectSubId = 0;

	host.classId = HostRelationId;
	host.objectId = DatumGetObjectId(datum[Anum_mgr_node_nodehost-1]);
	host.objectSubId = 0;
	recordDependencyOn(&myself, &host, DEPENDENCY_NORMAL);
}

void mgr_alter_node(MGRAlterNode *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple oldtuple;
	HeapTuple	new_tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	char *nodestring;
	NameData name;
	NameData mastername;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	bool primary;
	HeapTuple searchHostTuple;	
	TupleDesc cndn_dsc;
	NameData hostname;
	char nodetype = '\0';			/*coordinator master/slave or datanode master/slave*/
	Form_mgr_node mgr_node;
	Assert(node && node->name);
	
	/*get node type*/
	if(node->is_coordinator && node->is_master)
	{
		nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		nodestring = "coordinator master";
	}
	else if(node->is_coordinator && (!node->is_master))
	{
		nodetype = CNDN_TYPE_COORDINATOR_SLAVE;
		Assert(node->mastername);
		namestrcpy(&mastername, node->mastername);
		nodestring = "coordinator slave";
	}
	else if((!node->is_coordinator) && node->is_master)
	{
		nodetype = CNDN_TYPE_DATANODE_MASTER;
		nodestring = "datanode master";
	}
	else if((!node->is_coordinator) && (!node->is_master))
	{
		nodetype = CNDN_TYPE_DATANODE_SLAVE;
		Assert(node->mastername);
		namestrcpy(&mastername, node->mastername);
		nodestring = "datanode slave";
	}
	
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	cndn_dsc = RelationGetDescr(rel);
	namestrcpy(&name, node->name);
	/* check exists */
	oldtuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
	if(!(HeapTupleIsValid(oldtuple)))
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" doesnot exists", nodestring, NameStr(name))));
	}
	/*check this tuple initd or not, if it has inited and in cluster, cannot be alter*/
	mgr_node = (Form_mgr_node)GETSTRUCT(oldtuple);
	Assert(mgr_node);
	if(mgr_node->nodeincluster)
	{
		heap_freetuple(oldtuple);
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be changed", nodestring, NameStr(name))));
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_node_nodename-1] = NameGetDatum(&name);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "host") == 0)
		{		
			if(got[Anum_mgr_node_nodehost-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			/* find host oid */
			namestrcpy(&hostname, defGetString(def));
			searchHostTuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&hostname));
			if(!HeapTupleIsValid(searchHostTuple))
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("host \"%s\" not exists", defGetString(def))));
			}
			datum[Anum_mgr_node_nodehost-1] = ObjectIdGetDatum(HeapTupleGetOid(searchHostTuple));
			got[Anum_mgr_node_nodehost-1] = true;
			ReleaseSysCache(searchHostTuple);
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_node_nodeport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_node_nodeport-1] = Int32GetDatum(port);
			got[Anum_mgr_node_nodeport-1] = true;
		}else if(strcmp(def->defname, "path") == 0)
		{
			if(got[Anum_mgr_node_nodepath-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_node_nodepath-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_node_nodepath-1] = true;
		}else if(strcmp(def->defname, "primary") == 0)
		{
			if(got[Anum_mgr_node_nodeprimary-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			primary = defGetBoolean(def);
			datum[Anum_mgr_node_nodeprimary-1] = BoolGetDatum(primary);
			got[Anum_mgr_node_nodeprimary-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is host, port, path and primary")));
		}
		datum[Anum_mgr_node_nodetype-1] = CharGetDatum(nodetype);
	}
	new_tuple = heap_modify_tuple(oldtuple, cndn_dsc, datum,isnull, got);
	simple_heap_update(rel, &oldtuple->t_self, new_tuple);
	CatalogUpdateIndexes(rel, new_tuple);
	heap_freetuple(oldtuple);
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
}

void mgr_drop_node(MGRDropNode *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	MemoryContext context, old_context;
	NameData name;
	char nodetype;
	char *nodestring;
	Form_mgr_node mgr_node;

	/*get node type*/
	if(node->is_coordinator && node->is_master)
	{
		nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		nodestring = "coordinator master";
	}
	else if(node->is_coordinator && (!node->is_master))
	{
		nodetype = CNDN_TYPE_COORDINATOR_SLAVE;
		nodestring = "coordinator slave";
	}
	else if((!node->is_coordinator) && node->is_master)
	{
		nodetype = CNDN_TYPE_DATANODE_MASTER;
		nodestring = "datanode master";
	}
	else if((!node->is_coordinator) && (!node->is_master))
	{
		nodetype = CNDN_TYPE_DATANODE_SLAVE;
		nodestring = "datanode slave";
	}

	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP NODE"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, node->hosts)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
		if(!HeapTupleIsValid(tuple))
		{
			if(node->if_exists)
				continue;
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("%s \"%s\" dose not exists", nodestring, NameStr(name))));
		}
		/*check this tuple initd or not, if it has inited and in cluster, cannot be dropped*/
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodeincluster)
		{
			heap_freetuple(tuple);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be dropped", nodestring, NameStr(name))));
		}
		/* todo chech used by other */
		heap_freetuple(tuple);
	}

	/* now we can delete node(s) */
	foreach(lc, node->hosts)
	{
		val = lfirst(lc);
		Assert(val  && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
		if(HeapTupleIsValid(tuple))
		{
			simple_heap_delete(rel, &(tuple->t_self));
			heap_freetuple(tuple);
		}
	}

	heap_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
}


/*
* init coordinator master dn1,dn2...
* init coordinator master all
*/
Datum 
mgr_init_cn_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CNDN_CNDN_INIT, fcinfo, takeplaparm_n);
}

/*
* init datanode master dn1,dn2...
* init datanode master all
*/
Datum 
mgr_init_dn_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_CNDN_CNDN_INIT, fcinfo, takeplaparm_n);
}

/*
* execute init datanode slave, send infomation to agent to init it 
*/
Datum 
mgr_init_dn_slave(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tuple
			,aimtuple
			,mastertuple;
	Relation rel_node;
	HeapScanDesc scan;
	Form_mgr_node mgr_node;
	bool gettuple = false;
	ScanKeyData key[2];
	uint32 masterport;
	Oid masterhostOid;
	char *masterhostaddress;
	char *mastername;
	FuncCallContext *funcctx;
	const char *nodename = PG_GETARG_CSTRING(0);
	Assert(nodename);
	
	/*output the exec result: col1 hostname,col2 SUCCESS(t/f),col3 description*/	
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		/* get the master name */
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodename
			,BTEqualStrategyNumber, F_NAMEEQ
			,NameGetDatum(nodename));
		ScanKeyInit(&key[1]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
		rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(strcmp(NameStr(mgr_node->nodename), nodename) == 0)
			{
				/*check the nodetype*/
				if(mgr_node->nodetype != CNDN_TYPE_DATANODE_SLAVE)
				{
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
						, errmsg("the type is not datanode slave, use \"list node\" to check")));
				}
				aimtuple = tuple;
				gettuple = true;
				break;
			}
			
		}
		if(gettuple == false)
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
				, errmsg("the need infomation does not in system table of node, use \"list node\" to check")));
		}
		/*get the master port, master host address*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				, errmsg("node master dosen't exist")));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
		Assert(mastertuple);
		masterport = mgr_node->nodeport;
		masterhostOid = mgr_node->nodehost;
		mastername = NameStr(mgr_node->nodename);
		masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
		ReleaseSysCache(mastertuple);
		
		mgr_init_dn_slave_get_result(AGT_CMD_CNDN_SLAVE_INIT, &getAgentCmdRst, rel_node, aimtuple, masterhostaddress,masterport, mastername);
		tuple = build_common_command_tuple(
			&(getAgentCmdRst.nodename)
			, getAgentCmdRst.ret
			, getAgentCmdRst.description.data);
		pfree(getAgentCmdRst.description.data);
		pfree(masterhostaddress);
		heap_endscan(scan);
		heap_close(rel_node, RowExclusiveLock);
		MemoryContextSwitchTo(oldcontext);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	/* we have only one datanode slave for given name, returnd at first time */
	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	SRF_RETURN_DONE(funcctx);
}

/*
*	execute init datanode slave all, send infomation to agent to init 
*/
Datum 
mgr_init_dn_slave_all(PG_FUNCTION_ARGS)
{
	InitNodeInfo *info;
	GetAgentCmdRst getAgentCmdRst;
	Form_mgr_node mgr_node;
	FuncCallContext *funcctx;
	HeapTuple tuple
			,tup_result,
			mastertuple;
	ScanKeyData key[1];
	uint32 masterport;
	Oid masterhostOid;
	char *masterhostaddress;
	char *mastername;
	
	/*output the exec result: col1 hostname,col2 SUCCESS(t/f),col3 description*/	
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
		/* save info */
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;
	Assert(info);
	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}
	/*get nodename*/
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	/*get the master port, master host address*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
	if(!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("node master dosen't exist")));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mastertuple);
	masterport = mgr_node->nodeport;
	masterhostOid = mgr_node->nodehost;
	mastername = NameStr(mgr_node->nodename);
	masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
	ReleaseSysCache(mastertuple);
	mgr_init_dn_slave_get_result(AGT_CMD_CNDN_SLAVE_INIT, &getAgentCmdRst, info->rel_node, tuple, masterhostaddress, masterport, mastername);
	pfree(masterhostaddress);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

void mgr_init_dn_slave_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *masterhostaddress, uint32 masterport, char *mastername)
{
	/*get datanode slave path from adbmgr.node*/
	Datum datumPath;
	char *cndnPath;
	char *cndnnametmp;
	char nodetype;
	Oid hostOid,
		masteroid,
		tupleOid;
	StringInfoData buf;
	StringInfoData infosendmsg,
				strinfocoordport;
	ManagerAgent *ma;
	bool initdone = false;
	bool isNull = false;
	Form_mgr_node mgr_node;
	int cndnport;
	bool ismasterrunning = false;
	Datum DatumStartDnMaster,
		DatumStopDnMaster;

	initStringInfo(&(getAgentCmdRst->description));
	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	/*get column values from aimtuple*/	
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	cndnnametmp = NameStr(mgr_node->nodename);
	hostOid = mgr_node->nodehost;
	/*get the port*/
	cndnport = mgr_node->nodeport;
	/*get master oid*/
	masteroid = mgr_node->nodemasternameoid; 
	/*get nodetype*/
	nodetype = mgr_node->nodetype;
	/*get tuple oid*/
	tupleOid = HeapTupleGetOid(aimtuple);
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), cndnnametmp);
	/*check node init or not*/
	if (mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "the node \"%s\" has inited", cndnnametmp);
		getAgentCmdRst->ret = false;
		return;
	}
	/*get cndnPath from aimtuple*/
	datumPath = heap_getattr(aimtuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column cndnpath is null")));
	}
	/*if datanode master doesnot running, first make it running*/
	initStringInfo(&strinfocoordport);
	appendStringInfo(&strinfocoordport, "%d", masterport);
	ismasterrunning = pingNode(masterhostaddress, strinfocoordport.data);
	pfree(strinfocoordport.data);	
	if(ismasterrunning != 0)
	{
		/*it need start datanode master*/
		DatumStartDnMaster = DirectFunctionCall1(mgr_start_one_dn_master, CStringGetDatum(mastername));
		if(DatumGetObjectId(DatumStartDnMaster) == InvalidOid)
			elog(ERROR, "start datanode master \"%s\" fail", mastername);
	}
	cndnPath = TextDatumGetCString(datumPath);		
	appendStringInfo(&infosendmsg, " -p %u", masterport);
	appendStringInfo(&infosendmsg, " -h %s", masterhostaddress);
	appendStringInfo(&infosendmsg, " -D %s", cndnPath);
	appendStringInfo(&infosendmsg, " -x");
	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		return;
	}

	/*send path*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	initdone = mgr_recv_msg(ma, getAgentCmdRst);
	ma_close(ma);
	/*stop datanode master if we start it*/
	if(ismasterrunning != 0)
	{
		/*it need start datanode master*/
		DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_dn_master, CStringGetDatum(mastername));
		if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
			elog(ERROR, "stop datanode master \"%s\" fail", mastername);
	}
	/*update node system table's column to set initial is true*/
	if (initdone)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, cndnnametmp, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(cndnnametmp, masteroid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	pfree(infosendmsg.data);
}
/*
* get the datanode/coordinator name list
*/
List *
start_cn_master_internal(const char *sepstr, int argidx,
	FunctionCallInfo fcinfo
#ifdef ADB
	, void (*check_value_func_ptr)(char*)
#endif
	)
{
	StringInfoData str;
	bool first_arg = true;
	int i;
	char *nodename;
	List *nodenamelist =NIL;
	
	/* Normal case without explicit VARIADIC marker */
	initStringInfo(&str);

	for (i = argidx; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			Datum value = PG_GETARG_DATUM(i);
			Oid valtype;
			Oid typOutput;
			bool typIsVarlena;
			/* add separator if appropriate */
			if (first_arg)
				first_arg = false;
			else
				appendStringInfoString(&str, sepstr);

			/* call the appropriate type output function*/
			valtype = get_fn_expr_argtype(fcinfo->flinfo, i);
			if (!OidIsValid(valtype))
				elog(ERROR, "could not determine data type of mgr_start_cn_master() input");
			getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
			nodename = OidOutputFunctionCall(typOutput, value);
			nodenamelist = lappend(nodenamelist, nodename);
		}
	}

	pfree(str.data);
	return nodenamelist;
}

/*
* start coordinator master dn1,dn2...
* start coordinator master all
*/
Datum mgr_start_cn_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_START, fcinfo, takeplaparm_n);
}

/*
* start datanode master dn1,dn2...
* start datanode master all
*/
Datum mgr_start_dn_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_START, fcinfo, takeplaparm_n);	
}

/*
* start datanode master dn1
*/
Datum mgr_start_one_dn_master(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result
			,aimtuple;
	char *nodename;
	InitNodeInfo *info;

	nodename = PG_GETARG_CSTRING(0);
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, nodename, CNDN_TYPE_DATANODE_MASTER);
	if (!HeapTupleIsValid(aimtuple))
	{
		elog(ERROR, "cache lookup failed for datanode master %s", nodename);
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(AGT_CMD_DN_START, &getAgentCmdRst, info->rel_node, aimtuple, takeplaparm_n);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	heap_close(info->rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);
	pfree(info);
	return HeapTupleGetDatum(tup_result);	
}

/*
* start datanode slave dn1,dn2...
* start datanode slave all
*/
Datum mgr_start_dn_slave(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_START, fcinfo, takeplaparm_n);	
}

void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *shutdown_mode)
{
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_node_dnmaster;
	Oid hostOid;
	Datum datumPath;
	Datum DatumStopDnMaster;
	StringInfoData buf;
	StringInfoData infosendmsg;
	ManagerAgent *ma;
	bool isNull = false,
		execok = false,
		getrefresh;
	char *hostaddress,
		*cndnPath,
		*cmdmode,
		*zmode,
		*cndnname,
		*dnmastername;
	char nodetype;
	int32 cndnport;
	Oid nodemasternameoid,
			tupleOid;
	bool getmaster = false;
	bool isprimary = false;
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	char *masterpath;
	HeapTuple tuple;
	HeapTuple mastertuple;

	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	/*get column values from aimtuple*/	
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	hostOid = mgr_node->nodehost;
	/*get host address*/
	hostaddress = get_hostname_from_hostoid(hostOid);
	Assert(hostaddress);
	/*get nodename*/
	cndnname = NameStr(mgr_node->nodename);
	isprimary = mgr_node->nodeprimary;
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), cndnname);
	/*check node init or not*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype && mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "the node \"%s\" has inited", cndnname);
		getAgentCmdRst->ret = false;
		return;
	}
	if(AGT_CMD_CNDN_CNDN_INIT != cmdtype && !mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "the node \"%s\" has not inited", cndnname);
		getAgentCmdRst->ret = false;
		return;
	}
	/*get the port*/
	cndnport = mgr_node->nodeport;
	/*get node master oid*/
	nodemasternameoid = mgr_node->nodemasternameoid;
	/*get node type*/
	nodetype = mgr_node->nodetype;
	/*get tuple oid*/
	tupleOid = HeapTupleGetOid(aimtuple);
	datumPath = heap_getattr(aimtuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column cndnpath is null")));
	}
	/*get cndnPath from aimtuple*/
	cndnPath = TextDatumGetCString(datumPath);	
	switch(cmdtype)
	{
		case AGT_CMD_CN_START:
			cmdmode = "start";
			zmode = "coordinator";
			break;
		case AGT_CMD_CN_STOP:
			cmdmode = "stop";
			zmode = "coordinator";
			break;
		case AGT_CMD_DN_START:
			cmdmode = "start";
			zmode = "datanode";
			break;
		case AGT_CMD_DN_RESTART:
			cmdmode = "restart";
			zmode = "datanode";
			break;
		case AGT_CMD_DN_STOP:
			cmdmode = "stop";
			zmode = "datanode";
			break;
		case AGT_CMD_DN_FAILOVER:
			cmdmode = "promote";
			zmode = "datanode";
			break;
		default:
			/*never come here*/
			cmdmode = "node";
			zmode = "node";
			break;
	}
	/*init coordinator/datanode*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype)
	{
		appendStringInfo(&infosendmsg, " -D %s", cndnPath);
		appendStringInfo(&infosendmsg, " --nodename %s --locale=C", cndnname);
	}
	/*stop coordinator/datanode*/
	else if(AGT_CMD_CN_STOP == cmdtype || AGT_CMD_DN_STOP == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c -l %s/logfile", zmode, shutdown_mode, cndnPath);
	}
	else
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -o -i -w -c -l %s/logfile", zmode, cndnPath);
	}

	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		return;
	}

	/*send cmd*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	execok = mgr_recv_msg(ma, getAgentCmdRst);
	Assert(execok == getAgentCmdRst->ret);
	ma_close(ma);
	/*update node system table's column to set initial is true when cmd is init*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype && execok)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, cndnname, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	/*failover execute success*/
	if(AGT_CMD_DN_FAILOVER == cmdtype && execok)
	{
		/*0. restart datanode*/
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, getAgentCmdRst, noderel, aimtuple, takeplaparm_n);
		if(!getAgentCmdRst->ret)
		{
			elog(LOG, "pg_ctl restart datanode slave fail");
			return;
		}
		/*1.refresh pgxc_node systable */
		resetStringInfo(&(getAgentCmdRst->description));
		getrefresh = mgr_refresh_pgxc_node_tbl(cndnname, cndnport, hostaddress, isprimary, nodemasternameoid, getAgentCmdRst);
		if(!getrefresh)
		{
			/*rename recovery.done to recovery.conf*/
			resetStringInfo(&(getAgentCmdRst->description));
			mgr_rename_recovery_to_conf(AGT_CMD_CNDN_RENAME_RECOVERCONF, hostOid,  cndnPath, getAgentCmdRst);
			resetStringInfo(&(getAgentCmdRst->description));
			appendStringInfoString(&(getAgentCmdRst->description),"ERROR: refresh system table of pgxc_node on coordinators fail, please check pgxc_node on every coordinator");
			if(!getAgentCmdRst->ret)
				appendStringInfo(&(getAgentCmdRst->description),"\n rename datanode slave %s/recovery.done to %s/recovery.conf fail", cndnPath, cndnPath);
			getAgentCmdRst->ret = getrefresh;
			return;
		}
		/*2.stop the old datanode master*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("datanode master \"%s\" dosen't exist", cndnname)));
		}
		/*get master name*/
		mgr_node_dnmaster = (Form_mgr_node)GETSTRUCT(mastertuple);
		Assert(mgr_node_dnmaster);
		dnmastername = NameStr(mgr_node_dnmaster->nodename);
		DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_dn_master, CStringGetDatum(dnmastername));
		if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
			elog(ERROR, "stop datanode master \"%s\" fail", dnmastername);
		/*3.delete old master record in node systbl*/
		simple_heap_delete(noderel, &mastertuple->t_self);
		CatalogUpdateIndexes(noderel, mastertuple);
		ReleaseSysCache(mastertuple);
		/*4.change slave type to master type*/
		mgr_node->nodeinited = true;
		mgr_node->nodetype = CNDN_TYPE_DATANODE_MASTER;
		mgr_node->nodemasternameoid = 0;
		heap_inplace_update(noderel, aimtuple);
		/*5. refresh postgresql.conf*/
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	/*if stop datanode slave, we should refresh its datanode master's 
	*postgresql.conf:synchronous_standby_names = '' 
	*/
	if((AGT_CMD_DN_STOP == cmdtype ||  AGT_CMD_DN_START == cmdtype) && nodetype == CNDN_TYPE_DATANODE_SLAVE && execok)
	{	
		/*get datanode master:path, hostoid*/
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
		rel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(nodemasternameoid == HeapTupleGetOid(tuple))
			{
				hostOid = mgr_node->nodehost;
				datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
				if(isNull)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
						, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
						, errmsg("column cndnpath is null")));
				}
				masterpath = TextDatumGetCString(datumPath);
				getmaster = true;
				break;
			}
		}
		heap_endscan(rel_scan);
		
		if(getmaster)
		{
			resetStringInfo(&(getAgentCmdRst->description));
			resetStringInfo(&infosendmsg);
			if(AGT_CMD_DN_STOP == cmdtype)
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", cndnname, &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, masterpath, &infosendmsg, hostOid, getAgentCmdRst);
		}
	}

	pfree(infosendmsg.data);
	pfree(hostaddress);
}

/*
* stop coordinator master cn1,cn2...
* stop coordinator master all
*/
Datum mgr_stop_cn_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_STOP, fcinfo, shutdown_s);
}

Datum mgr_stop_cn_master_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_STOP, fcinfo, shutdown_f);
}

Datum mgr_stop_cn_master_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_STOP, fcinfo, shutdown_i);
}
/*
* stop datanode master cn1,cn2...
* stop datanode master all
*/
Datum mgr_stop_dn_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_CN_STOP, fcinfo, shutdown_s);
}

Datum mgr_stop_dn_master_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_CN_STOP, fcinfo, shutdown_f);
}

Datum mgr_stop_dn_master_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_CN_STOP, fcinfo, shutdown_i);
}

/*
* stop datanode master dn1
*/
Datum mgr_stop_one_dn_master(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result
			,aimtuple;
	char *nodename;
	InitNodeInfo *info;

	info = palloc(sizeof(*info));	
	nodename = PG_GETARG_CSTRING(0);	
	info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, nodename, CNDN_TYPE_DATANODE_MASTER);
	if (!HeapTupleIsValid(aimtuple))
	{
		elog(ERROR, "cache lookup failed for datanode master %s", nodename);
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(AGT_CMD_DN_STOP, &getAgentCmdRst, info->rel_node, aimtuple, shutdown_i);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	heap_close(info->rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);
	pfree(info);
	return HeapTupleGetDatum(tup_result);	
}

/*
* stop datanode slave dn1,dn2...
* stop datanode slave all
*/
Datum mgr_stop_dn_slave(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_STOP, fcinfo, shutdown_s);
}

Datum mgr_stop_dn_slave_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_STOP, fcinfo, shutdown_f);
}

Datum mgr_stop_dn_slave_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_STOP, fcinfo, shutdown_i);
}

/*
* get the result of start/stop coordinator master/slave, datanode master/slave
*/
Datum mgr_runmode_cndn(char nodetype, char cmdtype, PG_FUNCTION_ARGS, char *shutdown_mode)
{
	List *nodenamelist;
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result
			,tuple
			,aimtuple =NULL;
	FuncCallContext *funcctx;
	ListCell **lcp;
	InitNodeInfo *info;
	char *nodestrname;
	NameData nodenamedata;
	Form_mgr_node mgr_node;
	ScanKeyData key[2];
	
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
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		if(PG_ARGISNULL(0)) /* no argument, start all */
		{
			/*add all the type of node name to list*/
			ScanKeyInit(&key[0],
				Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
			info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
			while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
			{
					mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
					Assert(mgr_node);
					nodestrname = NameStr(mgr_node->nodename);
					nodenamelist = lappend(nodenamelist, nodestrname);
			}
			heap_endscan(info->rel_scan);
		}
		else
		{
			#ifdef ADB
				nodenamelist = start_cn_master_internal("", 0, fcinfo, NULL);
			#else
				nodenamelist = start_cn_master_internal("", 0, fcinfo);
			#endif
		}
		*(info->lcp) = list_head(nodenamelist);
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();	
	info = funcctx->user_fctx;
	Assert(info);
	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		SRF_RETURN_DONE(funcctx);
	}
	nodestrname = (char *) lfirst(*lcp);
	*lcp = lnext(*lcp);
	if(namestrcpy(&nodenamedata, nodestrname) != 0)
	{
		elog(ERROR, "namestrcpy %s fail", nodestrname);
	}
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, NameStr(nodenamedata), nodetype);
	if (!HeapTupleIsValid(aimtuple))
	{
		elog(ERROR, "cache lookup failed for %s", nodestrname);
	}
	/*check the type is given type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	if(nodetype != mgr_node->nodetype)
	{
		heap_freetuple(aimtuple);
		elog(ERROR, "the type of  %s is not right, use \"list node\" to check", nodestrname);
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, info->rel_node, aimtuple, shutdown_mode);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * MONITOR ALL
 */
Datum mgr_monitor_all(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
    HeapTuple tup;
    HeapTuple tup_result;
    Form_mgr_node mgr_node;
    StringInfoData port;
    char *host_addr;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        info = palloc(sizeof(*info));
        info->rel_node = heap_open(NodeRelationId, AccessShareLock);
        info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 0, NULL);
        info->lcp =NULL;
			
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

    tup = heap_getnext(info->rel_scan, ForwardScanDirection);
    if(tup == NULL)
    {
        /* end of row */
        heap_endscan(info->rel_scan);
        heap_close(info->rel_node, AccessShareLock);
        pfree(info);
        SRF_RETURN_DONE(funcctx);
    }

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * MONITOR COORDINATOR ALL
 */
Datum mgr_monitor_coord_all(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
    HeapTuple tup;
    HeapTuple tup_result;
    Form_mgr_node mgr_node;
    ScanKeyData  key[1];
    StringInfoData port;
    char *host_addr;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        info = palloc(sizeof(*info));
        info->rel_node = heap_open(NodeRelationId, AccessShareLock);
   
        ScanKeyInit(&key[0]
                    ,Anum_mgr_node_nodetype
                    ,BTEqualStrategyNumber
                    ,F_CHAREQ
                    ,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
        info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
        info->lcp =NULL;
			
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

    tup = heap_getnext(info->rel_scan, ForwardScanDirection);
    if(tup == NULL)
    {
        /* end of row */
        heap_endscan(info->rel_scan);
        heap_close(info->rel_node, AccessShareLock);
        pfree(info);
        SRF_RETURN_DONE(funcctx);
    }

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
 * MONITOR COORDINATOR coord1 coord2 ...
 */
Datum mgr_monitor_coord_namelist(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
	ListCell **lcp;
	List *nodenamelist=NIL;
    HeapTuple tup, tup_result;
    Form_mgr_node mgr_node;
    StringInfoData port;
    char *host_addr;
	char *coordname;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		#ifdef ADB
			nodenamelist = start_cn_master_internal("", 0, fcinfo, NULL);
		#else
			nodenamelist = start_cn_master_internal("", 0, fcinfo);
		#endif
		
		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		*(info->lcp) = list_head(nodenamelist);
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }
	

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	coordname = (char *)lfirst(*lcp);
	*lcp = lnext(*lcp);
	tup = mgr_get_tuple_node_from_name_type(info->rel_node, coordname, CNDN_TYPE_COORDINATOR_MASTER);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "node name is invalid: %s", coordname);

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);
	
	if (CNDN_TYPE_COORDINATOR_MASTER != mgr_node->nodetype)
		elog(ERROR, "node type is not coordinator: %s", coordname);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
	heap_freetuple(tup);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
 * MONITOR DATANODE MASTER db1 db2 ...
 */
Datum mgr_monitor_dnmaster_namelist(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
	ListCell **lcp;
	List *nodenamelist=NIL;
    HeapTuple tup, tup_result;
    Form_mgr_node mgr_node;
    StringInfoData port;
    char *host_addr;
	char *dnmastername;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		#ifdef ADB
			nodenamelist = start_cn_master_internal("", 0, fcinfo, NULL);
		#else
			nodenamelist = start_cn_master_internal("", 0, fcinfo);
		#endif
		
		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		*(info->lcp) = list_head(nodenamelist);
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }
	

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	dnmastername = (char *)lfirst(*lcp);
	*lcp = lnext(*lcp);
	tup = mgr_get_tuple_node_from_name_type(info->rel_node, dnmastername, CNDN_TYPE_DATANODE_MASTER);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "node name is invalid: %s", dnmastername);

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);
	
	if (CNDN_TYPE_DATANODE_MASTER != mgr_node->nodetype)
		elog(ERROR, "node type is not datanode master: %s", dnmastername);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
	heap_freetuple(tup);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * MONITOR DATANODE SLAVE db1 db2 ...
 */
Datum mgr_monitor_dnslave_namelist(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
	ListCell **lcp;
	List *nodenamelist=NIL;
    HeapTuple tup, tup_result;
    Form_mgr_node mgr_node;
    StringInfoData port;
    char *host_addr;
	char *dnslavename;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		#ifdef ADB
			nodenamelist = start_cn_master_internal("", 0, fcinfo, NULL);
		#else
			nodenamelist = start_cn_master_internal("", 0, fcinfo);
		#endif
		
		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		*(info->lcp) = list_head(nodenamelist);
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }
	

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	dnslavename = (char *)lfirst(*lcp);
	*lcp = lnext(*lcp);
	tup = mgr_get_tuple_node_from_name_type(info->rel_node, dnslavename, CNDN_TYPE_DATANODE_SLAVE);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "node name is invalid: %s", dnslavename);

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);
	
	if (CNDN_TYPE_DATANODE_SLAVE != mgr_node->nodetype)
		elog(ERROR, "node type is not datanode slave: %s", dnslavename);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
	heap_freetuple(tup);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
 * MONITOR DATANODE MASTER ALL
 */
Datum mgr_monitor_dnmaster_all(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
    HeapTuple tup;
    HeapTuple tup_result;
    Form_mgr_node mgr_node;
    ScanKeyData  key[1];
    StringInfoData port;
    char *host_addr;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        info = palloc(sizeof(*info));
        info->rel_node = heap_open(NodeRelationId, AccessShareLock);
   
        ScanKeyInit(&key[0]
                    ,Anum_mgr_node_nodetype
                    ,BTEqualStrategyNumber
                    ,F_CHAREQ
                    ,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
        info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
        info->lcp =NULL;
			
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

    tup = heap_getnext(info->rel_scan, ForwardScanDirection);
    if(tup == NULL)
    {
        /* end of row */
        heap_endscan(info->rel_scan);
        heap_close(info->rel_node, AccessShareLock);
        pfree(info);
        SRF_RETURN_DONE(funcctx);
    }

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
 * MONITOR DATANODE SLAVE ALL
 */
Datum mgr_monitor_dnslave_all(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitNodeInfo *info;
    HeapTuple tup;
    HeapTuple tup_result;
    Form_mgr_node mgr_node;
    ScanKeyData  key[1];
    StringInfoData port;
    char *host_addr;
    int ret;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        info = palloc(sizeof(*info));
        info->rel_node = heap_open(NodeRelationId, AccessShareLock);
   
        ScanKeyInit(&key[0]
                    ,Anum_mgr_node_nodetype
                    ,BTEqualStrategyNumber
                    ,F_CHAREQ
                    ,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
        info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
        info->lcp =NULL;

        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

    tup = heap_getnext(info->rel_scan, ForwardScanDirection);
    if(tup == NULL)
    {
        /* end of row */
        heap_endscan(info->rel_scan);
        heap_close(info->rel_node, AccessShareLock);
        pfree(info);
        SRF_RETURN_DONE(funcctx);
    }

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);

    host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
    initStringInfo(&port);
    appendStringInfo(&port, "%d", mgr_node->nodeport);
    ret = pingNode(host_addr, port.data);

    tup_result = build_common_command_tuple_for_monitor(
                &(mgr_node->nodename)
                ,mgr_node->nodetype
                ,ret == 0 ? true:false
                ,ret == 0 ? "running":"not running"
                );
    pfree(port.data);
	pfree(host_addr);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

static HeapTuple build_common_command_tuple_for_monitor(const Name name
                                                        ,char type
                                                        ,bool status
                                                        ,const char *description)
{
    Datum datums[4];
    bool nulls[4];
    TupleDesc desc;
    AssertArg(name && description);
    desc = get_common_command_tuple_desc_for_monitor();

    AssertArg(desc && desc->natts == 4
        && desc->attrs[0]->atttypid == NAMEOID
        && desc->attrs[1]->atttypid == NAMEOID
        && desc->attrs[2]->atttypid == BOOLOID
        && desc->attrs[3]->atttypid == TEXTOID);

    switch(type)
    {
        case GTM_TYPE_GTM:
                datums[1] = NameGetDatum(pstrdup("gtm"));
                break;
        case GTM_TYPE_STANDBY:
                datums[1] = NameGetDatum(pstrdup("gtm standby"));
                break;
        case CNDN_TYPE_COORDINATOR_MASTER:
                datums[1] = NameGetDatum(pstrdup("coordinator"));
                break;
        case CNDN_TYPE_DATANODE_MASTER:
                datums[1] = NameGetDatum(pstrdup("datanode master"));
                break;
        case CNDN_TYPE_DATANODE_SLAVE:
                datums[1] = NameGetDatum(pstrdup("datanode slave"));
                break;
        default:
                datums[1] = NameGetDatum(pstrdup("unknown type"));
                break;
    }

    datums[0] = NameGetDatum(name);
    datums[2] = BoolGetDatum(status);
    datums[3] = CStringGetTextDatum(description);
    nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
    return heap_form_tuple(desc, datums, nulls);
}

static TupleDesc get_common_command_tuple_desc_for_monitor(void)
{
    if(common_command_tuple_desc == NULL)
    {
        MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
        TupleDesc volatile desc = NULL;
        PG_TRY();
        {
            desc = CreateTemplateTupleDesc(4, false);
            TupleDescInitEntry(desc, (AttrNumber) 1, "nodename",
                               NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 2, "nodetype",
                               NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 3, "status",
                               BOOLOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 4, "description",
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

/*
 * APPEND DATANODE MASTER nodename
 */
Datum mgr_append_dnmaster(PG_FUNCTION_ARGS)
{
    InitNodeInfo *info;
	ScanKeyData key[2];
	char *nodename = NULL;
	HeapTuple	tuple;
	Form_mgr_node mgr_node;
	//Datum datumPath;
	bool isNull = false;
	GetAgentCmdRst getAgentCmdRst;



	
	nodename = PG_GETARG_CSTRING(0);
	Assert(nodename);
	
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(nodename));

	ScanKeyInit(&key[1]
                ,Anum_mgr_node_nodetype
                ,BTEqualStrategyNumber
                ,F_CHAREQ
                ,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 2, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
        heap_endscan(info->rel_scan);
        heap_close(info->rel_node, AccessShareLock);
        pfree(info);
		elog(ERROR, "node type is not datanode master: %s", nodename);
	}
	
    mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
    Assert(mgr_node);
	
	/*get nodepath from tuple*/
	//datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}

	if(mgr_node->nodeinited)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodeinited should be false")));

	}
	
	//char *nodepath = TextDatumGetCString(datumPath);
	//char *nodename = NameStr(mgr_node->nodename);
	//char nodetype = mgr_node->nodetype;
	//char *host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	//int32 nodeport = mgr_node->nodeport;

	
	
	/* step 1: init workdir  */
	mgr_append_init_dnmaster(AGT_CMD_CNDN_CNDN_INIT, info->rel_node, tuple, &getAgentCmdRst);

	/* step 2: update datanode master's postgresql.conf. */
	/* step 3: update datanode master's pg_hba.conf */
	/* step 4: block all the DDL lock */
	/* step 5: dumpall catalog message */
	/* step 6: start the datanode master with restoremode mode, and input all catalog message */
	/* step 7: stop the datanode master with restoremode, and then start it with "datanode" mode */
	/* step 8: create node on all the coordinator */
	/* step 9: release the DDL lock */

	return 0;
}

static void mgr_append_init_dnmaster(const char cmdtype
									,Relation noderel
									,HeapTuple	tuple
									,GetAgentCmdRst *getAgentCmdRst)
{
	StringInfoData  infosendmsg;
	ManagerAgent *ma;
	StringInfoData buf;
	bool execok = false;
	Form_mgr_node mgr_node;
	bool isNull = false;
	Datum datumPath;
	char *nodepath;

	initStringInfo(&infosendmsg);
	
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
    Assert(mgr_node);
	
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodepath = TextDatumGetCString(datumPath);
	
	/*init datanode*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype)
	{
		appendStringInfo(&infosendmsg, " -D %s", nodepath);
		appendStringInfo(&infosendmsg, " --nodename %s", NameStr(mgr_node->nodename));
	}

	/* connection agent */
	ma = ma_connect_hostoid(mgr_node->nodehost);
	if(!ma_isconnected(ma))
	{
		
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		return;
	}

	/*send cmd*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,infosendmsg.data);
	pfree(infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	
	/*check the receive msg*/
	execok = mgr_recv_msg(ma, getAgentCmdRst);
	Assert(execok == getAgentCmdRst->ret);
	ma_close(ma);
	
	/*update node system table's column to set initial is true when cmd is init*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype && execok)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, tuple);
		return;
	}
}

Datum 
mgr_failover_one_dn(PG_FUNCTION_ARGS)
{
	List *nodenamelist;
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result
			,aimtuple
			,tuple;
	FuncCallContext *funcctx;
	ListCell **lcp;
	InitNodeInfo *info;
	char *nodename;
	StringInfoData getnotslavename;
	Form_mgr_node mgr_node;
	ScanKeyData key[1];
	char cmdtype = AGT_CMD_DN_FAILOVER;
	
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
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		if(PG_ARGISNULL(0)) /* no argument, start all */
		{
			/*add all the type of node name to list*/
			ScanKeyInit(&key[0],
				Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
			info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
			while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
			{
					mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
					Assert(mgr_node);
					nodename = NameStr(mgr_node->nodename);
					nodenamelist = lappend(nodenamelist, nodename);
			}
			heap_endscan(info->rel_scan);
		}
		else
		{
			#ifdef ADB
				nodenamelist = start_cn_master_internal("", 0, fcinfo, NULL);
			#else
				nodenamelist = start_cn_master_internal("", 0, fcinfo);
			#endif
		}
		/*check all inputs nodename are datanode slaves*/
		check_dn_slave(nodenamelist, info->rel_node, &getnotslavename);
		if(getnotslavename.maxlen != 0 && getnotslavename.data[0] != '\0')
		{
			/*let the hostname is empty*/
			namestrcpy(&(getAgentCmdRst.nodename), getnotslavename.data);
			getAgentCmdRst.ret = false;
			initStringInfo(&(getAgentCmdRst.description));
			appendStringInfo(&(getAgentCmdRst.description), "ERROR: %s is not a datanode slave", getnotslavename.data);
			tup_result = build_common_command_tuple(
				&(getAgentCmdRst.nodename)
				, getAgentCmdRst.ret
				, getAgentCmdRst.description.data);
			pfree(getAgentCmdRst.description.data);
			pfree(getnotslavename.data);
			heap_close(info->rel_node, RowExclusiveLock);
			return HeapTupleGetDatum(tup_result);	
		}
		*(info->lcp) = list_head(nodenamelist);
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();	
	info = funcctx->user_fctx;
	Assert(info);
	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		SRF_RETURN_DONE(funcctx);
	}
	nodename = (char *) lfirst(*lcp);
	*lcp = lnext(*lcp);
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, nodename, CNDN_TYPE_DATANODE_SLAVE);
	if (!HeapTupleIsValid(aimtuple))
		elog(ERROR, "cache lookup failed for %s", nodename);
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, info->rel_node, aimtuple, takeplaparm_n);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*check all the given nodename are datanode slaves*/
void 
check_dn_slave(List *nodenamelist, Relation rel_node, StringInfo infosendmsg)
{
	char *nodename;
	bool getnode = false;
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	ListCell  *lcp;
	HeapTuple tuple;
	lcp = list_head(nodenamelist);	
	initStringInfo(infosendmsg);
	
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
	while(NULL != lcp )
	{
		nodename = (char *) lfirst(lcp);
		ScanKeyInit(&key[1]
			,Anum_mgr_node_nodename
			,BTEqualStrategyNumber, F_NAMEEQ
			,NameGetDatum(nodename));
		lcp = lnext(lcp);
		getnode = false;
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			getnode = true;
		}
		
		if(false == getnode)
		{
			appendStringInfo(infosendmsg, " %s", nodename);
		}
		heap_endscan(rel_scan);
	}
}

/*
* cndnname is datanode slave's name, cndnmasternameoid is the datanode slave's master's 
*tuple oid. use the cndnname cndnport cndnaddress to add node in pgxc_node, use 
*cndnmasternameoid to delete the slave's master node in pgxc_node
*/
bool mgr_refresh_pgxc_node_tbl(char *cndnname, int32 cndnport, char *cndnaddress, bool isprimary, Oid cndnmasternameoid, GetAgentCmdRst *getAgentCmdRst)
{
	
	int ret;
	char *coordaddress;
	HeapTuple mastertuple,
			tuple;
	StringInfoData infosendmsg,
				strinfocoordport;
	ManagerAgent *ma;
	StringInfoData buf;
	Form_mgr_node mgr_node;
	ScanKeyData key[1];
	Relation rel_node;
	HeapScanDesc rel_scan;
	bool execok;
	int32 cnmasterport;
	int normalcoordnum = 0;
	
	/*check the datanode master is exists in node table*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(cndnmasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_NAME)
			,errmsg("datanode master dosen't exist")));
	}
	ReleaseSysCache(mastertuple);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*send "alter node masternode(host = nodeaddress, port = nodeport, primary = slave_primary)",
	* select pgxc_pool_reload(); to agent
	*/
	initStringInfo(&infosendmsg);
	initStringInfo(&strinfocoordport);
	namestrcpy(&(getAgentCmdRst->nodename), cndnname);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		/*skip the coordinator not in the cluster*/
		if(!mgr_node->nodeincluster)
			continue;
		/*check the coordinator is normal, otherwise skip it*/
		coordaddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
		resetStringInfo(&strinfocoordport);
		appendStringInfo(&strinfocoordport, "%d", mgr_node->nodeport);
		ret = pingNode(coordaddress, strinfocoordport.data);
		pfree(coordaddress);
		/*skip the coordinator which is not normal*/
		if(ret != 0)
		{
			continue;
		}
		normalcoordnum++;
		cnmasterport = mgr_node->nodeport;	
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst->description));
		appendStringInfo(&infosendmsg, " -p %d -d postgres -c \"",  cnmasterport);
		appendStringInfo(&infosendmsg, "alter node %s with(host='%s',port=%d, primary = %s);", cndnname, cndnaddress, cndnport, isprimary != 0 ? "true":"false");
		appendStringInfoString(&infosendmsg, "select pgxc_pool_reload();\"");
		/* connection agent */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		if(!ma_isconnected(ma))
		{
			/* report error message */
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			return false;
		}
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,infosendmsg.data);
		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
			ma_close(ma);
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			return false;
		}
		/*check the receive msg*/
		execok = mgr_recv_msg(ma, getAgentCmdRst);
		Assert(execok == getAgentCmdRst->ret);
		ma_close(ma);
		if(execok != true)
		{
			pfree(infosendmsg.data);
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);	
			return false;
		}
	}
	pfree(infosendmsg.data);
	pfree(strinfocoordport.data);
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	/*check all coordinators are not normal*/
	if(0 == normalcoordnum)
		return false;
	return true;
}


/*
 * last step for init all
 * we need cofigure all nodes information to pgxc_node table
 */
Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info_out, *info_in, *info_dn;
	HeapTuple tuple_out, tuple_in, tuple_dn, tup_result;
	ScanKeyData key_out[1], key_in[1], key_dn[1];
	Form_mgr_node mgr_node_out, mgr_node_in, mgr_node_dn;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData cmdstring;
	StringInfoData buf;
	ManagerAgent *ma;
	bool execok = false;


	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info_out = palloc(sizeof(*info_out));
		info_out->rel_node = heap_open(NodeRelationId, AccessShareLock);
		ScanKeyInit(&key_out[0]
					,Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
		info_out->rel_scan = heap_beginscan(info_out->rel_node, SnapshotNow, 1, key_out);
		info_out->lcp = NULL;

		/* save info */
		funcctx->user_fctx = info_out;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info_out = funcctx->user_fctx;
	Assert(info_out);

	tuple_out = heap_getnext(info_out->rel_scan, ForwardScanDirection);
	if(tuple_out == NULL)
	{
		/* end of row */
		/*mark the tuple in node systbl is in cluster*/
		mgr_mark_node_in_cluster(info_out->rel_node);
		heap_endscan(info_out->rel_scan);
		heap_close(info_out->rel_node, AccessShareLock);
		pfree(info_out);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
	Assert(mgr_node_out);
	initStringInfo(&(getAgentCmdRst.description));
	namestrcpy(&(getAgentCmdRst.nodename), NameStr(mgr_node_out->nodename));
	//getAgentCmdRst.nodename = get_hostname_from_hostoid(mgr_node_out->nodehost);

	initStringInfo(&cmdstring);
	appendStringInfo(&cmdstring, " -h %s -p %u -d %s -U %s -a -c \""
					,get_hostaddress_from_hostoid(mgr_node_out->nodehost)
					,mgr_node_out->nodeport
					,DEFAULT_DB
					,get_hostuser_from_hostoid(mgr_node_out->nodehost));

	info_in = palloc(sizeof(*info_in));
	info_in->rel_node = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key_in[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	info_in->rel_scan = heap_beginscan(info_in->rel_node, SnapshotNow, 1, key_in);
	info_in->lcp =NULL;

	while ((tuple_in = heap_getnext(info_in->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
		Assert(mgr_node_in);

		if (strcmp(NameStr(mgr_node_in->nodename), NameStr(mgr_node_out->nodename)) == 0)
		{
			appendStringInfo(&cmdstring, "ALTER NODE %s WITH (HOST='%s', PORT=%d);"
							,NameStr(mgr_node_in->nodename)
							,get_hostname_from_hostoid(mgr_node_in->nodehost)
							,mgr_node_in->nodeport);
		}
		else
		{
			appendStringInfo(&cmdstring, " CREATE NODE %s WITH (TYPE='coordinator', HOST='%s', PORT=%d);"
							,NameStr(mgr_node_in->nodename)
							,get_hostname_from_hostoid(mgr_node_in->nodehost)
							,mgr_node_in->nodeport);
		}
	}

	heap_endscan(info_in->rel_scan);
	heap_close(info_in->rel_node, AccessShareLock);
	pfree(info_in);

	info_dn = palloc(sizeof(*info_dn));
	info_dn->rel_node = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key_dn[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	info_dn->rel_scan = heap_beginscan(info_dn->rel_node, SnapshotNow, 1, key_dn);
	info_dn->lcp =NULL;

	while ((tuple_dn = heap_getnext(info_dn->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node_dn = (Form_mgr_node)GETSTRUCT(tuple_dn);
		Assert(mgr_node_dn);

		if (mgr_node_dn->nodeprimary)
		{
			if (strcmp(get_hostname_from_hostoid(mgr_node_dn->nodehost), get_hostname_from_hostoid(mgr_node_out->nodehost)) == 0)
			{
				appendStringInfo(&cmdstring, " CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY, PREFERRED);"
								,NameStr(mgr_node_dn->nodename)
								,get_hostname_from_hostoid(mgr_node_dn->nodehost)
								,mgr_node_dn->nodeport);
			}
			else
			{
				appendStringInfo(&cmdstring, " CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY);"
								,NameStr(mgr_node_dn->nodename)
								,get_hostname_from_hostoid(mgr_node_dn->nodehost)
								,mgr_node_dn->nodeport);
			}
		}
		else
		{
			if (strcmp(get_hostname_from_hostoid(mgr_node_dn->nodehost), get_hostname_from_hostoid(mgr_node_out->nodehost)) == 0)
			{
				appendStringInfo(&cmdstring, " CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d,PREFERRED);"
								,NameStr(mgr_node_dn->nodename)
								,get_hostname_from_hostoid(mgr_node_dn->nodehost)
								,mgr_node_dn->nodeport);
			}
			else
			{
				appendStringInfo(&cmdstring, " CREATE NODE %s WITH (TYPE='datanode', HOST='%s', PORT=%d);"
								,NameStr(mgr_node_dn->nodename)
								,get_hostname_from_hostoid(mgr_node_dn->nodehost)
								,mgr_node_dn->nodeport);
			}
		}
	}

	heap_endscan(info_dn->rel_scan);
	heap_close(info_dn->rel_node, AccessShareLock);
	pfree(info_dn);

	appendStringInfo(&cmdstring, " \"");

	/* connection agent */
	ma = ma_connect_hostoid(mgr_node_out->nodehost);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
	ma_sendstring(&buf,cmdstring.data);
	pfree(cmdstring.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	
	tup_result = build_common_command_tuple( &(getAgentCmdRst.nodename)
											,getAgentCmdRst.ret
											,getAgentCmdRst.ret == true ? "success":getAgentCmdRst.description.data);

	ma_close(ma);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));	
}


/*
* send paramters for postgresql.conf which need refresh to agent
* datapath: the absolute path for postgresql.conf
* infosendmsg: which include the paramters and its values, the interval is '\0', the two bytes of string are two '\0'
* hostoid: the hostoid which agent it need send 
* getAgentCmdRst: the execute result in it
*/
void mgr_send_conf_parameters(char filetype, char *datapath, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg
									,buf;
	bool execok;
	
	initStringInfo(&sendstrmsg);
	appendStringInfoString(&sendstrmsg, datapath);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	mgr_append_infostr_infostr(&sendstrmsg, infosendmsg);
	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		return;
	}
	getAgentCmdRst->ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, filetype);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	execok = mgr_recv_msg(ma, getAgentCmdRst);
	Assert(execok == getAgentCmdRst->ret);
	ma_close(ma);	
}

/*
* add key value to infosendmsg, use '\0' to interval, both the key value the type are char*
*/
void mgr_append_pgconf_paras_str_str(char *key, char *value, StringInfo infosendmsg)
{
	Assert(key != '\0' && value != '\0' && &(infosendmsg->data) != '\0');
	appendStringInfoString(infosendmsg, key);
	appendStringInfoCharMacro(infosendmsg, '\0');
	appendStringInfoString(infosendmsg, value);
	appendStringInfoCharMacro(infosendmsg, '\0');
}

/*
* add key value to infosendmsg, use '\0' to interval, the type of key is char*, the type of value is int
*/
void mgr_append_pgconf_paras_str_int(char *key, int value, StringInfo infosendmsg)
{
	Assert(key != '\0' && value != '\0' && &(infosendmsg->data) != '\0');
	appendStringInfoString(infosendmsg, key);
	appendStringInfoCharMacro(infosendmsg, '\0');
	appendStringInfo(infosendmsg, "%d", value);
	appendStringInfoCharMacro(infosendmsg, '\0');
}

/*
* add key value to infosendmsg, use '\0' to interval, both the key value the type are char* and need in quota
*/
void mgr_append_pgconf_paras_str_quotastr(char *key, char *value, StringInfo infosendmsg)
{
	Assert(key != '\0' && value != '\0' && &(infosendmsg->data) != '\0');
	appendStringInfoString(infosendmsg, key);
	appendStringInfoCharMacro(infosendmsg, '\0');
	appendStringInfo(infosendmsg, "'%s'", value);
	appendStringInfoCharMacro(infosendmsg, '\0');
}

/*
* read gtm_port gtm_host from system table:gtm, add gtm_host gtm_port to infosendmsg
* ,use '\0' to interval
*/
void mgr_get_gtm_host_port(StringInfo infosendmsg)
{
	char *gtm_host;
	Relation rel_gtm;
	HeapScanDesc rel_scan;
	Form_mgr_gtm mgr_gtm;
	ScanKeyData key[1];
	HeapTuple tuple;
	bool gettuple = false;
	/*get the gtm_port, gtm_host*/
	ScanKeyInit(&key[0],
		Anum_mgr_gtm_gtmtype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(GTM_TYPE_GTM));
	rel_gtm = heap_open(GtmRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_gtm, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_gtm = (Form_mgr_gtm)GETSTRUCT(tuple);
		Assert(mgr_gtm);
		gtm_host = get_hostaddress_from_hostoid(mgr_gtm->gtmhost);
		gettuple = true;
		break;
	}
	heap_endscan(rel_scan);
	heap_close(rel_gtm, RowExclusiveLock);
	if(!gettuple)
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("can't find the gtm master information in the system table of gtm")));
	}
	mgr_append_pgconf_paras_str_quotastr("agtm_host", gtm_host, infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", mgr_gtm->gtmport, infosendmsg);
	pfree(gtm_host);
}

/*
* add the content of sourceinfostr to infostr, the string in sourceinfostr use '\0' to interval
*/
void mgr_append_infostr_infostr(StringInfo infostr, StringInfo sourceinfostr)
{
	int len = 0;
	char *ptmp = sourceinfostr->data;
	while(*ptmp != '\0')
	{
		appendStringInfoString(infostr, ptmp);
		appendStringInfoCharMacro(infostr, '\0');
		len = strlen(ptmp);
		ptmp = ptmp + len + 1;
	}
}

/*
* the parameters which need refresh for postgresql.conf
*/
void mgr_add_parameters_pgsqlconf(Oid tupleOid, char nodetype, int cndnport, char *nodename, StringInfo infosendparamsg)
{
	char *slavename = NULL;
	if(nodetype == CNDN_TYPE_DATANODE_MASTER)
		slavename = mgr_get_dnmaster_slavename(tupleOid, nodetype);
	/*refresh postgresql.conf of this node*/
	if (nodetype == CNDN_TYPE_DATANODE_MASTER && slavename != NULL)
	{
		mgr_append_pgconf_paras_str_str("synchronous_commit", "on", infosendparamsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", slavename, infosendparamsg);
		pfree(slavename);
		mgr_append_pgconf_paras_str_int("max_wal_senders", MAX_WAL_SENDERS_NUM, infosendparamsg);
		mgr_append_pgconf_paras_str_int("wal_keep_segments", WAL_KEEP_SEGMENTS_NUM, infosendparamsg);
		mgr_append_pgconf_paras_str_str("wal_level", WAL_LEVEL_MODE, infosendparamsg);
	}
	if(nodetype == CNDN_TYPE_DATANODE_SLAVE)
	{
		mgr_append_pgconf_paras_str_str("hot_standby", "on", infosendparamsg);
	}
	mgr_append_pgconf_paras_str_int("port", cndnport, infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("listen_addresses", "*", infosendparamsg);
	mgr_append_pgconf_paras_str_int("max_prepared_transactions", MAX_PREPARED_TRANSACTIONS_DEFAULT, infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_destination", "stderr", infosendparamsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendparamsg);
	mgr_get_gtm_host_port(infosendparamsg);	
}

/*
* the parameters which need refresh for recovery.conf
*/
void mgr_add_parameters_recoveryconf(char *slavename, Oid masteroid, StringInfo infosendparamsg)
{
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	HeapTuple mastertuple,
			tup;
	int32 masterport;
	Oid masterhostOid;
	char *masterhostaddress;
	char *username;
	StringInfoData primary_conninfo_value;
	
	/*get the master port, master host address*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(masteroid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errmsg("node oid \"%u\" not exist", masteroid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mastertuple);
	masterport = mgr_node->nodeport;
	masterhostOid = mgr_node->nodehost;
	masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
	ReleaseSysCache(mastertuple);
	
	/*get host user from system: host*/
	tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(masterhostOid));
	if(!(HeapTupleIsValid(tup)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", masterhostOid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	username = NameStr(mgr_host->hostuser);
	ReleaseSysCache(tup);
	
	/*primary_conninfo*/
	initStringInfo(&primary_conninfo_value);
	appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, masterport, username, slavename);
	mgr_append_pgconf_paras_str_str("standby_mode", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, infosendparamsg);
	pfree(primary_conninfo_value.data);
	pfree(masterhostaddress);
}

/*
* the parameters which need refresh for pg_hba.conf
*/
void mgr_add_parameters_hbaconf(char nodetype, StringInfo infosendhbamsg)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	Oid hostoid;
	char *cnuser;
	char *cnaddress;
	Form_mgr_node mgr_node;	
	HeapTuple tuple;
	
	/*get all coordinator master/datanode master ip*/
	if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		rel_node = heap_open(NodeRelationId, AccessShareLock);
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/*hostoid*/
			hostoid = mgr_node->nodehost;
			/*database user for this coordinator*/
			cnuser = get_hostuser_from_hostoid(hostoid);
			/*get coordinator address*/
			cnaddress = get_hostaddress_from_hostoid(hostoid);
			mgr_add_oneline_info_pghbaconf(2, "all", cnuser, cnaddress, 32, "trust", infosendhbamsg);
			pfree(cnuser);
			pfree(cnaddress);
		}
		heap_endscan(rel_scan);
		heap_close(rel_node, AccessShareLock);
	}
	else if (CNDN_TYPE_DATANODE_MASTER == nodetype)
	{
		rel_node = heap_open(NodeRelationId, AccessShareLock);
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/*hostoid*/
			hostoid = mgr_node->nodehost;
			/*database user for this coordinator*/
			cnuser = get_hostuser_from_hostoid(hostoid);
			/*get coordinator address*/
			cnaddress = get_hostaddress_from_hostoid(hostoid);
			if(CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype)
			{
				mgr_add_oneline_info_pghbaconf(2, "replication", cnuser, cnaddress, 32, "trust", infosendhbamsg);
			}
			pfree(cnuser);
			pfree(cnaddress);
		}
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/*hostoid*/
			hostoid = mgr_node->nodehost;
			/*database user for this coordinator*/
			cnuser = get_hostuser_from_hostoid(hostoid);
			/*get coordinator address*/
			cnaddress = get_hostaddress_from_hostoid(hostoid);
			mgr_add_oneline_info_pghbaconf(2, "all", cnuser, cnaddress, 32, "trust", infosendhbamsg);
			pfree(cnuser);
			pfree(cnaddress);
		}
		heap_endscan(rel_scan);
		heap_close(rel_node, AccessShareLock);
	}


}

/*
* add one line content to infosendhbamsg, which will send to agent to refresh pg_hba.conf, the word in this line interval by '\0',donot change the order
*/
void mgr_add_oneline_info_pghbaconf(int type, char *database, char *user, char *addr, int addr_mark, char *auth_method, StringInfo infosendhbamsg)
{
	appendStringInfo(infosendhbamsg, "%c", type);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, database);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, user);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, addr);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfo(infosendhbamsg, "%d", addr_mark);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, auth_method);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
}

char *mgr_get_dnmaster_slavename(Oid tupleOid, char nodetype)
{
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	ScanKeyData key[1];

	Relation rel_node;
	HeapScanDesc rel_scan;
	char *slavename = NULL;

	if (nodetype != CNDN_TYPE_DATANODE_MASTER)
		return NULL;
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);	
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodemasternameoid == tupleOid)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			slavename = pstrdup(NameStr(mgr_node->nodename));
			return slavename;
		}
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);	
	return NULL;
}

void mgr_rename_recovery_to_conf(char cmdtype, Oid hostOid, char* cndnpath, GetAgentCmdRst *getAgentCmdRst)
{
	StringInfoData buf;
	StringInfoData infosendmsg;
	ManagerAgent *ma;

	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	initStringInfo(&buf);
	appendStringInfoString(&infosendmsg, cndnpath);
	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		return;
	}

	/*send cmd*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,infosendmsg.data);
	pfree(infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	mgr_recv_msg(ma, getAgentCmdRst);
	ma_close(ma);	
	
}

/*
* give nodename, nodetype to get tuple from node systbl, 
*/
HeapTuple mgr_get_tuple_node_from_name_type(Relation rel, char *nodename, char nodetype)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	HeapTuple tupleret;
	NameData nameattrdata;
	
	namestrcpy(&nameattrdata, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&nameattrdata));
	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	tupleret = heap_copytuple(tuple);
	heap_endscan(rel_scan);
	return tupleret;	
}

/*mark the node in node systbl is in cluster*/
void mgr_mark_node_in_cluster(Relation rel)
{
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		mgr_node->nodeincluster = true;
		heap_inplace_update(rel, tuple);
	}
	heap_endscan(rel_scan);
}
