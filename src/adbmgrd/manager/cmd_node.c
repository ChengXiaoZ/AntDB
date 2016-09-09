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
#include "catalog/mgr_cndnnode.h"
#include "catalog/mgr_updateparm.h"
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
#include "../../interfaces/libpq/libpq-fe.h"

#define DEFAULT_DB "postgres"
#define shutdown_s  "smart"
#define shutdown_f  "fast"
#define shutdown_i  "immediate"
#define takeplaparm_n  "none"
#define MAX_PREPARED_TRANSACTIONS_DEFAULT	100
#define PG_DUMPALL_TEMP_FILE "/tmp/pg_dumpall_temp.txt"
#define MAX_WAL_SENDERS_NUM	5
#define WAL_KEEP_SEGMENTS_NUM	32
#define WAL_LEVEL_MODE	"hot_standby"
#define APPEND_DNMASTER  1
#define APPEND_CNMASTER  2

typedef struct AppendNodeInfo
{
    char *nodename;
	char *nodepath;
	char  nodetype;
    Oid   nodehost;
	char *nodeaddr;
	int32 nodeport;
    Oid   nodemasteroid;
    char *nodeusername;
}AppendNodeInfo;

static TupleDesc common_command_tuple_desc = NULL;
static TupleDesc get_common_command_tuple_desc_for_monitor(void);
static HeapTuple build_common_command_tuple_for_monitor(const Name name
                                                        ,char type             
                                                        ,bool status               
                                                        ,const char *description);
static void mgr_get_appendnodeinfo(char node_type, AppendNodeInfo *appendnodeinfo);
static void mgr_append_init_cndnmaster(AppendNodeInfo *appendnodeinfo);
static void mgr_get_agtm_host_and_port(StringInfo infosendmsg);
static void mgr_get_other_parm(char node_type, StringInfo infosendmsg);
static void mgr_get_active_hostoid_and_port(char node_type, Oid *hostoid, int32 *hostport, AppendNodeInfo *appendnodeinfo);
static void mgr_pg_dumpall(Oid hostoid, int32 hostport, Oid dnmasteroid);
static void mgr_stop_node_with_restoremode(const char *nodepath, Oid hostoid);
static void mgr_pg_dumpall_input_node(const Oid dn_master_oid, const int32 dn_master_port);
static void mgr_rm_dumpall_temp_file(Oid dnhostoid);
static void mgr_start_node_with_restoremode(const char *nodepath, Oid hostoid);
static void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid);
static void mgr_create_node_on_all_coord(PG_FUNCTION_ARGS, char *dnname, Oid dnhostoid, int32 dnport);
static void mgr_set_inited_incluster(char *nodename, char nodetype, bool checkvalue, bool setvalue);
static void mgr_add_hbaconf(char nodetype, char *dnusername, char *dnaddr);
static void mgr_add_hbaconf_all(char *dnusername, char *dnaddr);
static void mgr_after_gtm_failover_handle(char *hostaddress, int cndnport, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath);
static void mgr_reload_conf(Oid hostoid, char *nodepath);
static bool mgr_start_one_gtm_master(void);
static void mgr_alter_pgxc_node(PG_FUNCTION_ARGS, char *nodename, Oid nodehostoid, int32 nodeport);
static void mgr_after_datanode_failover_handle(Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, char aimtuplenodetype);
static void mgr_get_parent_appendnodeinfo(Oid nodemasternameoid, AppendNodeInfo *parentnodeinfo);
static bool is_node_running(char *hostaddr, int32 hostport);
static void get_nodeinfo(char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
static void mgr_pgbasebackup(AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo);
static Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange);
static void mgr_clean_node_folder(char cmdtype, Oid hostoid, char *nodepath, GetAgentCmdRst *getAgentCmdRst);
static Datum mgr_prepare_clean_all(PG_FUNCTION_ARGS);
static bool mgr_node_has_slave_extra(Relation rel, Oid mastertupeoid);

#if (Natts_mgr_node != 9)
#error "need change code"
#endif

typedef struct InitNodeInfo
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitNodeInfo;

/*the values see agt_cmd.c, used for pg_hba.conf add content*/
typedef enum ConnectType
{
	CONNECT_LOCAL=1,
	CONNECT_HOST,
	CONNECT_HOSTSSL,
	CONNECT_HOSTNOSSL
}ConnectType;

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
	ObjectAddress myself;
	ObjectAddress host;
	Oid cndn_oid;
	char nodetype;			/*coordinator or datanode master/slave*/
	Assert(node && node->name);
	
	nodetype = node->nodetype;
	nodestring = mgr_nodetype_str(nodetype);
	
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	Assert(node->name);
	namestrcpy(&name, node->name);
	/*master/slave/extra has the same name*/
	namestrcpy(&mastername, node->name);
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
	pfree(nodestring);
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
					, errmsg("host \"%s\" does not exist", defGetString(def))));
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
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is host, port and path")));
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
			, errmsg("option \"path\" must be given")));
	}
	if(got[Anum_mgr_node_nodehost-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"host\" must be given")));
	}
	if(got[Anum_mgr_node_nodeport-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"port\" must be given")));
	}
	if(got[Anum_mgr_node_nodemasternameOid-1] == false)
	{
		if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_COORDINATOR_MASTER == nodetype || GTM_TYPE_GTM_MASTER == nodetype)
			datum[Anum_mgr_node_nodemasternameOid-1] = UInt32GetDatum(0);
		else if(CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype)
		{
			mastertuple = mgr_get_tuple_node_from_name_type(rel, NameStr(mastername), CNDN_TYPE_DATANODE_MASTER);
			if(!HeapTupleIsValid(mastertuple))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("datanode master \"%s\" does not exist", NameStr(mastername))));
			}
			datum[Anum_mgr_node_nodemasternameOid-1] = ObjectIdGetDatum(HeapTupleGetOid(mastertuple));
			heap_freetuple(mastertuple);
		}
		else
		{
			mastertuple = mgr_get_tuple_node_from_name_type(rel, NameStr(mastername), GTM_TYPE_GTM_MASTER);
			if(!HeapTupleIsValid(mastertuple))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("gtm master \"%s\" does not exist", NameStr(mastername))));
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
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	HeapTuple searchHostTuple;	
	TupleDesc cndn_dsc;
	NameData hostname;
	char nodetype = '\0';			/*coordinator master/slave or datanode master/slave/extra*/
	Form_mgr_node mgr_node;
	Assert(node && node->name);
	
	nodetype = node->nodetype;
	nodestring = mgr_nodetype_str(nodetype);
	
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	cndn_dsc = RelationGetDescr(rel);
	namestrcpy(&name, node->name);
	/* check exists */
	oldtuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
	if(!(HeapTupleIsValid(oldtuple)))
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", nodestring, NameStr(name))));
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
	pfree(nodestring);
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
					, errmsg("host \"%s\" does not exist", defGetString(def))));
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
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is host, port and path")));
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
	Relation rel_updateparm;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	MemoryContext context, old_context;
	NameData name;
	NameData nametmp;
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	char nodetype;
	char *nodestring;
	Form_mgr_node mgr_node;
	int getnum = 0;
	int nodenum = 0;

	nodetype = node->nodetype;
	nodestring = mgr_nodetype_str(nodetype);

	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP NODE"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, node->names)
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
					,errmsg("%s \"%s\" does not exist", nodestring, NameStr(name))));
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
		/*check the node has been used by its slave or extra*/
		if (CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype|| GTM_TYPE_GTM_MASTER == mgr_node->nodetype)
		{
			if (mgr_node_has_slave_extra(rel, HeapTupleGetOid(tuple)))
			{
				heap_freetuple(tuple);
				heap_close(rel, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
						 ,errmsg("%s \"%s\" has been used by slave or extra, cannot be dropped", nodestring, NameStr(name))));
			}
		}
		nodenum++;
		/* todo chech used by other */
		heap_freetuple(tuple);
	}
	pfree(nodestring);

	/* now we can delete node(s) */
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	namestrcpy(&nametmp, MACRO_STAND_FOR_ALL_NODENAME);
	rel_scan = heap_beginscan(rel, SnapshotNow, 1, key);
	getnum = 0;
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if(HeapTupleIsValid(tuple))
		{
			getnum++;
		}
	}
	heap_endscan(rel_scan);
	foreach(lc, node->names)
	{
		val = lfirst(lc);
		Assert(val  && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
		if(HeapTupleIsValid(tuple))
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/*delete the parm in mgr_updateparm for this type of node*/
			mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, &(mgr_node->nodename), nodetype);
			simple_heap_delete(rel, &(tuple->t_self));
			CatalogUpdateIndexes(rel, tuple);
			heap_freetuple(tuple);
		}
	}
	/*delete the parm in mgr_updateparm for this type and nodename in mgr_updateparm is MACRO_STAND_FOR_ALL_NODENAME*/
	if (getnum == nodenum)
	{
		mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, &nametmp, nodetype);
	}
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
}

/*
* execute init gtm master, send infomation to agent to init gtm master 
*/
Datum 
mgr_init_gtm_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_INIT, fcinfo, takeplaparm_n);
}

/*
* execute init gtm slave, send infomation to agent to init gtm slave 
*/
Datum 
mgr_init_gtm_slave(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_SLAVE_INIT, fcinfo, takeplaparm_n);
}
/*
* execute init gtm extra, send infomation to agent to init gtm extra 
*/
Datum 
mgr_init_gtm_extra(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_SLAVE_INIT, fcinfo, takeplaparm_n);
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
						, errmsg("the type of \"%s\" is not datanode slave, use \"list node\" to check", nodename)));
				}
				aimtuple = tuple;
				gettuple = true;
				break;
			}
			
		}
		if(gettuple == false)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("datanode slave \"%s\" does not exist", nodename)));
		}
		/*get the master port, master host address*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("datanode master \"%s\" does not exist", nodename)));
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
* execute init datanode extra, send infomation to agent to init it 
*/
Datum 
mgr_init_dn_extra(PG_FUNCTION_ARGS)
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
			,CharGetDatum(CNDN_TYPE_DATANODE_EXTRA));
		rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(strcmp(NameStr(mgr_node->nodename), nodename) == 0)
			{
				/*check the nodetype*/
				if(mgr_node->nodetype != CNDN_TYPE_DATANODE_EXTRA)
				{
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
						, errmsg("the type of \"%s\" is not datanode extra, use \"list node\" to check", nodename)));
				}
				aimtuple = tuple;
				gettuple = true;
				break;
			}
			
		}
		if(gettuple == false)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("datanode slave \"%s\" does not exist", nodename)));
		}
		/*get the master port, master host address*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				, errmsg("datanode master \"%s\" does not exist", nodename)));
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
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("datanode master \"%s\" does not exist", NameStr(mgr_node->nodename))));
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

/*
*	execute init datanode extra all, send infomation to agent to init 
*/
Datum 
mgr_init_dn_extra_all(PG_FUNCTION_ARGS)
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
		,CharGetDatum(CNDN_TYPE_DATANODE_EXTRA));
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
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("datanode master \"%s\" does not exist", NameStr(mgr_node->nodename))));
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
			ereport(ERROR,
				(errmsg("start datanode master \"%s\" fail", mastername)));
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
		ma_close(ma);
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
			ereport(ERROR,
				(errmsg("stop datanode master \"%s\" fail", mastername)));
	}
	/*update node system table's column to set initial is true*/
	if (initdone)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnnametmp, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, "slave", masteroid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	pfree(infosendmsg.data);
}
/*
* get the datanode/coordinator name list
*/
List *
get_fcinfo_namelist(const char *sepstr, int argidx,
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
				ereport(ERROR,
					(errmsg("could not determine data type of mgr_start_cn_master() input")));
			getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
			nodename = OidOutputFunctionCall(typOutput, value);
			nodenamelist = lappend(nodenamelist, nodename);
		}
	}

	pfree(str.data);
	return nodenamelist;
}

/*
* start gtm master
*/
Datum mgr_start_gtm_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_START_MASTER, fcinfo, takeplaparm_n);
}

/*
* start one gtm master
*/
static bool mgr_start_one_gtm_master(void)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result;
	HeapTuple aimtuple = NULL;
	ScanKeyData key[0];
	Relation rel_node;
	HeapScanDesc rel_scan;
	
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(GTM_TYPE_GTM_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((aimtuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	if (!HeapTupleIsValid(aimtuple))
	{
		ereport(ERROR,
			(errmsg("gtm master does not exist")));
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	//tupleret = heap_copytuple(aimtuple);
	mgr_runmode_cndn_get_result(AGT_CMD_GTM_START_MASTER, &getAgentCmdRst, rel_node, aimtuple, takeplaparm_n);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);

	return getAgentCmdRst.ret;
}

/*
* start gtm slave
*/
Datum mgr_start_gtm_slave(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_START_SLAVE, fcinfo, takeplaparm_n);
}
/*
* start gtm extra
*/
Datum mgr_start_gtm_extra(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_START_SLAVE, fcinfo, takeplaparm_n);
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
		ereport(ERROR,
			(errmsg("datanode master \"%s\" does not exist", nodename)));
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

/*
* start datanode extra dn1,dn2...
* start datanode extra all
*/
Datum mgr_start_dn_extra(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_EXTRA, AGT_CMD_DN_START, fcinfo, takeplaparm_n);	
}

void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *shutdown_mode)
{
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_node_dnmaster;
	Form_mgr_node mgr_node_gtm;
	Datum datumPath;
	Datum DatumStopDnMaster;
	Datum DatumMaster;
	StringInfoData buf;
	StringInfoData infosendmsg;
	StringInfoData strinfoport;
	ManagerAgent *ma;
	bool isNull = false,
		execok = false,
		getrefresh;
	char *hostaddress;
	char *cndnPath;
	char *cmdmode;
	char *zmode;
	char *cndnname;
	char *dnmastername;
	char *masterhostaddress;
	char *masterpath;
	char *mastername;
	char nodetype;
	int32 cndnport;
	int masterport;
	Oid hostOid;
	Oid nodemasternameoid;
	Oid	tupleOid;
	Oid	masterhostOid;
	bool getmaster = false;
	bool isprimary = false;
	bool ismasterrunning = 0;
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	HeapTuple mastertuple;
	HeapTuple gtmmastertuple;

	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	/*get column values from aimtuple*/	
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	hostOid = mgr_node->nodehost;
	/*get host address*/
	hostaddress = get_hostaddress_from_hostoid(hostOid);
	Assert(hostaddress);
	/*get nodename*/
	cndnname = NameStr(mgr_node->nodename);
	isprimary = mgr_node->nodeprimary;
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), cndnname);
	/*check node init or not*/
	if ((AGT_CMD_CNDN_CNDN_INIT == cmdtype || AGT_CMD_GTM_INIT == cmdtype || AGT_CMD_GTM_SLAVE_INIT == cmdtype ) && mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "the node \"%s\" has inited", cndnname);
		getAgentCmdRst->ret = false;
		return;
	}
	if(AGT_CMD_CNDN_CNDN_INIT != cmdtype && AGT_CMD_GTM_INIT != cmdtype && AGT_CMD_GTM_SLAVE_INIT != cmdtype && !mgr_node->nodeinited)
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
		case AGT_CMD_GTM_START_MASTER:
		case AGT_CMD_GTM_START_SLAVE:
			cmdmode = "start";
			break;
		case AGT_CMD_GTM_STOP_MASTER:
		case AGT_CMD_GTM_STOP_SLAVE:
			cmdmode = "stop";
			break;
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
		case AGT_CMD_CN_RESTART:
			cmdmode = "restart";
			zmode = "coordinator";
			break;
		case AGT_CMD_DN_STOP:
			cmdmode = "stop";
			zmode = "datanode";
			break;
		case AGT_CMD_DN_FAILOVER:
			cmdmode = "promote";
			zmode = "datanode";
			break;
		case AGT_CMD_GTM_SLAVE_FAILOVER:
			cmdmode = "promote";
			zmode = "node";
			break;
		case AGT_CMD_AGTM_RESTART:
			cmdmode = "restart";
			zmode = "node";
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
		appendStringInfo(&infosendmsg, " --nodename %s -E UTF8 --locale=C", cndnname);
	} /*init gtm*/
	else if (AGT_CMD_GTM_INIT == cmdtype)
	{
		appendStringInfo(&infosendmsg, "-U \"" AGTM_USER "\" -D %s -E UTF8 --locale=C", cndnPath);
	} /*init gtm slave*/
	else if (AGT_CMD_GTM_SLAVE_INIT == cmdtype)
	{
		/*get gtm masterport, masterhostaddress*/
		gtmmastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		if(!HeapTupleIsValid(gtmmastertuple))
		{
			appendStringInfo(&(getAgentCmdRst->description), "gtm master dosen't exist");
			getAgentCmdRst->ret = false;
			ereport(LOG, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("gtm master does not exist")));
			pfree(infosendmsg.data);
			pfree(hostaddress);
			return;
		}
		mgr_node_gtm = (Form_mgr_node)GETSTRUCT(gtmmastertuple);
		Assert(gtmmastertuple);
		masterport = mgr_node_gtm->nodeport;
		masterhostOid = mgr_node_gtm->nodehost;
		mastername = NameStr(mgr_node_gtm->nodename);
		masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
		appendStringInfo(&infosendmsg, " -p %u", masterport);
		appendStringInfo(&infosendmsg, " -h %s", masterhostaddress);
		appendStringInfo(&infosendmsg, " -D %s", cndnPath);
		appendStringInfo(&infosendmsg, " -U %s", AGTM_USER);
		appendStringInfo(&infosendmsg, " -x");
		ReleaseSysCache(gtmmastertuple);
		/*check it need start gtm master*/
		initStringInfo(&strinfoport);
		appendStringInfo(&strinfoport, "%d", masterport);
		ismasterrunning = pingNode(masterhostaddress, strinfoport.data);
		pfree(masterhostaddress);
		pfree(strinfoport.data);	
		if(ismasterrunning != 0)
		{
			if(!mgr_start_one_gtm_master())
			{
				appendStringInfo(&(getAgentCmdRst->description), "start gtm master \"%s\" fail", mastername);
				getAgentCmdRst->ret = false;
				ereport(LOG,
						(errmsg("start gtm master \"%s\" fail", mastername)));
				pfree(infosendmsg.data);
				pfree(hostaddress);
				return;
			}	
		}
	}
	else if (AGT_CMD_GTM_START_MASTER == cmdtype || AGT_CMD_GTM_START_SLAVE == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s -o -i -w -c -l %s/logfile", cmdmode, cndnPath, cndnPath);
	}
	else if (AGT_CMD_GTM_STOP_MASTER == cmdtype || AGT_CMD_GTM_STOP_SLAVE == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s -m %s -o -i -w -c -l %s/logfile", cmdmode, cndnPath, shutdown_mode, cndnPath);
	}
	/*stop coordinator/datanode*/
	else if(AGT_CMD_CN_STOP == cmdtype || AGT_CMD_DN_STOP == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c -l %s/logfile", zmode, shutdown_mode, cndnPath);
	}
	else if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
	}
	else if (AGT_CMD_AGTM_RESTART == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s -m %s -l %s/logfile", cmdmode, cndnPath, shutdown_mode, cndnPath);
	}
	else if (AGT_CMD_DN_RESTART == cmdtype || AGT_CMD_CN_RESTART == cmdtype)
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
		ma_close(ma);
		return;
	}

	/*send cmd*/
	ereport(LOG,
		(errmsg("%s, cmdtype=%d, %s", hostaddress, cmdtype, infosendmsg.data)));
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
	
	if (AGT_CMD_GTM_SLAVE_INIT == cmdtype)
	{
		/*stop gtm master if we start it*/
		if(ismasterrunning != 0)
		{
			/*it need stop gtm master*/
			DatumMaster = DirectFunctionCall1(mgr_stop_one_gtm_master, (Datum)0);
			if(DatumGetObjectId(DatumMaster) == InvalidOid)
				ereport(ERROR,
						(errmsg("stop gtm master \"%s\" fail", mastername)));
		}
	}
	/*when init, 1. update gtm system table's column to set initial is true 2. refresh postgresql.conf*/
	if (execok && AGT_CMD_GTM_INIT == cmdtype)
	{
		/*update node system table's column to set initial is true when cmd is init*/
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnname, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, GTM_TYPE_GTM_MASTER, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	/*when init, 1. update gtm system table's column to set initial is true 2. refresh postgresql.conf*/
	if (execok && AGT_CMD_GTM_SLAVE_INIT == cmdtype)
	{
		/*update node system table's column to set initial is true when cmd is init*/
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnname, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, GTM_TYPE_GTM_MASTER, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, "slave", nodemasternameoid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	
	/*update node system table's column to set initial is true when cmd is init*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype && execok)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnname, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	/*failover execute success*/
	if(AGT_CMD_DN_FAILOVER == cmdtype && execok)
	{
		/*0.stop the old datanode master*/
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
			ereport(WARNING, (errmsg("stop datanode master \"%s\" fail", dnmastername)));
		/*1. restart datanode*/
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, getAgentCmdRst, noderel, aimtuple, shutdown_f);
		if(!getAgentCmdRst->ret)
		{
			ereport(ERROR,
				(errmsg("pg_ctl restart datanode fail: path=%s", cndnPath)));
			return;
		}
		/*2.refresh pgxc_node systable */
		resetStringInfo(&(getAgentCmdRst->description));
		getrefresh = mgr_refresh_pgxc_node_tbl(cndnname, cndnport, hostaddress, isprimary, nodemasternameoid, getAgentCmdRst);
		if(!getrefresh)
		{
			resetStringInfo(&(getAgentCmdRst->description));
			appendStringInfoString(&(getAgentCmdRst->description),"ERROR: refresh system table of pgxc_node on coordinators fail, please check pgxc_node on every coordinator");
			getAgentCmdRst->ret = getrefresh;
			return;
		}
		/*3.delete old master record in node systbl*/
		simple_heap_delete(noderel, &mastertuple->t_self);
		CatalogUpdateIndexes(noderel, mastertuple);
		ReleaseSysCache(mastertuple);
		/*4.change slave type to master type*/
		mgr_node->nodeinited = true;
		mgr_node->nodetype = CNDN_TYPE_DATANODE_MASTER;
		mgr_node->nodemasternameoid = 0;
		heap_inplace_update(noderel, aimtuple);
		/*5.refresh extra recovery.conf*/
		mgr_after_datanode_failover_handle(noderel, getAgentCmdRst, aimtuple, cndnPath, nodetype);
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
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, masterpath, &infosendmsg, hostOid, getAgentCmdRst);
		}
	}
	/*gtm failover*/
	if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype && execok)
	{
		mgr_after_gtm_failover_handle(hostaddress, cndnport, noderel, getAgentCmdRst, aimtuple, cndnPath);
	}

	pfree(infosendmsg.data);
	pfree(hostaddress);
}

/*
* stop gtm master
*/
Datum mgr_stop_gtm_master(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_STOP_MASTER, fcinfo, shutdown_s);
}

Datum mgr_stop_gtm_master_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_STOP_MASTER, fcinfo, shutdown_f);
}

Datum mgr_stop_gtm_master_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_STOP_MASTER, fcinfo, shutdown_i);
}

/*
* stop gtm master ,used for DirectFunctionCall1
*/
Datum mgr_stop_one_gtm_master(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result;
	HeapTuple aimtuple = NULL;
	ScanKeyData key[0];
	Relation rel_node;
	HeapScanDesc rel_scan;
	
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(GTM_TYPE_GTM_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((aimtuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	if (!HeapTupleIsValid(aimtuple))
	{
		ereport(ERROR, (errmsg("gtm master does not exist")));
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	//tupleret = heap_copytuple(aimtuple);
	mgr_runmode_cndn_get_result(AGT_CMD_GTM_STOP_MASTER, &getAgentCmdRst, rel_node, aimtuple, shutdown_i);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);

	return HeapTupleGetDatum(tup_result);
}

/*
* stop gtm slave
*/
Datum mgr_stop_gtm_slave(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_STOP_SLAVE, fcinfo, shutdown_s);
}

Datum mgr_stop_gtm_slave_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_STOP_SLAVE, fcinfo, shutdown_f);
}

Datum mgr_stop_gtm_slave_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_STOP_SLAVE, fcinfo, shutdown_i);
}
/*stop gtm extra*/
Datum mgr_stop_gtm_extra(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_STOP_SLAVE, fcinfo, shutdown_s);
}

Datum mgr_stop_gtm_extra_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_STOP_SLAVE, fcinfo, shutdown_f);
}

Datum mgr_stop_gtm_extra_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_STOP_SLAVE, fcinfo, shutdown_i);
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
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_STOP, fcinfo, shutdown_s);
}

Datum mgr_stop_dn_master_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_STOP, fcinfo, shutdown_f);
}

Datum mgr_stop_dn_master_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_STOP, fcinfo, shutdown_i);
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
		ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", nodename)));
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
* stop datanode extra dn1,dn2...
* stop datanode extra all
*/
Datum mgr_stop_dn_extra(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_EXTRA, AGT_CMD_DN_STOP, fcinfo, shutdown_s);
}

Datum mgr_stop_dn_extra_f(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_EXTRA, AGT_CMD_DN_STOP, fcinfo, shutdown_f);
}

Datum mgr_stop_dn_extra_i(PG_FUNCTION_ARGS)
{
	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_EXTRA, AGT_CMD_DN_STOP, fcinfo, shutdown_i);
}

/*
* get the result of start/stop/init gtm master/slave, coordinator master/slave, datanode master/slave
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
			/*check init gtm master/slave/extra; start|stop gtm master/slave/extra */
			if ((GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
					&& (AGT_CMD_GTM_INIT == cmdtype || AGT_CMD_GTM_SLAVE_INIT == cmdtype || AGT_CMD_GTM_START_MASTER == cmdtype 
					|| AGT_CMD_GTM_START_SLAVE == cmdtype || AGT_CMD_GTM_STOP_MASTER == cmdtype || AGT_CMD_GTM_STOP_SLAVE == cmdtype))
			{
				/*get gtm name*/
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
						break;
				}
				heap_endscan(info->rel_scan);
				/*check gtm master/slave/extra exist*/
				if (nodenamelist == NIL)
				{
					heap_close(info->rel_node, RowExclusiveLock);
					ereport(ERROR, 
					(errcode(ERRCODE_UNDEFINED_OBJECT), 
						errmsg("%s does not exist", mgr_nodetype_str(nodetype))));
				}
			}
			else
			{
				#ifdef ADB
					nodenamelist = get_fcinfo_namelist("", 0, fcinfo, NULL);
				#else
					nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
				#endif
			}
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
		ereport(ERROR, (errmsg("namestrcpy %s fail", nodestrname)));
	}
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, NameStr(nodenamedata), nodetype);
	if (!HeapTupleIsValid(aimtuple))
	{
		ereport(ERROR, 
		(errcode(ERRCODE_UNDEFINED_OBJECT), 
			errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodestrname)));
	}
	/*check the type is given type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	if(nodetype != mgr_node->nodetype)
	{
		heap_freetuple(aimtuple);
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
			errmsg("the type of  %s is not right, use \"list node\" to check", nodestrname)));
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
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo, NULL);
		#else
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
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
		ereport(ERROR, (errmsg("node name is invalid: %s", coordname)));

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);
	
	if (CNDN_TYPE_COORDINATOR_MASTER != mgr_node->nodetype)
		ereport(ERROR, (errmsg("node type is not coordinator: %s", coordname)));

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
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo, NULL);
		#else
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
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
		ereport(ERROR, (errmsg("node name is invalid: %s", dnmastername)));

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);
	
	if (CNDN_TYPE_DATANODE_MASTER != mgr_node->nodetype)
		ereport(ERROR, (errmsg("node type is not datanode master: %s", dnmastername)));

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
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo, NULL);
		#else
			nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
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
		ereport(ERROR, (errmsg("node name is invalid: %s", dnslavename)));

    mgr_node = (Form_mgr_node)GETSTRUCT(tup);
    Assert(mgr_node);
	
	if (CNDN_TYPE_DATANODE_SLAVE != mgr_node->nodetype)
		ereport(ERROR, (errmsg("node type is not datanode slave: %s", dnslavename)));

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
        case GTM_TYPE_GTM_MASTER:
                datums[1] = NameGetDatum(pstrdup("gtm master"));
                break;
        case GTM_TYPE_GTM_SLAVE:
                datums[1] = NameGetDatum(pstrdup("gtm slave"));
                break;
        case GTM_TYPE_GTM_EXTRA:
                datums[1] = NameGetDatum(pstrdup("gtm extra"));
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
        case CNDN_TYPE_DATANODE_EXTRA:
                datums[1] = NameGetDatum(pstrdup("datanode extra"));
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
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo, agtm_s_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	StringInfoData  infosendmsg;
	volatile bool catcherr = false;
	StringInfoData catcherrmsg;
	NameData nodename;
	Oid coordhostoid;
	int32 coordport;
	char *coordhost;
	Oid dnhostoid;
	int32 dnport;
	PGconn *pg_conn = NULL;
	PGresult *res;
	HeapTuple tup_result;
	HeapTuple aimtuple = NULL;
	char coordport_buf[10];
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY_HOLD();
	{
		/* get node info for append datanode master */
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_MASTER, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_m_nodeinfo.nodepath,
										&infosendmsg,
										agtm_m_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm master */
				mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);
			}
			else
				{ ereport(ERROR, (errmsg("agtm master is not running.")));}
		}
		else
		{ ereport(ERROR, (errmsg("agtm master is not exist.")));}
		
		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_s_nodeinfo.nodepath,
										&infosendmsg,
										agtm_s_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm slave */
				mgr_reload_conf(agtm_s_nodeinfo.nodehost, agtm_s_nodeinfo.nodepath);
			}
			else
			{ ereport(ERROR, (errmsg("agtm slave is not running.")));}
		}

		/* step 1: init workdir */
		mgr_append_init_cndnmaster(&appendnodeinfo);

		/* step 2: update datanode master's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_get_agtm_host_and_port(&infosendmsg);
		mgr_get_other_parm(CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 3: update datanode master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_add_oneline_info_pghbaconf(2, "all", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		/* add host line for agtm */
		mgr_add_hbaconf(GTM_TYPE_GTM_MASTER, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);
		mgr_add_hbaconf(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);
		mgr_add_hbaconf(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* step 4: block all the DDL lock */
		mgr_get_active_hostoid_and_port(CNDN_TYPE_COORDINATOR_MASTER, &coordhostoid, &coordport, &appendnodeinfo);
		coordhost = get_hostaddress_from_hostoid(coordhostoid);
		sprintf(coordport_buf, "%d", coordport);
		pg_conn = PQsetdbLogin(coordhost
								,coordport_buf
								,NULL, NULL
								,DEFAULT_DB
								,appendnodeinfo.nodeusername
								,NULL);

		if (pg_conn == NULL || PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to coordinator %s", PQerrorMessage((PGconn*)pg_conn)),
				errhint("coordinator info(host=%s port=%d dbname=%s user=%s)",
					coordhost, coordport, DEFAULT_DB, appendnodeinfo.nodeusername)));
		}

		pfree(coordhost);

		res = PQexec(pg_conn, "select pgxc_lock_for_backup();");
		if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			ereport(ERROR,
				(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)pg_conn)),
				errhint("execute command failed: select pgxc_lock_for_backup().")));
		}

		/* step 5: dumpall catalog message */
		mgr_get_active_hostoid_and_port(CNDN_TYPE_DATANODE_MASTER, &dnhostoid, &dnport, &appendnodeinfo);
		mgr_pg_dumpall(dnhostoid, dnport, appendnodeinfo.nodehost);

		/* step 6: start the datanode master with restoremode mode, and input all catalog message */
		mgr_start_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_pg_dumpall_input_node(appendnodeinfo.nodehost, appendnodeinfo.nodeport);
		mgr_rm_dumpall_temp_file(appendnodeinfo.nodehost);

		/* step 7: stop the datanode master with restoremode, and then start it with "datanode" mode */
		mgr_stop_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 8: create node on all the coordinator */
		mgr_create_node_on_all_coord(fcinfo, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);

		/* step 9: release the DDL lock */
		PQclear(res);
		PQfinish(pg_conn);
		pg_conn = NULL;

		/* step10: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, false, true);
	}PG_CATCH_HOLD();
	{
		catcherr = true;
	}PG_END_TRY_HOLD();

	if (catcherr)
	{
		initStringInfo(&catcherrmsg);
		geterrmsg(&catcherrmsg);
		errdump();
	}

	if(pg_conn)
	{
		PQfinish(pg_conn);
		pg_conn = NULL;
	}

	tup_result = build_common_command_tuple(
		&nodename
		,catcherr == false ? true : false
		,catcherr == false ? "success" : catcherrmsg.data);

	if (catcherr)
		pfree(catcherrmsg.data);

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND DATANODE SLAVE nodename
 */
Datum mgr_append_dnslave(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo, parentnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo, agtm_s_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	StringInfoData  infosendmsg;
	volatile bool catcherr = false;
	StringInfoData catcherrmsg, primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY_HOLD();
	{
		/* get node info both slave and master node. */
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_SLAVE, &appendnodeinfo);
		mgr_get_parent_appendnodeinfo(appendnodeinfo.nodemasteroid, &parentnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);

		/* step 1: make sure datanode master, agtm master or agtm slave is running. */
		is_node_running(parentnodeinfo.nodeaddr, parentnodeinfo.nodeport);
		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_m_nodeinfo.nodepath,
										&infosendmsg,
										agtm_m_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm master */
				mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);
			}
			else
				{	ereport(ERROR, (errmsg("agtm master is not running.")));}
		}
		else
		{	ereport(ERROR, (errmsg("agtm master is not exist.")));}
		
		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_s_nodeinfo.nodepath,
										&infosendmsg,
										agtm_s_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm slave */
				mgr_reload_conf(agtm_s_nodeinfo.nodehost, agtm_s_nodeinfo.nodepath);

			}
			else
			{	ereport(ERROR, (errmsg("agtm slave is not running.")));}
		}

		/* step 2: update datanode master's postgresql.conf. */
		// to do nothing now

		/* step 3: update datanode master's pg_hba.conf. */
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								parentnodeinfo.nodepath,
								&infosendmsg,
								parentnodeinfo.nodehost,
								&getAgentCmdRst);

		/* step 4: reload datanode master. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 5: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(&appendnodeinfo, &parentnodeinfo);

		/* step 6: update datanode slave's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 7: update datanode slave's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodeport,
						get_hostuser_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodename);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 8: start datanode slave. */
		mgr_start_node(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 9: update datanode master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", appendnodeinfo.nodename, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								parentnodeinfo.nodepath,
								&infosendmsg, 
								parentnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 10: reload datanode master's postgresql.conf. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 11: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE, false, true);

	}PG_CATCH_HOLD();
	{
		catcherr = true;
		errdump();
	}PG_END_TRY_HOLD();

	if (catcherr)
	{
		initStringInfo(&catcherrmsg);
		geterrmsg(&catcherrmsg);
		errdump();
	}

	tup_result = build_common_command_tuple(
		&nodename
		,catcherr == false ? true : false
		,catcherr == false ? "success" : catcherrmsg.data);

	if (catcherr)
		pfree(catcherrmsg.data);

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND DATANODE EXTRA nodename
 */
Datum mgr_append_dnextra(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo, parentnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo, agtm_s_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	StringInfoData  infosendmsg;
	volatile bool catcherr = false;
	StringInfoData catcherrmsg, primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY_HOLD();
	{
		/* get node info both slave and master node. */
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_EXTRA, &appendnodeinfo);
		mgr_get_parent_appendnodeinfo(appendnodeinfo.nodemasteroid, &parentnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);

		/* step 1: make sure datanode master, agtm master or agtm slave is running. */
		is_node_running(parentnodeinfo.nodeaddr, parentnodeinfo.nodeport);
		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_m_nodeinfo.nodepath,
										&infosendmsg,
										agtm_m_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm master */
				mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);
			}
			else
				{	ereport(ERROR, (errmsg("agtm master is not running.")));}
		}
		else
		{	ereport(ERROR, (errmsg("agtm master is not exist.")));}
		
		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_s_nodeinfo.nodepath,
										&infosendmsg,
										agtm_s_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm slave */
				mgr_reload_conf(agtm_s_nodeinfo.nodehost, agtm_s_nodeinfo.nodepath);

			}
			else
			{	ereport(ERROR, (errmsg("agtm slave is not running.")));}
		}

		/* step 2: update datanode master's postgresql.conf. */
		// to do nothing now

		/* step 3: update datanode master's pg_hba.conf. */
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								parentnodeinfo.nodepath,
								&infosendmsg,
								parentnodeinfo.nodehost,
								&getAgentCmdRst);

		/* step 4: reload datanode master. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 5: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(&appendnodeinfo, &parentnodeinfo);

		/* step 6: update datanode extra's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 7: update datanode extra's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodeport,
						get_hostuser_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodename);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 8: start datanode extra. */
		mgr_start_node(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 9: update datanode master's postgresql.conf.*/
		/* resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								parentnodeinfo.nodepath,
								&infosendmsg, 
								parentnodeinfo.nodehost, 
								&getAgentCmdRst); */

		/* step 10: reload datanode master's postgresql.conf. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 11: update node system table's column to set initial is true*/
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_EXTRA, false, true);

	}PG_CATCH_HOLD();
	{
		catcherr = true;
		errdump();
	}PG_END_TRY_HOLD();

	if (catcherr)
	{
		initStringInfo(&catcherrmsg);
		geterrmsg(&catcherrmsg);
		errdump();
	}

	tup_result = build_common_command_tuple(
		&nodename
		,catcherr == false ? true : false
		,catcherr == false ? "success" : catcherrmsg.data);

	if (catcherr)
		pfree(catcherrmsg.data);

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND COORDINATOR MASTER nodename
 */
Datum mgr_append_coordmaster(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo, agtm_s_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	char *coordhost;
	Oid coordhostoid;
	int32 coordport;
	PGconn *pg_conn = NULL;
	PGresult *res;
	HeapTuple aimtuple = NULL;
	HeapTuple tup_result;
	char coordport_buf[10];
	volatile bool catcherr = false;
	StringInfoData catcherrmsg;
	NameData nodename;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	/* get node info for append coordinator master */
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);
	PG_TRY_HOLD();
	{
		/* get node info for append coordinator master */
		mgr_get_appendnodeinfo(CNDN_TYPE_COORDINATOR_MASTER, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_m_nodeinfo.nodepath,
										&infosendmsg,
										agtm_m_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm master */
				mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);
			}
			else
				{	ereport(ERROR, (errmsg("agtm master is not running.")));}
		}
		else
		{	ereport(ERROR, (errmsg("agtm master is not exist.")));}

		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf. */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_s_nodeinfo.nodepath,
										&infosendmsg,
										agtm_s_nodeinfo.nodehost,
										&getAgentCmdRst);

				/* reload agtm slave */
				mgr_reload_conf(agtm_s_nodeinfo.nodehost, agtm_s_nodeinfo.nodepath);

			}
			else
			{	ereport(ERROR, (errmsg("agtm slave is not running.")));}
		}
		/* step 1: init workdir */
		mgr_append_init_cndnmaster(&appendnodeinfo);

		/* step 2: update coordinator master's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_get_agtm_host_and_port(&infosendmsg);
		mgr_get_other_parm(CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 3: update coordinator master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_add_oneline_info_pghbaconf(2, "all", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		/* add host line for exist already */
		mgr_add_hbaconf_all(appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* step 4: block all the DDL lock */
		mgr_get_active_hostoid_and_port(CNDN_TYPE_COORDINATOR_MASTER, &coordhostoid, &coordport, &appendnodeinfo);
		coordhost = get_hostaddress_from_hostoid(coordhostoid);
		sprintf(coordport_buf, "%d", coordport);
		pg_conn = PQsetdbLogin(coordhost
								,coordport_buf
								,NULL, NULL
								,DEFAULT_DB
								,appendnodeinfo.nodeusername
								,NULL);

		if (pg_conn == NULL || PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to coordinator %s", PQerrorMessage((PGconn*)pg_conn)),
				errhint("coordinator info(host=%s port=%d dbname=%s user=%s)",
					coordhost, coordport, DEFAULT_DB, appendnodeinfo.nodeusername)));
		}

		pfree(coordhost);

		res = PQexec(pg_conn, "select pgxc_lock_for_backup();");
		if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
		ereport(ERROR,
			(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)pg_conn)),
			errhint("execute command failed: select pgxc_lock_for_backup().")));
		}

		/* step 5: dumpall catalog message */
		mgr_pg_dumpall(coordhostoid, coordport, appendnodeinfo.nodehost);

		/* step 6: start the datanode master with restoremode mode, and input all catalog message */
		mgr_start_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_pg_dumpall_input_node(appendnodeinfo.nodehost, appendnodeinfo.nodeport);
		mgr_rm_dumpall_temp_file(appendnodeinfo.nodehost);

		/* step 7: stop the datanode master with restoremode, and then start it with "coordinator" mode */
		mgr_stop_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_start_node(CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 8: create node on all the coordinator */
		mgr_create_node_on_all_coord(fcinfo, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);

		/* step 9:*/
		mgr_alter_pgxc_node(fcinfo, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);

		/* step 10: release the DDL lock */
		PQclear(res);
		PQfinish(pg_conn);
		pg_conn = NULL;

		/* step 11: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_COORDINATOR_MASTER, false, true);
	}PG_CATCH_HOLD();
	{
		catcherr = true;
	}PG_END_TRY_HOLD();
	
	if (catcherr)
	{
		initStringInfo(&catcherrmsg);
		geterrmsg(&catcherrmsg);
		errdump();
	}
	if(pg_conn)
	{
		PQfinish(pg_conn);
		pg_conn = NULL;
	}

	tup_result = build_common_command_tuple(
		&nodename
		,catcherr == false ? true : false
		,catcherr == false ? "success" : catcherrmsg.data);

	if (catcherr)
		pfree(catcherrmsg.data);

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_append_agtmslave(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	StringInfoData  infosendmsg;
	volatile bool catcherr = false;
	StringInfoData catcherrmsg, primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY_HOLD();
	{
		/* get agtm slave and agtm master node info. */
		mgr_get_appendnodeinfo(GTM_TYPE_GTM_SLAVE, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		
		if (!agtm_m_is_exist)
		{
			ereport(ERROR, (errmsg("agtm master is not exist.")));
		}
		
		if (!agtm_m_is_running)
		{
			ereport(ERROR, (errmsg("agtm master is not running.")));
		}

		/* step 1: update agtm master's pg_hba.conf. */
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								agtm_m_nodeinfo.nodepath,
								&infosendmsg,
								agtm_m_nodeinfo.nodehost,
								&getAgentCmdRst);

		/* step 2: reload agtm master. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 3: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(&appendnodeinfo, &agtm_m_nodeinfo);

		/* step 4: update agtm slave's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 5: update agtm slave's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodeport,
						get_hostuser_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodename);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 6: start agtm slave. */
		mgr_start_node(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 7: update agtm master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", appendnodeinfo.nodename, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								agtm_m_nodeinfo.nodepath,
								&infosendmsg, 
								agtm_m_nodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 8: reload agtm master's postgresql.conf. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 9: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, GTM_TYPE_GTM_SLAVE, false, true);

	}PG_CATCH_HOLD();
	{
		catcherr = true;
		errdump();
	}PG_END_TRY_HOLD();

	if (catcherr)
	{
		initStringInfo(&catcherrmsg);
		geterrmsg(&catcherrmsg);
		errdump();
	}

	tup_result = build_common_command_tuple(
		&nodename
		,catcherr == false ? true : false
		,catcherr == false ? "success" : catcherrmsg.data);

	if (catcherr)
		pfree(catcherrmsg.data);

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_append_agtmextra(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	//bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	StringInfoData  infosendmsg;
	volatile bool catcherr = false;
	StringInfoData catcherrmsg, primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY_HOLD();
	{
		/* get agtm extra, agtm master and agtm slave node info. */
		mgr_get_appendnodeinfo(GTM_TYPE_GTM_EXTRA, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		//get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);

		if (!agtm_m_is_exist)
		{
			ereport(ERROR, (errmsg("agtm master is not exist.")));
		}
		
		if (!agtm_m_is_running)
		{
			ereport(ERROR, (errmsg("agtm master is not running.")));
		}

        /* step 1: update agtm master's pg_hba.conf. */
        resetStringInfo(&infosendmsg);
        mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
        mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
                                agtm_m_nodeinfo.nodepath,
                                &infosendmsg,
                                agtm_m_nodeinfo.nodehost,
                                &getAgentCmdRst);

        /* step 2: reload agtm master. */
        mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);
	
		/* step 3: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(&appendnodeinfo, &agtm_m_nodeinfo);

		/* step 4: update agtm extra's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 5: update agtm extra's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodeport,
						get_hostuser_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodename);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);

		/* step 6: start agtm extra. */
		mgr_start_node(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 7: update agtm master's postgresql.conf.*/
		/*resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", appendnodeinfo.nodename, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								agtm_m_nodeinfo.nodepath,
								&infosendmsg, 
								agtm_m_nodeinfo.nodehost, 
								&getAgentCmdRst); */

		/* step 8: reload agtm master's postgresql.conf. */
		//mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 9: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, GTM_TYPE_GTM_SLAVE, false, true);

	}PG_CATCH_HOLD();
	{
		catcherr = true;
		errdump();
	}PG_END_TRY_HOLD();

	if (catcherr)
	{
		initStringInfo(&catcherrmsg);
		geterrmsg(&catcherrmsg);
		errdump();
	}

	tup_result = build_common_command_tuple(
		&nodename
		,catcherr == false ? true : false
		,catcherr == false ? "success" : catcherrmsg.data);

	if (catcherr)
		pfree(catcherrmsg.data);

	return HeapTupleGetDatum(tup_result);
}

static void get_nodeinfo(char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[3];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	char * hostaddr;
	bool isNull = false;

	*is_exist = true;
	*is_running = true;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 3, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		
		*is_exist = false;
		return;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	nodeinfo->nodename = NameStr(mgr_node->nodename);
	nodeinfo->nodetype = mgr_node->nodetype;
	nodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeport = mgr_node->nodeport;
	nodeinfo->nodehost = mgr_node->nodehost;
	nodeinfo->nodemasteroid = mgr_node->nodemasternameoid;

	/*get nodepath from tuple*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if (isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodeinfo->nodepath = TextDatumGetCString(datumPath);
	hostaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);

	if ( !is_node_running(nodeinfo->nodeaddr, nodeinfo->nodeport))
		*is_running = false;

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(hostaddr);
}

static void mgr_pgbasebackup(AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo)
{

	ManagerAgent *ma;
	StringInfoData sendstrmsg, buf;
	GetAgentCmdRst getAgentCmdRst;
	bool execok;

	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&sendstrmsg, " -h %s -p %d -D %s -Xs -Fp -R", 
								get_hostaddress_from_hostoid(parentnodeinfo->nodehost)
								,parentnodeinfo->nodeport
								,appendnodeinfo->nodepath);

	ma = ma_connect_hostoid(appendnodeinfo->nodehost);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
        
        ereport(ERROR, (errmsg("could not connect socket for agent.")));
		return;
	}
	getAgentCmdRst.ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_CNDN_SLAVE_INIT);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

static bool is_node_running(char *hostaddr, int32 hostport)
{
	StringInfoData port;
	int ret;

	initStringInfo(&port);
	appendStringInfo(&port, "%d", hostport);

	ret = pingNode(hostaddr, port.data);
	if (ret != 0)
	{
		ereport(ERROR, (errmsg("its datanode master is not running.")));
        return false;
	}

	pfree(port.data);

    return true;
}

static void mgr_get_parent_appendnodeinfo(Oid nodemasternameoid, AppendNodeInfo *parentnodeinfo)
{
	Relation noderelation;
	HeapTuple mastertuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	char * hostaddr;
	bool isNull = false;

	noderelation = heap_open(NodeRelationId, AccessShareLock);

	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("could not find datanode master."))); 
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mgr_node);

	parentnodeinfo->nodename = NameStr(mgr_node->nodename);
	parentnodeinfo->nodetype = mgr_node->nodetype;
	parentnodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	parentnodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	parentnodeinfo->nodeport = mgr_node->nodeport;
	parentnodeinfo->nodehost = mgr_node->nodehost;

	/*get nodepath from tuple*/
	datumPath = heap_getattr(mastertuple, Anum_mgr_node_nodepath, RelationGetDescr(noderelation), &isNull);
	if (isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}

	parentnodeinfo->nodepath = TextDatumGetCString(datumPath);
	hostaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);

	ReleaseSysCache(mastertuple);
	heap_close(noderelation, AccessShareLock);
	pfree(hostaddr);
}

static void mgr_alter_pgxc_node(PG_FUNCTION_ARGS, char *nodename, Oid nodehostoid, int32 nodeport)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	ManagerAgent *ma;
	Form_mgr_node mgr_node;
	StringInfoData psql_cmd;
	bool execok = false;
	StringInfoData buf;
	char *user = NULL;
	char *address = NULL;

	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodename));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 2, key);
	info->lcp = NULL;

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return ;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	/* connection agent */
	ma = ma_connect_hostoid(mgr_node->nodehost);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	initStringInfo(&psql_cmd);
	user = get_hostuser_from_hostoid(nodehostoid);
	address = get_hostaddress_from_hostoid(nodehostoid);
	appendStringInfo(&psql_cmd, " -h %s -p %u -d %s -U %s -a -c \""
					,address
					,nodeport
					,DEFAULT_DB
					,user);

	appendStringInfo(&psql_cmd, " ALTER NODE \\\"%s\\\" WITH (TYPE = 'coordinator', HOST='%s', PORT=%d);"
					,nodename
					,address
					,nodeport);
	pfree(user);
	pfree(address);
	appendStringInfo(&psql_cmd, " select pgxc_pool_reload();\"");

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
	ma_sendstring(&buf, psql_cmd.data);
	pfree(psql_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void mgr_add_hbaconf_all(char *dnusername, char *dnaddr)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	HeapTuple tuple;
	Datum datumPath;
	bool isNull;
	Form_mgr_node mgr_node;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 2, key);
	info->lcp =NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}

		mgr_add_oneline_info_pghbaconf(2, "all", dnusername, dnaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							TextDatumGetCString(datumPath),
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);

		mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void mgr_add_hbaconf(char nodetype, char *dnusername, char *dnaddr)
{

	InitNodeInfo *info;
	ScanKeyData key[2];
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	HeapTuple tuple;
	Datum datumPath;
	bool isNull;
	Form_mgr_node mgr_node;
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 2, key);
	info->lcp =NULL;

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return ;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	/*get nodepath from tuple*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if (isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}

	mgr_add_oneline_info_pghbaconf(2, "all", dnusername, dnaddr, 32, "trust", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							TextDatumGetCString(datumPath),
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);

	mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void mgr_reload_conf(Oid hostoid, char *nodepath)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg, buf;
	GetAgentCmdRst getAgentCmdRst;
	bool execok;

	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&sendstrmsg, " reload -D %s", nodepath); /* pg_ctl reload -D pathdir */

	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
        
        ereport(ERROR, (errmsg("could not connect socket for agent.")));
		return;
	}
	getAgentCmdRst.ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_NODE_RELOAD);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

static void mgr_set_inited_incluster(char *nodename, char nodetype, bool checkvalue, bool setvalue)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodename));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(checkvalue));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(checkvalue));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 4, key);
	info->lcp =NULL;

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return ;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	mgr_node->nodeinited = setvalue;
	mgr_node->nodeincluster = setvalue;
	heap_inplace_update(info->rel_node, tuple);

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void mgr_rm_dumpall_temp_file(Oid dnhostoid)
{
	StringInfoData cmd_str;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;

	initStringInfo(&cmd_str);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	appendStringInfo(&cmd_str, "rm -f %s", PG_DUMPALL_TEMP_FILE);

	/* connection agent */
	ma = ma_connect_hostoid(dnhostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_RM);
	ma_sendstring(&buf, cmd_str.data);
	pfree(cmd_str.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

static void mgr_create_node_on_all_coord(PG_FUNCTION_ARGS, char *dnname, Oid dnhostoid, int32 dnport)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	ManagerAgent *ma;
	Form_mgr_node mgr_node;
	StringInfoData psql_cmd;
	bool execok = false;
	StringInfoData buf;
	char *addressconnect = NULL;
	char *addressnode = NULL;
	char *user = NULL;

	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
    
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 2, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		
		/* connection agent */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);
			return;
		}

		initStringInfo(&psql_cmd);
		addressconnect = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		appendStringInfo(&psql_cmd, " -h %s -p %u -d %s -U %s -a -c \""
						,addressconnect
						,mgr_node->nodeport
						,DEFAULT_DB
						,user);
		
		addressnode = get_hostaddress_from_hostoid(dnhostoid);
		appendStringInfo(&psql_cmd, " CREATE NODE \\\"%s\\\" WITH (TYPE = 'coordinator', HOST='%s', PORT=%d);"
						,dnname
						,addressnode
						,dnport);
		appendStringInfo(&psql_cmd, " select pgxc_pool_reload();\"");

		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf, psql_cmd.data);
		pfree(psql_cmd.data);
		pfree(addressconnect);
		pfree(addressnode);
		pfree(user);
		ma_endmessage(&buf, ma);

		if (! ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);
			return;
		}

		/*check the receive msg*/
		execok = mgr_recv_msg(ma, &getAgentCmdRst);
		Assert(execok == getAgentCmdRst.ret);
		ma_close(ma);
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;

	initStringInfo(&start_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));
    
	switch (nodetype)
	{
		case CNDN_TYPE_COORDINATOR_MASTER:
			appendStringInfo(&start_cmd, " start -Z coordinator -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		case CNDN_TYPE_DATANODE_MASTER:
		case CNDN_TYPE_DATANODE_SLAVE:
		case CNDN_TYPE_DATANODE_EXTRA:
			appendStringInfo(&start_cmd, " start -Z datanode -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		case GTM_TYPE_GTM_SLAVE:
		case GTM_TYPE_GTM_EXTRA:
			appendStringInfo(&start_cmd, " start -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		default:
			ereport(ERROR, (errmsg("node type \"%c\" not exist.", nodetype)));
			break;
	}

	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);

		ereport(ERROR, (errmsg("could not connect socket for agent.")));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_DN_START);
	ma_sendstring(&buf, start_cmd.data);
	pfree(start_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

static void mgr_stop_node_with_restoremode(const char *nodepath, Oid hostoid)
{
	StringInfoData stop_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;

	initStringInfo(&stop_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	appendStringInfo(&stop_cmd, " stop -Z restoremode -D %s", nodepath);

	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_DN_STOP);
	ma_sendstring(&buf, stop_cmd.data);
	pfree(stop_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

static void mgr_pg_dumpall_input_node(const Oid dn_master_oid, const int32 dn_master_port)
{
	StringInfoData pgsql_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	char *dn_master_addr;
	bool execok = false;

	initStringInfo(&pgsql_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	dn_master_addr = get_hostaddress_from_hostoid(dn_master_oid);
	appendStringInfo(&pgsql_cmd, " -h %s -p %d -d %s -f %s", dn_master_addr, dn_master_port, DEFAULT_DB, PG_DUMPALL_TEMP_FILE);

	/* connection agent */
	ma = ma_connect_hostoid(dn_master_oid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
	ma_sendstring(&buf, pgsql_cmd.data);
	pfree(pgsql_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
	pfree(dn_master_addr);
}

static void mgr_start_node_with_restoremode(const char *nodepath, Oid hostoid)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;

	initStringInfo(&start_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	appendStringInfo(&start_cmd, " start -Z restoremode -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);

	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_DN_START);
	ma_sendstring(&buf, start_cmd.data);
	pfree(start_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

static void mgr_pg_dumpall(Oid hostoid, int32 hostport, Oid dnmasteroid)
{
	StringInfoData pg_dumpall_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;
	char * hostaddr;

	initStringInfo(&pg_dumpall_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	hostaddr = get_hostaddress_from_hostoid(hostoid);
	appendStringInfo(&pg_dumpall_cmd, " -h %s -p %d -s --include-nodes --dump-nodes -f %s", hostaddr, hostport, PG_DUMPALL_TEMP_FILE);

	/* connection agent */
	ma = ma_connect_hostoid(dnmasteroid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PGDUMPALL);
	ma_sendstring(&buf, pg_dumpall_cmd.data);
	pfree(pg_dumpall_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
	pfree(hostaddr);
}

static void mgr_get_active_hostoid_and_port(char node_type, Oid *hostoid, int32 *hostport, AppendNodeInfo *appendnodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * host;
	StringInfoData port;
	int ret;
	bool isNull;
	Datum datumPath;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 2, key);
	info->lcp =NULL;

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return ;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	host = get_hostaddress_from_hostoid(mgr_node->nodehost);

	initStringInfo(&port);
	appendStringInfo(&port, "%d", mgr_node->nodeport);
	ret = pingNode(host, port.data);
	if (ret == 0)
	{
		if (hostoid)
			*hostoid = mgr_node->nodehost;
		if (hostport)
			*hostport = mgr_node->nodeport;
	}

	if (node_type == CNDN_TYPE_DATANODE_MASTER)
	{
		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		mgr_add_oneline_info_pghbaconf(2, "all", appendnodeinfo->nodeusername, appendnodeinfo->nodeaddr,
										32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								TextDatumGetCString(datumPath),
								&infosendmsg,
								mgr_node->nodehost,
								&getAgentCmdRst);
		
		mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(host);
}

static void mgr_get_agtm_host_and_port(StringInfo infosendmsg)
{
	InitNodeInfo *info;
	ScanKeyData key[1];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * agtm_host;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(GTM_TYPE_GTM_MASTER));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 1, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("node type is not exist agtm master in node table.")));
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	agtm_host = get_hostname_from_hostoid(mgr_node->nodehost);
    
	mgr_append_pgconf_paras_str_quotastr("agtm_host", agtm_host, infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", mgr_node->nodeport, infosendmsg);

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(agtm_host);
}

static void mgr_get_other_parm(char node_type, StringInfo infosendmsg)
{
	mgr_append_pgconf_paras_str_str("synchronous_commit", "on", infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", infosendmsg);
	mgr_append_pgconf_paras_str_int("max_wal_senders", MAX_WAL_SENDERS_NUM, infosendmsg);
	mgr_append_pgconf_paras_str_int("wal_keep_segments", WAL_KEEP_SEGMENTS_NUM, infosendmsg);
	mgr_append_pgconf_paras_str_str("wal_level", WAL_LEVEL_MODE, infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("listen_addresses", "*", infosendmsg);
	mgr_append_pgconf_paras_str_int("max_prepared_transactions", MAX_PREPARED_TRANSACTIONS_DEFAULT, infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("log_destination", "stderr", infosendmsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("log_line_prefix", "%u %d %h %m %e %x", infosendmsg);
}

static void mgr_get_appendnodeinfo(char node_type, AppendNodeInfo *appendnodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
    char * hostaddr;
	bool isNull = false;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(appendnodeinfo->nodename));
    
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));


	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 4, key);
	info->lcp =NULL;
	
	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		switch (node_type)
		{
		case CNDN_TYPE_COORDINATOR_MASTER:
			ereport(ERROR, (errmsg("can not find the coordinator %s (initialized =false and incluster = false) in node table", appendnodeinfo->nodename)));
			break;
		case CNDN_TYPE_DATANODE_MASTER:
			ereport(ERROR, (errmsg("can not find the datanode master %s (initialized =false and incluster = false) in node table", appendnodeinfo->nodename)));
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			ereport(ERROR, (errmsg("can not find the datanode slave %s (initialized =false and incluster = false) in node table", appendnodeinfo->nodename)));
			break;
		case CNDN_TYPE_DATANODE_EXTRA:
			ereport(ERROR, (errmsg("can not find the datanode extra %s (initialized =false and incluster = false) in node table", appendnodeinfo->nodename)));
			break;
		case GTM_TYPE_GTM_SLAVE:
			ereport(ERROR, (errmsg("can not find the gtm slave %s (initialized =false and incluster = false) in node table", appendnodeinfo->nodename)));
			break;
		case GTM_TYPE_GTM_EXTRA:
			ereport(ERROR, (errmsg("can not find the gtm extra %s (initialized =false and incluster = false) in node table", appendnodeinfo->nodename)));
			break;
		default:
			ereport(ERROR, (errmsg("node type \"%c\" not exist.", node_type)));
			break;
		}
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	appendnodeinfo->nodetype = mgr_node->nodetype;
	appendnodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	appendnodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	appendnodeinfo->nodeport = mgr_node->nodeport;
	appendnodeinfo->nodehost = mgr_node->nodehost;
    appendnodeinfo->nodemasteroid = mgr_node->nodemasternameoid;

	/*get nodepath from tuple*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if (isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	appendnodeinfo->nodepath = TextDatumGetCString(datumPath);
	hostaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
    pfree(hostaddr);
}

static void mgr_append_init_cndnmaster(AppendNodeInfo *appendnodeinfo)
{
	StringInfoData  infosendmsg;
	ManagerAgent *ma;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	bool execok = false;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));

	/*init datanode*/
	appendStringInfo(&infosendmsg, " -D %s", appendnodeinfo->nodepath);
	appendStringInfo(&infosendmsg, " --nodename %s -E UTF8 --locale=C", appendnodeinfo->nodename);

	/* connection agent */
	ma = ma_connect_hostoid(appendnodeinfo->nodehost);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*send cmd*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_CNDN_CNDN_INIT);
	ma_sendstring(&buf, infosendmsg.data);
	pfree(infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	ma_close(ma);
}

/*
* failover datanode slave dnname: PG_GETARG_CSTRING(0) is "slave"
* failover datanode extra dnname: PG_GETARG_CSTRING(0) is "extra"
* failover datanode dnname: PG_GETARG_CSTRING(0) is "either", if datanode slave dnname exists, using datanode slave dnname; 
* otherwise using datanode extra dnname
*/
Datum mgr_failover_one_dn(PG_FUNCTION_ARGS)
{
	char *typestr = PG_GETARG_CSTRING(0);
	char *nodename = PG_GETARG_CSTRING(1);
	char cmdtype = AGT_CMD_DN_FAILOVER;
	char nodetype;
	bool nodetypechange = false;
	if (strcmp(typestr, "slave") == 0)
	{
		nodetype = CNDN_TYPE_DATANODE_SLAVE;
	}
	else if (strcmp(typestr, "extra") == 0)
	{
		nodetype = CNDN_TYPE_DATANODE_EXTRA;
	}
	else if (strcmp(typestr, "either") == 0)
	{
		nodetype = CNDN_TYPE_DATANODE_SLAVE;
		nodetypechange = true;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("no such node type: %s", typestr)));
	}

	return mgr_failover_one_dn_inner_func(nodename, cmdtype, nodetype, nodetypechange);
}

/*
* inner function, userd for node failover
*/
static Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange)
{
	Relation rel_node;
	HeapTuple aimtuple;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	char nodetypesecond = CNDN_TYPE_NONE_TYPE;
	ScanKeyData key[1];
	HeapScanDesc scan;
	Form_mgr_node mgr_node;

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*failover datanode [slave|extra] dnname*/
	if (AGT_CMD_DN_FAILOVER == cmdtype)
	{
		aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodename, nodetype);
		if (!HeapTupleIsValid(aimtuple))
		{
			/*cannot find datanode slave, so find datanode extra*/
			if (nodetypechange)
			{
				switch(nodetype)
				{
					case CNDN_TYPE_DATANODE_SLAVE:
						nodetypesecond = CNDN_TYPE_DATANODE_EXTRA;
						break;
					case CNDN_TYPE_DATANODE_EXTRA:
						nodetypesecond = CNDN_TYPE_DATANODE_SLAVE;
						break;
					default:
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
							,errmsg("no such node type: %c", nodetype)));
				}
				aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodename, nodetypesecond);
			}
			if (!HeapTupleIsValid(aimtuple))
			{
				heap_close(rel_node, RowExclusiveLock);
				if (nodetype == CNDN_TYPE_DATANODE_SLAVE && !nodetypechange)
					ereport(ERROR, (errmsg("datanode slave \"%s\" does not exist", nodename)));
				else if (nodetype == CNDN_TYPE_DATANODE_EXTRA && !nodetypechange)
					ereport(ERROR, (errmsg("datanode extra \"%s\" does not exist", nodename)));
				else
					ereport(ERROR, (errmsg("datanode slave or extra \"%s\" does not exist", nodename)));
			}
		}
	}
	/*failover gtm [slave|extra]*/
	else if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype)
	{
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
		while ((aimtuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			break;
		}
		if (!HeapTupleIsValid(aimtuple))
		{
			/*cannot find gtm slave, so find gtm extra*/
			if (nodetypechange)
			{
				switch(nodetype)
				{
					case GTM_TYPE_GTM_SLAVE:
						nodetypesecond = GTM_TYPE_GTM_EXTRA;
						break;
					case GTM_TYPE_GTM_EXTRA:
						nodetypesecond = GTM_TYPE_GTM_SLAVE;
						break;
					default:
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
							,errmsg("no such node type: %c", nodetype)));
				}

				ScanKeyInit(&key[0]
					,Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetypesecond));
				heap_endscan(scan);
				scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
				while ((aimtuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
				{
					break;
				}
			}
			if (!HeapTupleIsValid(aimtuple))
			{
				heap_close(rel_node, RowExclusiveLock);
				if (nodetype == GTM_TYPE_GTM_SLAVE && !nodetypechange)
					ereport(ERROR, (errmsg("gtm slave does not exist")));
				else if (nodetype == GTM_TYPE_GTM_EXTRA && !nodetypechange)
					ereport(ERROR, (errmsg("gtm extra does not exist")));
				else
					ereport(ERROR, (errmsg("gtm slave or extra does not exist")));				
			}
		}
	}
	/* not support the comamnd type*/
	else
	{
		ereport(ERROR, (errmsg("not support this command: %c", cmdtype)));
	}
	
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, rel_node, aimtuple, takeplaparm_n);
	/*get nodename of the return result*/
	if (AGT_CMD_DN_FAILOVER == cmdtype)
	{
		namestrcpy(&(getAgentCmdRst.nodename), nodename);
	}
	else if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		namestrcpy(&(getAgentCmdRst.nodename),NameStr(mgr_node->nodename));
	}
	
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	if (AGT_CMD_DN_FAILOVER == cmdtype)
	{
		heap_freetuple(aimtuple);
	}
	else if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype)
	{
		heap_endscan(scan);
	}
	pfree(getAgentCmdRst.description.data);
	heap_close(rel_node, RowExclusiveLock);
	return HeapTupleGetDatum(tup_result);
}

/*check all the given nodename are datanode slaves*/
void 
check_dn_slave(char nodetype, List *nodenamelist, Relation rel_node, StringInfo infosendmsg)
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
		,CharGetDatum(nodetype));
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
			,errmsg("datanode master does not exist")));
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
		appendStringInfo(&infosendmsg, "alter node \\\"%s\\\" with(host='%s',port=%d, primary = %s);", cndnname, cndnaddress, cndnport, isprimary != 0 ? "true":"false");
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
			ma_close(ma);
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
	char *address = NULL;
	char *addressout = NULL;


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

		address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
		if (strcmp(NameStr(mgr_node_in->nodename), NameStr(mgr_node_out->nodename)) == 0)
		{
			appendStringInfo(&cmdstring, "ALTER NODE \\\"%s\\\" WITH (HOST='%s', PORT=%d);"
							,NameStr(mgr_node_in->nodename)
							,address
							,mgr_node_in->nodeport);
		}
		else
		{
			appendStringInfo(&cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='coordinator', HOST='%s', PORT=%d);"
							,NameStr(mgr_node_in->nodename)
							,address
							,mgr_node_in->nodeport);
		}
		pfree(address);
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
	
	addressout = get_hostaddress_from_hostoid(mgr_node_out->nodehost);
	while ((tuple_dn = heap_getnext(info_dn->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node_dn = (Form_mgr_node)GETSTRUCT(tuple_dn);
		Assert(mgr_node_dn);

		address = get_hostaddress_from_hostoid(mgr_node_dn->nodehost);
		if (mgr_node_dn->nodeprimary)
		{
			if (strcmp(address, addressout) == 0)
			{
				appendStringInfo(&cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY, PREFERRED);"
								,NameStr(mgr_node_dn->nodename)
								,address
								,mgr_node_dn->nodeport);
			}
			else
			{
				appendStringInfo(&cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='datanode', HOST='%s', PORT=%d, PRIMARY);"
								,NameStr(mgr_node_dn->nodename)
								,address
								,mgr_node_dn->nodeport);
			}
		}
		else
		{
			if (strcmp(address, addressout) == 0)
			{
				appendStringInfo(&cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='datanode', HOST='%s', PORT=%d,PREFERRED);"
								,NameStr(mgr_node_dn->nodename)
								,address
								,mgr_node_dn->nodeport);
			}
			else
			{
				appendStringInfo(&cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='datanode', HOST='%s', PORT=%d);"
								,NameStr(mgr_node_dn->nodename)
								,address
								,mgr_node_dn->nodeport);
			}
		}
		pfree(address);
	}

	pfree(addressout);
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
		goto func_end;
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
		goto func_end;
	}

	/*check the receive msg*/
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	func_end:
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
		ma_close(ma);
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
	Relation rel_node;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	ScanKeyData key[1];
	HeapTuple tuple;
	bool gettuple = false;
	/*get the gtm_port, gtm_host*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(GTM_TYPE_GTM_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		gtm_host = get_hostaddress_from_hostoid(mgr_node->nodehost);
		gettuple = true;
		break;
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	if(!gettuple)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("gtm master does not exist")));
	}
	mgr_append_pgconf_paras_str_quotastr("agtm_host", gtm_host, infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", mgr_node->nodeport, infosendmsg);
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
void mgr_add_parameters_pgsqlconf(Oid tupleOid, char nodetype, int cndnport, StringInfo infosendparamsg)
{
	char *slavename = NULL;
	if(nodetype == CNDN_TYPE_DATANODE_MASTER || nodetype == GTM_TYPE_GTM_MASTER)
		slavename = mgr_get_slavename(tupleOid, nodetype);
	/*refresh postgresql.conf of this node*/
	if (slavename != NULL)
	{
		pfree(slavename);
	}
	if(nodetype == CNDN_TYPE_DATANODE_SLAVE || nodetype == CNDN_TYPE_DATANODE_EXTRA || nodetype == GTM_TYPE_GTM_SLAVE || nodetype == GTM_TYPE_GTM_EXTRA)
	{
		mgr_append_pgconf_paras_str_str("hot_standby", "on", infosendparamsg);
	}

	mgr_append_pgconf_paras_str_str("synchronous_commit", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", infosendparamsg);
	mgr_append_pgconf_paras_str_int("max_wal_senders", MAX_WAL_SENDERS_NUM, infosendparamsg);
	mgr_append_pgconf_paras_str_int("wal_keep_segments", WAL_KEEP_SEGMENTS_NUM, infosendparamsg);
	mgr_append_pgconf_paras_str_str("wal_level", WAL_LEVEL_MODE, infosendparamsg);	mgr_append_pgconf_paras_str_int("port", cndnport, infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("listen_addresses", "*", infosendparamsg);
	mgr_append_pgconf_paras_str_int("max_prepared_transactions", MAX_PREPARED_TRANSACTIONS_DEFAULT, infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_destination", "stderr", infosendparamsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_line_prefix", "%u %d %h %m %e %x", infosendparamsg);
	/*agtm postgresql.conf does not need these*/
	if(GTM_TYPE_GTM_MASTER != nodetype && GTM_TYPE_GTM_SLAVE != nodetype && GTM_TYPE_GTM_EXTRA != nodetype)
	{
		mgr_get_gtm_host_port(infosendparamsg);
	}
}

/*
* the parameters which need refresh for recovery.conf
*/
void mgr_add_parameters_recoveryconf(char nodetype, char *slavename, Oid tupleoid, StringInfo infosendparamsg)
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
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(tupleoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errmsg("node oid \"%u\" not exist", tupleoid)
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
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	if (GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
	{
		username =AGTM_USER;
	}
	else
	{
		username = NameStr(mgr_host->hostuser);
	}
	ReleaseSysCache(tup);
	
	/*primary_conninfo*/
	initStringInfo(&primary_conninfo_value);
	if (GTM_TYPE_GTM_SLAVE == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, masterport, username, "slave");
	else
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, masterport, username, "extra");
	mgr_append_pgconf_paras_str_str("recovery_target_timeline", "latest", infosendparamsg);
	mgr_append_pgconf_paras_str_str("standby_mode", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, infosendparamsg);
	pfree(primary_conninfo_value.data);
	pfree(masterhostaddress);
}

/*
* the parameters which need refresh for pg_hba.conf
* gtm : include all gtm master/slave/extra ip and all coordinators ip and datanode masters/slave/extra ip
* coordinator: include all coordinators ip
* datanode master: include all coordinators ip
*/
void mgr_add_parameters_hbaconf(HeapTuple aimtuple, char nodetype, StringInfo infosendhbamsg)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	Oid hostoid;
	char *cnuser;
	char *cnaddress;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	Oid masterOid;
	
	/*get all coordinator master ip*/
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
			if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype)
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", cnuser, cnaddress, 32, "trust", infosendhbamsg);
			pfree(cnuser);
			pfree(cnaddress);
		}
		heap_endscan(rel_scan);
		heap_close(rel_node, AccessShareLock);
	} /*get all coordinator master ip*/
	else if (CNDN_TYPE_DATANODE_MASTER == nodetype || GTM_TYPE_GTM_MASTER == nodetype)
	{
		rel_node = heap_open(NodeRelationId, AccessShareLock);
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
		/*for datanode or gtm replication*/
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
			if ( ((CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype || CNDN_TYPE_DATANODE_EXTRA == mgr_node->nodetype) && CNDN_TYPE_DATANODE_MASTER == nodetype)
				|| ((GTM_TYPE_GTM_SLAVE == mgr_node->nodetype || GTM_TYPE_GTM_EXTRA == mgr_node->nodetype) && GTM_TYPE_GTM_MASTER == nodetype) )
			{
				if(HeapTupleIsValid(aimtuple))
				{
					masterOid = HeapTupleGetOid(aimtuple);
					if (masterOid == mgr_node->nodemasternameoid)
					{
						if (GTM_TYPE_GTM_MASTER == nodetype)
							mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, cnaddress, 32, "trust", infosendhbamsg);
						else
							mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", cnuser, cnaddress, 32, "trust", infosendhbamsg);
					}
				}
			}
			pfree(cnuser);
			pfree(cnaddress);
		}
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype)
			{
				/*hostoid*/
				hostoid = mgr_node->nodehost;
				/*database user for this coordinator*/
				cnuser = get_hostuser_from_hostoid(hostoid);
				/*get address*/
				cnaddress = get_hostaddress_from_hostoid(hostoid);
				if (GTM_TYPE_GTM_MASTER == nodetype)
					mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, cnaddress, 32, "trust", infosendhbamsg);
				else
					mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", cnuser, cnaddress, 32, "trust", infosendhbamsg);
				pfree(cnuser);
				pfree(cnaddress);
			}
			else if ((CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype || CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype 
				|| CNDN_TYPE_DATANODE_EXTRA == mgr_node->nodetype) && GTM_TYPE_GTM_MASTER == nodetype)
			{
				/*hostoid*/
				hostoid = mgr_node->nodehost;
				/*database user for this coordinator*/
				cnuser = get_hostuser_from_hostoid(hostoid);
				/*get address*/
				cnaddress = get_hostaddress_from_hostoid(hostoid);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, cnaddress, 32, "trust", infosendhbamsg);
				pfree(cnuser);
				pfree(cnaddress);
			}
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

/*
* get slave string used for synchronous_standby_names, if the master has only slave, the func will return 'slave', if has only extra, the func will return 'extra', if has slave and extra, the func will return 'slave,extra'
*/
char *mgr_get_slavename(Oid tupleOid, char nodetype)
{
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Relation rel_node;
	HeapScanDesc rel_scan;
	char *slavename = NULL;
	StringInfoData strinfoslavename;
	bool getslave = false;
	bool getextra = false;
	
	initStringInfo(&strinfoslavename);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);	
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodemasternameoid == tupleOid)
		{
			if (GTM_TYPE_GTM_MASTER == nodetype)
			{
				if (GTM_TYPE_GTM_SLAVE == mgr_node->nodetype)
					getslave = true;
				else if (GTM_TYPE_GTM_EXTRA == mgr_node->nodetype)
					getextra = true;
			}
			else if (CNDN_TYPE_DATANODE_MASTER == nodetype)
			{
				if(CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype)
					getslave = true;
				else if (CNDN_TYPE_DATANODE_EXTRA == mgr_node->nodetype)
					getextra = true;
			}
		}
	}
	if (getslave && !getextra)
		appendStringInfo(&strinfoslavename,"%s","slave");
	else if (!getslave && getextra)
		appendStringInfo(&strinfoslavename,"%s","extra");
	else if (getslave && getextra)
		appendStringInfo(&strinfoslavename,"%s","slave,extra");
		
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	if (!getslave && !getextra)
		return NULL;
	else 
	{
		slavename = pstrdup(strinfoslavename.data);
		pfree(strinfoslavename.data);
		return slavename;
	}
}

/*the function used to rename recovery.done to recovery.conf*/
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
		ma_close(ma);
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
		if (mgr_node->nodeinited)
		{
			mgr_node->nodeincluster = true;
			heap_inplace_update(rel, tuple);
		}
	}
	heap_endscan(rel_scan);
}

/*
* gtm failover
*/
Datum mgr_failover_gtm(PG_FUNCTION_ARGS)
{
	char *typestr = PG_GETARG_CSTRING(0);
	char cmdtype = AGT_CMD_GTM_SLAVE_FAILOVER;
	char nodetype = GTM_TYPE_GTM_SLAVE;
	bool nodetypechange = false;
	char *nodename = "gtm"; /*just use for input parameter*/
	
	if (strcmp(typestr, "slave") == 0)
	{
		nodetype = GTM_TYPE_GTM_SLAVE;
	}
	else if (strcmp(typestr, "extra") == 0)
	{
		nodetype = GTM_TYPE_GTM_EXTRA;
	}
	else if (strcmp(typestr, "either") == 0)
	{
		nodetype = GTM_TYPE_GTM_SLAVE;
		nodetypechange = true;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("no such gtm type: %s", typestr)));
	}
	
	return mgr_failover_one_dn_inner_func(nodename, cmdtype, nodetype, nodetypechange);
}

/*
* after gtm slave promote to master, some work need to do: 
* 1.stop the old gtm master
* 2. refresh all coordinator/datanode postgresql.conf:agtm_port,agtm_host
* 3.delete old master record in node systbl
* 4.change slave type to master type
* 5.new gtm master: refresh postgresql.conf and restart it
* 6.refresh gtm extra nodemasternameoid in node systbl and recovery.confs and restart gtm extra
*/
static void mgr_after_gtm_failover_handle(char *hostaddress, int cndnport, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath)
{
	StringInfoData infosendmsg;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodetmp;
	Form_mgr_node mgr_node_dnmaster;
	HeapTuple tuple;
	HeapTuple mastertuple;
	Oid hostOidtmp;
	Oid hostOid;
	Oid nodemasternameoid;
	Oid newgtmtupleoid;
	Datum datumPath;
	Datum DatumStopDnMaster;
	bool isNull;
	char *cndnPathtmp;
	char *dnmastername;
	char *cndnname;
	char *strlabel;
	char aimtuplenodetype;
	char nodetype;
	ScanKeyData key[1];


	initStringInfo(&infosendmsg);
	newgtmtupleoid = HeapTupleGetOid(aimtuple);
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	hostOid = mgr_node->nodehost;
	nodemasternameoid = mgr_node->nodemasternameoid;
	aimtuplenodetype = mgr_node->nodetype;
	nodetype = (aimtuplenodetype == GTM_TYPE_GTM_SLAVE ? GTM_TYPE_GTM_EXTRA:GTM_TYPE_GTM_SLAVE);
	strlabel = (nodetype == GTM_TYPE_GTM_EXTRA ? "extra":"slave");
	/*get nodename*/
	cndnname = NameStr(mgr_node->nodename);
	
	/*1.stop the old gtm master*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("gtm master \"%s\" does not exist", cndnname)));
	}
	/*get master name*/
	mgr_node_dnmaster = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mgr_node_dnmaster);
	dnmastername = NameStr(mgr_node_dnmaster->nodename);
	DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_gtm_master, (Datum)0);
	if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
		ereport(WARNING, (errmsg("stop gtm master \"%s\" fail", dnmastername)));
	/*2.refresh all coordinator/datanode postgresql.conf:agtm_port,agtm_host*/
	/*get agtm_port,agtm_host*/
	resetStringInfo(&infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("agtm_host", hostaddress, &infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", cndnport, &infosendmsg);
	/*get all datanode master/slave/extra, coordinator path and hostoid to refresh postgresql.conf: agtm_port, agtm_host*/
	rel_scan = heap_beginscan(noderel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		if(mgr_nodetmp->nodeinited && (mgr_nodetmp->nodetype == CNDN_TYPE_COORDINATOR_MASTER || 
		mgr_nodetmp->nodetype == CNDN_TYPE_DATANODE_MASTER || mgr_nodetmp->nodetype == 
		CNDN_TYPE_DATANODE_SLAVE || mgr_nodetmp->nodetype == CNDN_TYPE_DATANODE_EXTRA))
		{
			hostOidtmp = mgr_nodetmp->nodehost;
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp")
					, errmsg("column cndnpath is null")));
			}
			cndnPathtmp = TextDatumGetCString(datumPath);
			resetStringInfo(&(getAgentCmdRst->description));		
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPathtmp, &infosendmsg, hostOidtmp, getAgentCmdRst);	
		}
	}
	heap_endscan(rel_scan);
	/*3.delete old master record in node systbl*/
	simple_heap_delete(noderel, &mastertuple->t_self);
	CatalogUpdateIndexes(noderel, mastertuple);
	ReleaseSysCache(mastertuple);
	/*4.change slave or extra type to master type*/
	mgr_node->nodetype = GTM_TYPE_GTM_MASTER;
	mgr_node->nodemasternameoid = 0;
	heap_inplace_update(noderel, aimtuple);
	/*5. refresh new master postgresql.conf*/
	resetStringInfo(&infosendmsg);
	resetStringInfo(&(getAgentCmdRst->description));
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	/*restart gtm extra or slave*/
	resetStringInfo(&(getAgentCmdRst->description));
	mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, getAgentCmdRst, noderel, aimtuple, shutdown_f);
	if(!getAgentCmdRst->ret)
	{
		ereport(ERROR, (errmsg("agtm_ctl restart gtm %s fail", "master")));
	}
	/*6.update gtm extra or slave nodemasternameoid, refresh gtm extra recovery.conf*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		mgr_nodetmp->nodemasternameoid = newgtmtupleoid;
		heap_inplace_update(noderel, tuple);
		/*refresh gtm extra recovery.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, strlabel, HeapTupleGetOid(aimtuple), &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		/*get cndnPathtmp from tuple*/
		cndnPathtmp = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPathtmp, &infosendmsg, mgr_nodetmp->nodehost, getAgentCmdRst);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh agtm %s fail", strlabel)));
		}
		/*restart gtm extra or slave*/
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, getAgentCmdRst, noderel, tuple, shutdown_f);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("agtm_ctl restart gtm %s fail", strlabel)));
		}	
	}
	heap_endscan(rel_scan);

	pfree(infosendmsg.data);
}

/*
* after datanode slave/extra failover, some work need to do.
* cmd: failover datanode slave/extra dn1
* 1.if the cluster has slave and extra, change the mastername for datanode extra dn1 in node systbl 
* 2. change the datanode  extra dn1's recovery.conf:host,port
* 3. restart datanode extra dn1
*/
static void mgr_after_datanode_failover_handle(Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, char aimtuplenodetype)
{
	StringInfoData infosendmsg;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node_master;
	Form_mgr_node mgr_nodetmp;
	HeapTuple tuple;
	Oid masterhostOid;
	Oid newmastertupleoid;
	Datum datumPath;
	bool isNull;
	char *cndnPathtmp;
	char *strtmp;
	ScanKeyData key[2];
	char nodetype;
	NameData nodename;


	initStringInfo(&infosendmsg);
	newmastertupleoid = HeapTupleGetOid(aimtuple);
	mgr_node_master = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node_master);
	masterhostOid = mgr_node_master->nodehost;
	namecpy(&nodename,&(mgr_node_master->nodename));
	/*1.refresh master's postgresql.conf*/
	resetStringInfo(&(getAgentCmdRst->description));
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, masterhostOid, getAgentCmdRst);
	if(!getAgentCmdRst->ret)
	{
		ereport(LOG, (errmsg("refresh postgresql.conf of datanode %s master fail", NameStr(mgr_node_master->nodename))));
	}
	/*2.update datanode extra/slave nodemasternameoid, refresh recovery.conf, restart the node*/
	nodetype = (aimtuplenodetype == CNDN_TYPE_DATANODE_SLAVE ? CNDN_TYPE_DATANODE_EXTRA:CNDN_TYPE_DATANODE_SLAVE);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodename));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		/*update datanode extra/slave nodemasternameoid*/
		mgr_nodetmp->nodemasternameoid = newmastertupleoid;
		heap_inplace_update(noderel, tuple);
		/*refresh datanode extra/slave recovery.conf*/
		strtmp = (aimtuplenodetype == CNDN_TYPE_DATANODE_SLAVE ? "extra":"slave");
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, strtmp, HeapTupleGetOid(aimtuple), &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("datanode %s %s column cndnpath is null", NameStr(mgr_nodetmp->nodename), strtmp)));
		}
		/*get cndnPathtmp from tuple*/
		cndnPathtmp = TextDatumGetCString(datumPath);
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPathtmp, &infosendmsg, mgr_nodetmp->nodehost, getAgentCmdRst);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh recovery.conf of datanode %s %s fail", NameStr(mgr_nodetmp->nodename), strtmp)));
		}
		/*restart datanode extra/slave*/
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, getAgentCmdRst, noderel, tuple, shutdown_f);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("pg_ctl restart datanode %s %s fail", NameStr(mgr_nodetmp->nodename), strtmp)));
		}
	}
	heap_endscan(rel_scan);
	pfree(infosendmsg.data);
}

char *mgr_nodetype_str(char nodetype)
{
	char *nodestring;
	char *retstr;
		switch(nodetype)
	{
		case GTM_TYPE_GTM_MASTER:
			nodestring = "gtm master";
			break;
		case GTM_TYPE_GTM_SLAVE:
			nodestring = "gtm slave";
			break;
		case GTM_TYPE_GTM_EXTRA:
			nodestring = "gtm extra";
			break;
		case CNDN_TYPE_COORDINATOR_MASTER:
			nodestring = "coordinator master";
			break;
		case CNDN_TYPE_COORDINATOR_SLAVE:
			nodestring = "coordinator slave";
			break;
		case CNDN_TYPE_DATANODE_MASTER:
			nodestring = "datanode master";
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			nodestring = "datanode slave";
			break;
		case CNDN_TYPE_DATANODE_EXTRA:
			nodestring = "datanode extra";
			break;
		default:
			nodestring = "none node type";
			/*never come here*/
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				, errmsg("node is not recognized")
				, errhint("option type is gtm or coordinator or datanode master/slave/extra")));
			break;
	}
	retstr = pstrdup(nodestring);
	return retstr;
}

/*
* clean all: 1. check the database cluster running, if it running(check gtm master), give the tip: stop cluster first; if not 
* running, clean node. clean gtm, clean coordinator, clean datanode master, clean datanode slave
*/
Datum mgr_clean_all(PG_FUNCTION_ARGS)
{
	Relation rel_node;
	Form_mgr_node mgr_node;
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	StringInfoData strinfoport;
	HeapTuple tuple;
	int ismasterrunning;
	char *hostaddress;

	/*check the cluster running or not, if it is running, stop the dbcluster first*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(GTM_TYPE_GTM_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		hostaddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
		initStringInfo(&strinfoport);
		appendStringInfo(&strinfoport, "%d", mgr_node->nodeport);
		ismasterrunning = pingNode(hostaddress, strinfoport.data);
		pfree(hostaddress);
		pfree(strinfoport.data);
		if (0 == ismasterrunning)
		{
			ereport(ERROR, (errmsg("The ADB cluster is still running. Please stop it first!")));
		}
		break;
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	/*clean gtm master/slave/extra, clean coordinator, clean datanode master/slave/extra*/
	return mgr_prepare_clean_all(fcinfo);
}

/*clean the node folder*/
static void mgr_clean_node_folder(char cmdtype, Oid hostoid, char *nodepath, GetAgentCmdRst *getAgentCmdRst)
{
	StringInfoData buf;
	StringInfoData infosendmsg;
	ManagerAgent *ma;
	
	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst->description));
	initStringInfo(&buf);
	appendStringInfo(&infosendmsg, "rm -rf %s; mkdir -p %s; chmod 0700 %s", nodepath, nodepath, nodepath);
	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
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

/*clean all node: gtm/datanode/coordinator which in cluster*/
static Datum mgr_prepare_clean_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tuple;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	Datum datumpath;
	GetAgentCmdRst getAgentCmdRst;
	ScanKeyData key[1];
	char *nodepath;
	bool isNull;
	char cmdtype = AGT_CMD_CLEAN_NODE;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
					ScanKeyInit(&key[0],
						Anum_mgr_node_nodeincluster
						,BTEqualStrategyNumber
						,F_BOOLEQ
						,BoolGetDatum(true));
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
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

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	/*clean one node folder*/
	datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("%s %s column cndnpath is null", mgr_nodetype_str(mgr_node->nodetype),  NameStr(mgr_node->nodename))));
	}
	/*get nodepath from tuple*/
	nodepath = TextDatumGetCString(datumpath);
	mgr_clean_node_folder(cmdtype, mgr_node->nodehost, nodepath, &getAgentCmdRst);
	/*update node systbl, set inited and incluster to false*/
	if ( true == getAgentCmdRst.ret)
	{
		mgr_set_inited_incluster(NameStr(mgr_node->nodename), mgr_node->nodetype, true, false);
	}
	tup_result = build_common_command_tuple_for_monitor(
		&(mgr_node->nodename)
		,mgr_node->nodetype
		,getAgentCmdRst.ret
		,getAgentCmdRst.description.data
		);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*check the oid has been used by slave or extra*/
static bool mgr_node_has_slave_extra(Relation rel, Oid mastertupeoid)
{
	ScanKeyData key[1];
	HeapTuple tuple;
	HeapScanDesc scan;
	
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameOid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupeoid));
	scan = heap_beginscan(rel, SnapshotNow, 1, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		heap_endscan(scan);
		return true;
	}
	heap_endscan(scan);
	return false;
}
