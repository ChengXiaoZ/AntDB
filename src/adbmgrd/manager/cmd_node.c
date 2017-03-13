/*
 * commands of node
 */
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include<pwd.h>

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/pg_authid.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/mgr_updateparm.h"
#include "catalog/mgr_parm.h"
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
#include "utils/acl.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "nodes/makefuncs.h"

#define DEFAULT_DB "postgres"
#define MAX_PREPARED_TRANSACTIONS_DEFAULT	100
#define PG_DUMPALL_TEMP_FILE "/tmp/pg_dumpall_temp"
#define MAX_WAL_SENDERS_NUM	5
#define WAL_KEEP_SEGMENTS_NUM	32
#define WAL_LEVEL_MODE	"hot_standby"
#define APPEND_DNMASTER  1
#define APPEND_CNMASTER  2
#define SYNC            't'
#define ASYNC           'f'
#define SPACE           ' '
#define DEFAULT_WAIT	60

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
	Oid		tupleoid;
}AppendNodeInfo;
typedef enum 
{
	CONFIG,
	APPEND,
	FAILOVER
}pgxc_node_operator;

struct tuple_cndn
{
	List *coordiantor_list;
	List *datanode_list;
};
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
static bool mgr_get_active_hostoid_and_port(char node_type, Oid *hostoid, int32 *hostport, AppendNodeInfo *appendnodeinfo, bool set_ip);
static void mgr_pg_dumpall(Oid hostoid, int32 hostport, Oid dnmasteroid, char *temp_file);
static void mgr_stop_node_with_restoremode(const char *nodepath, Oid hostoid);
static void mgr_pg_dumpall_input_node(const Oid dn_master_oid, const int32 dn_master_port, char *temp_file);
static void mgr_rm_dumpall_temp_file(Oid dnhostoid,char *temp_file);
static void mgr_start_node_with_restoremode(const char *nodepath, Oid hostoid);
static void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid);
static void mgr_create_node_on_all_coord(PG_FUNCTION_ARGS, char nodetype, char *dnname, Oid dnhostoid, int32 dnport);
static void mgr_set_inited_incluster(char *nodename, char nodetype, bool checkvalue, bool setvalue);
static void mgr_add_hbaconf(char nodetype, char *dnusername, char *dnaddr);
static void mgr_add_hbaconf_all(char *dnusername, char *dnaddr);
static void mgr_after_gtm_failover_handle(char *hostaddress, int cndnport, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, PGconn **pg_conn, Oid cnoid);
static bool mgr_start_one_gtm_master(void);
static void mgr_after_datanode_failover_handle(Oid nodemasternameoid, Name cndnname, int cndnport, char *hostaddress, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, char aimtuplenodetype, PGconn **pg_conn, Oid cnoid);
static void mgr_get_parent_appendnodeinfo(Oid nodemasternameoid, AppendNodeInfo *parentnodeinfo);
static bool is_node_running(char *hostaddr, int32 hostport);
static void mgr_make_sure_all_running(char node_type);
static char *get_temp_file_name(void);
static void get_nodeinfo(char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
static void mgr_pgbasebackup(char nodetype, AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo);
static Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange, bool bforce);
static void mgr_clean_node_folder(char cmdtype, Oid hostoid, char *nodepath, GetAgentCmdRst *getAgentCmdRst);
static Datum mgr_prepare_clean_all(PG_FUNCTION_ARGS);
static bool mgr_node_has_slave_extra(Relation rel, Oid mastertupeoid);
static bool is_sync(char nodetype, char *nodename);
static void get_nodestatus(char nodetype, char *nodename, bool *is_exist, bool *is_sync);
static void mgr_set_master_sync(void);
static void mgr_alter_master_sync(char nodetype, char *nodename, bool new_sync);
static Datum get_failover_node_type(char *node_name, char slave_type, char extra_type, bool force);
static void mgr_get_cmd_head_word(char cmdtype, char *str);
static void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
static void mgr_check_appendnodeinfo(char node_type, char *append_node_name);
static struct tuple_cndn *get_new_pgxc_node(pgxc_node_operator cmd, char *node_name, char node_type);
static bool mgr_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst);
static void mgr_modify_port_after_initd(Relation rel_node, HeapTuple nodetuple, char *nodename, char nodetype, int32 newport);
static bool mgr_modify_node_parameter_after_initd(Relation rel_node, HeapTuple nodetuple, StringInfo infosendmsg, bool brestart);
static void mgr_modify_port_recoveryconf(Relation rel_node, HeapTuple aimtuple, int32 master_newport);
static bool mgr_modify_coord_pgxc_node(Relation rel_node, StringInfo infostrdata);
static void mgr_check_all_agent(void);
static bool mgr_add_extension_sqlcmd(char *sqlstr);
static char *get_username_list_str(List *user_list);
static void mgr_manage_flush(char command_type, char *user_list_str);
static void mgr_manage_stop_func(StringInfo commandsql);
static void mgr_manage_stop_view(StringInfo commandsql);
static void mgr_manage_stop(char command_type, char *user_list_str);
static void mgr_manage_deploy(char command_type, char *user_list_str);
static void mgr_manage_reset(char command_type, char *user_list_str);
static void mgr_manage_set(char command_type, char *user_list_str);
static void mgr_manage_alter(char command_type, char *user_list_str);
static void mgr_manage_drop(char command_type, char *user_list_str);
static void mgr_manage_add(char command_type, char *user_list_str);
static void mgr_manage_start(char command_type, char *user_list_str);
static void mgr_manage_show(char command_type, char *user_list_str);
static void mgr_manage_monitor(char command_type, char *user_list_str);
static void mgr_manage_init(char command_type, char *user_list_str);
static void mgr_manage_append(char command_type, char *user_list_str);
static void mgr_manage_failover(char command_type, char *user_list_str);
static void mgr_manage_clean(char command_type, char *user_list_str);
static void mgr_manage_list(char command_type, char *user_list_str);
static void mgr_check_username_valid(List *username_list);
static void mgr_check_command_valid(List *command_list);
void mgr_reload_conf(Oid hostoid, char *nodepath);
static List *get_username_list(void);
static void mgr_get_acl_by_username(char *username, StringInfo acl);
static bool mgr_acl_flush(char *username);
static bool mgr_acl_stop(char *username);
static bool mgr_acl_deploy(char *username);
static bool mgr_acl_reset(char *username);
static bool mgr_acl_set(char *username);
static bool mgr_acl_alter(char *username);
static bool mgr_acl_drop(char *username);
static bool mgr_acl_add(char *username);
static bool mgr_acl_start(char *username);
static bool mgr_acl_show(char *username);
static bool mgr_acl_monitor(char *username);
static bool mgr_acl_list(char *username);
static bool mgr_acl_append(char *username);
static bool mgr_acl_failover(char *username);
static bool mgr_acl_clean(char *username);
static bool mgr_acl_init(char *username);
static bool mgr_has_table_priv(char *rolename, char *tablename, char *priv_type);
static bool mgr_has_func_priv(char *rolename, char *funcname, char *priv_type);
static List *get_username_list(void);
static Oid mgr_get_role_oid_or_public(const char *rolname);
static void mgr_priv_all(char command_type, char *username_list_str);
extern void mgr_clean_hba_table(void);
static void mgr_lock_cluster(PGconn **pg_conn, Oid *cnoid);
static void mgr_unlock_cluster(PGconn **pg_conn);
static bool mgr_pqexec_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst, PGconn **pg_conn, Oid cnoid);
static int mgr_pqexec_boolsql_try_maxnum(PGconn **pg_conn, char *sqlstr, const int maxnum);
static bool mgr_extension_pg_stat_statements(char cmdtype, char *extension_name);
static void mgr_get_self_address(char *server_address, int server_port, Name self_address);
static bool mgr_check_node_recovery_finish(char nodetype, Oid hostoid, int nodeport, char *address);
static bool mgr_check_param_reload_postgresqlconf(char nodetype, Oid hostoid, int nodeport, char *address, char *check_param, char *expect_result);
static char mgr_get_other_type(char nodetype);
static bool mgr_check_sync_node_exist(Relation rel, Name nodename, char nodetype);
static bool mgr_check_node_path(Relation rel, Oid hostoid, char *path);
static bool mgr_check_node_port(Relation rel, Oid hostoid, int port);

#if (Natts_mgr_node != 9)
#error "need change code"
#endif

typedef struct InitNodeInfo
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitNodeInfo;

typedef struct InitAclInfo
{
	Relation rel_authid;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitAclInfo;

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
	if (mgr_has_priv_add())
	{
		DirectFunctionCall4(mgr_add_node_func,
									BoolGetDatum(node->if_not_exists),
									CharGetDatum(node->nodetype),
									CStringGetDatum(node->name),
									PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_add_node_func(PG_FUNCTION_ARGS)
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
	char pathstr[MAXPGPATH];
	NameData name;
	NameData mastername;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	ObjectAddress myself;
	ObjectAddress host;
	Oid cndn_oid;
	Oid hostoid;
	int32 port;
	char nodetype;   /*coordinator or datanode master/slave*/
	bool if_not_exists = PG_GETARG_BOOL(0);
	char *nodename = PG_GETARG_CSTRING(2);
	List *options = (List *)PG_GETARG_POINTER(3);
	NameData hostname;

	nodetype = PG_GETARG_CHAR(1);
	nodestring = mgr_nodetype_str(nodetype);
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	Assert(nodename);
	namestrcpy(&name, nodename);

	/*master/slave/extra has the same name*/
	namestrcpy(&mastername, nodename);

	/* check exists */
	checktuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
	if (HeapTupleIsValid(checktuple))
	{
		heap_freetuple(checktuple);
		if(if_not_exists)
		{
			heap_close(rel, RowExclusiveLock);
			ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("%s \"%s\" already exists, skipping", nodestring, NameStr(name))));
			PG_RETURN_BOOL(false);
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
	foreach(lc, options)
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
			tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&hostname));
			if(!HeapTupleIsValid(tuple))
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("host \"%s\" does not exist", defGetString(def))));
			}
			hostoid = HeapTupleGetOid(tuple);
			datum[Anum_mgr_node_nodehost-1] = ObjectIdGetDatum(hostoid);
			got[Anum_mgr_node_nodehost-1] = true;
			ReleaseSysCache(tuple);
		}else if(strcmp(def->defname, "port") == 0)
		{
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
			strncpy(pathstr, str, strlen(str)>MAXPGPATH ? MAXPGPATH:strlen(str));
		}else if(strcmp(def->defname, "sync") == 0)
		{
			if(got[Anum_mgr_node_nodesync-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);

			if(strcmp(str, "true") == 0 || strcmp(str, "on") == 0 || strcmp(str, "t") == 0)
			{
				/*check the other slave exist and is sync=t, then this can not be sync=t*/
				if (mgr_check_sync_node_exist(rel, &name, nodetype))
				{
					heap_close(rel, RowExclusiveLock);
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("not support this node as synchronous node now, its master already has synchronous node")));
				}
				datum[Anum_mgr_node_nodesync-1] = CharGetDatum(SYNC);
				got[Anum_mgr_node_nodesync-1] = true;
			}else if(strcmp(str, "false") == 0 || strcmp(str, "off") == 0 || strcmp(str, "f") == 0)
			{
				datum[Anum_mgr_node_nodesync-1] = CharGetDatum(ASYNC);
				got[Anum_mgr_node_nodesync-1] = true;
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"sync\": \"%s\", must be \"true|t|on\" or \"false|f|off\"", str)));
			}
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is host, port, sync and path")));
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

	/*check path not used*/
	if (mgr_check_node_path(rel, hostoid, pathstr))
	{
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("on host \"%s\" the path \"%s\" has already been used in node table", hostname.data, pathstr)
				,errhint("try \"list node\" for more infomation")));
	}

	if (mgr_check_node_port(rel, hostoid, port))
	{
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("on host \"%s\" the port \"%d\" has already been used in node table", hostname.data, port)
				,errhint("try \"list node\" for more infomation")));
	}

	if(got[Anum_mgr_node_nodesync-1] == false) /* default values for user do not set sync in add slave/extra. */
	{
		if(CNDN_TYPE_COORDINATOR_SLAVE == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || GTM_TYPE_GTM_SLAVE == nodetype)
		{
			if (!mgr_check_sync_node_exist(rel, &name, nodetype))
				datum[Anum_mgr_node_nodesync-1] = CharGetDatum(SYNC);
			else
				datum[Anum_mgr_node_nodesync-1] = CharGetDatum(ASYNC);
		}
		
		if(CNDN_TYPE_DATANODE_EXTRA == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
		{
			datum[Anum_mgr_node_nodesync-1] = CharGetDatum(ASYNC);
		} 
	}
	if(got[Anum_mgr_node_nodesync-1] == true) /* default values for user set sync in add gtm/coord/datanode master.  */
	{
		if(CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype || GTM_TYPE_GTM_MASTER == nodetype)
		{
			datum[Anum_mgr_node_nodesync-1] = CharGetDatum(SPACE);
		}
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
	PG_RETURN_BOOL(true);
}

void mgr_alter_node(MGRAlterNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_alter())
	{
		DirectFunctionCall4(mgr_alter_node_func,
									BoolGetDatum(node->if_not_exists),
									CharGetDatum(node->nodetype),
									CStringGetDatum(node->name),
									PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}

}

Datum mgr_alter_node_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple oldtuple;
	HeapTuple new_tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	char *nodestring;
	NameData name;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	bool old_sync;
	bool new_sync;
	int32 oldport;
	int32 newport;
	HeapTuple searchHostTuple;
	TupleDesc cndn_dsc;
	NameData hostname;
	char nodetype = '\0'; /*coordinator master/slave or datanode master/slave/extra*/
	Form_mgr_node mgr_node;
	//bool if_not_exists = PG_GETARG_BOOL(0);
	List *options = (List *)PG_GETARG_POINTER(3);
	char *name_str = PG_GETARG_CSTRING(2);
	Oid hostoid;
	Assert(name_str);
	nodetype = PG_GETARG_CHAR(1);
	nodestring = mgr_nodetype_str(nodetype);

	rel = heap_open(NodeRelationId, RowExclusiveLock);
	cndn_dsc = RelationGetDescr(rel);
	namestrcpy(&name, name_str);

	/* check exists */
	oldtuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
	if(!(HeapTupleIsValid(oldtuple)))
	{
		 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", nodestring, NameStr(name))));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(oldtuple);
	Assert(mgr_node);
	pfree(nodestring);
	oldport = mgr_node->nodeport;
	hostoid = mgr_node->nodehost;
	old_sync = ( 't' == mgr_node->nodesync ? true:false);
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_node_nodename-1] = NameGetDatum(&name);
	foreach(lc, options)
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
			newport = port;
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
		}else if(strcmp(def->defname, "sync") == 0)
		{
			if(got[Anum_mgr_node_nodesync-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);

			if(strcmp(str, "true") == 0 || strcmp(str, "on") == 0 || strcmp(str, "t") == 0)
			{
				/*check the other slave exist and is sync=t, then this can not be sync=t*/
				if (mgr_check_sync_node_exist(rel, &name, nodetype))
				{
					heap_freetuple(oldtuple);
					heap_close(rel, RowExclusiveLock);
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("not support this node as synchronous node now, its master already has synchronous node")));
				}
				datum[Anum_mgr_node_nodesync-1] = CharGetDatum(SYNC);
				got[Anum_mgr_node_nodesync-1] = true;
				new_sync = true;
			}else if(strcmp(str, "false") == 0 || strcmp(str, "off") == 0 || strcmp(str, "f") == 0)
			{				
				datum[Anum_mgr_node_nodesync-1] = CharGetDatum(ASYNC);
				got[Anum_mgr_node_nodesync-1] = true;
				new_sync = false;
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"sync\": \"%s\", must be \"true|t|on\" or \"false|f|off\"", str)));
			}			
		}else      
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is host, port, sync and path")));
		}
		datum[Anum_mgr_node_nodetype-1] = CharGetDatum(nodetype);
	}
	/*check port*/
	if (mgr_check_node_port(rel, mgr_node->nodehost, newport))
	{
		heap_freetuple(oldtuple);
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("on host \"%s\" the port \"%d\" has already been used in node table", get_hostname_from_hostoid(hostoid), newport)
				,errhint("try \"list node\" for more infomation")));
	}
	/*check this tuple initd or not, if it has inited and in cluster, check whether it can be alter*/		
	if(mgr_node->nodeincluster)
	{
		if((got[Anum_mgr_node_nodehost-1] == true)||(got[Anum_mgr_node_nodepath-1] == true))
		{
			heap_freetuple(oldtuple);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be changed", nodestring, NameStr(name))));
		}		
		if(got[Anum_mgr_node_nodesync-1] == true && old_sync != new_sync) 
			mgr_alter_master_sync(nodetype, NameStr(name), new_sync);	
		if (got[Anum_mgr_node_nodeport-1] == true && oldport != newport)
			mgr_modify_port_after_initd(rel, oldtuple, name.data, nodetype, newport);
	}
	
	new_tuple = heap_modify_tuple(oldtuple, cndn_dsc, datum,isnull, got);
	simple_heap_update(rel, &oldtuple->t_self, new_tuple);
	CatalogUpdateIndexes(rel, new_tuple);	
		
	heap_freetuple(oldtuple);
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
	PG_RETURN_BOOL(true);
}

void mgr_drop_node(MGRDropNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_drop())
	{
		DirectFunctionCall3(mgr_drop_node_func,
									BoolGetDatum(node->if_exists),
									CharGetDatum(node->nodetype),
									PointerGetDatum(node->names));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_drop_node_func(PG_FUNCTION_ARGS)
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
	bool if_exists = PG_GETARG_BOOL(0);
	List *name_list = (List *)PG_GETARG_POINTER(2);
	nodetype = PG_GETARG_CHAR(1);

	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP NODE"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, name_list)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name), nodetype);
		if(!HeapTupleIsValid(tuple))
		{
			if(if_exists)
			{
				nodestring = mgr_nodetype_str(nodetype);
				ereport(NOTICE,  (errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("%s \"%s\" does not exist, skipping", nodestring, NameStr(name))));
				pfree(nodestring);
				continue;
			}
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), NameStr(name))));
		}
		/*check this tuple initd or not, if it has inited and in cluster, cannot be dropped*/
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodeincluster)
		{
			heap_freetuple(tuple);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be dropped", mgr_nodetype_str(nodetype), NameStr(name))));
		}
		/*check the node has been used by its slave or extra*/
		if (CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype|| GTM_TYPE_GTM_MASTER == mgr_node->nodetype)
		{
			if (mgr_node_has_slave_extra(rel, HeapTupleGetOid(tuple)))
			{
				heap_freetuple(tuple);
				heap_close(rel, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
						 ,errmsg("%s \"%s\" has been used by slave or extra, cannot be dropped", mgr_nodetype_str(nodetype), NameStr(name))));
			}
		}
		nodenum++;
		/* todo chech used by other */
		heap_freetuple(tuple);
	}

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
	foreach(lc, name_list)
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
	PG_RETURN_BOOL(true);
}

/*
* execute init gtm master, send infomation to agent to init gtm master 
*/
Datum 
mgr_init_gtm_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_MASTER);
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* execute init gtm slave, send infomation to agent to init gtm slave 
*/
Datum 
mgr_init_gtm_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_SLAVE);
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_SLAVE_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
}
/*
* execute init gtm extra, send infomation to agent to init gtm extra 
*/
Datum 
mgr_init_gtm_extra(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_EXTRA);
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_SLAVE_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
}
/*
* init coordinator master dn1,dn2...
* init coordinator master all
*/
Datum 
mgr_init_cn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CNDN_CNDN_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo); 
}

/*
* init datanode master dn1,dn2...
* init datanode master all
*/
Datum 
mgr_init_dn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_CNDN_CNDN_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
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
	if(!HeapTupleIsValid(mastertuple))
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
	initStringInfo(&(getAgentCmdRst.description));
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
	if(!HeapTupleIsValid(mastertuple))
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
	initStringInfo(&(getAgentCmdRst.description));
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
	char *nodetypestr;
	char nodetype;
	Oid hostOid;
	Oid	masteroid;
	Oid	tupleOid;
	StringInfoData buf;
	StringInfoData infosendmsg;
	StringInfoData strinfocoordport;
	ManagerAgent *ma;
	bool initdone = false;
	bool isNull = false;
	bool ismasterrunning = false;
	Form_mgr_node mgr_node;
	int cndnport;
	Datum DatumStartDnMaster,
	DatumStopDnMaster;

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
		nodetypestr = mgr_nodetype_str(nodetype);
		appendStringInfo(&(getAgentCmdRst->description), "%s \"%s\" has been initialized", nodetypestr, cndnnametmp);
		getAgentCmdRst->ret = false;
		pfree(nodetypestr);
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
get_fcinfo_namelist(const char *sepstr, int argidx, FunctionCallInfo fcinfo)
{
	int i;
	char *nodename;
	List *nodenamelist = NIL;

	for (i = argidx; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			nodename = PG_GETARG_CSTRING(i);
			nodenamelist = lappend(nodenamelist, nodename);
		}
	}

	return nodenamelist;
}

/*
* start gtm master
*/
Datum mgr_start_gtm_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *nodename;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_MASTER);
	else
	{
		nodename = PG_GETARG_CSTRING(0);
		nodenamelist = lappend(nodenamelist, nodename);
	}

	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_START_MASTER, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* start one gtm master
*/
static bool mgr_start_one_gtm_master(void)
{
	GetAgentCmdRst getAgentCmdRst;
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
	mgr_runmode_cndn_get_result(AGT_CMD_GTM_START_MASTER, &getAgentCmdRst, rel_node, aimtuple, TAKEPLAPARM_N);
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
	List *nodenamelist = NIL;
	char *nodename;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_SLAVE);
	else
	{
		nodename = PG_GETARG_CSTRING(0);
		nodenamelist = lappend(nodenamelist, nodename);
	}

	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_START_SLAVE, nodenamelist, TAKEPLAPARM_N, fcinfo); 
}
/*
* start gtm extra
*/
Datum mgr_start_gtm_extra(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *nodename;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_EXTRA);
	else
	{
		nodename = PG_GETARG_CSTRING(0);
		nodenamelist = lappend(nodenamelist, nodename);
	}

	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_START_SLAVE, nodenamelist, TAKEPLAPARM_N, fcinfo);
}
/*
* start coordinator master dn1,dn2...
* start coordinator master all
*/
Datum mgr_start_cn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* start datanode master dn1,dn2...
* start datanode master all
*/
Datum mgr_start_dn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
	
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
	mgr_runmode_cndn_get_result(AGT_CMD_DN_START, &getAgentCmdRst, info->rel_node, aimtuple, TAKEPLAPARM_N);
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
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_SLAVE);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* start datanode extra dn1,dn2...
* start datanode extra all
*/
Datum mgr_start_dn_extra(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_EXTRA);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_EXTRA, AGT_CMD_DN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *shutdown_mode)
{
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_node_gtm;
	Datum datumPath;
	Datum DatumMaster;
	Datum DatumStopDnMaster;
	StringInfoData buf;
	StringInfoData infosendmsg;
	StringInfoData strinfoport;
	ManagerAgent *ma;
	bool isNull = false,
		execok = false;
	char *hostaddress;
	char *cndnPath;
	char *cmdmode;
	char *zmode;
	char *cndnname;
	char *masterhostaddress;
	char *mastername;
	char *nodetypestr;
	char cmdheadstr[64];
	char nodetype;
	int32 cndnport;
	int masterport;
	Oid hostOid;
	Oid nodemasternameoid;
	Oid	tupleOid;
	Oid	masterhostOid;
	Oid cnoid;
	bool ismasterrunning = 0;
	HeapTuple gtmmastertuple;
	NameData cndnnamedata;
	HeapTuple mastertuple;
	PGconn *pg_conn;

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
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), cndnname);
	/*get node type*/
	nodetype = mgr_node->nodetype;
	nodetypestr = mgr_nodetype_str(nodetype);
	/*check node init or not*/
	if ((AGT_CMD_CNDN_CNDN_INIT == cmdtype || AGT_CMD_GTM_INIT == cmdtype || AGT_CMD_GTM_SLAVE_INIT == cmdtype ) && mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "%s \"%s\" has been initialized", nodetypestr, cndnname);
		getAgentCmdRst->ret = false;
		pfree(nodetypestr);
		return;
	}
	if(AGT_CMD_CNDN_CNDN_INIT != cmdtype && AGT_CMD_GTM_INIT != cmdtype && AGT_CMD_GTM_SLAVE_INIT != cmdtype &&
		AGT_CMD_CLEAN_NODE != cmdtype && AGT_CMD_GTM_STOP_MASTER != cmdtype && AGT_CMD_GTM_STOP_SLAVE != cmdtype && 
		AGT_CMD_CN_STOP != cmdtype && AGT_CMD_DN_STOP != cmdtype && !mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "%s \"%s\" has not been initialized", nodetypestr, cndnname);
		getAgentCmdRst->ret = false;
		pfree(nodetypestr);
		return;
	}
	
	pfree(nodetypestr);
	/*get the port*/
	cndnport = mgr_node->nodeport;
	/*get node master oid*/
	nodemasternameoid = mgr_node->nodemasternameoid;
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
		case AGT_CMD_CLEAN_NODE:
			cmdmode = "rm -rf";
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
		appendStringInfo(&infosendmsg, " -U \"" AGTM_USER "\" -D %s -E UTF8 --locale=C", cndnPath);
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
		appendStringInfo(&infosendmsg, " %s -D %s -m %s -o -i -w -c", cmdmode, cndnPath, shutdown_mode);
	}
	/*stop coordinator/datanode*/
	else if(AGT_CMD_CN_STOP == cmdtype || AGT_CMD_DN_STOP == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c", zmode, shutdown_mode);
	}
	else if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype)
	{
		/*pause cluster*/
		mgr_lock_cluster(&pg_conn, &cnoid);
		/*stop gtm master*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("gtm master \"%s\" does not exist", cndnname)));
		}
		DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_gtm_master, (Datum)0);
		if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
			ereport(WARNING, (errmsg("stop gtm master \"%s\" fail", cndnname)));
		ReleaseSysCache(mastertuple);

		appendStringInfo(&infosendmsg, " %s -w -D %s", cmdmode, cndnPath);
	}
	else if (AGT_CMD_DN_FAILOVER == cmdtype)
	{
		/*pause cluster*/
		mgr_lock_cluster(&pg_conn, &cnoid);
		/*stop datanode master*/
		 mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		 if(!HeapTupleIsValid(mastertuple))
		 {
						 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
										 ,errmsg("datanode master \"%s\" dosen't exist", cndnname)));
		 }
		 DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_dn_master, CStringGetDatum(cndnname));
		 if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
						 ereport(WARNING, (errmsg("stop datanode master \"%s\" fail", cndnname)));
			ReleaseSysCache(mastertuple);
			
		appendStringInfo(&infosendmsg, " %s -w -D %s", cmdmode, cndnPath);
	}
	else if (AGT_CMD_AGTM_RESTART == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s -w -m %s -l %s/logfile", cmdmode, cndnPath, shutdown_mode, cndnPath);
	}
	else if (AGT_CMD_DN_RESTART == cmdtype || AGT_CMD_CN_RESTART == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c -l %s/logfile", zmode, shutdown_mode, cndnPath);
	}
	else if (AGT_CMD_CLEAN_NODE == cmdtype)
	{
		appendStringInfo(&infosendmsg, "rm -rf %s; mkdir -p %s; chmod 0700 %s", cndnPath, cndnPath, cndnPath);
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
	mgr_get_cmd_head_word(cmdtype, cmdheadstr);
	ereport(LOG,
		(errmsg("%s, %s%s", hostaddress, cmdheadstr, infosendmsg.data)));
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
		namestrcpy(&cndnnamedata, cndnname);
		mgr_after_datanode_failover_handle(nodemasternameoid, &cndnnamedata, cndnport, hostaddress, noderel, getAgentCmdRst, aimtuple, cndnPath, nodetype, &pg_conn, cnoid);
	}

	/*gtm failover*/
	if (AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype && execok)
	{
		mgr_after_gtm_failover_handle(hostaddress, cndnport, noderel, getAgentCmdRst, aimtuple, cndnPath, &pg_conn, cnoid);
	}

	pfree(infosendmsg.data);
	pfree(hostaddress);
}

/*
* stop gtm master
*/
Datum mgr_stop_gtm_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;
	char *nodename;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_MASTER);
	else
	{
		nodename = PG_GETARG_CSTRING(1);
		nodenamelist = lappend(nodenamelist, nodename);
	}
	return mgr_runmode_cndn(GTM_TYPE_GTM_MASTER, AGT_CMD_GTM_STOP_MASTER, nodenamelist, stop_mode, fcinfo);
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
	mgr_runmode_cndn_get_result(AGT_CMD_GTM_STOP_MASTER, &getAgentCmdRst, rel_node, aimtuple, SHUTDOWN_I);
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
	List *nodenamelist = NIL;
	char *stop_mode;
	char *nodename;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_SLAVE);
	else
	{
		nodename = PG_GETARG_CSTRING(1);
		nodenamelist = lappend(nodenamelist, nodename);
	}
	return mgr_runmode_cndn(GTM_TYPE_GTM_SLAVE, AGT_CMD_GTM_STOP_SLAVE, nodenamelist, stop_mode, fcinfo);
}

/*stop gtm extra*/
Datum mgr_stop_gtm_extra(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;
	char *nodename;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(GTM_TYPE_GTM_EXTRA);
	else
	{
		nodename = PG_GETARG_CSTRING(1);
		nodenamelist = lappend(nodenamelist, nodename);
	}
	return mgr_runmode_cndn(GTM_TYPE_GTM_EXTRA, AGT_CMD_GTM_STOP_SLAVE, nodenamelist, stop_mode, fcinfo); 
}

/*
* stop coordinator master cn1,cn2...
* stop coordinator master all
*/
Datum mgr_stop_cn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_STOP, nodenamelist, stop_mode, fcinfo);
}

/*
* stop datanode master cn1,cn2...
* stop datanode master all
*/
Datum mgr_stop_dn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_STOP, nodenamelist, stop_mode, fcinfo);
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
	mgr_runmode_cndn_get_result(AGT_CMD_DN_STOP, &getAgentCmdRst, info->rel_node, aimtuple, SHUTDOWN_I);
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
	List *nodenamelist = NIL;
	char *stop_mode;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_SLAVE);
	else
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_STOP, nodenamelist, stop_mode, fcinfo);
}

/*
* stop datanode extra dn1,dn2...
* stop datanode extra all
*/
Datum mgr_stop_dn_extra(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_EXTRA);
	else
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_EXTRA, AGT_CMD_DN_STOP, nodenamelist, stop_mode, fcinfo);
}

/*
* get the result of start/stop/init gtm master/slave, coordinator master/slave, datanode master/slave
*/
Datum mgr_runmode_cndn(char nodetype, char cmdtype, List* nodenamelist , char *shutdown_mode, PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result;
	HeapTuple aimtuple =NULL;
	FuncCallContext *funcctx;
	ListCell **lcp;
	InitNodeInfo *info;
	char *nodestrname;
	NameData nodenamedata;
	Form_mgr_node mgr_node;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		/* allocate memory for user context */
		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
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
		list_free(nodenamelist);
		heap_close(info->rel_node, RowExclusiveLock);
		SRF_RETURN_DONE(funcctx);
	}
	nodestrname = (char *) lfirst(*lcp);
	*lcp = lnext(*lcp);
	if(namestrcpy(&nodenamedata, nodestrname) != 0)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("namestrcpy %s fail", nodestrname)));
	}
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, NameStr(nodenamedata), nodetype);
	if (!HeapTupleIsValid(aimtuple))
	{
		heap_close(info->rel_node, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
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
	char *host_addr = NULL;
	char *user = NULL;
	const char *error_str = NULL;
	bool is_valid = false;
	int ret = 0;

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
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	initStringInfo(&port);
	appendStringInfo(&port, "%d", mgr_node->nodeport);
	is_valid = is_valid_ip(host_addr);
	if (is_valid)
	{
		ret = pingNode_user(host_addr, port.data, user);

		switch (ret)
		{
			case PQPING_OK:
				error_str = "running";
				break;
			case PQPING_REJECT:
				error_str = "server is alive but rejecting connections";
				break;
			case PQPING_NO_RESPONSE:
				error_str = "not running";
				break;
			case PQPING_NO_ATTEMPT:
				error_str = "connection not attempted (bad params)";
				break;
			default:
				break;
		}
	}
	else
		error_str = "could not establish host connection";


	tup_result = build_common_command_tuple_for_monitor(
				&(mgr_node->nodename)
				,mgr_node->nodetype
				,ret == PQPING_OK ? true:false
				,error_str);

	pfree(port.data);
	pfree(host_addr);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * MONITOR DATANODE ALL;
 */
Datum mgr_monitor_datanode_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData port;
	char *host_addr = NULL;
	bool is_valid = false;
	const char *error_str = NULL;
	char *user;
	int ret = 0;

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

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is datanode master ,datanode slave ,datanode extra. */
		if (mgr_node->nodetype == 'd' || mgr_node->nodetype == 'b' || mgr_node->nodetype == 'n')
		{
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			user = get_hostuser_from_hostoid(mgr_node->nodehost);
			initStringInfo(&port);
			appendStringInfo(&port, "%d", mgr_node->nodeport);

			is_valid = is_valid_ip(host_addr);
			if (is_valid)
			{
				ret = pingNode_user(host_addr, port.data, user);
				switch (ret)
				{
					case PQPING_OK:
						error_str = "running";
						break;
					case PQPING_REJECT:
						error_str = "server is alive but rejecting connections";
						break;
					case PQPING_NO_RESPONSE:
						error_str = "not running";
						break;
					case PQPING_NO_ATTEMPT:
						error_str = "connection not attempted (bad params)";
						break;
					default:
						break;
				}
			}
			else
				error_str = "could not establish host connection";

			tup_result = build_common_command_tuple_for_monitor(
						&(mgr_node->nodename)
						,mgr_node->nodetype
						,ret == 0 ? true:false
						,error_str);

			pfree(port.data);
			pfree(host_addr);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * MONITOR GTM ALL;
 */
Datum mgr_monitor_gtm_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData port;
	char *host_addr;
	char *user;
	bool is_valid = false;
	const char *error_str = NULL;
	int ret = 0;

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

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is gtm master ,gtm slave ,gtm extra. */
		if (mgr_node->nodetype == 'g' || mgr_node->nodetype == 'p' || mgr_node->nodetype == 'e')
		{
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			user = get_hostuser_from_hostoid(mgr_node->nodehost);
			initStringInfo(&port);
			appendStringInfo(&port, "%d", mgr_node->nodeport);

			is_valid = is_valid_ip(host_addr);
			if (is_valid)
			{
				ret = pingNode_user(host_addr, port.data, user);
				switch (ret)
				{
					case PQPING_OK:
						error_str = "running";
						break;
					case PQPING_REJECT:
						error_str = "server is alive but rejecting connections";
						break;
					case PQPING_NO_RESPONSE:
						error_str = "not running";
						break;
					case PQPING_NO_ATTEMPT:
						error_str = "connection not attempted (bad params)";
						break;
					default:
						break;
				}
			}
			else
				error_str = "could not establish host connection";

			tup_result = build_common_command_tuple_for_monitor(
						&(mgr_node->nodename)
						,mgr_node->nodetype
						,ret == 0 ? true:false
						,error_str);
			pfree(port.data);
			pfree(host_addr);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * monitor nodetype(datanode master/slave/extra|coordinator|gtm master/slave/extra) namelist ...
 */
Datum mgr_monitor_nodetype_namelist(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	ListCell **lcp;
	List *nodenamelist=NIL;
	HeapTuple tup, tup_result;
	Form_mgr_node mgr_node;
	StringInfoData port;
	char *host_addr;
	char *nodename;
	bool is_valid = false;
	const char *error_str = NULL;
	char *user;
	int ret = 0;
	char nodetype;

	nodetype = PG_GETARG_CHAR(0);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

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

	nodename = (char *)lfirst(*lcp);
	*lcp = lnext(*lcp);
	tup = mgr_get_tuple_node_from_name_type(info->rel_node, nodename, nodetype);
	if (!HeapTupleIsValid(tup))
	{
		switch (nodetype)
		{
			case CNDN_TYPE_COORDINATOR_MASTER:
				ereport(ERROR, (errmsg("coordinator \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_MASTER:
				ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_SLAVE:
				ereport(ERROR, (errmsg("datanode slave \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_EXTRA:
				ereport(ERROR, (errmsg("datanode extra \"%s\" does not exist", nodename)));
				break;
			case GTM_TYPE_GTM_SLAVE:
				ereport(ERROR, (errmsg("gtm slave \"%s\" does not exist", nodename)));
				break;
			case GTM_TYPE_GTM_EXTRA:
				ereport(ERROR, (errmsg("gtm extra \"%s\" does not exist", nodename)));
				break;
			default:
				ereport(ERROR, (errmsg("node type \"%c\" does not exist", nodetype)));
				break;
		}
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);
	
	if (nodetype != mgr_node->nodetype)
		ereport(ERROR, (errmsg("node type is not right: %s", nodename)));

	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	initStringInfo(&port);
	appendStringInfo(&port, "%d", mgr_node->nodeport);

	is_valid = is_valid_ip(host_addr);
	if (is_valid)
	{
		ret = pingNode_user(host_addr, port.data, user);
		switch (ret)
		{
			case PQPING_OK:
				error_str = "running";
				break;
			case PQPING_REJECT:
				error_str = "server is alive but rejecting connections";
				break;
			case PQPING_NO_RESPONSE:
				error_str = "not running";
				break;
			case PQPING_NO_ATTEMPT:
				error_str = "connection not attempted (bad params)";
				break;
			default:
				break;
		}
	}
	else
		error_str = "could not establish host connection";

	tup_result = build_common_command_tuple_for_monitor(
				&(mgr_node->nodename)
				,mgr_node->nodetype
				,ret == 0 ? true:false
				,error_str);

	pfree(port.data);
	pfree(host_addr);
	heap_freetuple(tup);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * MONITOR nodetype(DATANODE MASTER/SLAVE/EXTRA |COORDINATOR |GTM MASTER|SLAVE/EXTRA) ALL
 */
Datum mgr_monitor_nodetype_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	ScanKeyData  key[1];
	StringInfoData port;
	char *host_addr;
	char *user;
	int ret = 0;
	char nodetype;
	bool is_valid = false;
	const char *error_str = NULL;

	nodetype = PG_GETARG_CHAR(0);

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
					,CharGetDatum(nodetype));
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
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	initStringInfo(&port);
	appendStringInfo(&port, "%d", mgr_node->nodeport);

	is_valid = is_valid_ip(host_addr);
	if (is_valid)
	{
		ret = pingNode_user(host_addr, port.data, user);
		switch (ret)
		{
			case PQPING_OK:
				error_str = "running";
				break;
			case PQPING_REJECT:
				error_str = "server is alive but rejecting connections";
				break;
			case PQPING_NO_RESPONSE:
				error_str = "not running";
				break;
			case PQPING_NO_ATTEMPT:
				error_str = "connection not attempted (bad params)";
				break;
			default:
				break;
		}
	}
	else
		error_str = "could not establish host connection";

	tup_result = build_common_command_tuple_for_monitor(
				&(mgr_node->nodename)
				,mgr_node->nodetype
				,ret == 0 ? true:false
				,error_str);

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
	AppendNodeInfo agtm_m_nodeinfo, agtm_s_nodeinfo, agtm_e_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	bool agtm_e_is_exist, agtm_e_is_running; /* agtm extra status */
	StringInfoData  infosendmsg;
	NameData nodename;
	Oid coordhostoid;
	int32 coordport;
	char *coordhost;
	char *temp_file;
	Oid dnhostoid;
	int32 dnport;
	PGconn * volatile pg_conn = NULL;
	PGresult * volatile res = NULL;
	HeapTuple tup_result;
	HeapTuple aimtuple = NULL;
	char coordport_buf[10];
	GetAgentCmdRst getAgentCmdRst;
	bool result = true;
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get node info for append datanode master */
		mgr_check_appendnodeinfo(CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_MASTER, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_EXTRA, &agtm_e_is_exist, &agtm_e_is_running, &agtm_e_nodeinfo);

		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_MASTER, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{ ereport(ERROR, (errmsg("gtm master is not running")));}
		}
		else
		{ ereport(ERROR, (errmsg("gtm master is not initialized")));}
		
		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_SLAVE, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{ ereport(ERROR, (errmsg("gtm slave is not running")));}
		}

		if (agtm_e_is_exist)
		{
			if (agtm_e_is_running)
			{
				/* append "host all postgres ip/32" for agtm extra pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_EXTRA, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{ ereport(ERROR, (errmsg("gtm extra is not running")));}
		}

		/* step 1: init workdir */
		mgr_append_init_cndnmaster(&appendnodeinfo);

		/* step 2: update datanode master's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_get_other_parm(CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_get_agtm_host_and_port(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 3: update datanode master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 4: block all the DDL lock */
		mgr_get_active_hostoid_and_port(CNDN_TYPE_COORDINATOR_MASTER, &coordhostoid, &coordport, &appendnodeinfo, true);
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
		mgr_get_active_hostoid_and_port(CNDN_TYPE_DATANODE_MASTER, &dnhostoid, &dnport, &appendnodeinfo, true);

		temp_file = get_temp_file_name();
		mgr_pg_dumpall(dnhostoid, dnport, appendnodeinfo.nodehost, temp_file);

		/* step 6: start the datanode master with restoremode mode, and input all catalog message */
		mgr_start_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_pg_dumpall_input_node(appendnodeinfo.nodehost, appendnodeinfo.nodeport, temp_file);
		mgr_rm_dumpall_temp_file(appendnodeinfo.nodehost, temp_file);

		/* step 7: stop the datanode master with restoremode, and then start it with "datanode" mode */
		mgr_stop_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 8: create node on all the coordinator */
		mgr_create_node_on_all_coord(fcinfo, CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);

		resetStringInfo(&(getAgentCmdRst.description));
		result = mgr_refresh_pgxc_node(APPEND, CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodename, &getAgentCmdRst);

		/* step 9: release the DDL lock */
		PQclear(res);
		PQfinish(pg_conn);
		pg_conn = NULL;

		/* step10: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, false, true);
	}PG_CATCH();
	{
		if(pg_conn)
		{
			PQclear(res);
			PQfinish(pg_conn);
			pg_conn = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, result, getAgentCmdRst.description.data);
	pfree(getAgentCmdRst.description.data);
	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND DATANODE SLAVE nodename
 */
Datum mgr_append_dnslave(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo parentnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	AppendNodeInfo agtm_s_nodeinfo;
	AppendNodeInfo agtm_e_nodeinfo;
	AppendNodeInfo dn_e_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	bool agtm_e_is_exist, agtm_e_is_running; /* agtm extra status */
	bool dn_e_is_exist, dn_e_is_running; /*datanode extra status */
	bool dnmaster_is_running; /* datanode master status */
	bool is_extra_exist, is_extra_sync;
	StringInfoData  infosendmsg;
	StringInfoData primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get node info both slave and master node. */
		mgr_check_appendnodeinfo(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_SLAVE, &appendnodeinfo);
		mgr_get_parent_appendnodeinfo(appendnodeinfo.nodemasteroid, &parentnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_EXTRA, &agtm_e_is_exist, &agtm_e_is_running, &agtm_e_nodeinfo);
		get_nodeinfo_byname(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_EXTRA,
							&dn_e_is_exist, &dn_e_is_running, &dn_e_nodeinfo);

		/* step 1: make sure datanode master, agtm master or agtm slave is running. */
		dnmaster_is_running = is_node_running(parentnodeinfo.nodeaddr, parentnodeinfo.nodeport);
		if (!dnmaster_is_running)
			ereport(ERROR, (errmsg("datanode master \"%s\" is not running", parentnodeinfo.nodename)));

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_MASTER, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
				{	ereport(ERROR, (errmsg("gtm master is not running")));}
		}
		else
		{	ereport(ERROR, (errmsg("gtm master is not initialized")));}
		
		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_SLAVE, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm slave is not running")));}
		}

		if (agtm_e_is_exist)
		{
			if (agtm_e_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
				mgr_add_hbaconf(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm extra is not running")));}
		}

		if (dn_e_is_exist)
		{
			if (dn_e_is_running)
			{
				/* flush datanode extra's pg_hba.conf "host replication postgres slave_ip/32 trust" if datanode extra exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										dn_e_nodeinfo.nodepath,
										&infosendmsg,
										dn_e_nodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(dn_e_nodeinfo.nodehost, dn_e_nodeinfo.nodepath);
			}
			else
			{	ereport(ERROR, (errmsg("datanode extra is not running")));}
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
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 4: reload datanode master. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 5: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(CNDN_TYPE_DATANODE_SLAVE, &appendnodeinfo, &parentnodeinfo);

		/* step 6: update datanode slave's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 7: update datanode slave's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodeport,
						get_hostuser_from_hostoid(parentnodeinfo.nodehost),
						"slave");

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 8: start datanode slave. */
		mgr_start_node(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		if (dn_e_is_exist)
		{
			if (dn_e_is_running)
			{
				/* flush datanode slave's pg_hba.conf "host replication postgres extra_ip/32 trust" if datanode slave exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", dn_e_nodeinfo.nodeusername, dn_e_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										appendnodeinfo.nodepath,
										&infosendmsg,
										appendnodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(appendnodeinfo.nodehost, appendnodeinfo.nodepath);

			}
			else
			{	ereport(ERROR, (errmsg("datanode extra is not running")));}
		}

		/* step 9: update datanode master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		get_nodestatus(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodename, &is_extra_exist, &is_extra_sync);
		if (is_extra_exist)
		{
			if (is_extra_sync)
			{
				if (is_sync(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra,slave", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
			}
			else
			{
				if (is_sync(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
			}
		}
		else
		{
			if (is_sync(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodename))
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		}

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								parentnodeinfo.nodepath,
								&infosendmsg, 
								parentnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 10: reload datanode master's postgresql.conf. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 11: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE, false, true);

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, "success");

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND DATANODE EXTRA nodename
 */
Datum mgr_append_dnextra(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo parentnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	AppendNodeInfo agtm_s_nodeinfo;
	AppendNodeInfo dn_s_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	bool agtm_e_is_exist, agtm_e_is_running; /* agtm extra status */
	bool dn_s_is_exist, dn_s_is_running;     /* datanode slave status */
	bool dnmaster_is_running; 			     /* datanode master status */
	bool is_slave_exist, is_slave_sync;
	StringInfoData  infosendmsg;
	StringInfoData primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get node info both slave and master node. */
		mgr_check_appendnodeinfo(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_EXTRA, &appendnodeinfo);
		mgr_get_parent_appendnodeinfo(appendnodeinfo.nodemasteroid, &parentnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_EXTRA, &agtm_e_is_exist, &agtm_e_is_running, &dn_s_nodeinfo);
		get_nodeinfo_byname(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE,
							&dn_s_is_exist, &dn_s_is_running, &dn_s_nodeinfo);

		/* step 1: make sure datanode master, agtm master or agtm slave is running. */
		dnmaster_is_running = is_node_running(parentnodeinfo.nodeaddr, parentnodeinfo.nodeport);
		if (!dnmaster_is_running)
			ereport(ERROR, (errmsg("datanode master \"%s\" is not running", parentnodeinfo.nodename)));

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_MASTER, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm master is not running")));}
		}
		else
		{	ereport(ERROR, (errmsg("gtm master is not initialized")));}

		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_SLAVE, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm slave is not running")));}
		}

		if (agtm_e_is_exist)
		{
			if (agtm_e_is_running)
			{
				/* append "host all postgres ip/32" for agtm extra pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_EXTRA, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm extra is not running")));}
		}

		if (dn_s_is_exist)
		{
			if (dn_s_is_running)
			{
				/* flush datanode slave's pg_hba.conf "host replication postgres slave_ip/32 trust" if datanode slave exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										dn_s_nodeinfo.nodepath,
										&infosendmsg,
										dn_s_nodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(dn_s_nodeinfo.nodehost, dn_s_nodeinfo.nodepath);
			}
			else
			{	ereport(ERROR, (errmsg("datanode slave is not running")));}
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
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 4: reload datanode master. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 5: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(CNDN_TYPE_DATANODE_EXTRA, &appendnodeinfo, &parentnodeinfo);

		/* step 6: update datanode extra's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_EXTRA, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 7: update datanode extra's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodeport,
						get_hostuser_from_hostoid(parentnodeinfo.nodehost),
						"extra");

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 8: start datanode extra. */
		mgr_start_node(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		if (dn_s_is_exist)
		{
			if (dn_s_is_running)
			{
				/* flush datanode extra's pg_hba.conf "host replication postgres slave_ip/32 trust" if datanode slave exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", dn_s_nodeinfo.nodeusername, dn_s_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										appendnodeinfo.nodepath,
										&infosendmsg,
										appendnodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(appendnodeinfo.nodehost, appendnodeinfo.nodepath);

			}
			else
			{	ereport(ERROR, (errmsg("datanode extra is not running")));}
		}

		/* step 9: update datanode master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		get_nodestatus(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodename, &is_slave_exist, &is_slave_sync);
		if (is_slave_exist)
		{
			if (is_slave_sync)
			{
				if (is_sync(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave,extra", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
			}
			else
			{
				if (is_sync(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
			}
		}
		else
		{
			if (is_sync(CNDN_TYPE_DATANODE_EXTRA, appendnodeinfo.nodename))
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		}

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								parentnodeinfo.nodepath,
								&infosendmsg, 
								parentnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 10: reload datanode master's postgresql.conf. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 11: update node system table's column to set initial is true*/
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_EXTRA, false, true);

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, "success");

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND COORDINATOR MASTER nodename
 */
Datum mgr_append_coordmaster(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo, agtm_s_nodeinfo, agtm_e_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	bool agtm_e_is_exist, agtm_e_is_running; /* agtm extra status */
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	char *coordhost;
	char *temp_file;
	Oid coordhostoid;
	int32 coordport;
	PGconn * volatile pg_conn = NULL;
	PGresult * volatile res =NULL;
	HeapTuple aimtuple = NULL;
	HeapTuple tup_result;
	char coordport_buf[10];
	NameData nodename;
	bool result = true;
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	/* get node info for append coordinator master */
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);
	PG_TRY();
	{
		/* get node info for append coordinator master */
		mgr_check_appendnodeinfo(CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_COORDINATOR_MASTER, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_EXTRA, &agtm_e_is_exist, &agtm_e_is_running, &agtm_e_nodeinfo);

		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_MASTER, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
				{	ereport(ERROR, (errmsg("gtm master is not running")));}
		}
		else
		{	ereport(ERROR, (errmsg("gtm master is not initialized")));}

		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_SLAVE, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm slave is not running")));}
		}

		if (agtm_e_is_exist)
		{
			if (agtm_e_is_running)
			{
				/* append "host all postgres ip/32" for agtm extra pg_hba.conf and reload it. */
				mgr_add_hbaconf(GTM_TYPE_GTM_EXTRA, AGTM_USER, appendnodeinfo.nodeaddr);
			}
			else
			{	ereport(ERROR, (errmsg("gtm extra is not running")));}
		}

		/* step 1: init workdir */
		mgr_append_init_cndnmaster(&appendnodeinfo);

		/* step 2: update coordinator master's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_get_other_parm(CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_get_agtm_host_and_port(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 3: update coordinator master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(aimtuple, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* add host line for exist already */
		mgr_add_hbaconf_all(appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* step 4: block all the DDL lock */
		mgr_get_active_hostoid_and_port(CNDN_TYPE_COORDINATOR_MASTER, &coordhostoid, &coordport, &appendnodeinfo, true);
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
		temp_file = get_temp_file_name();
		mgr_pg_dumpall(coordhostoid, coordport, appendnodeinfo.nodehost, temp_file);

		/* step 6: start the append coordiantor with restoremode mode, and input all catalog message */
		mgr_start_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_pg_dumpall_input_node(appendnodeinfo.nodehost, appendnodeinfo.nodeport, temp_file);
		mgr_rm_dumpall_temp_file(appendnodeinfo.nodehost, temp_file);

		/* step 7: stop the append coordiantor with restoremode, and then start it with "coordinator" mode */
		mgr_stop_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_start_node(CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 8: create node on all the coordinator */
		mgr_create_node_on_all_coord(fcinfo, CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);

		/* step 9: alter pgxc_node in append coordinator */
/*		mgr_alter_pgxc_node(fcinfo, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);
*/	
		resetStringInfo(&(getAgentCmdRst.description));
		result = mgr_refresh_pgxc_node(APPEND, CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodename, &getAgentCmdRst);
		/* step 10: release the DDL lock */
		PQclear(res);
		PQfinish(pg_conn);
		pg_conn = NULL;

		/* step 11: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_COORDINATOR_MASTER, false, true);
	}PG_CATCH();
	{
		if(pg_conn)
		{
			PQclear(res);
			PQfinish(pg_conn);
			pg_conn = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, result, getAgentCmdRst.description.data);
	pfree(getAgentCmdRst.description.data);

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_append_agtmslave(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	AppendNodeInfo agtm_e_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_e_is_exist, agtm_e_is_running; /* agtm extra status */
	bool is_extra_exist, is_extra_sync;
	StringInfoData infosendmsg;
	StringInfoData primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get agtm slave and agtm master node info. */
		mgr_check_appendnodeinfo(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(GTM_TYPE_GTM_SLAVE, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_EXTRA, &agtm_e_is_exist, &agtm_e_is_running, &agtm_e_nodeinfo);

		if (!agtm_m_is_exist)
			ereport(ERROR, (errmsg("gtm master is not initialized")));

		if (!agtm_m_is_running)
			ereport(ERROR, (errmsg("gtm master is not running")));

		if (agtm_e_is_exist)
		{
			if (agtm_e_is_running)
			{
				/* flush agtm extra's pg_hba.conf "host replication postgres slave_ip/32 trust" if agtm extra exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_e_nodeinfo.nodepath,
										&infosendmsg,
										agtm_e_nodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(agtm_e_nodeinfo.nodehost, agtm_e_nodeinfo.nodepath);
			}
			else
			{   ereport(ERROR, (errmsg("gtm extra is not running")));}
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
		mgr_pgbasebackup(GTM_TYPE_GTM_SLAVE, &appendnodeinfo, &agtm_m_nodeinfo);

		/* step 4: update agtm slave's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, GTM_TYPE_GTM_SLAVE, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 5: update agtm slave's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodeport,
						AGTM_USER,
						"slave");

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 6: start agtm slave. */
		mgr_start_node(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		if (agtm_e_is_exist)
		{
			if (agtm_e_is_running)
			{
				/*flush agtm slave's pg_hba.conf "host replication postgres extra_ip/32 trust" if agtm extra exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, agtm_e_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										appendnodeinfo.nodepath,
										&infosendmsg,
										appendnodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(appendnodeinfo.nodehost, appendnodeinfo.nodepath);
			}
			else
			{   ereport(ERROR, (errmsg("gtm extra is not running")));}
		}

		/* step 7: update agtm master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		get_nodestatus(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodename, &is_extra_exist, &is_extra_sync);
		if (is_extra_exist)
		{
			if (is_extra_sync)
			{
				if (is_sync(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra,slave", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
			}
			else
			{
				if (is_sync(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
			}
		}
		else
		{
			if (is_sync(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodename))
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		}

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								agtm_m_nodeinfo.nodepath,
								&infosendmsg, 
								agtm_m_nodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 8: reload agtm master's postgresql.conf. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 9: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, GTM_TYPE_GTM_SLAVE, false, true);

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, "success");

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_append_agtmextra(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	AppendNodeInfo agtm_s_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool agtm_s_is_exist, agtm_s_is_running; /* agtm slave status */
	bool is_slave_exist, is_slave_sync;
	StringInfoData  infosendmsg;
	StringInfoData primary_conninfo_value;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get agtm extra, agtm master and agtm slave node info. */
		mgr_check_appendnodeinfo(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(GTM_TYPE_GTM_EXTRA, &appendnodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		get_nodeinfo(GTM_TYPE_GTM_SLAVE, &agtm_s_is_exist, &agtm_s_is_running, &agtm_s_nodeinfo);

		if (!agtm_m_is_exist)
			ereport(ERROR, (errmsg("gtm master is not initialized")));
		
		if (!agtm_m_is_running)
			ereport(ERROR, (errmsg("gtm master is not running")));

		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/* flush agtm slave's pg_hba.conf "host replication postgres slave_ip/32 trust" if agtm slave exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										agtm_s_nodeinfo.nodepath,
										&infosendmsg,
										agtm_s_nodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(agtm_s_nodeinfo.nodehost, agtm_s_nodeinfo.nodepath);
			}
			else
			{   ereport(ERROR, (errmsg("gtm slave is not running")));}
		}

		/* step 1: update agtm master's pg_hba.conf. */
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								agtm_m_nodeinfo.nodepath,
								&infosendmsg,
								agtm_m_nodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 2: reload agtm master. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 3: basebackup for datanode master using pg_basebackup command. */
		mgr_pgbasebackup(GTM_TYPE_GTM_EXTRA, &appendnodeinfo, &agtm_m_nodeinfo);

		/* step 4: update agtm extra's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, GTM_TYPE_GTM_EXTRA, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								appendnodeinfo.nodepath,
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 5: update agtm extra's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodeport,
						AGTM_USER,
						"extra");

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath, 
								&infosendmsg, 
								appendnodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 6: start agtm extra. */
		mgr_start_node(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		if (agtm_s_is_exist)
		{
			if (agtm_s_is_running)
			{
				/*flush agtm extra's pg_hba.conf "host replication postgres extra_ip/32 trust" if agtm slave exist */
				resetStringInfo(&infosendmsg);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, agtm_s_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
										appendnodeinfo.nodepath,
										&infosendmsg,
										appendnodeinfo.nodehost,
										&getAgentCmdRst);
				mgr_reload_conf(appendnodeinfo.nodehost, appendnodeinfo.nodepath);
			}
		}

		/* step 7: update agtm master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		get_nodestatus(GTM_TYPE_GTM_SLAVE, appendnodeinfo.nodename, &is_slave_exist, &is_slave_sync);
		if (is_slave_exist)
		{
			if (is_slave_sync)
			{
				if (is_sync(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave,extra", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
			}
			else
			{
				if (is_sync(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodename))
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
				else
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
			}
		}
		else
		{
			if (is_sync(GTM_TYPE_GTM_EXTRA, appendnodeinfo.nodename))
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		}

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, 
								agtm_m_nodeinfo.nodepath,
								&infosendmsg, 
								agtm_m_nodeinfo.nodehost, 
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 8: reload agtm master's postgresql.conf. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 9: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, GTM_TYPE_GTM_EXTRA, false, true);

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, "success");

	return HeapTupleGetDatum(tup_result);
}

static char *get_temp_file_name()
{
	StringInfoData file_name_str;
	initStringInfo(&file_name_str);

	appendStringInfo(&file_name_str, "%s_%d.txt", PG_DUMPALL_TEMP_FILE, rand());

	return file_name_str.data;
}

static void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
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

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(node_name));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 4, key);
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
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

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
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

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

static void mgr_pgbasebackup(char nodetype, AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo)
{

	ManagerAgent *ma;
	StringInfoData sendstrmsg, buf;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));

	if (nodetype == GTM_TYPE_GTM_SLAVE || nodetype == GTM_TYPE_GTM_EXTRA)
	{
		appendStringInfo(&sendstrmsg, " -h %s -p %d -U %s -D %s -Xs -Fp -R", 
									get_hostaddress_from_hostoid(parentnodeinfo->nodehost)
									,parentnodeinfo->nodeport
									,AGTM_USER
									,appendnodeinfo->nodepath);
	
	}
	else if (nodetype == CNDN_TYPE_DATANODE_SLAVE || nodetype == CNDN_TYPE_DATANODE_EXTRA)
	{
		appendStringInfo(&sendstrmsg, " -h %s -p %d -D %s -Xs -Fp -R", 
									get_hostaddress_from_hostoid(parentnodeinfo->nodehost)
									,parentnodeinfo->nodeport
									,appendnodeinfo->nodepath);
	}

	ma = ma_connect_hostoid(appendnodeinfo->nodehost);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(appendnodeinfo->nodehost))));
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
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

}
static void mgr_make_sure_all_running(char node_type)
{
	InitNodeInfo *info;
	ScanKeyData key[3];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * hostaddr = NULL;
	char *nodetype_str = NULL;
	NameData nodetypestr_data;
	NameData nodename;

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
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		hostaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);

		if (!is_node_running(hostaddr, mgr_node->nodeport))
		{
			nodetype_str = mgr_nodetype_str(mgr_node->nodetype);
			namestrcpy(&nodename, NameStr(mgr_node->nodename));
			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);
			pfree(hostaddr);
			namestrcpy(&nodetypestr_data, nodetype_str);
			pfree(nodetype_str);
			ereport(ERROR, (errmsg("%s \"%s\" is not running", nodetypestr_data.data,nodename.data)));
		}
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);

	if (hostaddr != NULL)
		pfree(hostaddr);

	return;
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
		pfree(port.data);
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
		ReleaseSysCache(mastertuple);
		heap_close(noderelation, AccessShareLock);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("could not find datanode master"))); 
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mgr_node);

	parentnodeinfo->nodename = NameStr(mgr_node->nodename);
	parentnodeinfo->nodetype = mgr_node->nodetype;
	parentnodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	parentnodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	parentnodeinfo->nodeport = mgr_node->nodeport;
	parentnodeinfo->nodehost = mgr_node->nodehost;

	if (mgr_node->nodeinited == false)
		ereport(ERROR, (errmsg("datanode master \"%s\" does not initialized", NameStr(mgr_node->nodename))));

	/*get nodepath from tuple*/
	datumPath = heap_getattr(mastertuple, Anum_mgr_node_nodepath, RelationGetDescr(noderelation), &isNull);
	if (isNull)
	{
		ReleaseSysCache(mastertuple);
		heap_close(noderelation, AccessShareLock);

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
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype || GTM_TYPE_GTM_EXTRA == mgr_node->nodetype)
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, dnaddr, 32, "trust", &infosendmsg);
		else
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", dnaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							TextDatumGetCString(datumPath),
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);
		resetStringInfo(&infosendmsg);

		mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));
	}
	pfree(infosendmsg.data);
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
	Oid hostoid;
	char *nodepath;
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

	mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", dnusername, dnaddr, 32, "trust", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							TextDatumGetCString(datumPath),
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);

	hostoid = mgr_node->nodehost;
	nodepath = TextDatumGetCString(datumPath);

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);

	/* reload it at last */
	mgr_reload_conf(hostoid, nodepath);
}

void mgr_reload_conf(Oid hostoid, char *nodepath)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg, buf;
	GetAgentCmdRst getAgentCmdRst;
	bool execok = false;
	char *addr;
	NameData hostaddr;

	addr = get_hostname_from_hostoid(hostoid);
	namestrcpy(&hostaddr, addr);
	pfree(addr);
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
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						hostaddr.data)));
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
	ma_close(ma);
	if (!execok)
	{
		ereport(WARNING, (errmsg("%s reload -D %s fail %s",
			hostaddr.data, nodepath, getAgentCmdRst.description.data)));
	}
	pfree(getAgentCmdRst.description.data);
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

static void mgr_rm_dumpall_temp_file(Oid dnhostoid,char *temp_file)
{
	StringInfoData cmd_str;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;
	char *addr;
	NameData hostaddr;

	initStringInfo(&cmd_str);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	appendStringInfo(&cmd_str, "rm -f %s", temp_file);

	addr = get_hostname_from_hostoid(dnhostoid);
	namestrcpy(&hostaddr, addr);
	pfree(addr);
	/* connection agent */
	ma = ma_connect_hostoid(dnhostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent\"%s\".",
						hostaddr.data)));
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
	if(!execok)
		ereport(WARNING, (errmsg("%s rm -f %s fail %s", 
			hostaddr.data, temp_file, getAgentCmdRst.description.data)));
	ma_close(ma);
	pfree(getAgentCmdRst.description.data);
}

static void mgr_create_node_on_all_coord(PG_FUNCTION_ARGS, char nodetype, char *dnname, Oid dnhostoid, int32 dnport)
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

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			ereport(ERROR, (errmsg("could not connect socket for agent\"%s\".",
							get_hostname_from_hostoid(mgr_node->nodehost))));
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

		if (nodetype == CNDN_TYPE_COORDINATOR_MASTER)
			appendStringInfo(&psql_cmd, " CREATE NODE \\\"%s\\\" WITH (TYPE = 'coordinator', HOST='%s', PORT=%d);"
							,dnname
							,addressnode
							,dnport);
		if (nodetype == CNDN_TYPE_DATANODE_MASTER)
			appendStringInfo(&psql_cmd, " CREATE NODE \\\"%s\\\" WITH (TYPE = 'datanode', HOST='%s', PORT=%d);"
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

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			return;
		}

		/*check the receive msg*/
		execok = mgr_recv_msg(ma, &getAgentCmdRst);
		ma_close(ma);
		if (!execok)
			ereport(WARNING, (errmsg("create node on all coordinators fail %s", 
				getAgentCmdRst.description.data)));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(getAgentCmdRst.description.data);
}

static void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;

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
			ereport(ERROR, (errmsg("node type \"%c\" does not exist", nodetype)));
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
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(hostoid))));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);

	if (nodetype == GTM_TYPE_GTM_SLAVE || nodetype == GTM_TYPE_GTM_EXTRA)
		ma_sendbyte(&buf, AGT_CMD_GTM_START_SLAVE); /* agtm_ctl */
	else
		ma_sendbyte(&buf, AGT_CMD_DN_START);  /* pg_ctl  */
		
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
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static void mgr_stop_node_with_restoremode(const char *nodepath, Oid hostoid)
{
	StringInfoData stop_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;

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
		ereport(ERROR, (errmsg("could not connect socket for agent\"%s\".",
						get_hostname_from_hostoid(hostoid))));
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
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static void mgr_pg_dumpall_input_node(const Oid dn_master_oid, const int32 dn_master_port, char *temp_file)
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
	appendStringInfo(&pgsql_cmd, " -h %s -p %d -d %s -f %s", dn_master_addr, dn_master_port, DEFAULT_DB, temp_file);

	/* connection agent */
	ma = ma_connect_hostoid(dn_master_oid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent\"%s\".",
						get_hostname_from_hostoid(dn_master_oid))));
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
	if (!execok)
		ereport(WARNING, (errmsg("dump input node info fail %s", getAgentCmdRst.description.data)));
	ma_close(ma);
	pfree(dn_master_addr);
	pfree(getAgentCmdRst.description.data);
}

static void mgr_start_node_with_restoremode(const char *nodepath, Oid hostoid)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;

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
		ereport(ERROR, (errmsg("could not connect socket for agent\"%s\".",
						get_hostname_from_hostoid(hostoid))));
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
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);

	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static void mgr_pg_dumpall(Oid hostoid, int32 hostport, Oid dnmasteroid, char *temp_file)
{
	StringInfoData pg_dumpall_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	char * hostaddr;

	initStringInfo(&pg_dumpall_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	hostaddr = get_hostaddress_from_hostoid(hostoid);
	appendStringInfo(&pg_dumpall_cmd, " -h %s -p %d -s --include-nodes --dump-nodes -f %s", hostaddr, hostport, temp_file);

	/* connection agent */
	ma = ma_connect_hostoid(dnmasteroid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent\"%s\".",
						get_hostname_from_hostoid(dnmasteroid))));
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
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	pfree(hostaddr);

	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static bool mgr_get_active_hostoid_and_port(char node_type, Oid *hostoid, int32 *hostport, AppendNodeInfo *appendnodeinfo, bool set_ip)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * host;
	char coordportstr[19];
	bool isNull;
	bool bget = false;
	Datum datumPath;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;

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

	while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		/* check the coordinator active */
		sprintf(coordportstr, "%d", mgr_node->nodeport);
		host = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if(PQPING_OK != pingNode(host, coordportstr))
		{
			if (host)
				pfree(host);
			continue;
		}
		pfree(host);
		bget = true;
		break;
	}
	if (!bget)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return false;
	}
	appendnodeinfo->tupleoid = HeapTupleGetOid(tuple);
	if (hostoid)
		*hostoid = mgr_node->nodehost;
	if (hostport)
		*hostport = mgr_node->nodeport;

	if ((node_type == CNDN_TYPE_DATANODE_MASTER || node_type == CNDN_TYPE_COORDINATOR_MASTER) && set_ip)
	{
		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		initStringInfo(&infosendmsg);
		initStringInfo(&(getAgentCmdRst.description));
		getAgentCmdRst.ret = false;
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", appendnodeinfo->nodeaddr,
										32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								TextDatumGetCString(datumPath),
								&infosendmsg,
								mgr_node->nodehost,
								&getAgentCmdRst);
		pfree(infosendmsg.data);
		if (!getAgentCmdRst.ret)
		{
			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		}
		pfree(getAgentCmdRst.description.data);
		mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));
	}
	else
	{
		/*do nothing*/
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);

	return true;
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
			, errmsg("gtm master does not exist")));
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
	mgr_append_pgconf_paras_str_quotastr("log_destination", "csvlog", infosendmsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendmsg);
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
			ereport(ERROR, (errmsg("coordinator \"%s\" does not exist", appendnodeinfo->nodename)));
			break;
		case CNDN_TYPE_DATANODE_MASTER:
			ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", appendnodeinfo->nodename)));
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			ereport(ERROR, (errmsg("datanode slave \"%s\" does not exist", appendnodeinfo->nodename)));
			break;
		case CNDN_TYPE_DATANODE_EXTRA:
			ereport(ERROR, (errmsg("datanode extra \"%s\" does not exist", appendnodeinfo->nodename)));
			break;
		case GTM_TYPE_GTM_SLAVE:
			ereport(ERROR, (errmsg("gtm slave \"%s\" does not exist", appendnodeinfo->nodename)));
			break;
		case GTM_TYPE_GTM_EXTRA:
			ereport(ERROR, (errmsg("gtm extra \"%s\" does not exist", appendnodeinfo->nodename)));
			break;
		default:
			ereport(ERROR, (errmsg("node type \"%c\" does not exist", node_type)));
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
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

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
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(appendnodeinfo->nodehost))));
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
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);

	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
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
	bool force_get = PG_GETARG_BOOL(2);
	char cmdtype = AGT_CMD_DN_FAILOVER;
	char nodetype;
	bool force = false;
	bool nodetypechange = false;
	Datum datum;

	if(force_get)
		force = true;
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
		nodetypechange = true;
		datum = get_failover_node_type(nodename, CNDN_TYPE_DATANODE_SLAVE, CNDN_TYPE_DATANODE_EXTRA, force);
		nodetype = DatumGetChar(datum);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("no such node type: %s", typestr)));
	}
	if(CNDN_TYPE_NONE_TYPE == nodetype)
		ereport(ERROR, (errmsg("datanode slave or extra \"%s\" is not exist incluster", nodename)));
	return mgr_failover_one_dn_inner_func(nodename, cmdtype, nodetype, nodetypechange, force);
}

/*
* inner function, userd for node failover
*/
static Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange, bool bforce)
{
	Relation rel_node;
	HeapTuple aimtuple;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	char *nodestring;
	char *host_addr;
	Form_mgr_node mgr_node;
	StringInfoData port;
	int ret;

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	nodestring = mgr_nodetype_str(nodetype);
	aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodename, nodetype);
	if (!HeapTupleIsValid(aimtuple))
	{
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("%s \"%s\" does not exist", nodestring, nodename)));
	}
	/*check node is running normal and sync*/
	if (!nodetypechange)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		if ((!bforce) && mgr_node->nodesync != 't')
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("%s \"%s\" is async mode", nodestring, nodename)
				,errhint("you can add \'force\' at the end, and enforcing execute failover")));	
		}
		/*check running normal*/
		host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		initStringInfo(&port);
		appendStringInfo(&port, "%d", mgr_node->nodeport);
		ret = pingNode(host_addr, port.data);
		pfree(port.data);
		pfree(host_addr);
		if(ret != 0)
			ereport(ERROR, (errmsg("%s \"%s\" is not running normal", nodestring, nodename)));	
	}
	pfree(nodestring);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, rel_node, aimtuple, TAKEPLAPARM_N);
	heap_freetuple(aimtuple);
	namestrcpy(&(getAgentCmdRst.nodename),nodename);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
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
 * last step for init all
 * we need cofigure all nodes information to pgxc_node table
 */
Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info_out, *info_in;
	HeapTuple tuple_out, tuple_in, tup_result;
	ScanKeyData key_out[1], key_in[1];
	Form_mgr_node mgr_node_out, mgr_node_in;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData cmdstring;
	StringInfoData buf;
	ManagerAgent *ma;
	bool execok = false;
	char *address = NULL;

	bool is_preferred = false;
	bool is_primary = false;
	bool find_preferred = false;
	struct tuple_cndn *prefer_cndn;
	ListCell *cn_lc, *dn_lc;
	HeapTuple tuple_primary, tuple_preferred;
	int coordinator_num = 0, datanode_num = 0;
	
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
		/*set gtm or datanode master synchronous_standby_names*/
		mgr_set_master_sync();
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
		
	prefer_cndn = get_new_pgxc_node(CONFIG, NULL, 0);

	if(PointerIsValid(prefer_cndn->coordiantor_list))
		coordinator_num = prefer_cndn->coordiantor_list->length;
	if(PointerIsValid(prefer_cndn->datanode_list))
		datanode_num = prefer_cndn->datanode_list->length;

	/*get the datanode of primary in the pgxc_node*/
	if(coordinator_num < datanode_num)
	{
		dn_lc = list_tail(prefer_cndn->datanode_list);
		tuple_primary = (HeapTuple)lfirst(dn_lc);
	}
	else if(datanode_num >0)
	{
		dn_lc = list_head(prefer_cndn->datanode_list);
		tuple_primary = (HeapTuple)lfirst(dn_lc);
	}
	/*get the datanode of preferred in the pgxc_node*/
	forboth(cn_lc, prefer_cndn->coordiantor_list, dn_lc, prefer_cndn->datanode_list)
	{
		tuple_in = (HeapTuple)lfirst(cn_lc);
		if(HeapTupleGetOid(tuple_out) == HeapTupleGetOid(tuple_in))
		{
			tuple_preferred = (HeapTuple)lfirst(dn_lc);
			find_preferred = true;
			break;
		}
	}
	/*send msg to the coordinator and set pgxc_node*/
	
	foreach(dn_lc, prefer_cndn->datanode_list)
	{
		tuple_in = (HeapTuple)lfirst(dn_lc);
		mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
		Assert(mgr_node_in);
		address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
		if(true == find_preferred)
		{
			if(HeapTupleGetOid(tuple_preferred) == HeapTupleGetOid(tuple_in))
				is_preferred = true;
			else
				is_preferred = false;
		}
		else
		{
			is_preferred = false;
		}
		if(HeapTupleGetOid(tuple_primary) == HeapTupleGetOid(tuple_in))
		{
			is_primary = true;
		}
		else
		{
			is_primary = false;
		}
		appendStringInfo(&cmdstring, "create node \\\"%s\\\" with(type='datanode', host='%s', port=%d, primary = %s, preferred = %s);"
								,NameStr(mgr_node_in->nodename)
								,address
								,mgr_node_in->nodeport
								,true == is_primary ? "true":"false"
								,true == is_preferred ? "true":"false");	
		pfree(address);
	}
	appendStringInfoString(&cmdstring, "select pgxc_pool_reload();\"");
	
	foreach(cn_lc, prefer_cndn->coordiantor_list)
	{
		heap_freetuple((HeapTuple)lfirst(cn_lc));
	}
	foreach(dn_lc, prefer_cndn->datanode_list)
	{
		heap_freetuple((HeapTuple)lfirst(dn_lc));
	}
	if(PointerIsValid(prefer_cndn->coordiantor_list))
		list_free(prefer_cndn->coordiantor_list);
	if(PointerIsValid(prefer_cndn->datanode_list))
		list_free(prefer_cndn->datanode_list);
	pfree(prefer_cndn);
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
	if (!execok)
		ereport(WARNING, (errmsg("config all, create node on all coordinators fail %s", 
				getAgentCmdRst.description.data)));
	func_end:
		tup_result = build_common_command_tuple( &(getAgentCmdRst.nodename)
				,getAgentCmdRst.ret
				,getAgentCmdRst.ret == true ? "success":getAgentCmdRst.description.data);

	ma_close(ma);
	pfree(getAgentCmdRst.description.data);
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
	StringInfoData sendstrmsg;
	StringInfoData buf;
	
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
	mgr_recv_msg(ma, getAgentCmdRst);
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
	mgr_append_pgconf_paras_str_quotastr("log_destination", "csvlog", infosendparamsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendparamsg);
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
	NameData username;
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
		namestrcpy(&username, AGTM_USER);
	}
	else
	{
		namestrcpy(&username, NameStr(mgr_host->hostuser));
	}
	ReleaseSysCache(tup);
	
	/*primary_conninfo*/
	initStringInfo(&primary_conninfo_value);
	if (GTM_TYPE_GTM_SLAVE == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, masterport, username.data, "slave");
	else
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, masterport, username.data, "extra");
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
* datanode master: include all coordinators ip and the master's slave ip and extra ip
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
			/*get coordinator address*/
			cnaddress = get_hostaddress_from_hostoid(hostoid);
			if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype)
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", cnaddress, 32, "trust", infosendhbamsg);
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
				/*get address*/
				cnaddress = get_hostaddress_from_hostoid(hostoid);
				if (GTM_TYPE_GTM_MASTER == nodetype)
					mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, cnaddress, 32, "trust", infosendhbamsg);
				else
					mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", cnaddress, 32, "trust", infosendhbamsg);
				pfree(cnaddress);
			}
			else if ((CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype || CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype 
				|| CNDN_TYPE_DATANODE_EXTRA == mgr_node->nodetype) && GTM_TYPE_GTM_MASTER == nodetype)
			{
				/*hostoid*/
				hostoid = mgr_node->nodehost;
				/*get address*/
				cnaddress = get_hostaddress_from_hostoid(hostoid);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, cnaddress, 32, "trust", infosendhbamsg);
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
	HeapTuple tupleret = NULL;
	NameData nameattrdata;

	Assert(nodename);
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
	char *nodename = PG_GETARG_CSTRING(0);
	char *typestr = PG_GETARG_CSTRING(1);
	bool force_get = PG_GETARG_BOOL(2);
	char cmdtype = AGT_CMD_GTM_SLAVE_FAILOVER;
	char nodetype = GTM_TYPE_GTM_SLAVE;
	bool force = false;
	bool nodetypechange = false;
	Datum datum;

	if(force_get)
		force = true;

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
		nodetypechange = true;
		datum = get_failover_node_type(nodename, GTM_TYPE_GTM_SLAVE, GTM_TYPE_GTM_EXTRA, force);
		nodetype = DatumGetChar(datum);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("no such gtm type: %s", typestr)));
	}
	if(CNDN_TYPE_NONE_TYPE == nodetype)
		ereport(ERROR, (errmsg("gtm slave or extra does not exist in cluster")));
	return mgr_failover_one_dn_inner_func(nodename, cmdtype, nodetype, nodetypechange, force);
}

/*
* gtm slave promote to master, some work need to do: 
* 1.stop the old gtm master (before promote)
* 2.promote gtm slave to gtm master
* 3.wait the new master accept connect
* 4.refresh all datanode postgresql.conf:agtm_port,agtm_host and check reload, sync xid and check the result
* 5.refresh all coordinator postgresql.conf:agtm_port,agtm_host and check reload, sync xid and check the result
* 6.new gtm master: refresh postgresql.conf and reload it
* 7.delete old master record in node systbl
* 8.change slave type to master type
* 9.refresh gtm extra nodemasternameoid in node systbl and recovery.confs and restart gtm extra
*/
static void mgr_after_gtm_failover_handle(char *hostaddress, int cndnport, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, PGconn **pg_conn, Oid cnoid)
{
	StringInfoData infosendmsg;
	StringInfoData infosendsyncmsg;
	StringInfoData resultstrdata;
	StringInfoData recorderr;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodecn;
	Form_mgr_node mgr_nodetmp;
	Form_mgr_host mgr_host;
	HeapTuple tuple;
	HeapTuple mastertuple;
	HeapTuple cn_tuple;
	HeapTuple host_tuple;
	Oid hostOidtmp;
	Oid hostOid;
	Oid nodemasternameoid;
	Oid newgtmtupleoid;
	Datum datumPath;
	bool isNull;
	bool bget = false;
	bool reload_host = false;
	bool reload_port = false;
	char *cndnPathtmp;
	NameData cndnname;
	NameData cnnamedata;
	char *strlabel;
	char *address;
	char *strnodetype;
	char *pstr;
	char aimtuplenodetype;
	char nodetype;
	char nodeport_buf[10];
	ScanKeyData key[2];
	PGresult * volatile res = NULL;
	int maxtry = 15;
	int try = 0;
	int nrow = 0;

	initStringInfo(&infosendmsg);
	initStringInfo(&recorderr);
	newgtmtupleoid = HeapTupleGetOid(aimtuple);
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	hostOid = mgr_node->nodehost;
	nodemasternameoid = mgr_node->nodemasternameoid;
	aimtuplenodetype = mgr_node->nodetype;
	nodetype = (aimtuplenodetype == GTM_TYPE_GTM_SLAVE ? GTM_TYPE_GTM_EXTRA:GTM_TYPE_GTM_SLAVE);
	strlabel = (nodetype == GTM_TYPE_GTM_EXTRA ? "extra":"slave");
	/*get nodename*/
	namestrcpy(&cndnname,NameStr(mgr_node->nodename));
	address = get_hostaddress_from_hostoid(mgr_node->nodehost);
	sprintf(nodeport_buf, "%d", mgr_node->nodeport);

	/*wait the new master accept connect*/
	fputs(_("waiting for the new master can accept connections..."), stdout);
	fflush(stdout);
	/*check recovery finish*/
	while(1)
	{
		if (mgr_check_node_recovery_finish(mgr_node->nodetype, hostOid, mgr_node->nodeport, address))
			break;
		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}
	while(1)
	{
		if (pingNode(address, nodeport_buf) != 0)
		{
			fputs(_("."), stdout);
			fflush(stdout);
			pg_usleep(1 * 1000000L);
		}
		else
			break;
	}
	pfree(address);
	fputs(_(" done\n"), stdout);
	fflush(stdout);

	/*get agtm_port,agtm_host*/
	resetStringInfo(&infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("agtm_host", hostaddress, &infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", cndnport, &infosendmsg);

	initStringInfo(&resultstrdata);
	initStringInfo(&infosendsyncmsg);
	/*refresh datanode master/slave/extra reload agtm_port, agtm_host*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		if (mgr_nodetmp->nodetype == CNDN_TYPE_DATANODE_MASTER || mgr_nodetmp->nodetype == 
		CNDN_TYPE_DATANODE_SLAVE || mgr_nodetmp->nodetype == CNDN_TYPE_DATANODE_EXTRA)
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
			try = maxtry;
			address = get_hostaddress_from_hostoid(mgr_nodetmp->nodehost);
			strnodetype = mgr_nodetype_str(mgr_nodetmp->nodetype);
			ereport(LOG, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\"", strnodetype, NameStr(mgr_nodetmp->nodename))));
			while(try-- >= 0)
			{
				resetStringInfo(&(getAgentCmdRst->description));
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPathtmp, &infosendmsg, hostOidtmp, getAgentCmdRst);
				/*sleep 0.1s*/
				pg_usleep(100000L);

				/*check the agtm_host, agtm_port*/
				if(mgr_check_param_reload_postgresqlconf(mgr_nodetmp->nodetype, hostOidtmp, mgr_nodetmp->nodeport, address, "agtm_host", hostaddress)
					&& mgr_check_param_reload_postgresqlconf(mgr_nodetmp->nodetype, hostOidtmp, mgr_nodetmp->nodeport, address, "agtm_port", nodeport_buf))
				{
					break;
				}
			}
			if (try < 0)
			{
				ereport(WARNING, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\" fail", strnodetype, NameStr(mgr_nodetmp->nodename))));
				appendStringInfo(&recorderr, "on %s \"%s\" reload \"agtm_host\", \"agtm_port\" fail\n", strnodetype, NameStr(mgr_nodetmp->nodename));
			}
			pfree(strnodetype);

			/*datanode master: sync agtm xid*/
			if (CNDN_TYPE_DATANODE_MASTER == mgr_nodetmp->nodetype)
			{
				host_tuple = SearchSysCache1(HOSTHOSTOID, mgr_nodetmp->nodehost);
				if(!(HeapTupleIsValid(host_tuple)))
				{
					ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_nodetmp->nodehost)
						, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
						, errcode(ERRCODE_UNDEFINED_OBJECT)));
				}
				mgr_host= (Form_mgr_host)GETSTRUCT(host_tuple);
				Assert(mgr_host);
				try = maxtry;
				ereport(LOG, (errmsg("on datanode master \"%s\" execute \"%s\"", NameStr(mgr_nodetmp->nodename), "select * from sync_agtm_xid()")));
				while(try -- >= 0)
				{
					resetStringInfo(&resultstrdata);
					monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, "select * from sync_agtm_xid()", NameStr(mgr_host->hostuser), address, mgr_nodetmp->nodeport, DEFAULT_DB, &resultstrdata);
					pstr = resultstrdata.data;
					if (resultstrdata.len != 0 && strcasecmp(pstr, NameStr(mgr_nodetmp->nodename)) == 0)
					{
						break;
					}
				}
				ReleaseSysCache(host_tuple);
				if (try < 0)
				{
					ereport(WARNING, (errmsg("on datanode master \"%s\" execute \"%s\" fail", NameStr(mgr_nodetmp->nodename), "select * from sync_agtm_xid()")));
					appendStringInfo(&recorderr, "on datanode master \"%s\" execute \"%s\" fail\n", NameStr(mgr_nodetmp->nodename), "select * from sync_agtm_xid()");
				}
			}
			pfree(address);
		}
	}
	heap_endscan(rel_scan);

	/*get name of coordinator, whos oid is cnoid*/
	cn_tuple = SearchSysCache1(NODENODEOID, cnoid);
	if(!HeapTupleIsValid(cn_tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("oid \"%u\" of coordinator does not exist", cnoid)));
	}
	mgr_nodecn = (Form_mgr_node)GETSTRUCT(cn_tuple);
	Assert(cn_tuple);
	namestrcpy(&cnnamedata, NameStr(mgr_nodecn->nodename));
	ReleaseSysCache(cn_tuple);

	/*coordinator reload agtm_port, agtm_host*/
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		hostOidtmp = mgr_nodetmp->nodehost;
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp")
				, errmsg("column cndnpath is null")));
		}
		cndnPathtmp = TextDatumGetCString(datumPath);
		try = maxtry;
		ereport(LOG, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\"", NameStr(mgr_nodetmp->nodename))));
		while(try-- >=0)
		{
			resetStringInfo(&(getAgentCmdRst->description));
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPathtmp, &infosendmsg, hostOidtmp, getAgentCmdRst);

			pg_usleep(100000L);
			/*check the agtm_host, agtm_port*/
			reload_host = false;
			reload_port = false;
			resetStringInfo(&infosendsyncmsg);
			appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select setting from pg_settings where name=''agtm_host'';'", NameStr(mgr_nodetmp->nodename));
			res = PQexec(*pg_conn, infosendsyncmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				nrow = PQntuples(res);
				if (nrow > 0)
					if (strcasecmp(hostaddress, PQgetvalue(res, 0, 0)) == 0)
						reload_host = true;
			}
			PQclear(res);
			resetStringInfo(&infosendsyncmsg);
			appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select setting from pg_settings where name=''agtm_port'';'", NameStr(mgr_nodetmp->nodename));
			res = PQexec(*pg_conn, infosendsyncmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				nrow = PQntuples(res);
				if (nrow > 0)
					if (strcasecmp(nodeport_buf, PQgetvalue(res, 0, 0)) == 0)
						reload_port = true;
			}
			PQclear(res);
			if (reload_port && reload_host)
			{
				break;
			}
		}
		if (try < 0)
		{
			ereport(WARNING, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\" fail", NameStr(mgr_nodetmp->nodename))));
			appendStringInfo(&recorderr, "on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\" fail\n", NameStr(mgr_nodetmp->nodename));
		}
	}
	heap_endscan(rel_scan);

	/*send sync agtm xid*/
	rel_scan = heap_beginscan(noderel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		resetStringInfo(&infosendsyncmsg);
		appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select * from sync_agtm_xid()';", NameStr(mgr_nodetmp->nodename));
		ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, infosendsyncmsg.data)));
		try = maxtry;
		while(try-- >= 0)
		{
			res = PQexec(*pg_conn, infosendsyncmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				if (PQntuples(res) > 0)
					if (strcasecmp(NameStr(mgr_nodetmp->nodename), PQgetvalue(res, 0, 0)) == 0)
					{
						PQclear(res);
						break;
					}
			}
			PQclear(res);
		}
		if (try < 0)
		{
			ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("on coordinator \"%s\" execute \"%s\" fail", cnnamedata.data, infosendsyncmsg.data)));
			appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail\n", cnnamedata.data, infosendsyncmsg.data);
		}
	}
	heap_endscan(rel_scan);
	pfree(infosendsyncmsg.data);
	
	/*unlock cluster*/
	mgr_unlock_cluster(pg_conn);

	/*refresh new master postgresql.conf*/
	ereport(LOG, (errmsg("reload \"synchronous_standby_names\" in postgresql.conf of new gtm master \"%s\"", NameStr(mgr_node->nodename))));
	resetStringInfo(&infosendmsg);
	bget = mgr_check_node_exist_incluster(&cndnname, nodetype, true);
	address = get_hostaddress_from_hostoid(mgr_node->nodehost);
	if (bget)
	{
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", aimtuplenodetype == GTM_TYPE_GTM_SLAVE ? "extra":"slave", &infosendmsg);
	}
	else
	{
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	}
	try = maxtry;
	while (try-- >= 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*check*/
		if (mgr_check_param_reload_postgresqlconf(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, address, "synchronous_standby_names", bget ?(aimtuplenodetype == GTM_TYPE_GTM_SLAVE ? "extra":"slave"): ""))
			break;
	}
	pfree(address);
	if (try < 0)
	{
		ereport(WARNING, (errmsg("on gtm master \"%s\" reload \"synchronous_standby_names\" fail", NameStr(mgr_node->nodename))));
		appendStringInfo(&recorderr, "on gtm master \"%s\" reload \"synchronous_standby_names\" fail\n", NameStr(mgr_node->nodename));
	}

	ereport(LOG, (errmsg("refresh \"node\" table in ADB Manager for node \"%s\"", NameStr(mgr_node->nodename))));
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("gtm master \"%s\" does not exist", cndnname.data)));
	}
	else
	{
		/*delete old master record in node systbl*/
		simple_heap_delete(noderel, &mastertuple->t_self);
		CatalogUpdateIndexes(noderel, mastertuple);
		ReleaseSysCache(mastertuple);
	}
	/*change slave type to master type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	mgr_node->nodetype = GTM_TYPE_GTM_MASTER;
	mgr_node->nodemasternameoid = 0;
	mgr_node->nodesync = SPACE;
	heap_inplace_update(noderel, aimtuple);
	/*for mgr_updateparm systbl, drop the old master param, update slave parm info in the mgr_updateparm systbl*/
	ereport(LOG, (errmsg("refresh \"param\" table in ADB Manager for node \"%s\"", NameStr(mgr_node->nodename))));
	mgr_parm_after_gtm_failover_handle(&cndnname, GTM_TYPE_GTM_MASTER, &cndnname, aimtuplenodetype);
	
	if (!bget)
		ereport(WARNING, (errmsg("the new gtm master \"%s\" has no slave or extra, it is better to append a new gtm slave node", cndnname.data)));
	/*update gtm extra nodemasternameoid, refresh gtm extra recovery.conf*/
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
		mgr_nodetmp->nodesync = SYNC;
		heap_inplace_update(noderel, tuple);
		/*check the node is initialized or not*/
		if (!mgr_nodetmp->nodeincluster)
			continue;
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
		ereport(LOG, (errmsg("refresh recovery.conf of gtm %s \"%s\"", strlabel, NameStr(mgr_node->nodename))));
		cndnPathtmp = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPathtmp, &infosendmsg, mgr_nodetmp->nodehost, getAgentCmdRst);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh recovery.conf of agtm %s fail", strlabel)));
			appendStringInfo(&recorderr, "refresh recovery.conf of agtm %s fail\n", strlabel);
		}
		/*restart gtm extra*/
		ereport(LOG, (errmsg("agtm_ctl restart gtm %s", strlabel)));
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, getAgentCmdRst, noderel, tuple, SHUTDOWN_F);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("agtm_ctl restart gtm %s fail", strlabel)));
			appendStringInfo(&recorderr, "agtm_ctl restart gtm %s fail\n", strlabel);
		}
	}
	heap_endscan(rel_scan);

	pfree(infosendmsg.data);
	if (recorderr.len > 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		appendStringInfo(&(getAgentCmdRst->description), "%s", recorderr.data);
		getAgentCmdRst->ret = false;
	}
	pfree(recorderr.data);
}

/*
* datanode slave/extra failover, some work need to do.
* cmd: failover datanode slave/extra dn1
* 1.stop immediate old datanode master
* 2.promote datanode slave to datanode master
* 3.wait the new master accept connect
* 4.refresh pgxc_node on all coordinators
* 5. refresh synchronous_standby_names for new master
* 6.refresh node systbl: delete old master tuple and change slave type to master type
* 7.update param systbl
* 8.change the datanode  extra dn1's recovery.conf and restart it
* 
*/
static void mgr_after_datanode_failover_handle(Oid nodemasternameoid, Name cndnname, int cndnport,char *hostaddress, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, char aimtuplenodetype, PGconn **pg_conn, Oid cnoid)
{
	StringInfoData infosendmsg;
	StringInfoData recorderr;
	HeapScanDesc rel_scan;
	HeapTuple mastertuple;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodetmp;
	HeapTuple tuple;
	Oid newmastertupleoid;
	Datum datumPath;
	bool isNull;
	bool bgetextra = false;
	bool getrefresh = false;
	char *cndnPathtmp;
	char *strtmp;
	char *address;
	char secondnodetype;
	char coordport_buf[10];
	int maxtry = 15;
	int try;
	ScanKeyData key[3];

	initStringInfo(&recorderr);
	resetStringInfo(&(getAgentCmdRst->description));
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	newmastertupleoid = HeapTupleGetOid(aimtuple);
	address = get_hostaddress_from_hostoid(mgr_node->nodehost);
	sprintf(coordport_buf, "%d", mgr_node->nodeport);
	/*wait the new master accept connect*/
	fputs(_("waiting for the new master can accept connections..."), stdout);
	fflush(stdout);
	/*check recovery finish*/
	while(1)
	{
		if (mgr_check_node_recovery_finish(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, address))
			break;
		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}
	while(1)
	{
		if (pingNode(address, coordport_buf) != 0)
		{
			fputs(_("."), stdout);
			fflush(stdout);
			pg_usleep(1 * 1000000L);
		}
		else
			break;
	}
	fputs(_(" done\n"), stdout);
	fflush(stdout);

	/*refresh pgxc_node on all coordiantors*/
	getrefresh = mgr_pqexec_refresh_pgxc_node(FAILOVER, mgr_node->nodetype, NameStr(mgr_node->nodename), getAgentCmdRst, pg_conn, cnoid);
	if(!getrefresh)
	{
		getAgentCmdRst->ret = getrefresh;
		appendStringInfo(&recorderr, "%s\n", (getAgentCmdRst->description).data);
	}
	/*unlock cluster*/
	mgr_unlock_cluster(pg_conn);

	/*refresh new master synchronous_standby_names*/
	bgetextra = mgr_check_node_exist_incluster(cndnname, aimtuplenodetype==CNDN_TYPE_DATANODE_SLAVE ? CNDN_TYPE_DATANODE_EXTRA:CNDN_TYPE_DATANODE_SLAVE, true);
	/*refresh master's postgresql.conf*/
	ereport(LOG, (errmsg("reload \"synchronous_standby_names\" in postgresql.conf of new datanode master \"%s\"", NameStr(mgr_node->nodename))));
	initStringInfo(&infosendmsg);
	if(bgetextra)
	{
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", aimtuplenodetype==CNDN_TYPE_DATANODE_SLAVE ? "extra":"slave", &infosendmsg);
	}
	else
	{
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	}
	try = maxtry;
	while (try-- >= 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, mgr_node->nodehost, getAgentCmdRst);
		/*check*/
		if (mgr_check_param_reload_postgresqlconf(aimtuplenodetype, mgr_node->nodehost, mgr_node->nodeport, address, "synchronous_standby_names", bgetextra ?(aimtuplenodetype == CNDN_TYPE_DATANODE_SLAVE ? "extra":"slave"): ""))
				break;
	}
	if(try < 0)
	{
		ereport(WARNING, (errmsg("reload \"synchronous_standby_names\" in postgresql.conf of datanode master \"%s\" fail", NameStr(mgr_node->nodename))));
		appendStringInfo(&recorderr, "reload \"synchronous_standby_names\" in postgresql.conf of datanode master \"%s\" fail", NameStr(mgr_node->nodename));
	}
	pfree(address);

	/*delete old master record in node systbl*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("datanode master \"%s\" dosen't exist", cndnname->data)));
	}
	else
	{
		simple_heap_delete(noderel, &mastertuple->t_self);
		CatalogUpdateIndexes(noderel, mastertuple);
		ReleaseSysCache(mastertuple);
	}
	/*change slave type to master type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	mgr_node->nodeinited = true;
	mgr_node->nodetype = CNDN_TYPE_DATANODE_MASTER;
	mgr_node->nodemasternameoid = 0;
	mgr_node->nodesync = SPACE;
	heap_inplace_update(noderel, aimtuple);
	/*refresh parm systbl*/
	mgr_update_parm_after_dn_failover(cndnname, CNDN_TYPE_DATANODE_MASTER, cndnname, aimtuplenodetype);

	if (!bgetextra)
		ereport(WARNING, (errmsg("the datanode master \"%s\" has no slave or extra, it is better to append a new datanode slave node", cndnname->data)));
	
	secondnodetype = (aimtuplenodetype == CNDN_TYPE_DATANODE_SLAVE ? CNDN_TYPE_DATANODE_EXTRA:CNDN_TYPE_DATANODE_SLAVE);
	/*update datanode extra nodemasternameoid, refresh recovery.conf, restart the node*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(secondnodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(cndnname));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		/*update datanode extra/slave nodemasternameoid*/
		mgr_nodetmp->nodemasternameoid = newmastertupleoid;
		mgr_nodetmp->nodesync = SYNC;
		heap_inplace_update(noderel, tuple);
		/*check the node is initialized or not*/
		if (!mgr_nodetmp->nodeincluster)
			continue;
		/*refresh datanode extra/slave recovery.conf*/
		strtmp = (aimtuplenodetype == CNDN_TYPE_DATANODE_SLAVE ? "extra":"slave");
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(secondnodetype, strtmp, HeapTupleGetOid(aimtuple), &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("datanode %s %s column cndnpath is null", NameStr(mgr_nodetmp->nodename), strtmp)));
		}
		/*get cndnPathtmp from tuple*/
		ereport(LOG, (errmsg("refresh recovery.conf of datanode %s \"%s\"", strtmp, NameStr(mgr_node->nodename))));
		cndnPathtmp = TextDatumGetCString(datumPath);
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPathtmp, &infosendmsg, mgr_nodetmp->nodehost, getAgentCmdRst);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh recovery.conf of datanode %s %s fail", NameStr(mgr_nodetmp->nodename), strtmp)));
			appendStringInfo(&recorderr, "refresh recovery.conf of datanode %s %s fail\n", NameStr(mgr_nodetmp->nodename), strtmp);
		}
		/*restart datanode extra/slave*/
		ereport(LOG, (errmsg("pg_ctl restart datanode %s %s", NameStr(mgr_nodetmp->nodename), strtmp)));
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, getAgentCmdRst, noderel, tuple, SHUTDOWN_F);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("pg_ctl restart datanode %s %s fail", NameStr(mgr_nodetmp->nodename), strtmp)));
			appendStringInfo(&recorderr, "pg_ctl restart datanode %s %s fail\n", NameStr(mgr_nodetmp->nodename), strtmp);
		}
		break;
	}
	heap_endscan(rel_scan);
	pfree(infosendmsg.data);

	if (recorderr.len > 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		appendStringInfo(&(getAgentCmdRst->description), "%s", recorderr.data);
		getAgentCmdRst->ret = false;
	}
	pfree(recorderr.data);
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
			nodestring = "coordinator";
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
/*
* clean the given node: the command format: clean nodetype nodename
* clean gtm master/slave/extra gtm_name
* clean coordinator nodename, ...
* clean datanode master/slave/extra nodename, ...
*/

Datum mgr_clean_node(PG_FUNCTION_ARGS)
{
	char nodetype;
	char *nodename;
	char *user;
	char *address;
	NameData namedata;
	List *nodenamelist = NIL;
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ListCell   *cell;
	Form_mgr_node mgr_node;
	ScanKeyData key[2];
	char port_buf[10];
	int ret;
	/*ndoe type*/
	nodetype = PG_GETARG_CHAR(0);
	nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	/*check the node not in the cluster*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	foreach(cell, nodenamelist)
	{
		nodename = (char *) lfirst(cell);
		namestrcpy(&namedata, nodename);
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));
		
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
		if ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) == NULL)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		if (mgr_node->nodeincluster)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" already exists in cluster, cannot be cleaned", mgr_nodetype_str(nodetype), nodename)));
		}
		/*check node stoped*/
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		sprintf(port_buf, "%d", mgr_node->nodeport);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype || GTM_TYPE_GTM_EXTRA == mgr_node->nodetype)
			ret = pingNode_user(address, port_buf, AGTM_USER);
		else
			ret = pingNode_user(address, port_buf, user);
		pfree(address);
		pfree(user);
		if (ret == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" is running, cannot be cleaned, stop it first", mgr_nodetype_str(nodetype), nodename)));
		}
		heap_endscan(rel_scan);
	}
	heap_close(rel_node, RowExclusiveLock);

	return mgr_runmode_cndn(nodetype, AGT_CMD_CLEAN_NODE, nodenamelist, TAKEPLAPARM_N, fcinfo);
	
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
		mgr_clean_hba_table();/*clean the contxt of hba table*/
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

/*check given type of node exist*/
int mgr_check_node_exist_incluster(Name nodename, char nodetype, bool bincluster)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ScanKeyData key[3];
	HeapTuple tuple;
	bool getnode = false;
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
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(bincluster));

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 3, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		getnode = true;
	}

	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	return getnode;
}

static bool is_sync(char nodetype, char *nodename)
{
	Relation rel_node;
	Form_mgr_node mgr_node;
	HeapScanDesc rel_scan;
	ScanKeyData key[4];
	HeapTuple tuple;

	Assert(nodetype == CNDN_TYPE_COORDINATOR_SLAVE ||
			nodetype == CNDN_TYPE_DATANODE_SLAVE ||
			nodetype == CNDN_TYPE_DATANODE_EXTRA ||
			nodetype == GTM_TYPE_GTM_SLAVE ||
			nodetype == GTM_TYPE_GTM_EXTRA);

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
				,BoolGetDatum(false));
	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 4, key);
	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("Unable to find your append node.")));
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);

	if (mgr_node->nodesync == 't') /* sync */
	{
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		return true;
	}
	else if (mgr_node->nodesync == 'f') /* async */
	{
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		return false;
	}
	else
		ereport(ERROR, (errmsg("Unable to determine sync/async relationships.")));
}

static void get_nodestatus(char nodetype, char *nodename, bool *is_exist, bool *is_sync)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;

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
				,CharGetDatum(nodetype));
	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodename));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 4, key);
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
	*is_exist = true;
	if (mgr_node->nodesync == 't') /* sync */
	{
		*is_sync = true;
	}
	else if (mgr_node->nodesync == 'f') /* async */
	{
		*is_sync = false;
	}
	else
		ereport(ERROR, (errmsg("Unable to determine sync/async relationships.")));

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	return;   
}

/*acoording to the value of nodesync in node systable, refresh synchronous_standby_names in postgresql.conf of gtm 
* or datanode master.
*/
static void mgr_set_master_sync(void)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Datum datumpath;
	bool isNull = false;
	bool bslave_exist = false;
	bool bextra_exist = false;
	bool bslave_sync = false;
	bool bextra_sync = false;
	char *path;
	char *address;
	char *value;
	StringInfoData infosendmsg;
	Form_mgr_node mgr_node;
	GetAgentCmdRst getAgentCmdRst;
	
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	getAgentCmdRst.ret = false;
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (GTM_TYPE_GTM_MASTER != mgr_node->nodetype && CNDN_TYPE_DATANODE_MASTER != mgr_node->nodetype)
			continue;
		bslave_exist = false;
		bslave_sync = false;
		bextra_exist = false;
		bextra_sync = false;
		/*get master path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}		
		path = TextDatumGetCString(datumpath);
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype)
		{
			/*gtm slave sync status*/
			get_nodestatus(GTM_TYPE_GTM_SLAVE, NameStr(mgr_node->nodename), &bslave_exist, &bslave_sync);
			/*gtm extra sync status*/
			get_nodestatus(GTM_TYPE_GTM_EXTRA, NameStr(mgr_node->nodename), &bextra_exist, &bextra_sync);
		}
		else
		{
			/*datanode slave sync status*/
			get_nodestatus(CNDN_TYPE_DATANODE_SLAVE, NameStr(mgr_node->nodename), &bslave_exist, &bslave_sync);
			/*datanode extra sync status*/
			get_nodestatus(CNDN_TYPE_DATANODE_EXTRA, NameStr(mgr_node->nodename), &bextra_exist, &bextra_sync);
		}
		if (bslave_sync && bextra_sync)
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave,extra", &infosendmsg);
		else if (bslave_sync && (!bextra_sync))
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
		else if ((!bslave_sync) && bextra_sync)
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
		else
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, 
								path,
								&infosendmsg, 
								mgr_node->nodehost, 
								&getAgentCmdRst);

		value = &infosendmsg.data[strlen("synchronous_standby_names")+1];
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		ereport(LOG, (errmsg("%s, set %s synchronous_standby_names=%s.", address, path
								,value)));
		if (!getAgentCmdRst.ret)
		{			
			ereport(WARNING, (errmsg("%s, set %s synchronous_standby_names=%s failed.", address, path
					,value)));
		}
		pfree(address);
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);

}
/* the param:
 nodetype is owned to the slave or the extra 
 nodename is owned to the slave or the extra 
 new_sync is a new synchronous relationship of slave to master 
*/
static void mgr_alter_master_sync(char nodetype, char *nodename, bool new_sync)
{
	Relation rel;
	HeapTuple checktuple;
	HeapTuple tuple;
	Form_mgr_node mgr_master_node;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumpath;
	bool bslave_exist = false;
	bool bextra_exist = false;
	bool bslave_sync = false;
	bool bextra_sync = false;
	bool isNull = false;
	char *node_type_str;
	char *address;
	char *value;
	char *master_node_path;
	Oid hostoid;

	if(CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype || GTM_TYPE_GTM_MASTER == nodetype)
	{
		ereport(ERROR, (errmsg("synchronous relationship must set on the slave or the extra node")));
	}
	node_type_str = mgr_nodetype_str(nodetype);
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	/* check exists */
	checktuple = mgr_get_tuple_node_from_name_type(rel, nodename, nodetype);
	if (!HeapTupleIsValid(checktuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("%s \"%s\" dose not exists", node_type_str, nodename)));
	}
	heap_freetuple(checktuple);
	pfree(node_type_str);

	switch(nodetype)
	{
		case GTM_TYPE_GTM_SLAVE: 
			/*gtm extra sync status*/
			get_nodestatus(GTM_TYPE_GTM_EXTRA, nodename, &bextra_exist, &bextra_sync);
		    	bslave_sync = new_sync;
		break;
		case GTM_TYPE_GTM_EXTRA: 
			/*gtm slave sync status*/
			get_nodestatus(GTM_TYPE_GTM_SLAVE, nodename, &bslave_exist, &bslave_sync);
			bextra_sync = new_sync;
		break;
		case CNDN_TYPE_DATANODE_SLAVE: 
			/*datanode extra sync status*/
			get_nodestatus(CNDN_TYPE_DATANODE_EXTRA, nodename, &bextra_exist, &bextra_sync);	
			bslave_sync = new_sync;			
		break;
		case CNDN_TYPE_DATANODE_EXTRA: 
			/*datanode slave sync status*/
			get_nodestatus(CNDN_TYPE_DATANODE_SLAVE, nodename, &bslave_exist, &bslave_sync);
			bextra_sync = new_sync;
		break;
		default: break;
	}
	if((GTM_TYPE_GTM_SLAVE == nodetype)||(GTM_TYPE_GTM_EXTRA == nodetype))
	{

		node_type_str = mgr_nodetype_str(GTM_TYPE_GTM_MASTER);
		tuple = mgr_get_tuple_node_from_name_type(rel, nodename, GTM_TYPE_GTM_MASTER);
		if(!(HeapTupleIsValid(tuple)))
		{
			 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist",node_type_str, nodename))); 
		}
		mgr_master_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_master_node);
		hostoid = mgr_master_node->nodehost;
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}		
		master_node_path = TextDatumGetCString(datumpath);
		heap_freetuple(tuple);
		pfree(node_type_str);
		
	}else if((CNDN_TYPE_DATANODE_SLAVE == nodetype)||(CNDN_TYPE_DATANODE_EXTRA == nodetype))
	{
		node_type_str = mgr_nodetype_str(CNDN_TYPE_DATANODE_MASTER);
		tuple= mgr_get_tuple_node_from_name_type(rel, nodename, CNDN_TYPE_DATANODE_MASTER);
		if(!(HeapTupleIsValid(tuple)))
		{
			 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist",node_type_str, nodename))); 
		}
		mgr_master_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_master_node);
		hostoid = mgr_master_node->nodehost;
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}		
		master_node_path = TextDatumGetCString(datumpath);
		heap_freetuple(tuple);
		pfree(node_type_str);
	}
	else
	{
		heap_close(rel, RowExclusiveLock);
		return ;
	}
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	getAgentCmdRst.ret = false;
	/* step 1: update datanode master's postgresql.conf.*/
	resetStringInfo(&infosendmsg);
	if (bslave_sync && bextra_sync)
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave,extra", &infosendmsg);
	else if (bslave_sync && (!bextra_sync))
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "slave", &infosendmsg);
	else if ((!bslave_sync) && bextra_sync)
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "extra", &infosendmsg);
	else
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
							master_node_path,
							&infosendmsg,
							hostoid,
							&getAgentCmdRst);

	/* step 2: reload datanode master's postgresql.conf. */
	mgr_reload_conf(hostoid, master_node_path);
	value = &infosendmsg.data[strlen("synchronous_standby_names")+1];
	ereport(LOG, (errmsg("set hostoid %d path %s synchronous_standby_names=%s.", 
										hostoid, master_node_path,value)));
	if (!getAgentCmdRst.ret)
	{	
		address = get_hostaddress_from_hostoid(hostoid);
		ereport(WARNING, (errmsg("set address %s path %s synchronous_standby_names=%s failed.",
										address, master_node_path,value)));
		pfree(address);
	}
	heap_close(rel, RowExclusiveLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

static Datum get_failover_node_type(char *node_name, char slave_type, char extra_type, bool force)
{
	bool bslave_exist = false;
	bool bextra_exist = false;
	bool bslave_sync = false;
	bool bextra_sync = false;
	bool bslave_running = false;
	bool bextra_running = false;
	bool bslave_incluster = false;
	bool bextra_incluster = false;
	bool ret = false;

	Relation rel_node;
	HeapTuple aimtuple;
	Form_mgr_node mgr_node;
	StringInfoData port;
	char *host_addr = NULL;
	char node_type = CNDN_TYPE_NONE_TYPE;
	/*
		1、checking whether the standby node is incluster 
		2、checking whether the standby node is running
		3、sync mode priority higher than async mode
	*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	aimtuple = mgr_get_tuple_node_from_name_type(rel_node, node_name, slave_type);
	if (HeapTupleIsValid(aimtuple))
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		initStringInfo(&port);
		appendStringInfo(&port, "%d", mgr_node->nodeport);
		ret = pingNode(host_addr, port.data);
		if(ret == 0)
			bslave_running = true;
		else
			bslave_running = false;
		if(mgr_node->nodesync == 't')
			bslave_sync = true;
		else
			bslave_sync = false;

		bslave_incluster = mgr_node->nodeincluster;
		bslave_exist = true;
		pfree(port.data);
		pfree(host_addr);
		heap_freetuple(aimtuple);
	}		
	aimtuple = mgr_get_tuple_node_from_name_type(rel_node, node_name, extra_type);
	if (HeapTupleIsValid(aimtuple))
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		initStringInfo(&port);
		appendStringInfo(&port, "%d", mgr_node->nodeport);
		ret = pingNode(host_addr, port.data);
		if(ret == 0)
			bextra_running = true;
		else
			bextra_running = false;
		if(mgr_node->nodesync == 't')
			bextra_sync = true;
		else
			bextra_sync = false;
		bextra_incluster = mgr_node->nodeincluster;
		bextra_exist = true;
		pfree(port.data);
		pfree(host_addr);
		heap_freetuple(aimtuple);
	}
	
	if(bslave_exist == false && bextra_exist == false)
		ereport(ERROR, (errmsg("both of slave and extra \"%s\" do not exist", node_name)));
	if((bslave_running == false || bslave_incluster == false)&&(bextra_running == false || bextra_incluster == false))
		ereport(ERROR, (errmsg("both of slave and extra %s are not running or do not exist incluster", node_name)));	
	else
	{
		if(bslave_sync == true && bslave_running == true && bslave_incluster == true)
		{
			node_type = slave_type;
		}
		else if(bextra_sync == true && bextra_running == true && bextra_incluster == true)
		{
			node_type = extra_type;
		}
		else if(bslave_sync == false && bslave_running == true && bslave_incluster == true)
		{
			node_type = slave_type;
		}	
		else if(bextra_sync == false && bextra_running == true && bextra_incluster == true)
		{
			node_type = extra_type;
		}		
	}
	if(force == false)
	{		
		if(node_type == slave_type && bslave_sync == false)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("the slave node %s is async mode", node_name)
			, errhint("you can add \'force\' at the end, and enforcing execute failover")));	
		}
		else if(node_type == extra_type && bextra_sync == false)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("the extra node %s is async mode", node_name)
			, errhint("you can add \'force\' at the end, and enforcing execute failover")));
		}
	}
	/*close relation */
	heap_close(rel_node, RowExclusiveLock);
	return CharGetDatum(node_type);
}

/*
* get the command head word
*/
static void mgr_get_cmd_head_word(char cmdtype, char *str)
{
	Assert(str != NULL);

	switch(cmdtype)
	{
		case AGT_CMD_GTM_INIT:
		case AGT_CMD_GTM_SLAVE_INIT:
			strcpy(str, "initagtm");
			break;
		case AGT_CMD_GTM_START_MASTER:
		case AGT_CMD_GTM_START_SLAVE:
		case AGT_CMD_GTM_STOP_MASTER:
		case AGT_CMD_GTM_STOP_SLAVE:
		case AGT_CMD_GTM_SLAVE_FAILOVER:
		case AGT_CMD_AGTM_RESTART:
			strcpy(str, "agtm_ctl");
			break;
		case AGT_CMD_CN_RESTART:
		case AGT_CMD_CN_START:
		case AGT_CMD_CN_STOP:
		case AGT_CMD_DN_START:
		case AGT_CMD_DN_RESTART:
		case AGT_CMD_DN_STOP:
		case AGT_CMD_DN_FAILOVER:
		case AGT_CMD_NODE_RELOAD:
			strcpy(str, "pg_ctl");
			break;
		case AGT_CMD_GTM_CLEAN:
		case AGT_CMD_RM:
		case AGT_CMD_CLEAN_NODE:
			strcpy(str, "");
			break;
		case AGT_CMD_CNDN_CNDN_INIT:
			strcpy(str, "initdb");
			break;
		case AGT_CMD_CNDN_SLAVE_INIT:
			strcpy(str, "pg_basebackup");
			break;
		case AGT_CMD_PSQL_CMD:
			strcpy(str, "psql");
			break;
		case AGT_CMD_CNDN_REFRESH_PGSQLCONF:
		case AGT_CMD_CNDN_REFRESH_RECOVERCONF:
		case AGT_CMD_CNDN_REFRESH_PGHBACONF:
		case AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD:
		case AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE:
		case AGT_CMD_CNDN_RENAME_RECOVERCONF:
			strcpy(str, "update");
			break;
		case AGT_CMD_MONITOR_GETS_HOST_INFO:
			strcpy(str, "monitor");
			break;
		case AGT_CMD_PGDUMPALL:
			strcpy(str, "pg_dumpall");
			break;
		case AGT_CMD_STOP_AGENT:
			strcpy(str, "stop agent");
			break;
		case AGT_CMD_SHOW_AGTM_PARAM:
		case AGT_CMD_SHOW_CNDN_PARAM:
			strcpy(str, "show parameter");
			break;
		default:
			strcpy(str, "unknown cmd");
			break;
		str[strlen(str)-1]='\0';
	}
}

static struct tuple_cndn *get_new_pgxc_node(pgxc_node_operator cmd, char *node_name, char node_type)
{
	struct host
	{
		char *address;
		List *coordiantor_list;
		List *datanode_list;
	};
	StringInfoData file_name_str;
	Form_mgr_node mgr_dn_node, mgr_cn_node;
	
	Relation rel;
	HeapScanDesc scan;
	HeapTuple tup, temp_tuple;
	Form_mgr_node mgr_node;
	ListCell *lc_out, *lc_in, *cn_lc, *dn_lc;
	Datum host_addr;
	char *host_address;
	struct host *host_info = NULL;
	List *host_list = NIL;/*store cn and dn base on host*/
	struct tuple_cndn *leave_cndn = NULL;/*store the left cn and dn which */
	struct tuple_cndn *prefer_cndn = NULL;/*store the prefer datanode to the coordiantor one by one */
	bool isNull = false;
	StringInfoData str_port;
	char cn_dn_type;
	leave_cndn = palloc(sizeof(struct tuple_cndn));
	memset(leave_cndn,0,sizeof(struct tuple_cndn));
	prefer_cndn = palloc(sizeof(struct tuple_cndn));
	memset(prefer_cndn,0,sizeof(struct tuple_cndn));
	initStringInfo(&str_port);
	/*get dn and cn from mgr_host and mgr_node*/
	rel = heap_open(HostRelationId, AccessShareLock);
	scan = heap_beginscan(rel, SnapshotNow, 0, NULL);	
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{	
		host_addr = heap_getattr(tup, Anum_mgr_host_hostaddr, RelationGetDescr(rel), &isNull);
		host_address = pstrdup(TextDatumGetCString(host_addr));
		if(isNull)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddr is null")));
		host_info = palloc(sizeof(struct host));
		memset(host_info,0,sizeof(struct host));
		host_info->address = host_address;
		host_list = lappend(host_list, host_info);	
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	/*link the datanode and coordiantor to the list of host */
	rel= heap_open(NodeRelationId, AccessShareLock);
	scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);	
		cn_dn_type = mgr_node->nodetype;
		if((CNDN_TYPE_DATANODE_MASTER != cn_dn_type)&&(CNDN_TYPE_COORDINATOR_MASTER != cn_dn_type))
			continue;
		host_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if(true == mgr_node->nodeinited)
		{
			if(FAILOVER != cmd)
				temp_tuple = heap_copytuple(tup);
			else
			{
				if(strcmp( node_name, NameStr(mgr_node->nodename)) == 0)
				{				
					temp_tuple = mgr_get_tuple_node_from_name_type(rel, node_name, node_type);
					pfree(host_address);
					mgr_node = (Form_mgr_node)GETSTRUCT(temp_tuple);
					host_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
				}
				else
					temp_tuple = heap_copytuple(tup);
			}
		}	
		else
		{
			if(CONFIG == cmd)
			{
				resetStringInfo(&str_port);
				appendStringInfo(&str_port, "%d", mgr_node->nodeport);
				/*iust init ,but haven't alter the mgr_node table*/
				if(PQPING_OK == pingNode(host_address, str_port.data))
					temp_tuple = heap_copytuple(tup);
				else 
				{
					pfree(host_address);
					continue;
				}
			}
			else if(APPEND == cmd)
			{
				if(strcmp(node_name, NameStr(mgr_node->nodename)) == 0)
					temp_tuple = heap_copytuple(tup);
				else 
				{
					pfree(host_address);
					continue;
				}
			}/*may be operator FAILOVER ,and node table has member not init*/
			else 
			{
				pfree(host_address);
				continue;
			}		
		}				
		foreach(lc_out, host_list)
		{
			host_info = (struct host *)lfirst(lc_out);
			if(strcmp(host_info->address, host_address) == 0)				
				break;
		}
		/*not find host is correspind to node*/
		if(NULL == lc_out)
			continue;
		if(CNDN_TYPE_DATANODE_MASTER == cn_dn_type)
		{			
			host_info->datanode_list = lappend(host_info->datanode_list, temp_tuple);
		}				
		else if(CNDN_TYPE_COORDINATOR_MASTER == cn_dn_type)
		{
			host_info->coordiantor_list = lappend(host_info->coordiantor_list, temp_tuple);
		}
		pfree(host_address);
	}
	pfree(str_port.data);
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	/*calculate the prefer of pgxc_node */
	foreach(lc_out, host_list)
	{
		host_info = (struct host *)lfirst(lc_out);
		forboth(cn_lc, host_info->coordiantor_list, dn_lc, host_info->datanode_list)	
		{	
			temp_tuple = (HeapTuple)lfirst(cn_lc);
			prefer_cndn->coordiantor_list = lappend(prefer_cndn->coordiantor_list, temp_tuple);
			temp_tuple = (HeapTuple)lfirst(dn_lc);
			prefer_cndn->datanode_list = lappend(prefer_cndn->datanode_list, temp_tuple);
		}
		if(NULL == cn_lc )
		{
			for_each_cell(lc_in, dn_lc)
			{
				leave_cndn->datanode_list = lappend(leave_cndn->datanode_list, lfirst(lc_in));
			}			
		}
		else
		{
			for_each_cell(lc_in, cn_lc)
			{
				leave_cndn->coordiantor_list = lappend(leave_cndn->coordiantor_list, lfirst(lc_in));
			}						
		}	
		list_free(host_info->datanode_list);
		list_free(host_info->coordiantor_list);
	}
	list_free(host_list);
	foreach(cn_lc, leave_cndn->coordiantor_list)
	{	
		prefer_cndn->coordiantor_list = lappend(prefer_cndn->coordiantor_list, lfirst(cn_lc));
	}
	foreach(dn_lc, leave_cndn->datanode_list)	
	{
		prefer_cndn->datanode_list = lappend(prefer_cndn->datanode_list, lfirst(dn_lc));
	}
	list_free(leave_cndn->coordiantor_list);
	list_free(leave_cndn->datanode_list);
	pfree(leave_cndn);
	/*now the cn and prefer dn have store in list prefer_cndn
	but may be list leave_cndn still have member
	*/
	initStringInfo(&file_name_str);
	forboth(cn_lc, prefer_cndn->coordiantor_list, dn_lc, prefer_cndn->datanode_list)
	{
		temp_tuple =(HeapTuple)lfirst(cn_lc);
		mgr_cn_node = (Form_mgr_node)GETSTRUCT(temp_tuple);
		Assert(mgr_cn_node);
		temp_tuple =(HeapTuple)lfirst(dn_lc);
		mgr_dn_node = (Form_mgr_node)GETSTRUCT(temp_tuple);
		Assert(mgr_dn_node);
		appendStringInfo(&file_name_str, "%s\t%s",NameStr(mgr_cn_node->nodename),NameStr(mgr_dn_node->nodename));
	}
	
	return prefer_cndn;
}

static void mgr_check_appendnodeinfo(char node_type, char *append_node_name)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(append_node_name));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan(info->rel_node, SnapshotNow, 4, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		switch (node_type)
		{
			case CNDN_TYPE_COORDINATOR_MASTER:
				ereport(ERROR, (errmsg("coordinator \"%s\" already exists in cluster", append_node_name)));
				break;
			case CNDN_TYPE_DATANODE_MASTER:
				ereport(ERROR, (errmsg("datanode master \"%s\" already exists in cluster", append_node_name)));
				break;
			case CNDN_TYPE_DATANODE_SLAVE:
				ereport(ERROR, (errmsg("datanode slave \"%s\" already exists in cluster", append_node_name)));
				break;
			case CNDN_TYPE_DATANODE_EXTRA:
				ereport(ERROR, (errmsg("datanode extra \"%s\" already exists in cluster", append_node_name)));
				break;
			case GTM_TYPE_GTM_SLAVE:
				ereport(ERROR, (errmsg("gtm slave \"%s\" already exists in cluster", append_node_name)));
				break;
			case GTM_TYPE_GTM_EXTRA:
				ereport(ERROR, (errmsg("gtm extra \"%s\" already exists in cluster", append_node_name)));
				break;
			default:
				ereport(ERROR, (errmsg("node type \"%c\" already exists in cluster", node_type)));
				break;
		}
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static bool mgr_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst)
{
	struct tuple_cndn *prefer_cndn;
	ListCell *lc_out, *cn_lc, *dn_lc;
	int coordinator_num = 0, datanode_num = 0;
	HeapTuple tuple_in, tuple_out;
	StringInfoData cmdstring;
	StringInfoData buf;
	Form_mgr_node mgr_node_out, mgr_node_in;
	ManagerAgent *ma;
	char *host_address;
	bool is_preferred = false;
	bool execok = false;
	bool result = true;
	
	prefer_cndn = get_new_pgxc_node(cmd, dnname, nodetype);
	if(!PointerIsValid(prefer_cndn->coordiantor_list))
	{	
		appendStringInfoString(&(getAgentCmdRst->description),"not exist coordinator in the cluster");
		return false;
	}
		
	initStringInfo(&cmdstring);
	coordinator_num = 0;
	foreach(lc_out, prefer_cndn->coordiantor_list)
	{
		coordinator_num = coordinator_num + 1;
		tuple_out = (HeapTuple)lfirst(lc_out);		
		mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
		Assert(mgr_node_out);
		resetStringInfo(&(getAgentCmdRst->description));
		namestrcpy(&(getAgentCmdRst->nodename), NameStr(mgr_node_out->nodename));
		resetStringInfo(&cmdstring);
		host_address = get_hostaddress_from_hostoid(mgr_node_out->nodehost);
		appendStringInfo(&cmdstring, " -h %s -p %u -d %s -U %s -a -c \""
					,host_address
					,mgr_node_out->nodeport
					,DEFAULT_DB
					,get_hostuser_from_hostoid(mgr_node_out->nodehost));
		if(APPEND == cmd)
		{
			appendStringInfo(&cmdstring, "ALTER NODE \\\"%s\\\" WITH (HOST='%s', PORT=%d);"
								,NameStr(mgr_node_out->nodename)
								,host_address
								,mgr_node_out->nodeport);
		}
		pfree(host_address);
		datanode_num = 0;
		foreach(dn_lc, prefer_cndn->datanode_list)
		{
			datanode_num = datanode_num +1;
			tuple_in = (HeapTuple)lfirst(dn_lc);
			mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
			Assert(mgr_node_in);
			host_address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
			if(coordinator_num == datanode_num)
			{
				is_preferred = true;
			}
			else
			{
				is_preferred = false;
			}
			appendStringInfo(&cmdstring, "alter node \\\"%s\\\" with(host='%s', port=%d, preferred = %s);"
								,NameStr(mgr_node_in->nodename)
								,host_address
								,mgr_node_in->nodeport
								,true == is_preferred ? "true":"false");
			pfree(host_address);
		}
		appendStringInfoString(&cmdstring, "select pgxc_pool_reload();\"");

		/* connection agent */
		ma = ma_connect_hostoid(mgr_node_out->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));				
			result = false;
			break;
		}
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,cmdstring.data);
		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));	
			result = false;
			break;
		}
	}
	pfree(cmdstring.data);
	foreach(cn_lc, prefer_cndn->coordiantor_list)
	{
		heap_freetuple((HeapTuple)lfirst(cn_lc));
	}
	foreach(dn_lc, prefer_cndn->datanode_list)
	{
		heap_freetuple((HeapTuple)lfirst(dn_lc));
	}
	/*check the receive msg*/
	if(PointerIsValid(prefer_cndn->coordiantor_list))
	{
		execok = mgr_recv_msg(ma, getAgentCmdRst);
		if(execok != true)
		{
			result = false;
		}
		ma_close(ma);
	}
	if(PointerIsValid(prefer_cndn->coordiantor_list))
		list_free(prefer_cndn->coordiantor_list);
	if(PointerIsValid(prefer_cndn->datanode_list))
		list_free(prefer_cndn->datanode_list);
	pfree(prefer_cndn);
	return result;
}

/*
* modifty node port after initd cluster
*/

static void mgr_modify_port_after_initd(Relation rel_node, HeapTuple nodetuple, char *nodename, char nodetype, int32 newport)
{
	Form_mgr_node mgr_node;
	StringInfoData infosendmsg;
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	

	initStringInfo(&infosendmsg);
	/*if nodetype is slave or extra, need modfify its postgresql.conf for port*/
	if (GTM_TYPE_GTM_EXTRA == nodetype || GTM_TYPE_GTM_SLAVE == nodetype 
			|| CNDN_TYPE_DATANODE_EXTRA == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
	{
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", newport, &infosendmsg);
		mgr_modify_node_parameter_after_initd(rel_node, nodetuple, &infosendmsg, true);
	}
	/*if nodetype is gtm master, need modify its postgresql.conf and all datanodes、coordinators postgresql.conf for  agtm_port, agtm_host*/
	else if (GTM_TYPE_GTM_MASTER == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype)
	{
		/*gtm master*/
		if (CNDN_TYPE_DATANODE_MASTER == nodetype)
			mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", newport, &infosendmsg);
		mgr_modify_node_parameter_after_initd(rel_node, nodetuple, &infosendmsg, true);
		/*modify its slave/extra recovery.conf and datanodes coordinators postgresql.conf*/
		ScanKeyInit(&key[0]
					,Anum_mgr_node_nodeincluster
					,BTEqualStrategyNumber
					,F_BOOLEQ
					,BoolGetDatum(true));
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (GTM_TYPE_GTM_MASTER == nodetype)
			{
				if (GTM_TYPE_GTM_EXTRA == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype)
				{
					mgr_modify_port_recoveryconf(rel_node, tuple, newport);
				}
				else if (!(GTM_TYPE_GTM_MASTER == mgr_node->nodetype))
				{
					resetStringInfo(&infosendmsg);
					mgr_append_pgconf_paras_str_int("agtm_port", newport, &infosendmsg);
					mgr_modify_node_parameter_after_initd(rel_node, tuple, &infosendmsg, false);
				}
				else
				{
					/*do nothing*/
				}
			}
			else
			{
				if (CNDN_TYPE_DATANODE_EXTRA == mgr_node->nodetype || CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype)
				{
					if (strcmp(nodename, NameStr(mgr_node->nodename)) == 0)
						mgr_modify_port_recoveryconf(rel_node, tuple, newport);
				}
			}
		}
		heap_endscan(rel_scan);
		if (CNDN_TYPE_DATANODE_MASTER == nodetype)
		{
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, "ALTER NODE \\\"%s\\\" WITH (%s=%d);"
								,nodename
								,"port"
								,newport);
			mgr_modify_coord_pgxc_node(rel_node, &infosendmsg);
		}
	}
	else if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		/*refresh all pgxc_node all coordinators*/
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, "ALTER NODE \\\"%s\\\" WITH (%s=%d);"
							,nodename
							,"port"
							,newport);
		mgr_modify_coord_pgxc_node(rel_node, &infosendmsg);
		/*modify port*/
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", newport, &infosendmsg);
		mgr_modify_node_parameter_after_initd(rel_node, nodetuple, &infosendmsg, true);
	}
	else 
	{
		/*do nothing*/
	}
	
	pfree(infosendmsg.data);

}

/*
* modify the given node port after it initd
*/
static bool mgr_modify_node_parameter_after_initd(Relation rel_node, HeapTuple nodetuple, StringInfo infosendmsg, bool brestart)
{
	Form_mgr_node mgr_node;
	Datum datumpath;
	char *address;
	char *nodepath;
	char nodetype;
	bool isNull = false;
	bool bnormal = true;
	Oid hostoid;
	GetAgentCmdRst getAgentCmdRst;
	
	mgr_node = (Form_mgr_node)GETSTRUCT(nodetuple);
	Assert(mgr_node);
	/*get hostoid*/
	hostoid = mgr_node->nodehost;
	nodetype = mgr_node->nodetype;
	/*get path*/
	datumpath = heap_getattr(nodetuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodepath = TextDatumGetCString(datumpath);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodepath, infosendmsg, hostoid, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		ereport(WARNING, (errmsg("modify %s %s/postgresql.conf %s fail: %s", address, nodepath, infosendmsg->data, getAgentCmdRst.description.data)));
		pfree(address);
		bnormal = false;
	}
	if (brestart)
	{
		resetStringInfo(&(getAgentCmdRst.description));
		getAgentCmdRst.ret = false;
		switch(nodetype)
		{
			case GTM_TYPE_GTM_MASTER:
			case GTM_TYPE_GTM_SLAVE:
			case GTM_TYPE_GTM_EXTRA:
				mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, nodetuple, SHUTDOWN_F);
				break;
			case CNDN_TYPE_COORDINATOR_MASTER:
				mgr_runmode_cndn_get_result(AGT_CMD_CN_RESTART, &getAgentCmdRst, rel_node, nodetuple, SHUTDOWN_F);
				break;
			case CNDN_TYPE_DATANODE_MASTER:
			case CNDN_TYPE_DATANODE_SLAVE:
			case CNDN_TYPE_DATANODE_EXTRA:
				mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, nodetuple, SHUTDOWN_F);
				break;
			default:
				break;
		}
		if (!getAgentCmdRst.ret)
			bnormal = false;
		
	}
	pfree(getAgentCmdRst.description.data);
	return bnormal;
}

/*
* modify gtm or datanode slave or extra port in recovery.conf
*/
static void mgr_modify_port_recoveryconf(Relation rel_node, HeapTuple aimtuple, int32 master_newport)
{
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodemaster;
	Form_mgr_host mgr_host;
	HeapTuple mastertuple;
	HeapTuple tup;
	Datum datumpath;
	Oid masterhostoid;
	Oid mastertupleoid;
	Oid hostoid;
	char nodetype;
	char *masterhostaddress;
	char *nodepath;
	char *address;
	bool isNull = false;
	NameData username;
	StringInfoData primary_conninfo_value;
	StringInfoData infosendparamsg;
	GetAgentCmdRst getAgentCmdRst;
	
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	nodetype = mgr_node->nodetype;
	if (!(GTM_TYPE_GTM_SLAVE ==nodetype || GTM_TYPE_GTM_EXTRA == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype))
		return;
	mastertupleoid = mgr_node->nodemasternameoid;
	hostoid = mgr_node->nodehost;
	/*get path*/
	datumpath = heap_getattr(aimtuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodepath = TextDatumGetCString(datumpath);

	/*get the master port, master host address*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mastertupleoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errmsg("node oid \"%u\" not exist", mastertupleoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	mgr_nodemaster = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mastertuple);
	masterhostoid = mgr_nodemaster->nodehost;
	ReleaseSysCache(mastertuple);
	
	/*get host user from system: host*/
	tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(masterhostoid));
	if(!(HeapTupleIsValid(tup)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", masterhostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	if (GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
	{
		namestrcpy(&username, AGTM_USER);
	}
	else
	{
		namestrcpy(&username, NameStr(mgr_host->hostuser));
	}
	ReleaseSysCache(tup);
	
	/*primary_conninfo*/
	initStringInfo(&primary_conninfo_value);
	masterhostaddress = get_hostaddress_from_hostoid(masterhostoid);
	if (GTM_TYPE_GTM_SLAVE == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, master_newport, username.data, "slave");
	else
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, master_newport, username.data, "extra");
	initStringInfo(&infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendparamsg);
	pfree(primary_conninfo_value.data);
	pfree(masterhostaddress);
	
	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, nodepath, &infosendparamsg, hostoid, &getAgentCmdRst);
	pfree(infosendparamsg.data);
	if (!getAgentCmdRst.ret)
	{
		address = get_hostaddress_from_hostoid(hostoid);
		ereport(WARNING, (errmsg("modify %s %s/recovery.conf fail: %s", address, nodepath, getAgentCmdRst.description.data)));
		pfree(address);
	}
	switch(nodetype)
	{
		case GTM_TYPE_GTM_SLAVE:
		case GTM_TYPE_GTM_EXTRA:
			mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, aimtuple, SHUTDOWN_F);
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
		case CNDN_TYPE_DATANODE_EXTRA:
			mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, aimtuple, SHUTDOWN_F);
			break;
		default:
			break;
	}
	pfree(getAgentCmdRst.description.data);
}

/*
* modify coordinators port of pgxc_node
*/
static bool mgr_modify_coord_pgxc_node(Relation rel_node, StringInfo infostrdata)
{
	StringInfoData infosendmsg;
	StringInfoData buf;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	ScanKeyData key[2];
	char *host_address = "127.0.0.1";
	char *user;
	char *address;
	bool execok = false;
	bool bnormal= true;
	HeapScanDesc rel_scan;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, " -h %s -p %u -d %s -U %s -a -c \""
			,host_address
			,mgr_node->nodeport
			,DEFAULT_DB
			,user);
		appendStringInfo(&infosendmsg, "%s", infostrdata->data);
		appendStringInfo(&infosendmsg, " select pgxc_pool_reload();\"");
		pfree(user);
		/* connection agent */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */	
			ereport(WARNING, (errmsg("%s", ma_last_error_msg(ma))));
			ma_close(ma);
			break;
		}
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,infosendmsg.data);
		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			ereport(WARNING, (errmsg("%s", ma_last_error_msg(ma))));
			ma_close(ma);
			break;
		}
		resetStringInfo(&getAgentCmdRst.description);
		execok = mgr_recv_msg(ma, &getAgentCmdRst);
		ma_close(ma);
		if (!execok)
		{
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ereport(WARNING, (errmsg("refresh pgxc_node in %s fail: %s", address, getAgentCmdRst.description.data)));
			pfree(address);
			bnormal = false;
		}
	}
	heap_endscan(rel_scan);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);

	return bnormal;
}

/*
* modify address in host table after initd
* 1. alter all need address in host table (do this before this function)
* 2. check all node running normal, agent also running normal
* 3. add new address in pg_hba.conf of all nodes and reload it
* 4. refresh agtm_host of postgresql.conf in all coordinators and datanodes
* 5. refresh all pgxc_node of all coordinators
* 6. refresh recovery.conf of all slave and extra, then restart 
*/
void mgr_flushhost(MGRFlushHost *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall1(mgr_flush_host, (Datum)0);
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_flush_host(PG_FUNCTION_ARGS)
{
	ScanKeyData key[1];
	Form_mgr_node mgr_node;
	HeapScanDesc rel_scan;
	StringInfoData infosendmsg;
	StringInfoData infosqlsendmsg;
	HeapTuple tuple;
	Relation rel_node;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumpath;
	char nodetype;
	char *cndnpath;
	char *address = NULL;
	char *gtmmaster_address = NULL;
	bool isNull = false;
	bool bgetwarning = false;
	Oid hostoid;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*check agent running normal*/
	mgr_check_all_agent();
	/*check all master nodes running normal*/
	mgr_make_sure_all_running(GTM_TYPE_GTM_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);
	/*refresh pg_hba.conf*/
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype)
		{
			gtmmaster_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		}
		/*get master path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			if (gtmmaster_address)
				pfree(gtmmaster_address);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		hostoid = mgr_node->nodehost;
		cndnpath = TextDatumGetCString(datumpath);
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(tuple, mgr_node->nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnpath, &infosendmsg, hostoid, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ereport(WARNING, (errmsg("%s  add address in %s/pg_hba.conf fail: %s", address, cndnpath, getAgentCmdRst.description.data)));
			pfree(address);
			bgetwarning = true;
		}
		mgr_reload_conf(hostoid, cndnpath);
	}
	heap_endscan(rel_scan);

	initStringInfo(&infosqlsendmsg);
	/*refresh agtm_host of postgresql.conf in all coordinators and datanodes*/
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodetype = mgr_node->nodetype;
		if (nodetype == GTM_TYPE_GTM_MASTER || nodetype == GTM_TYPE_GTM_SLAVE || nodetype == GTM_TYPE_GTM_EXTRA)
			continue;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER)
			appendStringInfo(&infosqlsendmsg, "ALTER NODE \\\"%s\\\" WITH (%s='%s');"
							,NameStr(mgr_node->nodename)
							,"HOST"
							,address);
		pfree(address);
		/*get master path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			if (gtmmaster_address)
				pfree(gtmmaster_address);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("agtm_host", gtmmaster_address, &infosendmsg);
		if (!mgr_modify_node_parameter_after_initd(rel_node, tuple, &infosendmsg, false))
			bgetwarning = true;
	}
	if (gtmmaster_address)
		pfree(gtmmaster_address);
	heap_endscan(rel_scan);
	
	/*refresh all pgxc_node of all coordinators*/
	if(!mgr_modify_coord_pgxc_node(rel_node, &infosqlsendmsg))
		bgetwarning = true;
	
	/*refresh recovery.conf of all slave and extra, then restart*/
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodetype = mgr_node->nodetype;
		if (nodetype == GTM_TYPE_GTM_MASTER || nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER)
			continue;
		/*get node path*/
		isNull = false;
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		hostoid = mgr_node->nodehost;
		cndnpath = TextDatumGetCString(datumpath);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, "slave", mgr_node->nodemasternameoid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnpath, &infosendmsg, hostoid, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetwarning = true;
		}
		if (!getAgentCmdRst.ret)
		{
			address = get_hostaddress_from_hostoid(hostoid);
			ereport(WARNING, (errmsg("%s  add address in %s/recovery.conf fail: %s", address, cndnpath, getAgentCmdRst.description.data)));
			pfree(address);
			bgetwarning = true;
		}
		/*restart*/
		switch(nodetype)
		{
			case GTM_TYPE_GTM_SLAVE:
			case GTM_TYPE_GTM_EXTRA:
				mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
				break;
			case CNDN_TYPE_DATANODE_SLAVE:
			case CNDN_TYPE_DATANODE_EXTRA:
				mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
				break;
			default:
				break;
		}
		if (!getAgentCmdRst.ret)
			bgetwarning = true;
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	if (bgetwarning)
		PG_RETURN_BOOL(false);
	else
		PG_RETURN_BOOL(true);
}

static void mgr_check_all_agent(void)
{
	Form_mgr_host mgr_host;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Datum host_datumaddr;
	char *address;
	bool isNull = false;
	ManagerAgent *ma;
	Relation		rel_host;

	rel_host = heap_open(HostRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel_host, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
		Assert(mgr_host);
		/*get agent address and port*/
		host_datumaddr = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(rel_host), &isNull);
		if(isNull)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddr is null")));
		address = TextDatumGetCString(host_datumaddr);
		ma = ma_connect(address, mgr_host->hostagentport);
		if(!ma_isconnected(ma))
		{
			heap_endscan(rel_scan);
			heap_close(rel_host, AccessShareLock);
			ma_close(ma);
			ereport(ERROR, (errmsg("hostname \"%s\" : agent is not running", NameStr(mgr_host->hostname))));
		}
		ma_close(ma);
	}

	heap_endscan(rel_scan);
	heap_close(rel_host, AccessShareLock);
}

/*
* sql command for create extension
*/

static bool mgr_add_extension_sqlcmd(char *sqlstr)
{
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	ManagerAgent *ma;
	char *user;
	char *address;
	bool execok = false;
	StringInfoData infosendmsg;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	Relation rel_node;

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		break;
	}
	if (NULL == tuple)
	{
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("can not get right coordinator to execute \"%s\"", sqlstr)));
		return false;
	}
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " -h %s -p %u -d %s -U %s -a -c \""
		,"127.0.0.1"
		,mgr_node->nodeport
		,DEFAULT_DB
		,user);
	appendStringInfo(&infosendmsg, " %s\"", sqlstr);
	pfree(user);
	/* connection agent */
	ma = ma_connect_hostoid(mgr_node->nodehost);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("%s, %s", sqlstr, ma_last_error_msg(ma))));
		ma_close(ma);
		return false;
	}
	initStringInfo(&buf);
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
	ma_sendstring(&buf,infosendmsg.data);
	ma_endmessage(&buf, ma);
	pfree(infosendmsg.data);
	if (! ma_flush(ma, true))
	{
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("%s, %s", sqlstr, ma_last_error_msg(ma))));
		ma_close(ma);
		return false;
	}
	getAgentCmdRst.ret = false;
	initStringInfo(&getAgentCmdRst.description);
	execok = mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!execok)
	{
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg(" %s %s:  %s fail, %s", address, NameStr(mgr_node->nodename), sqlstr, getAgentCmdRst.description.data)));
		pfree(address);
	}

	pfree(getAgentCmdRst.description.data);
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);

	return execok;
}
Datum mgr_priv_list_to_all(PG_FUNCTION_ARGS)
{
	List *command_list = NIL;
	List *username_list = NIL;
	ListCell *lc = NULL;
	Value *command = NULL;
	char *username_list_str = NULL;
	Datum datum_command_list;

	char command_type = PG_GETARG_CHAR(0);
	Assert(command_type == PRIV_GRANT || command_type == PRIV_REVOKE);
	datum_command_list = PG_GETARG_DATUM(1);

	/* get command list and username list  */
	command_list = DecodeTextArrayToValueList(datum_command_list);
	username_list = get_username_list();

	/* check command is valid */
	mgr_check_command_valid(command_list);

	username_list_str = get_username_list_str(username_list);

	foreach(lc, command_list)
	{
		command = lfirst(lc);
		Assert(command && IsA(command, String));

		if (strcmp(strVal(command), "add") == 0)
			mgr_manage_add(command_type, username_list_str);
		else if (strcmp(strVal(command), "alter") == 0)
			mgr_manage_alter(command_type, username_list_str);
		else if (strcmp(strVal(command), "append") == 0)
			mgr_manage_append(command_type, username_list_str);
		else if (strcmp(strVal(command), "clean") == 0)
			mgr_manage_clean(command_type, username_list_str);
		else if (strcmp(strVal(command), "deploy") == 0)
			mgr_manage_deploy(command_type, username_list_str);
		else if (strcmp(strVal(command), "drop") == 0)
			mgr_manage_drop(command_type, username_list_str);
		else if (strcmp(strVal(command), "failover") == 0)
			mgr_manage_failover(command_type, username_list_str);
		else if (strcmp(strVal(command), "flush") == 0)
			mgr_manage_flush(command_type, username_list_str);
		else if (strcmp(strVal(command), "init") == 0)
			mgr_manage_init(command_type, username_list_str);
		else if (strcmp(strVal(command), "list") == 0)
			mgr_manage_list(command_type, username_list_str);
		else if (strcmp(strVal(command), "monitor") == 0)
			mgr_manage_monitor(command_type, username_list_str);
		else if (strcmp(strVal(command), "reset") == 0)
			mgr_manage_reset(command_type, username_list_str);
		else if (strcmp(strVal(command), "set") == 0)
			mgr_manage_set(command_type, username_list_str);
		else if (strcmp(strVal(command), "show") == 0)
			mgr_manage_show(command_type, username_list_str);
		else if (strcmp(strVal(command), "start") == 0)
			mgr_manage_start(command_type, username_list_str);
		else if (strcmp(strVal(command), "stop") == 0)
			mgr_manage_stop(command_type, username_list_str);
		else
			ereport(ERROR, (errmsg("unrecognized command type \"%s\"", strVal(command))));
	}

	if (command_type == PRIV_GRANT)
		PG_RETURN_TEXT_P(cstring_to_text("GRANT"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("REVOKE"));
}

Datum mgr_priv_all_to_username(PG_FUNCTION_ARGS)
{
	List *username_list = NIL;
	Datum datum_username_list;
	char *username_list_str = NULL;

	char command_type = PG_GETARG_CHAR(0);
	Assert(command_type == PRIV_GRANT || command_type == PRIV_REVOKE);

	datum_username_list = PG_GETARG_DATUM(1);
	username_list = DecodeTextArrayToValueList(datum_username_list);

	mgr_check_username_valid(username_list);

	username_list_str = get_username_list_str(username_list);
	mgr_priv_all(command_type, username_list_str);

	if (command_type == PRIV_GRANT)
		PG_RETURN_TEXT_P(cstring_to_text("GRANT"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("REVOKE"));
}

static void mgr_priv_all(char command_type, char *username_list_str)
{
	mgr_manage_add(command_type, username_list_str);
	mgr_manage_alter(command_type, username_list_str);
	mgr_manage_append(command_type, username_list_str);
	mgr_manage_clean(command_type, username_list_str);
	mgr_manage_deploy(command_type, username_list_str);
	mgr_manage_drop(command_type, username_list_str);
	mgr_manage_failover(command_type, username_list_str);
	mgr_manage_flush(command_type, username_list_str);
	mgr_manage_init(command_type, username_list_str);
	mgr_manage_list(command_type, username_list_str);
	mgr_manage_monitor(command_type, username_list_str);
	mgr_manage_reset(command_type, username_list_str);
	mgr_manage_set(command_type, username_list_str);
	mgr_manage_show(command_type, username_list_str);
	mgr_manage_start(command_type, username_list_str);
	mgr_manage_stop(command_type, username_list_str);

	return;
}

Datum mgr_priv_manage(PG_FUNCTION_ARGS)
{
	List *command_list = NIL;
	List *username_list = NIL;
	ListCell *lc = NULL;
	Value *command = NULL;
	char *username_list_str = NULL;
	Datum datum_command_list;
	Datum datum_username_list;

	char command_type = PG_GETARG_CHAR(0);
	Assert(command_type == PRIV_GRANT || command_type == PRIV_REVOKE);

	datum_command_list = PG_GETARG_DATUM(1);
	datum_username_list = PG_GETARG_DATUM(2);

	/* get command list and username list  */
	command_list = DecodeTextArrayToValueList(datum_command_list);
	username_list = DecodeTextArrayToValueList(datum_username_list);

	/* check command and username is valid */
	mgr_check_command_valid(command_list);
	mgr_check_username_valid(username_list);

	username_list_str = get_username_list_str(username_list);

	foreach(lc, command_list)
	{
		command = lfirst(lc);
		Assert(command && IsA(command, String));

		if (strcmp(strVal(command), "add") == 0)
			mgr_manage_add(command_type, username_list_str);
		else if (strcmp(strVal(command), "alter") == 0)
			mgr_manage_alter(command_type, username_list_str);
		else if (strcmp(strVal(command), "append") == 0)
			mgr_manage_append(command_type, username_list_str);
		else if (strcmp(strVal(command), "clean") == 0)
			mgr_manage_clean(command_type, username_list_str);
		else if (strcmp(strVal(command), "deploy") == 0)
			mgr_manage_deploy(command_type, username_list_str);
		else if (strcmp(strVal(command), "drop") == 0)
			mgr_manage_drop(command_type, username_list_str);
		else if (strcmp(strVal(command), "failover") == 0)
			mgr_manage_failover(command_type, username_list_str);
		else if (strcmp(strVal(command), "flush") == 0)
			mgr_manage_flush(command_type, username_list_str);
		else if (strcmp(strVal(command), "init") == 0)
			mgr_manage_init(command_type, username_list_str);
		else if (strcmp(strVal(command), "list") == 0)
			mgr_manage_list(command_type, username_list_str);
		else if (strcmp(strVal(command), "monitor") == 0)
			mgr_manage_monitor(command_type, username_list_str);
		else if (strcmp(strVal(command), "reset") == 0)
			mgr_manage_reset(command_type, username_list_str);
		else if (strcmp(strVal(command), "set") == 0)
			mgr_manage_set(command_type, username_list_str);
		else if (strcmp(strVal(command), "show") == 0)
			mgr_manage_show(command_type, username_list_str);
		else if (strcmp(strVal(command), "start") == 0)
			mgr_manage_start(command_type, username_list_str);
		else if (strcmp(strVal(command), "stop") == 0)
			mgr_manage_stop(command_type, username_list_str);
		else
			ereport(ERROR, (errmsg("unrecognized command type \"%s\"", strVal(command))));
	}

	if (command_type == PRIV_GRANT)
		PG_RETURN_TEXT_P(cstring_to_text("GRANT"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("REVOKE"));
}

static void mgr_manage_flush(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_flush_host() ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_flush_host() ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_stop_func(StringInfo commandsql)
{
	appendStringInfoString(commandsql, "mgr_stop_agent_all(), ");
	appendStringInfoString(commandsql, "mgr_stop_agent_hostnamelist(text[]), ");
	appendStringInfoString(commandsql, "mgr_stop_gtm_master(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_gtm_slave(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_gtm_extra(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_cn_master(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_dn_master(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_dn_slave(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_dn_extra(\"any\") ");

	return;
}

static void mgr_manage_stop_view(StringInfo commandsql)
{
	appendStringInfoString(commandsql, "adbmgr.stop_gtm_all, ");
	appendStringInfoString(commandsql, "adbmgr.stop_gtm_all_f, ");
	appendStringInfoString(commandsql, "adbmgr.stop_gtm_all_i, ");
	appendStringInfoString(commandsql, "adbmgr.stop_datanode_all, ");
	appendStringInfoString(commandsql, "adbmgr.stop_datanode_all_f, ");
	appendStringInfoString(commandsql, "adbmgr.stop_datanode_all_i, ");
	appendStringInfoString(commandsql, "adbmgr.stopall, ");
	appendStringInfoString(commandsql, "adbmgr.stopall_f, ");
	appendStringInfoString(commandsql, "adbmgr.stopall_i ");

	return;
}

static void mgr_manage_stop(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		mgr_manage_stop_func(&commandsql);
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		mgr_manage_stop_view(&commandsql);
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		mgr_manage_stop_func(&commandsql);
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		mgr_manage_stop_view(&commandsql);
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_deploy(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_deploy_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_deploy_hostnamelist(cstring, text[]) ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_deploy_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_deploy_hostnamelist(cstring, text[]) ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_reset(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_set(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_alter(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_alter_host_func(boolean, cstring, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_alter_node_func(boolean, \"char\", cstring, \"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_alter_host_func(boolean, cstring, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_alter_node_func(boolean, \"char\", cstring, \"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_drop(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_drop_host_func(boolean, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_node_func(boolean, \"char\", \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_hba(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_drop_host_func(boolean, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_node_func(boolean, \"char\", \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_hba(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_add(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_host_func(boolean,cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_node_func(boolean,\"char\",cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_hba(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_host_func(boolean,cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_node_func(boolean,\"char\",cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_hba(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_start(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_start_agent_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_start_agent_hostnamelist(cstring,text[]), ");
		appendStringInfoString(&commandsql, "mgr_start_gtm_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_gtm_slave(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_gtm_extra(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_cn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_slave(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_extra(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.start_gtm_all, ");
		appendStringInfoString(&commandsql, "adbmgr.start_datanode_all, ");
		appendStringInfoString(&commandsql, "adbmgr.startall ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_start_agent_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_start_agent_hostnamelist(cstring,text[]), ");
		appendStringInfoString(&commandsql, "mgr_start_gtm_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_gtm_slave(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_gtm_extra(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_cn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_slave(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_extra(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.start_gtm_all, ");
		appendStringInfoString(&commandsql, "adbmgr.start_datanode_all, ");
		appendStringInfoString(&commandsql, "adbmgr.startall ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_show(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_show_var_param(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_show_var_param(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_monitor(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_hostlist(text[]), ");
		appendStringInfoString(&commandsql, "mgr_monitor_gtm_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_datanode_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_namelist(bigint, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_all(bigint) ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.monitor_all ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_hostlist(text[]), ");
		appendStringInfoString(&commandsql, "mgr_monitor_gtm_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_datanode_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_namelist(bigint, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_all(bigint) ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.monitor_all ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_list(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_list_hba_by_name(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.host, ");
		appendStringInfoString(&commandsql, "adbmgr.node, ");
		appendStringInfoString(&commandsql, "adbmgr.updateparm, ");
		appendStringInfoString(&commandsql, "adbmgr.hba ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_list_hba_by_name(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.host, ");
		appendStringInfoString(&commandsql, "adbmgr.node, ");
		appendStringInfoString(&commandsql, "adbmgr.updateparm, ");
		appendStringInfoString(&commandsql, "adbmgr.hba ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_clean(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_clean_all(), ");
		appendStringInfoString(&commandsql, "mgr_clean_node(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_clean_all(), ");
		appendStringInfoString(&commandsql, "mgr_clean_node(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_failover(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_failover_one_dn(cstring, cstring, boolean), ");
		appendStringInfoString(&commandsql, "mgr_failover_gtm(cstring,cstring,boolean) ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_failover_one_dn(cstring, cstring, boolean), ");
		appendStringInfoString(&commandsql, "mgr_failover_gtm(cstring,cstring,boolean) ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_append(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_append_dnmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_dnslave(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_dnextra(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_coordmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_agtmslave(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_agtmextra(cstring) ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_append_dnmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_dnslave(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_dnextra(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_coordmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_agtmslave(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_agtmextra(cstring) ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_init(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant select on schema.view [, ...] to user [, ...] */
		appendStringInfoString(&commandsql, "GRANT select ON adbmgr.initall TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke select on schema.view [, ...] from user [, ...] */
		appendStringInfoString(&commandsql, "REVOKE select ON adbmgr.initall FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static char *get_username_list_str(List *username_list)
{
	StringInfoData username_list_str;
	ListCell *lc = NULL;
	Value *username = NULL;

	initStringInfo(&username_list_str);

	foreach(lc, username_list)
	{
		username = lfirst(lc);
		Assert(username && IsA(username, String));

		/* add double quotes for the user name */
		/* in order to make the user name in digital or pure digital effective */
		appendStringInfoChar(&username_list_str, '"');
		appendStringInfoString(&username_list_str, strVal(username));
		appendStringInfoChar(&username_list_str, '"');

		appendStringInfoChar(&username_list_str, ',');
	}

	username_list_str.data[username_list_str.len - 1] = '\0';
	return username_list_str.data;
}

static void mgr_check_command_valid(List *command_list)
{
	ListCell *lc = NULL;
	Value *command = NULL;
	char *command_str = NULL;

	foreach(lc, command_list)
	{
		command = lfirst(lc);
		Assert(command && IsA(command, String));

		command_str = strVal(command);

		if (strcmp(command_str, "add") == 0      ||
			strcmp(command_str, "alter") == 0    ||
			strcmp(command_str, "append") == 0   ||
			strcmp(command_str, "clean") == 0    ||
			strcmp(command_str, "deploy") == 0   ||
			strcmp(command_str, "drop") == 0     ||
			strcmp(command_str, "failover") == 0 ||
			strcmp(command_str, "flush") == 0    ||
			strcmp(command_str, "init") == 0     ||
			strcmp(command_str, "list") == 0     ||
			strcmp(command_str, "monitor") == 0  ||
			strcmp(command_str, "reset") == 0    ||
			strcmp(command_str, "set") == 0      ||
			strcmp(command_str, "show") == 0     ||
			strcmp(command_str, "start") == 0    ||
			strcmp(command_str, "stop") == 0 )
			continue;
		else
			ereport(ERROR, (errmsg("unrecognized command type \"%s\"", command_str)));
	}

	return ;
}

static void mgr_check_username_valid(List *username_list)
{
	ListCell *lc = NULL;
	Value *username = NULL;
	Oid oid;

	foreach(lc, username_list)
	{
		username = lfirst(lc);
		Assert(username && IsA(username, String));

		oid = GetSysCacheOid1(AUTHNAME, CStringGetDatum(strVal(username)));
		if (!OidIsValid(oid))
			ereport(ERROR, (errmsg("role \"%s\" does not exist", strVal(username))));
		else
			continue;
	}

	return ;
}

Datum mgr_list_acl_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitAclInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	char *username;
	Form_pg_authid pg_authid;
	StringInfoData acl;

	initStringInfo(&acl);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_authid = heap_open(AuthIdRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan(info->rel_authid, SnapshotNow, 0, NULL);
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
		heap_close(info->rel_authid, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	pg_authid = (Form_pg_authid)GETSTRUCT(tup);
	Assert(pg_authid);

	resetStringInfo(&acl);
	username = NameStr(pg_authid->rolname);
	mgr_get_acl_by_username(username, &acl);
	tup_result = build_list_acl_command_tuple(&(pg_authid->rolname), acl.data);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

static void mgr_get_acl_by_username(char *username, StringInfo acl)
{
	Oid roleid;

	roleid = mgr_get_role_oid_or_public(username);
	if (superuser_arg(roleid))
	{
		appendStringInfo(acl, "superuser");
		return ;
	}

	if (mgr_acl_add(username))
		appendStringInfo(acl, "add ");

	if (mgr_acl_alter(username))
		appendStringInfo(acl, "alter ");

	if (mgr_acl_append(username))
		appendStringInfo(acl, "append ");

	if (mgr_acl_clean(username))
		appendStringInfo(acl, "clean ");

	if (mgr_acl_deploy(username))
		appendStringInfo(acl, "deploy ");

	if (mgr_acl_drop(username))
		appendStringInfo(acl, "drop ");

	if (mgr_acl_failover(username))
		appendStringInfo(acl, "failover ");

	if (mgr_acl_flush(username))
		appendStringInfo(acl, "flush ");

	if (mgr_acl_init(username))
		appendStringInfo(acl, "init ");

	if (mgr_acl_list(username))
		appendStringInfo(acl, "list ");

	if (mgr_acl_monitor(username))
		appendStringInfo(acl, "monitor ");

	if (mgr_acl_reset(username))
		appendStringInfo(acl, "reset ");

	if (mgr_acl_set(username))
		appendStringInfo(acl, "set ");

	if (mgr_acl_show(username))
		appendStringInfo(acl, "show ");

	if (mgr_acl_start(username))
		appendStringInfo(acl, "start ");

	if (mgr_acl_stop(username))
		appendStringInfo(acl, "stop ");

	return;
}

static bool mgr_acl_flush(char *username)
{
	return mgr_has_func_priv(username, "mgr_flush_host()", "execute");
}

static bool mgr_acl_stop(char *username)
{
	bool f1, f2, f3, f4, f5, f6, f7, f8, f9;
	bool t1, t2, t3, t4, t5, t6, t7, t8, t9;

	f1 = mgr_has_func_priv(username, "mgr_stop_agent_all()", "execute");
	f2 = mgr_has_func_priv(username, "mgr_stop_agent_hostnamelist(text[])", "execute");
	f3 = mgr_has_func_priv(username, "mgr_stop_gtm_master(\"any\")", "execute");
	f4 = mgr_has_func_priv(username, "mgr_stop_gtm_slave(\"any\")", "execute");
	f5 = mgr_has_func_priv(username, "mgr_stop_gtm_extra(\"any\")", "execute");
	f6 = mgr_has_func_priv(username, "mgr_stop_cn_master(\"any\")", "execute");
	f7 = mgr_has_func_priv(username, "mgr_stop_dn_master(\"any\")", "execute");
	f8 = mgr_has_func_priv(username, "mgr_stop_dn_slave(\"any\")", "execute");
	f9 = mgr_has_func_priv(username, "mgr_stop_dn_extra(\"any\")", "execute");

	t1 = mgr_has_table_priv(username, "adbmgr.stop_gtm_all", "select");
	t2 = mgr_has_table_priv(username, "adbmgr.stop_gtm_all_f", "select");
	t3 = mgr_has_table_priv(username, "adbmgr.stop_gtm_all_i", "select");
	t4 = mgr_has_table_priv(username, "adbmgr.stop_datanode_all", "select");
	t5 = mgr_has_table_priv(username, "adbmgr.stop_datanode_all_f", "select");
	t6 = mgr_has_table_priv(username, "adbmgr.stop_datanode_all_i", "select");
	t7 = mgr_has_table_priv(username, "adbmgr.stopall", "select");
	t8 = mgr_has_table_priv(username, "adbmgr.stopall_f", "select");
	t9 = mgr_has_table_priv(username, "adbmgr.stopall_i", "select");

	return (f1 && f2 && f3 && f4 && f5 && f6 && f7 && f8 && f9 &&
			t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9);
}

static bool mgr_acl_deploy(char *username)
{
	bool f1, f2;

	f1 = mgr_has_func_priv(username, "mgr_deploy_all(cstring)", "execute");
	f2 = mgr_has_func_priv(username, "mgr_deploy_hostnamelist(cstring, text[])", "execute");

	return (f1 && f2);
}

bool mgr_has_priv_reset(void)
{
	bool f1;

	f1 = mgr_has_function_privilege_name("mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\")",
										"execute");
	return (f1);
}

static bool mgr_acl_reset(char *username)
{
	bool f1;

	f1 = mgr_has_func_priv(username,
							"mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\")",
							"execute");

	return f1;
}

bool mgr_has_priv_set(void)
{
	bool f1;

	f1 = mgr_has_function_privilege_name("mgr_add_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\")",
										"execute");
	return (f1);
}

static bool mgr_acl_set(char *username)
{
	bool f1;

	f1 = mgr_has_func_priv(username,
							"mgr_add_updateparm_func(\"char\", cstring, \"char\", boiolean, \"any\")",
							"execute");

	return f1;
}

bool mgr_has_priv_alter(void)
{
	bool f1, f2;

	f1 = mgr_has_function_privilege_name("mgr_alter_host_func(boolean, cstring, \"any\")", "execute");
	f2 = mgr_has_function_privilege_name("mgr_alter_node_func(boolean, \"char\", cstring, \"any\")", "execute");

	return (f1 && f2);
}

static bool mgr_acl_alter(char *username)
{
	bool f1, f2;

	f1 = mgr_has_func_priv(username, "mgr_alter_host_func(boolean, cstring, \"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_alter_node_func(boolean, \"char\", cstring, \"any\")", "execute");

	return (f1 && f2);
}

bool mgr_has_priv_drop(void)
{
	bool f1, f2, f3;

	f1 = mgr_has_function_privilege_name("mgr_drop_host_func(boolean, \"any\")", "execute");
	f2 = mgr_has_function_privilege_name("mgr_drop_node_func(boolean, \"char\", \"any\")", "execute");
	f3 = mgr_has_function_privilege_name("mgr_drop_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

static bool mgr_acl_drop(char *username)
{
	bool f1, f2, f3;

	f1 = mgr_has_func_priv(username, "mgr_drop_host_func(boolean,\"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_drop_node_func(boolean,\"char\",\"any\")", "execute");
	f3 = mgr_has_func_priv(username, "mgr_drop_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

bool mgr_has_priv_add(void)
{
	bool f1, f2, f3;

	f1 = mgr_has_function_privilege_name("mgr_add_host_func(boolean,cstring,\"any\")", "execute");
	f2 = mgr_has_function_privilege_name("mgr_add_node_func(boolean,\"char\",cstring,\"any\")", "execute");
	f3 = mgr_has_function_privilege_name("mgr_add_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

static bool mgr_acl_add(char *username)
{
	bool f1, f2, f3;

	f1 = mgr_has_func_priv(username, "mgr_add_host_func(boolean,cstring,\"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_add_node_func(boolean,\"char\",cstring,\"any\")", "execute");
	f3 = mgr_has_func_priv(username, "mgr_add_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

static bool mgr_acl_start(char *username)
{
	bool f1, f2, f3, f4, f5, f6, f7, f8, f9;
	bool t1, t2, t3;

	f1 = mgr_has_func_priv(username, "mgr_start_agent_all(cstring)", "execute");
	f2 = mgr_has_func_priv(username, "mgr_stop_agent_hostnamelist(text[])", "execute");
	f3 = mgr_has_func_priv(username, "mgr_start_gtm_master(\"any\")", "execute");
	f4 = mgr_has_func_priv(username, "mgr_start_gtm_slave(\"any\")", "execute");
	f5 = mgr_has_func_priv(username, "mgr_start_gtm_extra(\"any\")", "execute");
	f6 = mgr_has_func_priv(username, "mgr_start_cn_master(\"any\")", "execute");
	f7 = mgr_has_func_priv(username, "mgr_start_dn_master(\"any\")", "execute");
	f8 = mgr_has_func_priv(username, "mgr_start_dn_slave(\"any\")", "execute");
	f9 = mgr_has_func_priv(username, "mgr_start_dn_extra(\"any\")", "execute");

	t1 = mgr_has_table_priv(username, "adbmgr.start_gtm_all", "select");
	t2 = mgr_has_table_priv(username, "adbmgr.start_datanode_all", "select");
	t3 = mgr_has_table_priv(username, "adbmgr.startall", "select");

	return (f1 && f2 && f3 && f4 && f5 && f6 && f7 && f8 && f9 && t1 && t2 && t3);
}

static bool mgr_acl_show(char *username)
{
	return mgr_has_func_priv(username, "mgr_show_var_param(\"any\")", "execute");
}

static bool mgr_acl_monitor(char *username)
{
	bool f1, f2, f3, f4, f5, f6;
	bool t1;

	f1 = mgr_has_func_priv(username, "mgr_monitor_agent_all()", "execute");
	f2 = mgr_has_func_priv(username, "mgr_monitor_agent_hostlist(text[])", "execute");
	f3 = mgr_has_func_priv(username, "mgr_monitor_gtm_all()", "execute");
	f4 = mgr_has_func_priv(username, "mgr_monitor_datanode_all()", "execute");
	f5 = mgr_has_func_priv(username, "mgr_monitor_nodetype_namelist(bigint, \"any\")", "execute");
	f6 = mgr_has_func_priv(username, "mgr_monitor_nodetype_all(bigint)", "execute");

	t1 = mgr_has_table_priv(username, "adbmgr.monitor_all", "select");

	return (f1 && f2 && f3 && f4 && f5 && f6 && t1);
}

static bool mgr_acl_list(char *username)
{
	bool func;
	bool table_host;
	bool table_node;
	bool table_parm;
	bool table_hba;

	func       = mgr_has_func_priv(username, "mgr_list_hba_by_name(\"any\")", "execute");
	table_host = mgr_has_table_priv(username, "adbmgr.host", "select");
	table_node = mgr_has_table_priv(username, "adbmgr.node", "select");
	table_parm = mgr_has_table_priv(username, "adbmgr.updateparm", "select");
	table_hba  = mgr_has_table_priv(username, "adbmgr.hba", "select");

	return (func && table_host &&
			table_node && table_parm &&
			table_hba);
}

static bool mgr_acl_append(char *username)
{
	bool func_dnmaster;
	bool func_dnslave;
	bool func_dnextra;
	bool func_cdmaster;
	bool func_gtmslave;
	bool func_gtmextra;

	func_dnmaster = mgr_has_func_priv(username, "mgr_append_dnmaster(cstring)", "execute");
	func_dnslave  = mgr_has_func_priv(username, "mgr_append_dnslave(cstring)", "execute");
	func_dnextra  = mgr_has_func_priv(username, "mgr_append_dnextra(cstring)", "execute");
	func_cdmaster = mgr_has_func_priv(username, "mgr_append_coordmaster(cstring)", "execute");
	func_gtmslave = mgr_has_func_priv(username, "mgr_append_agtmslave(cstring)", "execute");
	func_gtmextra = mgr_has_func_priv(username, "mgr_append_agtmextra(cstring)", "execute");

	return (func_dnmaster && func_dnslave &&
			func_dnextra && func_cdmaster &&
			func_gtmslave && func_gtmextra);
}

static bool mgr_acl_failover(char *username)
{
	bool func_gtm;
	bool func_dn;

	func_dn  = mgr_has_func_priv(username, "mgr_failover_one_dn(cstring,cstring,boolean)", "execute");
	func_gtm = mgr_has_func_priv(username, "mgr_failover_gtm(cstring,cstring,boolean)", "execute");

	return (func_gtm && func_dn);
}

static bool mgr_acl_clean(char *username)
{
	bool f1, f2;

	f1 = mgr_has_func_priv(username, "mgr_clean_all()", "execute");
	f2 = mgr_has_func_priv(username, "mgr_clean_node (\"any\")", "execute");

	return (f1 && f2);
}

static bool mgr_acl_init(char *username)
{
	return mgr_has_table_priv(username, "adbmgr.initall", "select");
}

static bool mgr_has_table_priv(char *rolename, char *tablename, char *priv_type)
{
	Datum aclresult;
	NameData name;
	namestrcpy(&name, rolename);

	aclresult = DirectFunctionCall3(has_table_privilege_name_name,
									NameGetDatum(&name),
									CStringGetTextDatum(tablename),
									CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

static bool mgr_has_func_priv(char *rolename, char *funcname, char *priv_type)
{
	Datum aclresult;
	NameData name;
	namestrcpy(&name, rolename);

	aclresult = DirectFunctionCall3(has_function_privilege_name_name,
									NameGetDatum(&name),
									CStringGetTextDatum(funcname),
									CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

static List *get_username_list(void)
{
	Relation pg_authid_rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;

	Form_pg_authid pg_authid;
	List *username_list = NULL;

	pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);
	rel_scan =  heap_beginscan(pg_authid_rel, SnapshotNow, 0, NULL);

	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		pg_authid = (Form_pg_authid)GETSTRUCT(tuple);
		Assert(pg_authid);

		username_list = lappend(username_list, makeString(NameStr(pg_authid->rolname)));
	}

	heap_endscan(rel_scan);
	heap_close(pg_authid_rel, AccessShareLock);

	return username_list;
}

static Oid mgr_get_role_oid_or_public(const char *rolname)
{
	if (strcmp(rolname, "public") == 0)
		return ACL_ID_PUBLIC;

	return get_role_oid(rolname, false);
}

List* mgr_get_nodetype_namelist(char nodetype)
{
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Relation rel_node;
	List *nodenamelist =NIL;
	Form_mgr_node mgr_node;

	rel_node = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
					Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetype));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodenamelist = lappend(nodenamelist, NameStr(mgr_node->nodename));
		if (GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
						break;
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);
	return nodenamelist;
}

static void mgr_lock_cluster(PGconn **pg_conn, Oid *cnoid)
{
	Oid coordhostoid;
	int32 coordport;
	AppendNodeInfo appendnodeinfo;
	char *coordhost;
	char coordport_buf[10];
	char *current_user;
	char cnpath[1024];
	struct passwd *pwd;
	int try = 0;
	const int maxnum = 15;
	NameData self_address;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	Datum datumPath;
	Relation rel_node;
	HeapTuple tuple;
	bool isNull;

	pwd = getpwuid(getuid());
	current_user = pwd->pw_name;
	appendnodeinfo.nodeaddr = NULL;
	appendnodeinfo.nodeusername = current_user;
	if (!mgr_get_active_hostoid_and_port(CNDN_TYPE_COORDINATOR_MASTER, &coordhostoid, &coordport, &appendnodeinfo, false))
		ereport(ERROR, (errmsg("can not get active coordinator in cluster")));
	coordhost = get_hostaddress_from_hostoid(coordhostoid);
	/*get the adbmanager ip*/
	mgr_get_self_address(coordhost, coordport, &self_address);
	/*set adbmanager ip to the coordinator*/
	tuple = SearchSysCache1(NODENODEOID, appendnodeinfo.tupleoid);
	if(!(HeapTupleIsValid(tuple)))
	{
		ereport(ERROR, (errmsg("node oid \"%u\" not exist", appendnodeinfo.tupleoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
	if (isNull)
	{
		ReleaseSysCache(tuple);
		heap_close(rel_node, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	strncpy(cnpath, TextDatumGetCString(datumPath), 1024);
	ReleaseSysCache(tuple);
	heap_close(rel_node, AccessShareLock);
	mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", self_address.data,
										32, "trust", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cnpath, &infosendmsg, coordhostoid, &getAgentCmdRst);
	mgr_reload_conf(coordhostoid, cnpath);
	pfree(infosendmsg.data);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("set ADB Manager ip to %s coordinator %s/pg_hba,conf fail %s", coordhost, cnpath, getAgentCmdRst.description.data)));
	pfree(getAgentCmdRst.description.data);

	*cnoid = appendnodeinfo.tupleoid;
	sprintf(coordport_buf, "%d", coordport);
	*pg_conn = PQsetdbLogin(coordhost
							,coordport_buf
							,NULL, NULL
							,DEFAULT_DB
							,current_user
							,NULL);

	if (*pg_conn == NULL || PQstatus((PGconn*)*pg_conn) != CONNECTION_OK)
	{
		ereport(ERROR,
			(errmsg("Fail to connect to coordinator %s", PQerrorMessage((PGconn*)*pg_conn)),
			errhint("coordinator info(host=%s port=%d dbname=%s user=%s)",
				coordhost, coordport, DEFAULT_DB, appendnodeinfo.nodeusername)));
	}
	pfree(coordhost);

	ereport(LOG, (errmsg("%s", "SELECT PG_PAUSE_CLUSTER();")));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, "SELECT PG_PAUSE_CLUSTER();", maxnum);
	if (try < 0)
	{
		ereport(WARNING,
			(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)*pg_conn)),
			errhint("execute command failed: \"SELECT PG_PAUSE_CLUSTER()\".")));
	}
}

static void mgr_unlock_cluster(PGconn **pg_conn)
{
	int try = 0;
	const int maxnum = 15;
	char *sqlstr = "SELECT PG_UNPAUSE_CLUSTER();";

	ereport(LOG, (errmsg("%s", sqlstr)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, sqlstr, maxnum);
	if (try<0)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("execute \"%s\" fail %s", sqlstr, PQerrorMessage((PGconn*)*pg_conn))));
	}
	PQfinish(*pg_conn);
}

static bool mgr_pqexec_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst, PGconn **pg_conn, Oid cnoid)
{
	struct tuple_cndn *prefer_cndn;
	ListCell *lc_out, *dn_lc;
	int coordinator_num = 0, datanode_num = 0;
	HeapTuple tuple_in, tuple_out;
	StringInfoData cmdstring;
	StringInfoData recorderr;
	Form_mgr_node mgr_node_out, mgr_node_in;
	Form_mgr_node mgr_node;
	char *host_address;
	bool is_preferred = false;
	bool result = true;
	const int maxnum = 15;
	int try = 0;
	HeapTuple cn_tuple;
	NameData cnnamedata;

	initStringInfo(&recorderr);
	resetStringInfo(&(getAgentCmdRst->description));
	prefer_cndn = get_new_pgxc_node(cmd, dnname, nodetype);
	if(!PointerIsValid(prefer_cndn->coordiantor_list))
	{
		appendStringInfoString(&(getAgentCmdRst->description),"not exist coordinator in the cluster");
		appendStringInfoString(&recorderr, "not exist coordinator in the cluster\n");
		return false;
	}

	/*get name of coordinator, whos oid is cnoid*/
	cn_tuple = SearchSysCache1(NODENODEOID, cnoid);
	if(!HeapTupleIsValid(cn_tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("oid \"%u\" of coordinator does not exist", cnoid)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(cn_tuple);
	Assert(cn_tuple);
	namestrcpy(&cnnamedata, NameStr(mgr_node->nodename));
	ReleaseSysCache(cn_tuple);

	initStringInfo(&cmdstring);
	coordinator_num = 0;
	foreach(lc_out, prefer_cndn->coordiantor_list)
	{
		coordinator_num = coordinator_num + 1;
		tuple_out = (HeapTuple)lfirst(lc_out);
		mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
		Assert(mgr_node_out);
		datanode_num = 0;
		foreach(dn_lc, prefer_cndn->datanode_list)
		{
			datanode_num = datanode_num +1;
			tuple_in = (HeapTuple)lfirst(dn_lc);
			mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
			Assert(mgr_node_in);
			host_address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
			if(coordinator_num == datanode_num)
			{
				is_preferred = true;
			}
			else
			{
				is_preferred = false;
			}
			resetStringInfo(&cmdstring);
			if (cnoid == HeapTupleGetOid(tuple_out))
				appendStringInfo(&cmdstring, "select pg_alter_node('%s', '%s', %d, %s);"
								,NameStr(mgr_node_in->nodename)
								,host_address
								,mgr_node_in->nodeport
								,true == is_preferred ? "true":"false");
			else
				appendStringInfo(&cmdstring, "EXECUTE DIRECT ON (\"%s\") 'select pg_alter_node(''%s'', ''%s'', %d, %s);'"
								,NameStr(mgr_node_out->nodename)
								,NameStr(mgr_node_in->nodename)
								,host_address
								,mgr_node_in->nodeport
								,true == is_preferred ? "true":"false");
			pfree(host_address);
			ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, cmdstring.data)));
			try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum);
			if (try<0)
			{
				result = false;
				ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("on coordinator \"%s\" execute \"%s\" fail %s", cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
				appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail %s\n", cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn));
			}
		}
		resetStringInfo(&cmdstring);
		if (cnoid == HeapTupleGetOid(tuple_out))
			appendStringInfo(&cmdstring, "%s", "select pgxc_pool_reload();");
		else
			appendStringInfo(&cmdstring, "EXECUTE DIRECT ON (\"%s\") 'select pgxc_pool_reload();'", NameStr(mgr_node_out->nodename));
		pg_usleep(100000L);
		ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, cmdstring.data)));
		try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum);
		if (try < 0)
		{
			result = false;
			ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("on coordinator \"%s\" execute \"%s\" fail %s", cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
			appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail %s\n", cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn));
		}
	}
	pfree(cmdstring.data);
	if (recorderr.len > 0)
	{
		appendStringInfo(&(getAgentCmdRst->description), "%s", recorderr.data);
	}
	pfree(recorderr.data);

	return result;
}

/*
* try maxnum to execute the sql, the result of sql if bool type
*/
static int mgr_pqexec_boolsql_try_maxnum(PGconn **pg_conn, char *sqlstr, const int maxnum)
{
	int result = maxnum;
	PGresult *res;

	while(result-- >= 0)
	{
		res = PQexec(*pg_conn, sqlstr);
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			if (strcasecmp("t", PQgetvalue(res, 0, 0)) == 0)
			{
				PQclear(res);
				res = NULL;
				break;
			}
		}
		if (res)
		{
			PQclear(res);
			res = NULL;
		}
		pg_usleep(100000L);
	}

	return result;
}

/*
* ADD EXTENSION extension_name
*/
void mgr_extension(MgrExtensionAdd *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall2(mgr_extension_handle,
									CharGetDatum(node->cmdtype),
									CStringGetDatum(node->name));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*
* create extension
*/
Datum mgr_extension_handle(PG_FUNCTION_ARGS)
{
	char *extension_name;
	char cmdtype;
	StringInfoData cmdstring;
	bool ret;

	cmdtype = PG_GETARG_CHAR(0);
	extension_name = PG_GETARG_CSTRING(1);
	if (strcmp (extension_name, "pg_stat_statements") == 0)
	{
		ret = mgr_extension_pg_stat_statements(cmdtype, extension_name);
	}
	else
	{
		initStringInfo(&cmdstring);
		if (cmdtype == EXTENSION_CREATE)
			appendStringInfo(&cmdstring, "CREATE EXTENSION IF NOT EXISTS %s;", extension_name);
		else if (cmdtype == EXTENSION_DROP)
			appendStringInfo(&cmdstring, "DROP EXTENSION IF NOT EXISTS %s;", extension_name);
		else
		{
			pfree(cmdstring.data);
			ereport(ERROR, (errmsg("no such cmdtype '%c'", cmdtype)));
		}
		ret = mgr_add_extension_sqlcmd(cmdstring.data);
		pfree(cmdstring.data);

	}
	if (ret)
		ereport(NOTICE, (errmsg("need set the parameters for the extension \"%s\" and put its dynamic library file on the library path", extension_name)));

	PG_RETURN_BOOL(ret);
}

/*
* create or drop extension pg_stat_statements
*/

static bool mgr_extension_pg_stat_statements(char cmdtype, char *extension_name)
{
	MGRUpdateparm *nodestmt;
	MGRUpdateparmReset *resetnodestmt;
	StringInfoData cmdstring;

	initStringInfo(&cmdstring);
	/*create extension*/
	if (cmdtype == EXTENSION_CREATE)
	{
		/*create extension*/
		appendStringInfo(&cmdstring, "CREATE EXTENSION IF NOT EXISTS %s;", extension_name);
		if (!mgr_add_extension_sqlcmd(cmdstring.data))
			return false;

		nodestmt = makeNode(MGRUpdateparm);
		nodestmt->parmtype = PARM_TYPE_COORDINATOR;
		nodestmt->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		nodestmt->nodename = MACRO_STAND_FOR_ALL_NODENAME;
		nodestmt->is_force = false;
		nodestmt->options = lappend(nodestmt->options, makeDefElem("shared_preload_libraries", (Node *)makeString(extension_name)));
		mgr_add_updateparm(nodestmt, NULL, NULL);
	}
	else if (cmdtype == EXTENSION_DROP)
	{
		/*drop extension*/
		appendStringInfo(&cmdstring, "DROP EXTENSION IF EXISTS %s;", extension_name);
		if (!mgr_add_extension_sqlcmd(cmdstring.data))
			return false;

		resetnodestmt = makeNode(MGRUpdateparmReset);
		resetnodestmt->parmtype = PARM_TYPE_COORDINATOR;
		resetnodestmt->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		resetnodestmt->nodename = MACRO_STAND_FOR_ALL_NODENAME;
		resetnodestmt->is_force = false;
		resetnodestmt->options = lappend(resetnodestmt->options, makeDefElem("shared_preload_libraries", (Node *)makeString("''")));
		mgr_reset_updateparm(resetnodestmt, NULL, NULL);
	}
	else
	{
		pfree(cmdstring.data);
		ereport(ERROR, (errmsg("no such cmdtype '%c'", cmdtype)));
	}

	pfree(cmdstring.data);

	return true;
}

static void mgr_get_self_address(char *server_address, int server_port, Name self_address)
{
		int sock;
		int nRet;
		struct sockaddr_in serv_addr;
		struct sockaddr_in addr;
		socklen_t addr_len;

		Assert(server_address);
		memset(&serv_addr, 0, sizeof(serv_addr));

		sock = socket(PF_INET, SOCK_STREAM, 0);
		if (sock == -1)
		{
			ereport(ERROR, (errmsg("on ADB Manager create sock fail")));
		}

		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = inet_addr(server_address);
		serv_addr.sin_port = htons(server_port);

		if (connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr)) == -1)
		{
			ereport(ERROR, (errmsg("on ADB Manager sock connect \"%s\" \"%d\" fail", server_address, server_port)));
		}

		addr_len = sizeof(struct sockaddr_in);
		nRet = getsockname(sock,(struct sockaddr*)&addr,&addr_len);
		if(nRet == -1)
		{
			ereport(ERROR, (errmsg("on ADB Manager sock connect \"%s\" \"%d\" to getsockname fail", server_address, server_port)));
		}
		namestrcpy(self_address, inet_ntoa(addr.sin_addr));
		close(sock);

}

/*
* check the node is recovery or not
*/
static bool mgr_check_node_recovery_finish(char nodetype, Oid hostoid, int nodeport, char *address)
{
	StringInfoData resultstrdata;
	HeapTuple tuple;
	Form_mgr_host mgr_host;
	char *pstr;
	char *sqlstr = "select * from pg_is_in_recovery()";

	tuple = SearchSysCache1(HOSTHOSTOID, hostoid);
	if(!(HeapTupleIsValid(tuple)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", hostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	initStringInfo(&resultstrdata);
	if (GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstr, AGTM_USER, address, nodeport, DEFAULT_DB, &resultstrdata);
	else
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstr, NameStr(mgr_host->hostuser), address, nodeport, DEFAULT_DB, &resultstrdata);
	ReleaseSysCache(tuple);
	if (resultstrdata.len == 0)
	{
		return false;
	}
	pstr = resultstrdata.data;
	if (strcmp(pstr, "f") !=0)
	{
		pfree(resultstrdata.data);
		return false;
	}
	pfree(resultstrdata.data);

	return true;
}

/*
* check the param reload in postgresql.conf
*/
static bool mgr_check_param_reload_postgresqlconf(char nodetype, Oid hostoid, int nodeport, char *address, char *check_param, char *expect_result)
{
	StringInfoData resultstrdata;
	StringInfoData sqlstrdata;
	HeapTuple tuple;
	Form_mgr_host mgr_host;
	char *pstr;

	Assert(expect_result);
	tuple = SearchSysCache1(HOSTHOSTOID, hostoid);
	if(!(HeapTupleIsValid(tuple)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", hostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	initStringInfo(&resultstrdata);
	initStringInfo(&sqlstrdata);
	appendStringInfo(&sqlstrdata, "show %s", check_param);
	if (GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstrdata.data, AGTM_USER, address, nodeport, DEFAULT_DB, &resultstrdata);
	else
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstrdata.data, NameStr(mgr_host->hostuser), address, nodeport, DEFAULT_DB, &resultstrdata);
	ReleaseSysCache(tuple);
	pfree(sqlstrdata.data);
	if (resultstrdata.len == 0)
	{
		return false;
	}
	pstr = resultstrdata.data;
	if (strcmp(pstr, expect_result) !=0)
	{
		pfree(resultstrdata.data);
		return false;
	}
	pfree(resultstrdata.data);

	return true;
}

static char mgr_get_other_type(char nodetype)
{
	char other_type;

	switch(nodetype)
	{
		case GTM_TYPE_GTM_SLAVE:
			other_type = GTM_TYPE_GTM_EXTRA;
			break;
		case GTM_TYPE_GTM_EXTRA:
			other_type = GTM_TYPE_GTM_SLAVE;
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			other_type = CNDN_TYPE_DATANODE_EXTRA;
			break;
		case CNDN_TYPE_DATANODE_EXTRA:
			other_type = CNDN_TYPE_DATANODE_SLAVE;
			break;
		default:
			other_type = CNDN_TYPE_NONE_TYPE;
	}
	
	return other_type;
}

/*
* check the node has sync slave or extra node
*/
static bool mgr_check_sync_node_exist(Relation rel, Name nodename, char nodetype)
{
	char other_type;
	ScanKeyData key[3];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	bool bget = false;

	if (nodetype == GTM_TYPE_GTM_MASTER || nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER)
		return false;
	other_type = mgr_get_other_type(nodetype);

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(other_type));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(nodename));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodesync
		,BTEqualStrategyNumber, F_CHAREQ
		,NameGetDatum(SYNC));
	rel_scan = heap_beginscan(rel, SnapshotNow, 3, key);

	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		bget = true;
		break;
	}
	heap_endscan(rel_scan);

	return bget;
}

/*
* check the node hostname and path, not allow repeated with others
*/
static bool mgr_check_node_path(Relation rel, Oid hostoid, char *path)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	bool bget = false;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodehost
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(hostoid));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodepath
		,BTEqualStrategyNumber
		,F_TEXTEQ
		,CStringGetTextDatum(path));

	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);	
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		bget = true;
		break;
	}
	heap_endscan(rel_scan);
	
	return bget;
}

/*
* check the node hostname and path, not allow repeated with others
*/
static bool mgr_check_node_port(Relation rel, Oid hostoid, int port)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	bool bget = false;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodehost
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(hostoid));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodeport
		,BTEqualStrategyNumber
		,F_INT4EQ
		,Int32GetDatum(port));

	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);	
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		bget = true;
		break;
	}
	heap_endscan(rel_scan);
	
	return bget;
}

/*remove node from cluster*/
void mgr_remove_node(MgrRemoveNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_drop())
	{
		DirectFunctionCall2(mgr_remove_node_func,
									CharGetDatum(node->nodetype),
									PointerGetDatum(node->names));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*remove node from cluster*/
Datum mgr_remove_node_func(PG_FUNCTION_ARGS)
{
	char nodetype;
	char othertype;
	char *address;
	char *nodestring;
	char *masterpath;
	char port_buf[10];
	NameData namedata;
	List *nodenamelist = NIL;
	Relation rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	HeapTuple mastertuple;
	ListCell   *cell;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_masternode;
	ScanKeyData key[3];
	int iloop = 0;
	bool bsync_exist;
	bool isNull;
	Datum datumPath;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	Value *val;	
	
	/*ndoe type*/
	nodetype = PG_GETARG_CHAR(0);
	if (CNDN_TYPE_DATANODE_MASTER == nodetype || GTM_TYPE_GTM_MASTER == nodetype || CNDN_TYPE_COORDINATOR_MASTER == nodetype)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("it does not support remove master node now")));
	nodenamelist = (List *)PG_GETARG_POINTER(1);

	/*check the node in the cluster*/
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	foreach(cell, nodenamelist)
	{
		val = lfirst(cell);
		Assert(val && IsA(val,String));
		namestrcpy(&namedata, strVal(val));
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));
		ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));
		
		rel_scan = heap_beginscan(rel, SnapshotNow, 3, key);
		if ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) == NULL)
		{
			heap_endscan(rel_scan);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist in cluster", mgr_nodetype_str(nodetype), namedata.data)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		sprintf(port_buf, "%d", mgr_node->nodeport);
		iloop = 0;
		while (iloop++ < 2)
		{
			if (pingNode(address, port_buf) == 0 || pingNode(address, port_buf) == -2)
			{
				pfree(address);
				heap_endscan(rel_scan);
				heap_close(rel, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					,errmsg("\"%s\" is running, stop it first", NameStr(mgr_node->nodename))));
			}
		}
		heap_endscan(rel_scan);
		pfree(address);
	}

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	foreach(cell, nodenamelist)
	{
		val = lfirst(cell);
		Assert(val && IsA(val,String));
		namestrcpy(&namedata, strVal(val));
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));
		ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));
		rel_scan = heap_beginscan(rel, SnapshotNow, 3, key);
		tuple = heap_getnext(rel_scan, ForwardScanDirection);
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		/*if mgr_node->nodesync = SYNC, set its master as async*/
		othertype = mgr_get_other_type(nodetype);
		bsync_exist = mgr_check_sync_node_exist(rel, &namedata, nodetype);
		if (mgr_node->nodesync == SYNC && (!bsync_exist))
		{
			mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
			if(!HeapTupleIsValid(mastertuple))
			{
				heap_endscan(rel_scan);
				heap_close(rel, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the master \"%s\" does not exist", NameStr(mgr_node->nodename))));
			}
			mgr_masternode = (Form_mgr_node)GETSTRUCT(mastertuple);
			datumPath = heap_getattr(mastertuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
			if (isNull)
			{
				ReleaseSysCache(mastertuple);
				heap_close(rel, RowExclusiveLock);

				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column nodepath is null")));
			}
			resetStringInfo(&(getAgentCmdRst.description));
			resetStringInfo(&infosendmsg);
			masterpath = TextDatumGetCString(datumPath);
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
			nodestring = mgr_nodetype_str(mgr_masternode->nodetype);
			ereport(LOG, (errmsg("set \"synchronous_standby_names = ''\" in postgresql.conf of the %s \"%s\"", nodestring, NameStr(mgr_masternode->nodename))));
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, masterpath, &infosendmsg, mgr_masternode->nodehost, &getAgentCmdRst);
			if (!getAgentCmdRst.ret)
				ereport(WARNING, (errmsg("set synchronous_standby_names = '' in postgresql.conf of %s \"%s\"fail", nodestring, NameStr(mgr_masternode->nodename))));
			ReleaseSysCache(mastertuple);
			pfree(nodestring);
		}
		/*check its master has sync node*/
		if (!bsync_exist)
		{
			if (CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA== nodetype)
				ereport(WARNING, (errmsg("the datanode master \"%s\" has no synchronous slave or extra node", namedata.data)));
			else
				ereport(WARNING, (errmsg("the gtm master \"%s\" has no synchronous slave or extra node", namedata.data)));
		}
		/*update the tuple*/
		mgr_node->nodeinited = false;
		mgr_node->nodeincluster = false;
		heap_inplace_update(rel, tuple);
		heap_endscan(rel_scan);
	}
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}
