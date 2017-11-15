/*
 * commands of manual operate
 * src/adbmgrd/manager/mgr_manual.c
 */
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>

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
#include "catalog/monitor_job.h"
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
#include "access/xlog.h"

static struct enum_sync_state sync_state_tab[] =
{
	{SYNC_STATE_SYNC, "sync"},
	{SYNC_STATE_ASYNC, "async"},
	{SYNC_STATE_POTENTIAL, "potential"},
	{-1, NULL}
};

static bool mgr_execute_direct_on_all_coord(PGconn **pg_conn, const char *sql, const int iloop, const int res_type, StringInfo strinfo);
static void mgr_get_hba_replication_info(Oid masterTupleOid, StringInfo infosendmsg);
static int mgr_maxtime_check_xlog_diff(const char nodeType, const char *nodeName, AppendNodeInfo *nodeInfoM, const int maxSecond);
static bool mgr_check_active_locks_in_cluster(PGconn *pgConn, const Oid cnOid);
static bool mgr_check_active_connect_in_coordinator(PGconn *pgConn, const Oid cnOid);
static bool mgr_check_track_activities_on_coordinator(void);
static Oid mgr_get_tupleoid_from_nodename_type(char *nodename, char nodetype);

/*
* promote the node to master; delete the old master tuple in node systable, delete 
* the old master param in param table ; set type of the new master as master type in node
* table, update the type of the new master param as master type in param table
*/
Datum mgr_failover_manual_adbmgr_func(PG_FUNCTION_ARGS)
{
	char stop_cmdtype;
	char nodetype;
	char mastertype;
	char *nodetypestr;
	char *nodename;
	char *masternodename;
	bool master_is_exist = true;
	bool master_is_running = true;
	bool slave_is_exist = true;
	bool slave_is_running = true;
	bool res = false;
	bool hasOtherSlave = false;
	NameData nodenamedata;
	NameData masternodenamedata;
	NameData slaveNodeName;
	NameData sync_state_name;
	AppendNodeInfo master_nodeinfo;
	AppendNodeInfo slave_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData strinfo;
	HeapTuple masterTuple;
	HeapTuple slavetuple;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodetmp;
	Relation rel_node;
	GetAgentCmdRst getAgentCmdRst;
	ScanKeyData key[2];
	HeapScanDesc relScan;
	Oid oldMasterTupleOid;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	/*get the input variable*/
	nodetype = PG_GETARG_INT32(0);
	nodename = PG_GETARG_CSTRING(1);

	Assert(nodename);
	namestrcpy(&nodenamedata, nodename);
	
	/* check slave node */
	if (!mgr_check_node_exist_incluster(&nodenamedata, true))
	{
		nodetypestr = mgr_nodetype_str(nodetype);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("%s \"%s\" does not exist in cluster", nodetypestr, nodenamedata.data)));
	}

	masternodename = mgr_get_mastername_by_nodename_type(nodenamedata.data, nodetype);
	namestrcpy(&masternodenamedata, masternodename);
	pfree(masternodename);
	mastertype = mgr_get_master_type(nodetype);
	if (GTM_TYPE_GTM_MASTER == mastertype)
		stop_cmdtype = AGT_CMD_GTM_STOP_MASTER;
	else if (CNDN_TYPE_COORDINATOR_MASTER == mastertype)
		stop_cmdtype = AGT_CMD_CN_STOP;
	else if (CNDN_TYPE_DATANODE_MASTER == mastertype)
		stop_cmdtype = AGT_CMD_DN_STOP;
	else
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("unknown '%c' the type of the node's master",mastertype)));

	/*get the old slave info*/
	get_nodeinfo_byname(nodenamedata.data, nodetype, &slave_is_exist, &slave_is_running, &slave_nodeinfo);
	if (!slave_is_exist)
	{
		nodetypestr = mgr_nodetype_str(nodetype);
		ereport(ERROR, (errmsg("%s \"%s\" does not exist", nodetypestr, nodenamedata.data)));
	}

	initStringInfo(&strinfo);
	initStringInfo(&infosendmsg);
	/*get the old master info*/
	get_nodeinfo_byname(masternodenamedata.data, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
	if (master_is_exist && master_is_running)
	{
		/*stop the old master*/
		appendStringInfo(&infosendmsg, " stop -D %s -m i -o -i -w -c", master_nodeinfo.nodepath);
		nodetypestr = mgr_nodetype_str(mastertype);
		ereport(LOG, (errmsg("stop the old %s \"%s\"", nodetypestr, masternodenamedata.data)));

		res = mgr_ma_send_cmd(stop_cmdtype, infosendmsg.data, master_nodeinfo.nodehost, &strinfo);
		if (!res)
			ereport(WARNING, (errmsg("stop the old %s \"%s\" fail %s", nodetypestr, masternodenamedata.data, strinfo.data)));
		pfree(nodetypestr);
	}
	oldMasterTupleOid = master_nodeinfo.tupleoid;
	pfree_AppendNodeInfo(master_nodeinfo);

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);

	/*delete the old master tuple in node table*/
	nodetypestr = mgr_nodetype_str(mastertype);
	ereport(LOG, (errmsg("delete the old %s \"%s\" in the node table", nodetypestr, masternodenamedata.data)));
	masterTuple = SearchSysCache1(NODENODEOID, oldMasterTupleOid);
	pfree(nodetypestr);
	if(HeapTupleIsValid(masterTuple))
	{
		simple_heap_delete(rel_node, &masterTuple->t_self);
		CatalogUpdateIndexes(rel_node, masterTuple);
		ReleaseSysCache(masterTuple);
	}

	/*update the slave type as master type in node table*/
	/*get the slave info*/
	nodetypestr = mgr_nodetype_str(nodetype);
	if(slave_is_exist)
	{
		slavetuple = SearchSysCache1(NODENODEOID, slave_nodeinfo.tupleoid);
		if(HeapTupleIsValid(slavetuple))
		{
			ereport(LOG, (errmsg("update the old %s \"%s\" to master in the node table", nodetypestr, nodenamedata.data)));
			mgr_node = (Form_mgr_node)GETSTRUCT(slavetuple);
			Assert(mgr_node);
			mgr_node->nodetype = mastertype;
			mgr_node->nodemasternameoid = 0;
			namestrcpy(&(mgr_node->nodesync), "");
			heap_inplace_update(rel_node, slavetuple);
			ReleaseSysCache(slavetuple);
		}
			
	}
	heap_close(rel_node, RowExclusiveLock);
	if (!slave_is_running)
		ereport(WARNING, (errmsg("%s \"%s\" is not running normal", nodetypestr, nodenamedata.data)));

	/* set new master synchronous_standby_names */
	resetStringInfo(&strinfo);
	resetStringInfo(&infosendmsg);
	mgr_get_master_sync_string(oldMasterTupleOid, true, slave_nodeinfo.tupleoid, &strinfo);
	if(strinfo.len != 0)
	{
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo.data, &infosendmsg);
		int i = 0;
		while(i<strinfo.len && strinfo.data[i] != ',' && i<NAMEDATALEN)
		{
			slaveNodeName.data[i] = strinfo.data[i];
			i++;
		}
		if (i<NAMEDATALEN)
			slaveNodeName.data[i] = '\0';
		hasOtherSlave = true;
	}
	else
	{
		rel_node = heap_open(NodeRelationId, AccessShareLock);
		res = mgr_get_normal_slave_node(rel_node, oldMasterTupleOid, SYNC_STATE_ASYNC, slave_nodeinfo.tupleoid, &slaveNodeName);
		if (!res)
			res = mgr_get_slave_node(rel_node, oldMasterTupleOid, SYNC_STATE_ASYNC, slave_nodeinfo.tupleoid, &slaveNodeName);
		if (res)
		{
			appendStringInfo(&strinfo, "%s", slaveNodeName.data);
		}
		else
			hasOtherSlave = false;
		heap_close(rel_node, AccessShareLock);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo.len == 0 ? "" : strinfo.data, &infosendmsg);
	}
	ereport(LOG, (errmsg("reload \"synchronous_standby_names='%s'\" in postgresql.conf of new master \"%s\"", strinfo.len ? strinfo.data : "", slave_nodeinfo.nodename)));
	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD,
							slave_nodeinfo.nodepath,
							&infosendmsg,
							slave_nodeinfo.nodehost,
							&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
		ereport(WARNING, (errmsg("refresh synchronous_standby_names of datanode master \"%s\" fail, %s", nodenamedata.data, getAgentCmdRst.description.data)));

	if (!hasOtherSlave)
		ereport(WARNING, (errmsg("the master \"%s\" has no slave node, it is better to append a new datanode slave node", masternodenamedata.data)));

	/*update the other slave nodes with the same master: masteroid, and sync_stat*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodemasternameOid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(oldMasterTupleOid));
	relScan = heap_beginscan(rel_node, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		if (slave_nodeinfo.tupleoid == HeapTupleGetOid(tuple))
			continue;
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);	
		/* update datanode slave nodemasternameoid */
		mgr_nodetmp->nodemasternameoid = slave_nodeinfo.tupleoid;
		if (strcmp(NameStr(mgr_nodetmp->nodename), slaveNodeName.data) == 0)
		{
			namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
			namestrcpy(&(mgr_nodetmp->nodesync), sync_state_name.data);
		}
		heap_inplace_update(rel_node, tuple);
	}
	heap_endscan(relScan);
	heap_close(rel_node, RowExclusiveLock);	
	
	/*for mgr_updateparm systbl, drop the old master param, update slave parm info in the mgr_updateparm systbl*/
	ereport(LOG, (errmsg("refresh \"param\" table in ADB Manager, delete the old master parameters, and update %s \"%s\" as master type", nodetypestr, nodenamedata.data)));
	pfree(nodetypestr);
	mgr_parm_after_gtm_failover_handle(&masternodenamedata, mastertype, &nodenamedata, nodetype);
	
	pfree(getAgentCmdRst.description.data);
	pfree(strinfo.data);
	pfree(infosendmsg.data);
	pfree_AppendNodeInfo(slave_nodeinfo);

	PG_RETURN_BOOL(true);
}

/*
* promote the datanode slave|extra or gtm slave|extra to master
*
*/

Datum mgr_failover_manual_promote_func(PG_FUNCTION_ARGS)
{
	char nodetype;
	char cmdtype;
	char *nodetypestr;
	bool slave_is_exist = true;
	bool slave_is_running = true;
	bool res = false;
	NameData nodenamedata;
	AppendNodeInfo slave_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData strinfo;

	/*get the input variable*/
	nodetype = PG_GETARG_INT32(0);
	namestrcpy(&nodenamedata, PG_GETARG_CSTRING(1));


	/*get the old slave info*/
	get_nodeinfo_byname(nodenamedata.data, nodetype, &slave_is_exist, &slave_is_running, &slave_nodeinfo);
	nodetypestr = mgr_nodetype_str(nodetype);
	if (!slave_is_exist)
	{
		ereport(ERROR, (errmsg("%s \"%s\" does not exist", nodetypestr, nodenamedata.data)));
	}
	if (!slave_is_running)
		ereport(ERROR, (errmsg("%s \"%s\" is not running normal", nodetypestr, nodenamedata.data)));
	if (GTM_TYPE_GTM_SLAVE == nodetype)
		cmdtype = AGT_CMD_GTM_SLAVE_FAILOVER;
	else if (CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype)
		cmdtype = AGT_CMD_DN_FAILOVER;
	else
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("unknown '%c' the type of this node",nodetype)));

	initStringInfo(&strinfo);
	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " promote -D %s -w", slave_nodeinfo.nodepath);
	nodetypestr = mgr_nodetype_str(nodetype);
	ereport(LOG, (errmsg("promote %s \"%s\" to master", nodetypestr, nodenamedata.data)));
	res = mgr_ma_send_cmd(cmdtype, infosendmsg.data, slave_nodeinfo.nodehost, &strinfo);

	pfree(slave_nodeinfo.nodename);
	pfree(slave_nodeinfo.nodeusername);
	pfree(slave_nodeinfo.nodepath);

	if (!res)
		ereport(ERROR, (errmsg("promote %s \"%s\" to master fail, %s", nodetypestr, nodenamedata.data, strinfo.data)));

	/*wait the new master can accepts conenct*/
	mgr_check_node_recovery_finish(nodetype, slave_nodeinfo.nodehost, slave_nodeinfo.nodeport, slave_nodeinfo.nodeaddr);
	pfree(slave_nodeinfo.nodeaddr);

	PG_RETURN_BOOL(true);
}

/*
* update datanode new master info in pgxc_node
*
*/

Datum mgr_failover_manual_pgxcnode_func(PG_FUNCTION_ARGS)
{
	char nodetype;
	char mastertype;
	char *mastername;
	bool master_is_exist = true;
	bool master_is_running = true;
	bool cn_is_exist = false;
	bool cn_is_running = false;
	bool getrefresh = false;
	Oid cnoid;
	NameData nodenamedata;
	NameData nodemasternamedata;
	AppendNodeInfo master_nodeinfo;
	AppendNodeInfo cn_nodeinfo;
	ScanKeyData key[2];
	Relation rel_node;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	GetAgentCmdRst getAgentCmdRst;
	PGconn *pg_conn;

	/*get the input variable*/
	nodetype = PG_GETARG_INT32(0);
	namestrcpy(&nodenamedata, PG_GETARG_CSTRING(1));
	
	if (!mgr_check_node_exist_incluster(&nodenamedata, true))
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("datanode slave \"%s\" does not exist in cluster", nodenamedata.data)));

	/* get master name */
	mastername = mgr_get_mastername_by_nodename_type(nodenamedata.data, nodetype);
	namestrcpy(&nodemasternamedata, mastername);
	pfree(mastername);
	mastertype = mgr_get_master_type(nodetype);
	/*get the new master info*/
	get_nodeinfo_byname(nodemasternamedata.data, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
	if (master_is_exist)
		pfree_AppendNodeInfo(master_nodeinfo);

	if (!master_is_exist)
	{
		ereport(ERROR, (errmsg("datanode master \"%s\" does not exist",nodenamedata.data)));
	}
	
	/*check all coordinators running normal*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		get_nodeinfo_byname(NameStr(mgr_node->nodename), CNDN_TYPE_COORDINATOR_MASTER, &cn_is_exist, &cn_is_running, &cn_nodeinfo);
		if (!cn_is_exist || !cn_is_running)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, AccessShareLock);
			if (cn_is_exist)
				pfree_AppendNodeInfo(cn_nodeinfo);
			ereport(ERROR, (errmsg("coordinator \"%s\" is not running normal", NameStr(mgr_node->nodename))));
		}
		pfree_AppendNodeInfo(cn_nodeinfo);
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);

	PG_TRY();
	{
		/*pause cluster*/
		mgr_lock_cluster(&pg_conn, &cnoid);
		/*refresh pgxc_node on all coordiantors*/
		initStringInfo(&(getAgentCmdRst.description));
		getrefresh = mgr_pqexec_refresh_pgxc_node(PGXC_FAILOVER, nodetype, nodenamedata.data, &getAgentCmdRst, &pg_conn, cnoid);
		if(!getrefresh)
		{
			getAgentCmdRst.ret = getrefresh;
			ereport(WARNING, (errmsg("%s", (getAgentCmdRst.description).data)));
		}
	}PG_CATCH();
	{
		mgr_unlock_cluster(&pg_conn);
		PG_RE_THROW();
	}PG_END_TRY();
	
	/*unlock cluster*/
	mgr_unlock_cluster(&pg_conn);

	PG_RETURN_BOOL(true);
}

/*
* update datanode new master info in pgxc_node
*
*/

Datum mgr_failover_manual_rewind_func(PG_FUNCTION_ARGS)
{
	char nodetype;
	char mastertype;
	char *nodetypestr;
	char *str;
	char *masterName;
	bool master_is_exist = true;
	bool master_is_running = true;
	bool slave_is_exist = true;
	bool slave_is_running = true;
	bool res = false;
	bool get = false;
	bool incluster = false;
	NameData nodenamedata;
	NameData nodemasternamedata;
	NameData slave_sync;
	AppendNodeInfo master_nodeinfo;
	AppendNodeInfo slave_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData strinfo;
	StringInfoData strinfo_sync;
	StringInfoData primary_conninfo_value;
	HeapTuple slavetuple;
	Form_mgr_node mgr_node;
	Relation rel_node;
	GetAgentCmdRst getAgentCmdRst;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	/*get the input variable*/
	nodetype = PG_GETARG_INT32(0);
	namestrcpy(&nodenamedata, PG_GETARG_CSTRING(1));
	masterName = mgr_get_mastername_by_nodename_type(nodenamedata.data, nodetype);
	Assert(masterName);
	namestrcpy(&nodemasternamedata, masterName);

	if (nodetype == GTM_TYPE_GTM_SLAVE)
	{
		ereport(ERROR, (errmsg("not support for gtm slave or extra rewind now")));
	}
	nodetypestr = mgr_nodetype_str(nodetype);
	initStringInfo(&strinfo);
	initStringInfo(&strinfo_sync);

	res = mgr_rewind_node(nodetype, nodenamedata.data, &strinfo);
	if (!res)
	{
		ereport(ERROR, (errmsg("rewind %s \"%s\" fail, %s", nodetypestr, nodenamedata.data, strinfo.data)));
	}

	res = true;
	mastertype = mgr_get_master_type(nodetype);
	/*get the slave info*/
	mgr_get_nodeinfo_byname_type(nodenamedata.data, nodetype, false, &slave_is_exist, &slave_is_running, &slave_nodeinfo);

	/*get the master info*/
	get_nodeinfo_byname(nodemasternamedata.data, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
	/*get master old sync*/
	mgr_get_master_sync_string(master_nodeinfo.tupleoid, true, InvalidOid, &strinfo_sync);

	/*update the slave's masteroid, sync_state in its tuple*/
	slavetuple = SearchSysCache1(NODENODEOID, slave_nodeinfo.tupleoid);
	ereport(NOTICE, (errmsg("refresh mastername of %s \"%s\" in the node table", nodetypestr, nodenamedata.data)));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	if(HeapTupleIsValid(slavetuple))
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(slavetuple);
		Assert(mgr_node);
		incluster = mgr_node->nodeincluster;
		mgr_node->nodemasternameoid = master_nodeinfo.tupleoid;
		mgr_node->nodeinited = true;
		mgr_node->nodeincluster = true;
		if (strinfo_sync.len == 0)
			namestrcpy(&(mgr_node->nodesync), sync_state_tab[SYNC_STATE_SYNC].name);
		namestrcpy(&slave_sync, NameStr(mgr_node->nodesync));
		heap_inplace_update(rel_node, slavetuple);
		ReleaseSysCache(slavetuple);
		get = true;
	}
	heap_close(rel_node, RowExclusiveLock);

	if (!get)
	{
		pfree(strinfo.data);
		pfree(strinfo_sync.data);
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
		ereport(ERROR, (errmsg("the tuple of %s \"%s\" in the node table is not valid", nodetypestr, nodenamedata.data)));
	}

	/*refresh postgresql.conf of this node*/
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	ereport(NOTICE, (errmsg("set parameters in postgresql.conf of %s \"%s\"", nodetypestr, nodenamedata.data)));
	mgr_add_parameters_pgsqlconf(slave_nodeinfo.tupleoid, nodetype, slave_nodeinfo.nodeport, &infosendmsg);
	mgr_add_parm(nodenamedata.data, nodetype, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, slave_nodeinfo.nodepath, &infosendmsg, slave_nodeinfo.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		ereport(WARNING, (errmsg("set parameters of %s \"%s\" fail, %s", nodetypestr, nodenamedata.data, getAgentCmdRst.description.data)));
		res = false;
	}

	/*refresh recovery.conf of this node*/
	resetStringInfo(&infosendmsg);
	initStringInfo(&primary_conninfo_value);
	ereport(NOTICE, (errmsg("refresh recovery.conf of %s \"%s\"", nodetypestr, nodenamedata.data)));
	appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
					master_nodeinfo.nodeaddr,
					master_nodeinfo.nodeport,
					master_nodeinfo.nodeusername,
					nodenamedata.data);

	mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
	pfree(primary_conninfo_value.data);
	resetStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
							slave_nodeinfo.nodepath,
							&infosendmsg,
							slave_nodeinfo.nodehost,
							&getAgentCmdRst);

	if (!getAgentCmdRst.ret)
	{
		ereport(WARNING, (errmsg("refresh recovery.conf fail, %s", getAgentCmdRst.description.data)));
		res = false;
	}

	pfree(nodetypestr);

	/*start the node*/
	if (res)
	{
		resetStringInfo(&infosendmsg);
		resetStringInfo(&strinfo);
		if (GTM_TYPE_GTM_SLAVE == nodetype)
			appendStringInfo(&infosendmsg, " start -D %s -o -i -w -c -l %s/logfile", slave_nodeinfo.nodepath, slave_nodeinfo.nodepath);
		else
		appendStringInfo(&infosendmsg, " start -Z datanode -D %s -o -i -w -c -l %s/logfile", slave_nodeinfo.nodepath, slave_nodeinfo.nodepath);
	
		ereport(NOTICE, (errmsg("pg_ctl %s", infosendmsg.data)));
		res = mgr_ma_send_cmd(AGT_CMD_DN_START, infosendmsg.data, slave_nodeinfo.nodehost, &strinfo);
		if (!res)
			ereport(WARNING, (errmsg("pg_ctl %s fail, %s", infosendmsg.data, strinfo.data)));
	}

	/*set master synchronous_standby_names*/
	if (res)
	{
		resetStringInfo(&infosendmsg);
		if (strinfo_sync.len == 0)
		{
			if (strcmp(slave_sync.data, sync_state_tab[SYNC_STATE_SYNC].name) == 0)
			{
				appendStringInfo(&strinfo_sync, "%s", nodenamedata.data);
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo_sync.data, &infosendmsg);
			}
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		}
		else
		{
			if ((!incluster) && (strcmp(slave_sync.data, sync_state_tab[SYNC_STATE_SYNC].name) == 0 
				|| strcmp(slave_sync.data, sync_state_tab[SYNC_STATE_POTENTIAL].name)) == 0)
			{
					appendStringInfo(&strinfo_sync, ",%s", nodenamedata.data);
			}
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo_sync.len != 0 ? strinfo_sync.data: "", &infosendmsg);
		}
		resetStringInfo(&(getAgentCmdRst.description));
		str = mgr_nodetype_str(master_nodeinfo.nodetype);
		ereport(NOTICE, (errmsg("refresh %s \"%s\" synchronous_standby_names='%s'", str,
			nodenamedata.data, strinfo_sync.len == 0 ? "''" : strinfo_sync.data)));
		pfree(str);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD,
								master_nodeinfo.nodepath,
								&infosendmsg,
								master_nodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			ereport(WARNING, (errmsg("refresh synchronous_standby_names of datanode master \"%s\" fail, %s", nodenamedata.data, getAgentCmdRst.description.data)));
			res = false;
		}
	}

	pfree_AppendNodeInfo(master_nodeinfo);
	pfree(getAgentCmdRst.description.data);
	pfree(strinfo.data);
	pfree(strinfo_sync.data);
	pfree(infosendmsg.data);
	pfree_AppendNodeInfo(slave_nodeinfo);

	PG_RETURN_BOOL(res);
}

/*
* use pg_basebackup to add a new coordinator as the given coordiantor's slave
*/
Datum mgr_append_coord_to_coord(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	AppendNodeInfo src_nodeinfo;
	AppendNodeInfo dest_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData strerr;
	HeapTuple tup_result;
	HeapTuple tuple;
	NameData nodename;
	Relation rel_node;
	Form_mgr_node mgr_node;
	HeapScanDesc rel_scan;
	ScanKeyData key[2];
	Datum datumPath;
	char port_buf[10];
	char *m_coordname;
	char *s_coordname;
	char *nodepath;
	char *nodetypestr;
	bool b_exist_src = false;
	bool b_running_src = false;
	bool b_exist_dest = false;
	bool b_running_dest = false;
	bool res = false;
	bool isNull = false;
	int iloop = 0;

	/* get the input variable */
	m_coordname = PG_GETARG_CSTRING(0);
	s_coordname = PG_GETARG_CSTRING(1);

	namestrcpy(&nodename, s_coordname);
	/* check the source coordinator status */
	get_nodeinfo_byname(m_coordname, CNDN_TYPE_COORDINATOR_MASTER, &b_exist_src, &b_running_src, &src_nodeinfo);
	if (!b_exist_src)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("coordinator \"%s\" does not exist in cluster", m_coordname)));
	}
	if (!b_running_src)
	{
		pfree_AppendNodeInfo(src_nodeinfo);
		ereport(ERROR, (errmsg("coordinator \"%s\" is not running normal", m_coordname)));
	}
	/* check the source coordinator the parameters in postgresql.conf */
	if (!mgr_check_param_reload_postgresqlconf(CNDN_TYPE_COORDINATOR_MASTER, src_nodeinfo.nodehost, src_nodeinfo.nodeport, src_nodeinfo.nodeaddr, "wal_level", "hot_standby"))
	{
		pfree_AppendNodeInfo(src_nodeinfo);
		ereport(ERROR, (errmsg("the parameter \"wal_level\" in coordinator \"%s\" postgresql.conf is not \"hot_standby\"", m_coordname)));
	}

	/* check dest coordinator */
	mgr_get_nodeinfo_byname_type(s_coordname, CNDN_TYPE_COORDINATOR_MASTER, false, &b_exist_dest, &b_running_dest, &dest_nodeinfo);
	if (!b_exist_dest)
	{
		pfree_AppendNodeInfo(src_nodeinfo);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("coordinator \"%s\" does not exist", s_coordname)));
	}
	if (mgr_check_node_exist_incluster(&nodename, true))
	{
		pfree_AppendNodeInfo(src_nodeinfo);
		pfree_AppendNodeInfo(dest_nodeinfo);
		ereport(ERROR, (errmsg("coordinator \"%s\" already exists in cluster", s_coordname)));
	}
	
	memset(port_buf, 0, sizeof(char)*10);
	snprintf(port_buf, sizeof(port_buf), "%d", dest_nodeinfo.nodeport);
	res = pingNode_user(dest_nodeinfo.nodeaddr, port_buf, dest_nodeinfo.nodeusername);
	if (PQPING_OK == res || PQPING_REJECT == res)
		ereport(ERROR, (errmsg("%s on port %d, coordinator \"%s\" is running", dest_nodeinfo.nodeaddr, dest_nodeinfo.nodeport, s_coordname)));	
	/* check the folder of dest coordinator */
	mgr_check_dir_exist_and_priv(dest_nodeinfo.nodehost, dest_nodeinfo.nodepath);

	/* make source coordinator to allow build stream replication */
	initStringInfo(&infosendmsg);
	ereport(LOG, (errmsg("update pg_hba.conf of coordinator \"%s\"", m_coordname)));
	ereport(NOTICE, (errmsg("update pg_hba.conf of coordinator \"%s\"", m_coordname)));
	mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", dest_nodeinfo.nodeusername, dest_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF
								,src_nodeinfo.nodepath
								,&infosendmsg
								,src_nodeinfo.nodehost
								,&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		pfree(infosendmsg.data);
		pfree_AppendNodeInfo(src_nodeinfo);
		pfree_AppendNodeInfo(dest_nodeinfo);
		ereport(ERROR, (errmsg("update pg_hba.conf of coordinator \"%s\" fail, %s", m_coordname, getAgentCmdRst.description.data)));
	}
	mgr_reload_conf(src_nodeinfo.nodehost, src_nodeinfo.nodepath);

	/*base backup*/
	initStringInfo(&restmsg);
	resetStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " -h %s -p %d -U %s -D %s -Xs -Fp -c fast -R", src_nodeinfo.nodeaddr
										, src_nodeinfo.nodeport, src_nodeinfo.nodeusername, dest_nodeinfo.nodepath);
	if (!mgr_ma_send_cmd(AGT_CMD_CNDN_SLAVE_INIT, infosendmsg.data, dest_nodeinfo.nodehost, &restmsg))
	{
		pfree_AppendNodeInfo(src_nodeinfo);
		pfree_AppendNodeInfo(dest_nodeinfo);
		ereport(ERROR, (errmsg("execute command \"pg_basebackup %s\" fail, %s", infosendmsg.data, restmsg.data)));
	}

	/* change the dest coordiantor port and hot_standby*/
	initStringInfo(&strerr);
	resetStringInfo(&infosendmsg);
	resetStringInfo(&(getAgentCmdRst.description));
	mgr_add_parm(s_coordname, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
	mgr_append_pgconf_paras_str_int("port", dest_nodeinfo.nodeport, &infosendmsg);
	mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
	ereport(LOG, (errmsg("update port=%d, hot_standby=on in postgresql.conf of coordinator \"%s\"", dest_nodeinfo.nodeport, s_coordname)));
	ereport(NOTICE, (errmsg("update port=%d, hot_standby=on in postgresql.conf of coordinator \"%s\"", dest_nodeinfo.nodeport, s_coordname)));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, dest_nodeinfo.nodepath, &infosendmsg, dest_nodeinfo.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		appendStringInfo(&strerr, "update \"port=%d, hot_standby=on\" in postgresql.conf of coordinator \"%s\" fail, %s\n"
		, dest_nodeinfo.nodeport, s_coordname, getAgentCmdRst.description.data);
		ereport(WARNING, (errmsg("update port=%d, hot_standby=on in postgresql.conf of coordinator \"%s\" fail, %s"
		, dest_nodeinfo.nodeport, s_coordname, getAgentCmdRst.description.data)));
	}
	/* update recovery.conf of coordinator*/
	resetStringInfo(&restmsg);
	resetStringInfo(&infosendmsg);
	resetStringInfo(&(getAgentCmdRst.description));
	ereport(LOG, (errmsg("update recovery.conf of coordinator \"%s\"", s_coordname)));
	ereport(NOTICE, (errmsg("update recovery.conf of coordinator \"%s\"", s_coordname)));
	appendStringInfo(&restmsg, "host=%s port=%d user=%s application_name=%s", src_nodeinfo.nodeaddr
		, src_nodeinfo.nodeport, dest_nodeinfo.nodeusername, dest_nodeinfo.nodename);
	mgr_append_pgconf_paras_str_str("recovery_target_timeline", "latest", &infosendmsg);
	mgr_append_pgconf_paras_str_str("standby_mode", "on", &infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", restmsg.data, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, dest_nodeinfo.nodepath, &infosendmsg, dest_nodeinfo.nodehost, &getAgentCmdRst);
	pfree_AppendNodeInfo(src_nodeinfo);
	if (!getAgentCmdRst.ret)
	{
		appendStringInfo(&strerr, "update \"standby_mode=on, recovery_target_timeline=latest\n,primary_conninfo='%s'\" \n in recovery.conf of coordinator \"%s\" fail, %s\n"
			, restmsg.data, s_coordname, getAgentCmdRst.description.data);
		ereport(WARNING, (errmsg("update recovery.conf of coordinator \"%s\" fail, %s", s_coordname
			, getAgentCmdRst.description.data)));
	}

	/* rm .s.PGPOOL.lock, .s.PGRXACT.lock in s_coordname path*/
	iloop = 0;
	while(iloop++ < 2)
	{
		resetStringInfo(&restmsg);
		resetStringInfo(&infosendmsg);
		if (1 == iloop)
			appendStringInfo(&infosendmsg, "%s/.s.PGPOOL.lock", dest_nodeinfo.nodepath);
		else
			appendStringInfo(&infosendmsg, "%s/.s.PGRXACT.lock", dest_nodeinfo.nodepath);
		res = mgr_ma_send_cmd(AGT_CMD_RM, infosendmsg.data, dest_nodeinfo.nodehost, &restmsg);
		if (!res)
		{
			appendStringInfo(&strerr,"%s rm %s fail, %s\n", dest_nodeinfo.nodeaddr, infosendmsg.data, restmsg.data);
			ereport(WARNING, (errmsg("%s rm %s fail, %s", dest_nodeinfo.nodeaddr, infosendmsg.data, restmsg.data)));
		}
	}
	
	/* start the coordinator */
	resetStringInfo(&restmsg);
	resetStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " start -Z coordinator -D %s -o -i -w -c -l %s/logfile -t 10"
		, dest_nodeinfo.nodepath, dest_nodeinfo.nodepath);
	res = mgr_ma_send_cmd(AGT_CMD_CN_START, infosendmsg.data, dest_nodeinfo.nodehost, &restmsg);
	if (!res)
	{
		appendStringInfo(&strerr, "pg_ctl %s fail\n, %s", infosendmsg.data, restmsg.data);
		ereport(WARNING, (errmsg("pg_ctl %s fail, %s", infosendmsg.data, restmsg.data)));
	}
	
	/* set all node's pg_hba.conf to allow the new coordiantor to connect */
	ereport(NOTICE, (errmsg("add address of coordinator \"%s\" on all nodes pg_hba.conf in cluster", s_coordname)));
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (!(mgr_node->nodeincluster == true || ObjectIdGetDatum(tuple) == dest_nodeinfo.tupleoid))
			continue;
		nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		ereport(NOTICE, (errmsg("update pg_hba.conf of %s \"%s\"", nodetypestr, NameStr(mgr_node->nodename))));
		pfree(nodetypestr);
		if (mgr_node->nodetype == GTM_TYPE_GTM_MASTER || mgr_node->nodetype == GTM_TYPE_GTM_SLAVE)
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", AGTM_USER, dest_nodeinfo.nodeaddr
				, 32, "trust", &infosendmsg);
		else
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", dest_nodeinfo.nodeusername, dest_nodeinfo.nodeaddr, 32, "trust"
			, &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		nodepath = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF
								,nodepath
								,&infosendmsg
								,mgr_node->nodehost
								,&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			ereport(WARNING, (errmsg("add address coordinator \"%s\" on \"%s\" pg_hba.conf fail, %s", s_coordname
				, NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
			appendStringInfo(&strerr, "add address coordinator \"%s\" on \"%s\" pg_hba.conf fail\n, %s\n"
				, s_coordname, NameStr(mgr_node->nodename), getAgentCmdRst.description.data);
		}
		mgr_reload_conf(mgr_node->nodehost, nodepath);
	}

	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);

	pfree(restmsg.data);
	pfree(infosendmsg.data);
	pfree_AppendNodeInfo(dest_nodeinfo);
	pfree(getAgentCmdRst.description.data);

	if (strerr.len == 0)
	{
		res = true;
		appendStringInfo(&strerr, "success");
	}
	ereport(LOG, (errmsg("the command of append coordinator %s to %s, result is %s, description is: %s"
		, m_coordname, s_coordname, res == true ? "true":"false", strerr.data)));
	tup_result = build_common_command_tuple(&nodename, res, strerr.data);
	pfree(strerr.data);
	
	return HeapTupleGetDatum(tup_result);
}

/*
* active coordinator slave change as coordinator master
*/

Datum mgr_append_activate_coord(PG_FUNCTION_ARGS)
{
	
	GetAgentCmdRst getAgentCmdRst;
	AppendNodeInfo dest_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData strerr;
	StringInfoData sqlstrmsg;
	HeapTuple tup_result;
	NameData m_nodename;
	NameData s_nodename;
	HeapTuple tuple;
	HeapTuple host_tuple;
	Relation rel_node;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	PGconn *pg_conn = NULL;
	PGresult *res = NULL;
	char port_buf[10];
	char *s_coordname;
	bool b_exist_dest = false;
	bool b_running_dest = false;
	bool rest = false;
	bool noneed_dropnode = true;
	int iloop = 0;
	int s_agent_port;
	int iMax = 90;
	Oid cnoid;
	Oid checkOid;

	/*check all gtm, coordinator, datanode master running normal*/
	mgr_make_sure_all_running(GTM_TYPE_GTM_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

	/* get the input variable */
	s_coordname = PG_GETARG_CSTRING(0);
	namestrcpy(&s_nodename, s_coordname);

	/*check node status*/
	mgr_get_nodeinfo_byname_type(s_coordname, CNDN_TYPE_COORDINATOR_MASTER, false, &b_exist_dest
		, &b_running_dest, &dest_nodeinfo);
	if (!b_exist_dest)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("coordinator \"%s\" does not exist", s_coordname)));
	}

	if (mgr_check_node_exist_incluster(&s_nodename, true))
	{
		pfree_AppendNodeInfo(dest_nodeinfo);
		ereport(ERROR, (errmsg("coordinator \"%s\" already exists in cluster", s_coordname)));
	}

	memset(port_buf, 0, sizeof(char)*10);
	snprintf(port_buf, sizeof(port_buf), "%d", dest_nodeinfo.nodeport);
	rest = pingNode_user(dest_nodeinfo.nodeaddr, port_buf, dest_nodeinfo.nodeusername);

	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);
	
	PG_TRY();
	{
		if (PQPING_NO_RESPONSE == rest)
		{
			ereport(WARNING, (errmsg("coordinator \"%s\" is not running, start it now", s_coordname)));
			appendStringInfo(&infosendmsg, " start -Z coordinator -D %s -o -i -w -c -l %s/logfile -t 10"
				, dest_nodeinfo.nodepath, dest_nodeinfo.nodepath);
			rest = mgr_ma_send_cmd(AGT_CMD_CN_START, infosendmsg.data, dest_nodeinfo.nodehost, &restmsg);
			if (!rest)
			{
				ereport(ERROR, (errmsg("pg_ctl %s fail, %s", infosendmsg.data, restmsg.data)));
			}
		}
		
		/*check again*/
		rest = pingNode_user(dest_nodeinfo.nodeaddr, port_buf, dest_nodeinfo.nodeusername);
		if (PQPING_OK != rest)
		{
			ereport(ERROR, (errmsg("coordinator \"%s\" is not running normal", s_coordname)));
		}

		host_tuple = SearchSysCache1(HOSTHOSTOID, dest_nodeinfo.nodehost);
		if(!(HeapTupleIsValid(host_tuple)))
		{
			ereport(ERROR, (errmsg("host oid \"%u\" not exist", dest_nodeinfo.nodehost)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errcode(ERRCODE_UNDEFINED_OBJECT)));
		}
		mgr_host= (Form_mgr_host)GETSTRUCT(host_tuple);
		Assert(mgr_host);
		s_agent_port = mgr_host->hostagentport;
		ReleaseSysCache(host_tuple);

		/*get the value of pgxc_node_name of s_coordname*/
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, s_agent_port, "show pgxc_node_name;"
			, dest_nodeinfo.nodeusername, dest_nodeinfo.nodeaddr, dest_nodeinfo.nodeport, DEFAULT_DB, &restmsg);
		if (restmsg.len == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("on coordinator \"%s\" get value of pgxc_node_name fail", s_coordname)));
		}
		namestrcpy(&m_nodename, restmsg.data);
	}PG_CATCH();
	{
		ereport(NOTICE, (errmsg("manual invocation to check before execute this command again")));
		pfree(restmsg.data);
		pfree(infosendmsg.data);
		pfree_AppendNodeInfo(dest_nodeinfo);
		PG_RE_THROW();
	}PG_END_TRY();

	initStringInfo(&strerr);
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&sqlstrmsg);

	PG_TRY();
	{
		/* lock the cluster */
		mgr_lock_cluster(&pg_conn, &cnoid);
		/*set xc_maintenance_mode=on  */
		res = PQexec(pg_conn, "set xc_maintenance_mode = on;");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			ereport(ERROR, (errmsg("execute \"xc_maintenance_mode=on\" on coordiantors oid=%d fail, %s"
				, cnoid, PQerrorMessage(pg_conn))));
		}
		PQclear(res);

		/* check the diff xlog */
		ereport(LOG, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iMax, m_nodename.data, s_coordname)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iMax, m_nodename.data, s_coordname)));
		resetStringInfo(&restmsg);
		checkOid = mgr_get_tupleoid_from_nodename_type(m_nodename.data, CNDN_TYPE_COORDINATOR_MASTER);
		if (checkOid == cnoid)
			appendStringInfo(&restmsg, "checkpoint;");
		else
			appendStringInfo(&restmsg, "EXECUTE DIRECT ON (\"%s\") 'checkpoint;'", m_nodename.data);

		if (checkOid == cnoid)
			appendStringInfo(&sqlstrmsg, "select pg_xlog_location_diff(pg_current_xlog_insert_location(),replay_location) = 0  from pg_stat_replication where application_name='%s';"
				,s_coordname);
		else
			appendStringInfo(&sqlstrmsg, "EXECUTE DIRECT ON (\"%s\") 'select pg_xlog_location_diff(pg_current_xlog_insert_location(),replay_location) = 0  from pg_stat_replication where application_name=''%s'';'"
			, m_nodename.data, s_coordname);
		iloop = 10;
		while (iloop-- > 0)
		{
			/*checkponit first*/
			res = PQexec(pg_conn, restmsg.data);
			if (PQresultStatus(res) == PGRES_COMMAND_OK)
			{
				PQclear(res);
				break;
			}
			PQclear(res);
			
		}

		iloop = iMax;
		while (iloop-- > 0)
		{
			res = PQexec(pg_conn, sqlstrmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
				if (strcasecmp("t", PQgetvalue(res, 0, 0) != NULL ? PQgetvalue(res, 0, 0):"") == 0)
					break;
			if (iloop)
			{
				PQclear(res);
				res = NULL;
			}
			pg_usleep(1000000L);
		}
		
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			ereport(ERROR, (errmsg("wait max seconds to check coordinator \"%s\", \"%s\" have the same xlog position fail"
				, m_nodename.data, s_coordname)));
		}
		PQclear(res);
		res = NULL;
		
		noneed_dropnode = false;
		/* send create node sql to all coordiantor*/
		ereport(LOG, (errmsg("create node \"%s\" on all coordiantors in cluster", s_coordname)));
		ereport(NOTICE, (errmsg("create node \"%s\" on all coordiantors in cluster", s_coordname)));

		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, "CREATE NODE \"%s\" with (TYPE=COORDINATOR, HOST=''%s'', PORT=%d);"
			,s_coordname, dest_nodeinfo.nodeaddr, dest_nodeinfo.nodeport);
		rest = mgr_execute_direct_on_all_coord(&pg_conn, infosendmsg.data, 2, PGRES_COMMAND_OK, &strerr);
		if (!rest)
			ereport(ERROR, (errmsg("create node \"%s\" on all coordiantors in cluster fail", s_coordname)));
		
		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, "SELECT PGXC_POOL_RELOAD();");		
		rest = mgr_execute_direct_on_all_coord(&pg_conn, infosendmsg.data, 2, PGRES_TUPLES_OK, &strerr);
		if (!rest)
			ereport(ERROR, (errmsg("execute \"SELECT PGXC_POOL_RELOAD()\" on all coordiantors in cluster fail")));

		/*check xlog position again*/
		ereport(LOG, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iMax, m_nodename.data, s_coordname)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iMax, m_nodename.data, s_coordname)));
		iloop = 10;
		while (iloop-- > 0)
		{
			/*checkponit first*/
			res = PQexec(pg_conn, restmsg.data);
			if (PQresultStatus(res) == PGRES_COMMAND_OK)
			{
				PQclear(res);
				break;
			}
			PQclear(res);
		}

		iloop = iMax;
		while (iloop-- > 0)
		{
			res = PQexec(pg_conn, sqlstrmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
				if (strcasecmp("t", PQgetvalue(res, 0, 0) != NULL ? PQgetvalue(res, 0, 0):"") == 0)
					break;
			if (iloop)
			{
				PQclear(res);
				res = NULL;
			}
			pg_usleep(1000000L);
		}

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			ereport(ERROR, (errmsg("wait max seconds to check coordinator \"%s\", \"%s\" have the same xlog position fail"
				, m_nodename.data, s_coordname)));
		}
		PQclear(res);
		res = NULL;
		
		/*rm recovery.conf*/
		resetStringInfo(&infosendmsg);
		resetStringInfo(&restmsg);
		appendStringInfo(&infosendmsg, "%s/recovery.conf", dest_nodeinfo.nodepath);
		rest = mgr_ma_send_cmd(AGT_CMD_RM, infosendmsg.data, dest_nodeinfo.nodehost, &restmsg);
		if (!rest)
		{
			ereport(ERROR, (errmsg("on coordinator \"%s\", rm %s fail, %s", s_coordname, infosendmsg.data, restmsg.data)));
		}

		/*set the coordinator*/
		ereport(LOG, (errmsg("on coordinator \"%s\", set hot_standby=off, pgxc_node_name='%s'", s_coordname, s_coordname)));
		resetStringInfo(&infosendmsg);
		ereport(NOTICE, (errmsg("on coordinator \"%s\", set hot_standby=off, pgxc_node_name='%s'", s_coordname, s_coordname)));
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_add_parm(s_coordname, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_str("pgxc_node_name", s_coordname, &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "off", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, dest_nodeinfo.nodepath, &infosendmsg
			, dest_nodeinfo.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			ereport(ERROR, (errmsg("on coordinator \"%s\", set hot_standby=off, pgxc_node_name='%s' fail, %s"
				, s_coordname, s_coordname, getAgentCmdRst.description.data)));
		}

		/*restart the coordinator*/
		resetStringInfo(&(getAgentCmdRst.description));
		rel_node = heap_open(NodeRelationId, AccessShareLock);
		tuple = mgr_get_tuple_node_from_name_type(rel_node, s_coordname);
		mgr_runmode_cndn_get_result(AGT_CMD_CN_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_I);
		heap_freetuple(tuple);
		heap_close(rel_node, AccessShareLock);
		if(!getAgentCmdRst.ret)
		{
			ereport(ERROR, (errmsg("restart coordinator \"%s\" fail, %s", s_coordname, getAgentCmdRst.description.data)));
		}
		/*check the node status*/
		rest = pingNode_user(dest_nodeinfo.nodeaddr, port_buf, dest_nodeinfo.nodeusername);
		if (PQPING_OK != rest)
		{
			ereport(WARNING, (errmsg("the coordinator \"%s\" is not running normal, sleep 10 seconds to check again",s_coordname)));
			pg_usleep(10000000L);
			rest = pingNode_user(dest_nodeinfo.nodeaddr, port_buf, dest_nodeinfo.nodeusername);
			if (PQPING_OK != rest)
				ereport(ERROR,
				(errmsg("the coordinator \"%s\" is not running normal", s_coordname),
					errhint("try \"monitor all\" to check the nodes status")));
		}
		
	}PG_CATCH();
	{
		/*drop node info on all coordinators in cluster if get error*/
		if (!noneed_dropnode)
		{
			ereport(WARNING, (errmsg("rollback, drop the node \"%s\" information in pgxc_node on all coordinators.\n\tif the coordinator pgxc_node has not coordinator \"%s\" information, \n\tthe \"DROP NODE\" command may reports WARNING, ignore the warning.\n\tif you want to execute the command \"APPEND ACTIVATE COORDINATOR %s\" again, \n\tmake the coordinator \"%s\" as slave and build the streaming replication with the coordinator \"%s\"", s_coordname, s_coordname, s_coordname, s_coordname, m_nodename.data)));
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, "DROP NODE \"%s\";", s_coordname);
			rest = mgr_execute_direct_on_all_coord(&pg_conn, infosendmsg.data, 2, PGRES_COMMAND_OK, &strerr);
			
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, "SELECT PGXC_POOL_RELOAD();");		
			rest = mgr_execute_direct_on_all_coord(&pg_conn, infosendmsg.data, 2, PGRES_TUPLES_OK, &strerr);
		}
		mgr_unlock_cluster(&pg_conn);
		pfree(sqlstrmsg.data);
		pfree(strerr.data);
		pfree(restmsg.data);
		pfree(infosendmsg.data);
		pfree(getAgentCmdRst.description.data);
		pfree_AppendNodeInfo(dest_nodeinfo);
		PG_RE_THROW();
	}PG_END_TRY();

	/*set coordinator s_coordname in cluster*/
	ereport(LOG, (errmsg("set coordinator \"%s\" in cluster", s_coordname)));
	ereport(NOTICE, (errmsg("set coordinator \"%s\" in cluster", s_coordname)));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	tuple = mgr_get_tuple_node_from_name_type(rel_node, s_coordname);
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	mgr_node->nodeinited = true;
	mgr_node->nodeincluster = true;
	heap_inplace_update(rel_node, tuple);
	heap_freetuple(tuple);
	heap_close(rel_node, RowExclusiveLock);

	pfree(sqlstrmsg.data);
	pfree(restmsg.data);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	pfree_AppendNodeInfo(dest_nodeinfo);

	/* unlock the cluster */
	mgr_unlock_cluster(&pg_conn);
	
	if (strerr.len == 0)
	{
		rest = true;
		appendStringInfoString(&strerr, "success");
	}
	else
		rest = false;
	ereport(LOG, (errmsg("the command of append active coordinator \"%s\", result is: %s, description is %s"
		, s_coordname, rest ? "true":"false", strerr.data)));
	tup_result = build_common_command_tuple(&s_nodename, rest, strerr.data);
	pfree(strerr.data);
	
	return HeapTupleGetDatum(tup_result);
	
}

static bool mgr_execute_direct_on_all_coord(PGconn **pg_conn, const char *sql, const int iloop, const int res_type, StringInfo strinfo)
{
	StringInfoData restmsg;
	ScanKeyData key[3];
	Relation rel_node;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	PGresult *res = NULL;
	bool rest = true;
	int num = iloop;

	initStringInfo(&restmsg);

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 3, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		resetStringInfo(&restmsg);

		ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", NameStr(mgr_node->nodename), sql)));
		ereport(NOTICE, (errmsg("on coordinator \"%s\" execute \"%s\"", NameStr(mgr_node->nodename), sql)));

		num = iloop;
		appendStringInfo(&restmsg, "EXECUTE DIRECT ON (\"%s\") '%s'", NameStr(mgr_node->nodename), sql);
		while (num-- > 0)
		{
			res = PQexec(*pg_conn, restmsg.data);
			if (PQresultStatus(res) == res_type)
			{
				break;
			}
			if (num)
			{
				PQclear(res);
				res = NULL;
			}
			pg_usleep(100000L);
		}
		
		if (PQresultStatus(res) != res_type)
		{
			rest = false;
			ereport(WARNING, (errmsg("on coordinator \"%s\" execute \"%s\" fail, %s", NameStr(mgr_node->nodename), sql, PQerrorMessage(*pg_conn))));
			appendStringInfo(strinfo, "on coordinator \"%s\" execute \"%s\" fail, %s\n", NameStr(mgr_node->nodename), sql, PQerrorMessage(*pg_conn));
		}
		PQclear(res);
		
	}
	
	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);
	pfree(restmsg.data);

	return rest;
}


/*
* datanode switchover, command format: switchover datanode slave|extra datanode_name [force]
* gtm switchover, command format: switchover gtm slave|extra datanode_name [force]
*/

Datum mgr_switchover_func(PG_FUNCTION_ARGS)
{
	char nodeType;
	char masterType;
	char *typestr;
	char *cndnPath;
	char *nodeMasterName;
	bool isExistS = false;
	bool isExistM = false;
	bool isRunningS = false;
	bool isRunningM = false;
	bool res = false;
	bool binfosendmsg = false;
	bool bgetAgentCmdRst = false;
	bool bStopOldMaster = false;
	bool brestmsg = false;
	bool bRefreshParam = false;
	bool rest = true;
	bool isNull = false;
	bool bgtmKind = false;
	int bforce = 0;
	int iloop = 0;
	int nodePort;
	int nodeSlaveSyncKind = SYNC_STATE_ASYNC;
	const int iMax = 90;
	HeapTuple tuple;
	HeapTuple tupResult;
	HeapTuple tupleS;
	NameData nodeNameData;
	NameData nodeMasterNameData;
	NameData nodeTypeStrData;
	NameData masterTypeStrData;
	NameData oldMSyncData;
	AppendNodeInfo nodeInfoS;
	AppendNodeInfo nodeInfoM;
	Form_mgr_node mgr_node;
	PGconn *pgConn;
	StringInfoData restmsg;
	StringInfoData infosendmsg;
	StringInfoData strerr;
	StringInfoData syncStateData;
	Oid cnOid;
	Relation nodeRel;
	HeapScanDesc relScan;
	ScanKeyData key[3];
	GetAgentCmdRst getAgentCmdRst;
	Datum datumPath;
	
	mgr_make_sure_all_running(GTM_TYPE_GTM_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

	/* get the input variable */
	nodeType = PG_GETARG_INT32(0);
	namestrcpy(&nodeNameData, PG_GETARG_CSTRING(1));
	bforce = PG_GETARG_INT32(2);

	/* check the type */
	if (CNDN_TYPE_DATANODE_MASTER == nodeType || GTM_TYPE_GTM_MASTER == nodeType)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("it is the %s, no need switchover", GTM_TYPE_GTM_MASTER == nodeType ? "gtm master":"datanode master")));
	}
	
	if (CNDN_TYPE_DATANODE_SLAVE != nodeType && GTM_TYPE_GTM_SLAVE != nodeType)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("unknown the node type : %c", nodeType)));
	}
	
	/* check the slave node exist */
	if (nodeType == CNDN_TYPE_DATANODE_SLAVE)
	{
		bgtmKind = false;
		masterType = CNDN_TYPE_DATANODE_MASTER;
		namestrcpy(&masterTypeStrData, "datanode master");
		namestrcpy(&nodeTypeStrData, nodeType == CNDN_TYPE_DATANODE_SLAVE ? "datanode slave":"datanode extra");
	}
	else
	{
		bgtmKind = true;
		masterType = GTM_TYPE_GTM_MASTER;
		namestrcpy(&masterTypeStrData, "gtm master");
		namestrcpy(&nodeTypeStrData, "gtm slave");	
	}
	
	PG_TRY();
	{
		get_nodeinfo_byname(nodeNameData.data, nodeType, &isExistS, &isRunningS, &nodeInfoS);
		if (false == isExistS)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("%s \"%s\" does not exist", nodeTypeStrData.data, nodeNameData.data)));
		}
		if (false == isRunningS)
		{
			ereport(ERROR, (errmsg("%s \"%s\" is not running normal", nodeTypeStrData.data, nodeNameData.data)));
		}
	}
	PG_CATCH();
	{
		pfree_AppendNodeInfo(nodeInfoS);
		PG_RE_THROW();
	}PG_END_TRY();

	/* check the node master */
	PG_TRY();
	{
		/* get master name */
		if (!bgtmKind)
		{
			nodeMasterName = mgr_get_mastername_by_nodename_type(nodeNameData.data, CNDN_TYPE_DATANODE_SLAVE);
			namestrcpy(&nodeMasterNameData, nodeMasterName);
			pfree(nodeMasterName);
			get_nodeinfo_byname(nodeMasterNameData.data, CNDN_TYPE_DATANODE_MASTER, &isExistM, &isRunningM, &nodeInfoM);
		}
		else
		{
			nodeMasterName = mgr_get_mastername_by_nodename_type(nodeNameData.data, GTM_TYPE_GTM_SLAVE);
			namestrcpy(&nodeMasterNameData, nodeMasterName);
			pfree(nodeMasterName);
			get_nodeinfo_byname(nodeMasterNameData.data, GTM_TYPE_GTM_MASTER, &isExistM, &isRunningM, &nodeInfoM);
		}

		if (false == isExistM)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("%s \"%s\" does not exist", masterTypeStrData.data, nodeMasterNameData.data)));
		}
		if (false == isRunningM)
		{
			ereport(ERROR, (errmsg("%s \"%s\" is not running normal", masterTypeStrData.data, nodeMasterNameData.data)));
		}
	}
	PG_CATCH();
	{
		pfree_AppendNodeInfo(nodeInfoS);
		pfree_AppendNodeInfo(nodeInfoM);

		PG_RE_THROW();
	}PG_END_TRY();
	
	/*check the parameter on coordinator*/
	ereport(LOG, (errmsg("check track_activities=on in postgresql.conf on coordinators")));
	ereport(NOTICE, (errmsg("check track_activities=on in postgresql.conf on coordinators")));

	res = mgr_check_track_activities_on_coordinator();
	if (!res)
	{
		pfree_AppendNodeInfo(nodeInfoS);
		pfree_AppendNodeInfo(nodeInfoM);
		ereport(ERROR, (errmsg("check track_activities=on in postgresql.conf on coordinators fail; do \"set coordinator (track_activities=on)\" please")));
	}
	/* get slave node sync state */
	tupleS = SearchSysCache1(NODENODEOID, nodeInfoS.tupleoid);
	if(!(HeapTupleIsValid(tupleS)))
	{
		ereport(ERROR, (errmsg("get original %s \"%s\" tuple information in node table error", nodeTypeStrData.data, nodeNameData.data)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(tupleS);
	Assert(mgr_node);
	nodeSlaveSyncKind = (strcasecmp(NameStr(mgr_node->nodesync), "sync") == 0 ? SYNC_STATE_SYNC :
		(strcasecmp(NameStr(mgr_node->nodesync), "potential") == 0 ? SYNC_STATE_POTENTIAL:SYNC_STATE_ASYNC));
	ReleaseSysCache(tupleS);

	initStringInfo(&restmsg);
	initStringInfo(&syncStateData);
	mgr_get_master_sync_string(nodeInfoM.tupleoid, true, nodeInfoS.tupleoid, &restmsg);
	if (restmsg.len != 0)
	{
		if (SYNC_STATE_SYNC == nodeSlaveSyncKind)
			appendStringInfo(&syncStateData, "%s,%s", nodeMasterNameData.data, restmsg.data);
		else if (SYNC_STATE_POTENTIAL == nodeSlaveSyncKind)
			appendStringInfo(&syncStateData, "%s,%s", restmsg.data, nodeMasterNameData.data);
		else
		{
			/* do notheing */
		}
	}
	else
	{
		if (SYNC_STATE_SYNC == nodeSlaveSyncKind || SYNC_STATE_POTENTIAL == nodeSlaveSyncKind)
			appendStringInfo(&syncStateData, "%s", nodeMasterNameData.data);
	}

	/* lock the cluster */
	if (bforce == 0)
	{
		iloop = iMax;
		ereport(LOG, (errmsg("wait max %d seconds to wait there is not active connections on coordinators and datanode masters", iloop)));
		ereport(NOTICE, (errmsg("wait max %d seconds to wait there is not active connections on coordinators and datanode masters", iloop)));
		HOLD_CANCEL_INTERRUPTS();
		while (iloop-- > 0)
		{
			mgr_lock_cluster(&pgConn, &cnOid);
			res = mgr_check_active_connect_in_coordinator(pgConn, cnOid);
			if (!res)
			{
				mgr_unlock_cluster(&pgConn);
			}
			else
				break;
		}
		RESUME_CANCEL_INTERRUPTS();

		if (!res)
		{
			pfree_AppendNodeInfo(nodeInfoS);
			pfree_AppendNodeInfo(nodeInfoM);		
			ereport(ERROR, (errmsg("there are active connect on coordinators or datanode masters")));
		}

	}
	else
		mgr_lock_cluster(&pgConn, &cnOid);

	/* check the xlog diff */
	PG_TRY();
	{
		resetStringInfo(&restmsg);
		initStringInfo(&infosendmsg);
		initStringInfo(&(getAgentCmdRst.description));
		ereport(LOG, (errmsg("wait max %d seconds to check there is not active locks in pg_locks table on all coordinators except the locks on pg_locks table", iMax)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check there is not active locks in pg_locks table on all coordinators except the locks on pg_locks table", iMax)));
		iloop = iMax;
		while (iloop-- > 0)
		{
			//chck three time
			res = mgr_check_active_locks_in_cluster(pgConn, cnOid);
			if(res)
				break;
			pg_usleep(1000000L);
		}
		
		if (iloop <= 0)
			ereport(ERROR, (errmsg("wait max %d seconds to check there is not active locks in pg_locks table on all coordinators except the locks on pg_locks table fail", iMax)));

		ereport(LOG, (errmsg("wait max %d seconds to check %s \"%s\", %s \"%s\" have the same xlog position"
				, iMax, masterTypeStrData.data, nodeMasterNameData.data,  nodeTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check %s \"%s\", %s \"%s\" have the same xlog position"
				, iMax, masterTypeStrData.data, nodeMasterNameData.data,  nodeTypeStrData.data, nodeNameData.data)));

		iloop = mgr_maxtime_check_xlog_diff(nodeType, nodeNameData.data, &nodeInfoM, iMax);
		if (iloop)
			iloop = mgr_maxtime_check_xlog_diff(nodeType, nodeNameData.data, &nodeInfoM, iloop);
		if (iloop)
			iloop = mgr_maxtime_check_xlog_diff(nodeType, nodeNameData.data, &nodeInfoM, iloop);
		if (iloop <= 0)
		{
			ereport(ERROR, (errmsg("wait max %d seconds to check %s \"%s\", %s \"%s\" have the same xlog position fail"
					, iMax, masterTypeStrData.data, nodeMasterNameData.data,  nodeTypeStrData.data, nodeNameData.data)));
		}
		
		/* stop datanode master mode i*/
		bStopOldMaster = true;
		appendStringInfo(&infosendmsg, " stop -D %s -m i -o -i -w -c", nodeInfoM.nodepath);
		if (!bgtmKind)
			res = mgr_ma_send_cmd(AGT_CMD_DN_STOP, infosendmsg.data, nodeInfoM.nodehost, &restmsg);
		else
			res = mgr_ma_send_cmd(AGT_CMD_GTM_STOP_MASTER, infosendmsg.data, nodeInfoM.nodehost, &restmsg);
		if (!res)
				ereport(ERROR, (errmsg("stop %s \"%s\" fail %s", masterTypeStrData.data, nodeNameData.data, restmsg.data)));

		bRefreshParam = true;
		/* set parameters the given slave node in postgresql.conf */
		resetStringInfo(&infosendmsg);
		ereport(LOG, (errmsg("on %s \"%s\" set synchronous_standby_names=%s", nodeTypeStrData.data, nodeNameData.data, syncStateData.data)));
		ereport(NOTICE, (errmsg("on %s \"%s\" set synchronous_standby_names=%s", nodeTypeStrData.data, nodeNameData.data, syncStateData.data)));
		if (!bgtmKind)
			mgr_add_parm(nodeNameData.data, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		else
			mgr_add_parm(nodeNameData.data, GTM_TYPE_GTM_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", syncStateData.data, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodeInfoS.nodepath, &infosendmsg
				, nodeInfoS.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetAgentCmdRst = true;
			ereport(ERROR, (errmsg("on %s \"%s\" set synchronous_standby_names=%s fail, %s"
				, nodeTypeStrData.data, nodeNameData.data, syncStateData.data, getAgentCmdRst.description.data)));
		}
		
		/* set the given slave node pg_hba.conf for streaming replication*/
		ereport(LOG, (errmsg("set %s \"%s\" pg_hba.conf", nodeTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("set %s \"%s\" pg_hba.conf", nodeTypeStrData.data, nodeNameData.data)));
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_get_hba_replication_info(nodeInfoS.nodemasteroid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, nodeInfoS.nodepath, &infosendmsg
				, nodeInfoS.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetAgentCmdRst = true;
			ereport(ERROR, (errmsg("on %s \"%s\", refresh pg_bha.conf fail, %s"
				, nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		}
		mgr_reload_conf(nodeInfoS.nodehost, nodeInfoS.nodepath);

		/* promote the given slave node */
		resetStringInfo(&restmsg);
		resetStringInfo(&infosendmsg);

		appendStringInfo(&infosendmsg, " promote -D %s -w", nodeInfoS.nodepath);
		if (!bgtmKind)
			res = mgr_ma_send_cmd(AGT_CMD_DN_FAILOVER, infosendmsg.data, nodeInfoS.nodehost, &restmsg);
		else
			res = mgr_ma_send_cmd(AGT_CMD_GTM_SLAVE_FAILOVER, infosendmsg.data, nodeInfoS.nodehost, &restmsg);
		if (!res)
		{
			brestmsg = true;
			ereport(ERROR, (errmsg("promote %s \"%s\" fail, %s", nodeTypeStrData.data, nodeNameData.data, restmsg.data)));
		}
		/*check recovery finish*/
		ereport(LOG, (errmsg("waiting for the new %s \"%s\" can accept connections...", masterTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("waiting for the new %s \"%s\" can accept connections...", masterTypeStrData.data, nodeNameData.data)));
		mgr_check_node_connect(nodeType, nodeInfoS.nodehost, nodeInfoS.nodeport);

	}
	PG_CATCH();
	{
		ereport(LOG, (errmsg("rollback start:")));
		ereport(NOTICE, (errmsg("rollback start:")));

		if (bStopOldMaster)
		{
			ereport(WARNING, (errmsg("make %s \"%s\" as %s fail, use \"monitor all\", \"monitor ha\" to check nodes status !!! you may need to make the original %s \"%s\" to run normal !!!",
			nodeTypeStrData.data, nodeNameData.data, masterTypeStrData.data, nodeTypeStrData.data
			, nodeNameData.data)));
			/* start the old master node */
			resetStringInfo(&(getAgentCmdRst.description));
			nodeRel = heap_open(NodeRelationId, AccessShareLock);
			if (!bgtmKind)
				tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data);
			else
				tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data);
			mgr_runmode_cndn_get_result(AGT_CMD_DN_START, &getAgentCmdRst, nodeRel, tuple, TAKEPLAPARM_N);
			heap_freetuple(tuple);
			heap_close(nodeRel, AccessShareLock);
			if(!getAgentCmdRst.ret)
			{
				ereport(WARNING, (errmsg("start original %s \"%s\" fail %s", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
			}
		}

		if (bRefreshParam)
		{
			/* set parameters the given slave node in postgresql.conf */
			resetStringInfo(&infosendmsg);
			resetStringInfo(&(getAgentCmdRst.description));
			ereport(LOG, (errmsg("on original %s \"%s\", set hot_standby=on", nodeTypeStrData.data, nodeNameData.data)));
			ereport(NOTICE, (errmsg("on original %s \"%s\", set hot_standby=on", nodeTypeStrData.data, nodeNameData.data)));
			if (!bgtmKind)
				mgr_add_parm(nodeNameData.data, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
			else
				mgr_add_parm(nodeNameData.data, GTM_TYPE_GTM_MASTER, &infosendmsg);
			mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodeInfoS.nodepath, &infosendmsg
					, nodeInfoS.nodehost, &getAgentCmdRst);
			if (!getAgentCmdRst.ret)
			{
				bgetAgentCmdRst = true;
				ereport(WARNING, (errmsg("on original %s \"%s\", set hot_standby=on fail, %s"
					, nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
			}
			/*restart the given slave node*/
			resetStringInfo(&(getAgentCmdRst.description));
			nodeRel = heap_open(NodeRelationId, AccessShareLock);
			tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data);
			if (!bgtmKind)
				mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
			else
				mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
			heap_freetuple(tuple);
			heap_close(nodeRel, AccessShareLock);
			if(!getAgentCmdRst.ret)
			{
				bgetAgentCmdRst = true;
				ereport(WARNING, (errmsg("restart original %s \"%s\" fail, %s", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
			}
		}

		mgr_unlock_cluster(&pgConn);
		pfree_AppendNodeInfo(nodeInfoS);
		pfree_AppendNodeInfo(nodeInfoM);
		if (!binfosendmsg)
			pfree(infosendmsg.data);
		if (!bgetAgentCmdRst)
			pfree(getAgentCmdRst.description.data);
		if (!brestmsg)
			pfree(restmsg.data);
		pfree(syncStateData.data);
		ereport(LOG, (errmsg("rollback end")));
		ereport(NOTICE, (errmsg("rollback end")));
		PG_RE_THROW();
	}PG_END_TRY();
	
	pfree(restmsg.data);
	pfree(syncStateData.data);

	initStringInfo(&strerr);
	if (!bgtmKind)
	{
		/* refresh pgxc_node on all coordinators */
		ereport(LOG, (errmsg("refresh the new datanode master \"%s\" information in pgxc_node on all coordinators", nodeNameData.data)));
		ereport(NOTICE, (errmsg("refresh the new datanode master \"%s\" information in pgxc_node on all coordinators", nodeNameData.data)));
		res = mgr_pqexec_refresh_pgxc_node(PGXC_FAILOVER, nodeType, nodeNameData.data, &getAgentCmdRst, &pgConn, cnOid);
		if (!res)
		{
			rest = false;
			ereport(WARNING, (errmsg("%s", getAgentCmdRst.description.data)));
			appendStringInfo(&strerr, "update pgxc_node on coordinators fail: %s\n", getAgentCmdRst.description.data);
		}
	}
	else
	{
		/*update agtm_port, agtm_host on all coordinators, datanodes*/
		tupleS = SearchSysCache1(NODENODEOID, nodeInfoS.tupleoid);
		if(!(HeapTupleIsValid(tupleS)))
		{
			ereport(ERROR, (errmsg("get original %s \"%s\" tuple information in node table error", nodeTypeStrData.data, nodeNameData.data)));
		}

		mgr_node = (Form_mgr_node)GETSTRUCT(tupleS);
		Assert(mgr_node);
		nodePort = mgr_node->nodeport;
		ReleaseSysCache(tupleS);
		PG_TRY();
		{
			mgr_update_agtm_port_host(&pgConn, nodeInfoS.nodeaddr, nodePort, cnOid, &strerr);
		}
		PG_CATCH();
		{
			ereport(LOG, (errmsg("rollback start:")));
			ereport(NOTICE, (errmsg("rollback start:")));
			mgr_unlock_cluster(&pgConn);
			ereport(WARNING, (errmsg("set agtm_post, agtm_host fail, use \"monitor all\", \"monitor ha\" to check nodes status !!! you may need to make the original %s \"%s\" to run normal !!! check agtm_host,agtm_port in postgresql.conf of all coordinators and datanodes, make the original %s \"%s\" to run normal!!! check hot_standby in its postgresql.conf"
			 ,masterTypeStrData.data, nodeMasterNameData.data, nodeTypeStrData.data, nodeNameData.data)));
			pfree_AppendNodeInfo(nodeInfoS);
			pfree_AppendNodeInfo(nodeInfoM);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			pfree(restmsg.data);
			pfree(syncStateData.data);
			ereport(LOG, (errmsg("rollback end")));
			ereport(NOTICE, (errmsg("rollback end")));
			PG_RE_THROW();
		}PG_END_TRY();
	}
	/*unlock cluster*/
	mgr_unlock_cluster(&pgConn);

	PG_TRY();
	{
		ereport(LOG, (errmsg("exchange the node type for %s \"%s\" and %s \"%s\" in node table", masterTypeStrData.data, nodeMasterNameData.data, nodeTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("exchange the node type for %s \"%s\" and %s \"%s\" in node table", masterTypeStrData.data, nodeMasterNameData.data, nodeTypeStrData.data, nodeNameData.data)));
		
		/* refresh new master info in node table */
		nodeRel = heap_open(NodeRelationId, RowExclusiveLock);

		tuple = SearchSysCache1(NODENODEOID, nodeInfoS.tupleoid);
		if(!(HeapTupleIsValid(tuple)))
		{
			heap_close(nodeRel, RowExclusiveLock);
			ereport(ERROR, (errmsg("get original %s \"%s\" tuple information in node table error", nodeTypeStrData.data, nodeNameData.data)));
		}
		tupleS = SearchSysCache1(NODENODEOID, nodeInfoM.tupleoid);
		if(!(HeapTupleIsValid(tupleS)))
		{
			ReleaseSysCache(tuple);
			heap_close(nodeRel, RowExclusiveLock);
			ereport(ERROR, (errmsg("get original %s \"%s\" tuple information in node table error", masterTypeStrData.data, nodeMasterNameData.data)));
		}

		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodeSlaveSyncKind = (strcasecmp(NameStr(mgr_node->nodesync), "sync") == 0 ? SYNC_STATE_SYNC :
			(strcasecmp(NameStr(mgr_node->nodesync), "potential") == 0 ? SYNC_STATE_POTENTIAL:SYNC_STATE_ASYNC));
		namestrcpy(&oldMSyncData, NameStr(mgr_node->nodesync));
		if (!bgtmKind)
			mgr_node->nodetype = CNDN_TYPE_DATANODE_MASTER;
		else
			mgr_node->nodetype = GTM_TYPE_GTM_MASTER;
		mgr_node->nodemasternameoid = 0;
		namestrcpy(&(mgr_node->nodesync), "");
		heap_inplace_update(nodeRel, tuple);	
		ReleaseSysCache(tuple);

		/* refresh new slave info in node table */
		mgr_node = (Form_mgr_node)GETSTRUCT(tupleS);
		Assert(mgr_node);
		mgr_node->nodetype = nodeType;
		mgr_node->nodemasternameoid = nodeInfoS.tupleoid;
		namestrcpy(&(mgr_node->nodesync), oldMSyncData.data);
		heap_inplace_update(nodeRel, tupleS);	
		ReleaseSysCache(tupleS);

		heap_close(nodeRel, RowExclusiveLock);
	}PG_CATCH();
	{
		ereport(LOG, (errmsg("rollback start:")));
		ereport(NOTICE, (errmsg("rollback start:")));

		ereport(WARNING, (errmsg("exchange the node type for %s \"%s\" and %s \"%s\" in node table fail, exchange them manual, include: nodetype, sync_state, mastername !!! use \"monitor all\", \"monitor ha\" to check nodes status; make the other datanode slave or datanode extra \"%s\" as new slave or extra for new %s: refresh its recovery.conf and its mastername in node table !!!", masterTypeStrData.data, nodeMasterNameData.data, nodeTypeStrData.data, nodeNameData.data, nodeNameData.data, masterTypeStrData.data)));
		
		pfree(strerr.data);
		pfree(infosendmsg.data);
		pfree(getAgentCmdRst.description.data);
		pfree_AppendNodeInfo(nodeInfoS);
		pfree_AppendNodeInfo(nodeInfoM);

		ereport(LOG, (errmsg("rollback end")));
		ereport(NOTICE, (errmsg("rollback end")));
		PG_RE_THROW();
	}PG_END_TRY();

	/* update the param set in mgr_updateparm table */
	Relation rel_updateparm;
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	mgr_parmr_update_tuple_nodename_nodetype(rel_updateparm, &nodeNameData, nodeType, masterType);
	mgr_parmr_update_tuple_nodename_nodetype(rel_updateparm, &nodeMasterNameData, masterType, nodeType);
	heap_close(rel_updateparm, RowExclusiveLock);

	/* update new slave postgresql.conf */
	resetStringInfo(&infosendmsg);
	resetStringInfo(&(getAgentCmdRst.description));
	ereport(LOG, (errmsg("on new %s \"%s\", set hot_standby=on, synchronous_standby_names=''", nodeTypeStrData.data, nodeMasterNameData.data)));
	ereport(NOTICE, (errmsg("on new %s \"%s\", set hot_standby=on, synchronous_standby_names=''", nodeTypeStrData.data, nodeMasterNameData.data)));
	mgr_add_parm(nodeMasterNameData.data, nodeType, &infosendmsg);
	mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodeInfoM.nodepath, &infosendmsg
			, nodeInfoM.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("on new %s \"%s\", set hot_standby=on, synchronous_standby_names='' fail, %s"
			, nodeTypeStrData.data, nodeMasterNameData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "on new %s \"%s\", set hot_standby=on, synchronous_standby_names='' fail, %s\n", nodeTypeStrData.data, nodeMasterNameData.data, getAgentCmdRst.description.data);
	}	
	
	/* update new slave recovery.conf */
	ereport(LOG, (errmsg("on new %s \"%s\" refresh recovery.conf", nodeTypeStrData.data, nodeMasterNameData.data)));
	ereport(NOTICE, (errmsg("on new %s \"%s\" refresh recovery.conf", nodeTypeStrData.data, nodeMasterNameData.data)));
	resetStringInfo(&(getAgentCmdRst.description));
	resetStringInfo(&infosendmsg);
	mgr_add_parameters_recoveryconf(nodeType, nodeMasterNameData.data, nodeInfoS.tupleoid, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, nodeInfoM.nodepath, &infosendmsg, nodeInfoM.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("on new %s \"%s\", refresh recovery.conf fail, %s"
			, nodeTypeStrData.data, nodeMasterNameData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "on new %s \"%s\", refresh recovery.conf fail, %s\n", nodeTypeStrData.data, nodeMasterNameData.data, getAgentCmdRst.description.data);
	}
	
	/* start the new slave node */
	resetStringInfo(&(getAgentCmdRst.description));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeMasterNameData.data);
	if (!bgtmKind)
		mgr_runmode_cndn_get_result(AGT_CMD_DN_START, &getAgentCmdRst, nodeRel, tuple, TAKEPLAPARM_N);
	else
		mgr_runmode_cndn_get_result(AGT_CMD_GTM_START_SLAVE, &getAgentCmdRst, nodeRel, tuple, TAKEPLAPARM_N);

	heap_freetuple(tuple);
	heap_close(nodeRel, AccessShareLock);

	if(!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("start new %s \"%s\" fail, %s", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "start new %s \"%s\" fail, %s\n", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data);
	}

	/* for other slave */
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodemasternameOid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(nodeInfoS.nodemasteroid));
	nodeRel = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_node->nodemasternameoid != nodeInfoM.tupleoid || (nodeInfoS.tupleoid == HeapTupleGetOid(tuple)))
			continue;
		typestr = mgr_nodetype_str(mgr_node->nodetype);

		ereport(LOG, (errmsg("refresh mastername of %s \"%s\" in node table", typestr, NameStr(mgr_node->nodename))));
		ereport(NOTICE, (errmsg("refresh mastername of %s \"%s\" in node table", typestr, NameStr(mgr_node->nodename))));
		mgr_node->nodemasternameoid = nodeInfoS.tupleoid;
		heap_inplace_update(nodeRel, tuple);

		/* update recovery.conf */
		ereport(LOG, (errmsg("refresh %s \"%s\" recovery.conf", typestr, NameStr(mgr_node->nodename))));
		ereport(NOTICE, (errmsg("refresh %s \"%s\" recovery.conf", typestr, NameStr(mgr_node->nodename))));
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(mgr_node->nodetype, NameStr(mgr_node->nodename), nodeInfoS.tupleoid, &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(nodeRel), &isNull);
		if (isNull)
		{
			heap_endscan(relScan);
			heap_close(nodeRel, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(strerr.data);
			pfree(getAgentCmdRst.description.data);
			pfree_AppendNodeInfo(nodeInfoS);
			pfree_AppendNodeInfo(nodeInfoM);
			ereport(WARNING, (errmsg("you should use \"monitor all\", \"monitor ha\" to check the node \"%s\" status, modify the mastername of %s slave in node table"
				, nodeNameData.data, nodeNameData.data)));
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node"), errmsg("column cndnpath is null")));
		}
		cndnPath = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPath, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			rest = false;
			ereport(WARNING, (errmsg("on %s \"%s\", refresh recovery.conf fail, %s"
				, typestr, NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
			appendStringInfo(&strerr, "on %s \"%s\", refresh recovery.conf fail, %s\n", typestr, NameStr(mgr_node->nodename), getAgentCmdRst.description.data);
		}
		
		/* restart the node */
		resetStringInfo(&(getAgentCmdRst.description));
		if (!bgtmKind)
			mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
		else
			mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
		if(!getAgentCmdRst.ret)
		{
			rest = false;
			ereport(WARNING, (errmsg("restart %s \"%s\" fail, %s", typestr, NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
			appendStringInfo(&strerr, "restart %s \"%s\" fail, %s\n", typestr, NameStr(mgr_node->nodename), getAgentCmdRst.description.data);
		}
		pfree(typestr);
	}
	heap_endscan(relScan);
	heap_close(nodeRel, RowExclusiveLock);

	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	pfree_AppendNodeInfo(nodeInfoS);
	pfree_AppendNodeInfo(nodeInfoM);

	if (strerr.len == 0)
		appendStringInfoString(&strerr, "success");
	ereport(LOG, (errmsg("the command of switchover result : status = %s , description is : %s", rest == true ? "true":"false", strerr.data)));
	tupResult = build_common_command_tuple(&nodeNameData, rest, strerr.data);
	pfree(strerr.data);
	return HeapTupleGetDatum(tupResult);
}


static void mgr_get_hba_replication_info(Oid masterTupleOid, StringInfo infosendmsg)
{
	ScanKeyData key[2];
	Relation nodeRel;
	HeapScanDesc relScan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char *hostAddr;
	char *userName;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if ((masterTupleOid != mgr_node->nodemasternameoid) && (masterTupleOid != HeapTupleGetOid(tuple)))
			continue;
		hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype)
		{
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", AGTM_USER, hostAddr, 32, "trust", infosendmsg);
		}
		else
		{
			userName = get_hostuser_from_hostoid(mgr_node->nodehost);
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", userName, hostAddr, 32, "trust", infosendmsg);
			pfree(userName);
		}
		pfree(hostAddr);
	}
	
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);
}


static int mgr_maxtime_check_xlog_diff(const char nodeType, const char *nodeName, AppendNodeInfo *nodeInfoM, const int maxSecond)
{
	int iloop = 0;
	int agentPortM;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	Form_mgr_host mgr_host;
	HeapTuple hostTupleM;
	
	Assert(CNDN_TYPE_DATANODE_SLAVE == nodeType || GTM_TYPE_GTM_SLAVE == nodeType);
	Assert(nodeName);
	Assert(nodeInfoM);

	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);
	appendStringInfo(&infosendmsg, "select pg_xlog_location_diff(pg_current_xlog_insert_location(),replay_location) = 0 from pg_stat_replication where application_name='%s';", nodeName);
	
	hostTupleM = SearchSysCache1(HOSTHOSTOID, nodeInfoM->nodehost);
	if(!(HeapTupleIsValid(hostTupleM)))
	{
		ereport(ERROR, (errmsg("get the datanode master \"%s\" information in node table fail", nodeInfoM->nodename)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(hostTupleM);
	Assert(mgr_host);
	agentPortM = mgr_host->hostagentport;
	ReleaseSysCache(hostTupleM);	
	/*checkponit first*/
	iloop = 10;
	while (iloop-- > 0)
	{
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES_COMMAND, agentPortM, "checkpoint;"
				, nodeInfoM->nodeusername, nodeInfoM->nodeaddr, nodeInfoM->nodeport, DEFAULT_DB, &restmsg);
		if (restmsg.len != 0)
		{
			if (strcasecmp(restmsg.data, "checkpoint") ==0)
			{
				break;
			}
		}
	}

	iloop = maxSecond;
	while (iloop-- > 0)
	{
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPortM, infosendmsg.data
			, nodeInfoM->nodeusername, nodeInfoM->nodeaddr, nodeInfoM->nodeport, DEFAULT_DB, &restmsg);
		if (restmsg.len != 0)
		{
			if (strcmp(restmsg.data, "t") == 0)
				break;
		}

		pg_usleep(1000000L);
	}
	
	pfree(infosendmsg.data);
	pfree(restmsg.data);
	
	return iloop;
}

static bool mgr_check_active_locks_in_cluster(PGconn *pgConn, const Oid cnOid)
{
	ScanKeyData key[2];
	Relation nodeRel;
	HeapScanDesc relScan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	StringInfoData cmdstring;
	PGresult *res;
	char *p = NULL;
	char *nodeTypeStr;
	bool rest = false;
	
	Assert(pgConn);
	Assert(cnOid);
	initStringInfo(&cmdstring);

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_node->nodetype != CNDN_TYPE_COORDINATOR_MASTER && mgr_node->nodetype !=
			CNDN_TYPE_DATANODE_MASTER)
			continue;
		nodeTypeStr = mgr_nodetype_str(mgr_node->nodetype);
		ereport(LOG, (errmsg("check active locks on %s %s", nodeTypeStr, NameStr(mgr_node->nodename))));
		ereport(NOTICE, (errmsg("check active locks on %s %s", nodeTypeStr, NameStr(mgr_node->nodename))));
		resetStringInfo(&cmdstring);
		pfree(nodeTypeStr);
		if (cnOid == HeapTupleGetOid(tuple))
			appendStringInfoString(&cmdstring, "select count(*)  from pg_locks where pid !=  pg_backend_pid();");
		else
			appendStringInfo(&cmdstring, "EXECUTE DIRECT ON (\"%s\") 'select count(*)  from pg_locks where pid !=  pg_backend_pid();'"
							,NameStr(mgr_node->nodename));

		res = PQexec(pgConn, cmdstring.data);						
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			p = PQgetvalue(res, 0, 0);
			if (p == NULL)
				rest = false;
			else if (strcmp(p, "0") != 0)
				rest = false;
			else
				rest = true;
			PQclear(res);
			res = NULL;
		}
		else
		{
			rest = false;
			ereport(WARNING, (errmsg("%s", PQerrorMessage(pgConn))));
		}		

		if (rest == false)
			break;
	}
	
	pfree(cmdstring.data);
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);
	
	return rest;
}

static bool mgr_check_active_connect_in_coordinator(PGconn *pgConn, const Oid cnOid)
{
	ScanKeyData key[2];
	Relation nodeRel;
	HeapScanDesc relScan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	StringInfoData cmdstring;
	PGresult *res;
	char *p = NULL;
	char *nodeTypeStr;
	bool rest = false;
	
	Assert(pgConn);
	Assert(cnOid);
	initStringInfo(&cmdstring);

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (CNDN_TYPE_COORDINATOR_MASTER != mgr_node->nodetype && CNDN_TYPE_DATANODE_MASTER != mgr_node->nodetype)
			continue;
		nodeTypeStr = mgr_nodetype_str(mgr_node->nodetype);
		ereport(LOG, (errmsg("check active connections on %s %s", nodeTypeStr, NameStr(mgr_node->nodename))));
		ereport(NOTICE, (errmsg("check active connections on %s %s", nodeTypeStr, NameStr(mgr_node->nodename))));
		resetStringInfo(&cmdstring);
		pfree(nodeTypeStr);	
		/*for coordiantor connect*/
		resetStringInfo(&cmdstring);
		if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype)
		{
			if (cnOid == HeapTupleGetOid(tuple))
				appendStringInfoString(&cmdstring, "SELECT count(*)-1  FROM pg_stat_activity WHERE state = \'active\';");
			else
				appendStringInfo(&cmdstring, "EXECUTE DIRECT ON (\"%s\") 'SELECT count(*)-1  FROM pg_stat_activity WHERE state = ''active'''"
								,NameStr(mgr_node->nodename));
		}
		else
		{
			if (cnOid == HeapTupleGetOid(tuple))
				appendStringInfoString(&cmdstring, "select  sum(numbackends)-1  from pg_stat_database where datname != \'template1\' and datname != \'template0\';");
			else
				appendStringInfo(&cmdstring, "EXECUTE DIRECT ON (\"%s\") 'select  sum(numbackends)-1  from pg_stat_database where datname != ''template1'' and datname != ''template0'';'"
								,NameStr(mgr_node->nodename));
		}
		res = PQexec(pgConn, cmdstring.data);						
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			p = PQgetvalue(res, 0, 0);
			if (p == NULL)
				rest = false;
			else if (strcmp(p, "0") != 0)
				rest = false;
			else
				rest = true;
			PQclear(res);
			res = NULL;
		}
		else
		{
			rest = false;
			ereport(WARNING, (errmsg("%s", PQerrorMessage(pgConn))));
		}
		
		if (rest == false)
			break;
	}
	
	pfree(cmdstring.data);
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);
	
	return rest;
}

bool mgr_update_agtm_port_host(PGconn **pg_conn, char *hostaddress, int cndnport, Oid cnoid, StringInfo recorderr)
{
	StringInfoData infosendmsg;
	StringInfoData infosendsyncmsg;
	HeapTuple tuple;
	ScanKeyData key[3];
	Relation nodeRel;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodecn;
	Datum datumPath;
	HeapTuple cn_tuple;
	NameData cnnamedata;
	int try;
	char *address;
	char *strnodetype;
	GetAgentCmdRst getAgentCmdRst;
	char *cndnPath;
	char nodeportBuf[10];
	int maxtry = 60;
	int nrow;
	bool reload_port;
	bool reload_host;
	bool isNull;
	Oid hostOid;
	Oid hostOidtmp;
	PGresult * volatile res = NULL;
	
	memset(nodeportBuf, 0, 10);
	sprintf(nodeportBuf, "%d", cndnport);

	/*get agtm_port,agtm_host*/
	initStringInfo(&infosendmsg);
	initStringInfo(&infosendsyncmsg);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_append_pgconf_paras_str_quotastr("agtm_host", hostaddress, &infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", cndnport, &infosendmsg);


	/*refresh datanode master/slave/extra reload agtm_port, agtm_host*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_node->nodetype == CNDN_TYPE_DATANODE_MASTER)
		{
			hostOid = mgr_node->nodehost;
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(nodeRel), &isNull);
			if(isNull)
			{
				heap_endscan(relScan);
				heap_close(nodeRel, AccessShareLock);

				pfree(infosendsyncmsg.data);
				pfree(infosendmsg.data);
				pfree((getAgentCmdRst.description.data));
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column cndnpath is null")));
			}
			cndnPath = TextDatumGetCString(datumPath);
			try = maxtry;
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			strnodetype = mgr_nodetype_str(mgr_node->nodetype);
			ereport(LOG, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\"", strnodetype, NameStr(mgr_node->nodename))));
			ereport(NOTICE, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\"", strnodetype, NameStr(mgr_node->nodename))));
			while(try-- >= 0)
			{
				resetStringInfo(&(getAgentCmdRst.description));
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOid, &getAgentCmdRst);
				/*sleep 0.1s*/
				pg_usleep(100000L);

				/*check the agtm_host, agtm_port*/
				if(mgr_check_param_reload_postgresqlconf(mgr_node->nodetype, hostOid, mgr_node->nodeport, address, "agtm_host", hostaddress)
					&& mgr_check_param_reload_postgresqlconf(mgr_node->nodetype, hostOid, mgr_node->nodeport, address, "agtm_port", nodeportBuf))
				{
					break;
				}
			}
			if (try < 0)
			{
				ereport(WARNING, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\" fail", strnodetype, NameStr(mgr_node->nodename))));
				appendStringInfo(recorderr, "on %s \"%s\" reload \"agtm_host\", \"agtm_port\" fail\n", strnodetype, NameStr(mgr_node->nodename));
			}
			pfree(strnodetype);
			pfree(address);
		}
	}
	heap_endscan(relScan);

	/*get name of coordinator, whos oid is cnoid*/
	cn_tuple = SearchSysCache1(NODENODEOID, cnoid);
	if(!HeapTupleIsValid(cn_tuple))
	{
		heap_close(nodeRel, AccessShareLock);
		pfree(infosendsyncmsg.data);
		pfree(infosendmsg.data);
		pfree((getAgentCmdRst.description.data));
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
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	relScan = heap_beginscan(nodeRel, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		hostOidtmp = mgr_node->nodehost;
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(nodeRel), &isNull);
		if(isNull)
		{
			heap_endscan(relScan);
			heap_close(nodeRel, AccessShareLock);
			pfree(infosendsyncmsg.data);
			pfree(infosendmsg.data);
			pfree((getAgentCmdRst.description.data));
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		cndnPath = TextDatumGetCString(datumPath);
		try = maxtry;
		ereport(LOG, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\"", NameStr(mgr_node->nodename))));
		ereport(NOTICE, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\"", NameStr(mgr_node->nodename))));
		while(try-- >=0)
		{
			resetStringInfo(&(getAgentCmdRst.description));
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOidtmp, &getAgentCmdRst);

			pg_usleep(100000L);
			/*check the agtm_host, agtm_port*/
			reload_host = false;
			reload_port = false;
			resetStringInfo(&infosendsyncmsg);
			appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select setting from pg_settings where name=''agtm_host'';'", NameStr(mgr_node->nodename));
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
			appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select setting from pg_settings where name=''agtm_port'';'", NameStr(mgr_node->nodename));
			res = PQexec(*pg_conn, infosendsyncmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				nrow = PQntuples(res);
				if (nrow > 0)
					if (strcasecmp(nodeportBuf, PQgetvalue(res, 0, 0)) == 0)
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
			ereport(WARNING, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\" fail", NameStr(mgr_node->nodename))));
			appendStringInfo(recorderr, "on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\" fail\n", NameStr(mgr_node->nodename));
		}
	}
	heap_endscan(relScan);

	/*send sync agtm xid*/
	relScan = heap_beginscan(nodeRel, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		resetStringInfo(&infosendsyncmsg);
		appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select pgxc_pool_reload()';", NameStr(mgr_node->nodename));
		ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, infosendsyncmsg.data)));
		ereport(NOTICE, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, infosendsyncmsg.data)));
		try = maxtry;
		while(try-- >= 0)
		{
			res = PQexec(*pg_conn, infosendsyncmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
					if (strcasecmp("t", PQgetvalue(res, 0, 0)) == 0)
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
			appendStringInfo(recorderr, "on coordinator \"%s\" execute \"%s\" fail\n", cnnamedata.data, infosendsyncmsg.data);
		}
	}
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);

	pfree(infosendsyncmsg.data);
	pfree(infosendmsg.data);
	pfree((getAgentCmdRst.description.data));
	return true;
}


static bool mgr_check_track_activities_on_coordinator(void)
{
	bool rest = true;
	char *address;
	Relation nodeRel;
	HeapTuple tuple;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	ScanKeyData key[3];
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_append_pgconf_paras_str_quotastr("track_activities", "on", &infosendmsg);

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
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		rest = mgr_check_param_reload_postgresqlconf(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, address, "track_activities", "on");
		pfree(address);
		if (!rest)
			break;
	}
	
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);
	
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	
	return rest;
}


static Oid mgr_get_tupleoid_from_nodename_type(char *nodename, char nodetype)
{
	Relation nodeRel;
	HeapScanDesc relScan;
	NameData nodenamedata;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Oid tupleOid = 0;
	
	namestrcpy(&nodenamedata, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2],
		Anum_mgr_node_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenamedata));
	ScanKeyInit(&key[3],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 4, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		tupleOid = HeapTupleGetOid(tuple);
		break;
	}
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);

	return tupleOid;
}
