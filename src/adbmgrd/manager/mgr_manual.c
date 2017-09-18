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
static void mgr_get_hba_replication_info(char *nodename, StringInfo infosendmsg);
static bool mgr_maxtime_check_xlog_diff(const char nodeType, const char *nodeName, AppendNodeInfo *nodeInfoM, const int maxSecond);
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
	char anothertype;
	char *nodetypestr;
	bool master_is_exist = true;
	bool master_is_running = true;
	bool slave_is_exist = true;
	bool slave_is_running = true;
	bool res = false;
	NameData nodenamedata;
	AppendNodeInfo master_nodeinfo;
	AppendNodeInfo slave_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData strinfo;
	HeapTuple mastertuple;
	HeapTuple slavetuple;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Relation rel_node;
	GetAgentCmdRst getAgentCmdRst;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	/*get the input variable*/
	nodetype = PG_GETARG_INT32(0);
	namestrcpy(&nodenamedata, PG_GETARG_CSTRING(1));

	mastertype = mgr_get_master_type(nodetype);
	if (GTM_TYPE_GTM_MASTER == mastertype)
		stop_cmdtype = AGT_CMD_GTM_STOP_MASTER;
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
	get_nodeinfo_byname(nodenamedata.data, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
	if (master_is_exist && master_is_running)
	{
		/*stop the old master*/
		appendStringInfo(&infosendmsg, " stop -D %s -m i -o -i -w -c", master_nodeinfo.nodepath);
		nodetypestr = mgr_nodetype_str(mastertype);
		ereport(LOG, (errmsg("stop the old %s \"%s\"", nodetypestr, nodenamedata.data)));

		res = mgr_ma_send_cmd(stop_cmdtype, infosendmsg.data, master_nodeinfo.nodehost, &strinfo);
		if (!res)
			ereport(WARNING, (errmsg("stop the old %s \"%s\" fail %s", nodetypestr, nodenamedata.data, strinfo.data)));
		pfree(nodetypestr);
	}

	pfree_AppendNodeInfo(master_nodeinfo);

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);

	/*delete the old master tuple in node table*/
	nodetypestr = mgr_nodetype_str(mastertype);
	ereport(LOG, (errmsg("delete the old %s \"%s\" in the node table", nodetypestr, nodenamedata.data)));
	mastertuple = SearchSysCache1(NODENODEOID, master_nodeinfo.tupleoid);
	if(HeapTupleIsValid(mastertuple))
	{
		simple_heap_delete(rel_node, &mastertuple->t_self);
		CatalogUpdateIndexes(rel_node, mastertuple);
		ReleaseSysCache(mastertuple);
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
		ereport(WARNING, (errmsg("%s \"%s\" does not running", nodetypestr, nodenamedata.data)));

	/*update the extra node: masteroid, and sync_stat*/
	switch(nodetype)
	{
		case GTM_TYPE_GTM_SLAVE:
			anothertype = GTM_TYPE_GTM_EXTRA;
			break;
		case GTM_TYPE_GTM_EXTRA:
			anothertype = GTM_TYPE_GTM_SLAVE;
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			anothertype = CNDN_TYPE_DATANODE_EXTRA;
			break;
		case CNDN_TYPE_DATANODE_EXTRA:
			anothertype = CNDN_TYPE_DATANODE_SLAVE;
			break;
		default:
			ereport(ERROR, (errmsg("unknown this type of node '%c'", nodetype)));
	};
	/* check another node exists */
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	tuple = mgr_get_tuple_node_from_name_type(rel_node, nodenamedata.data, anothertype);
	if((HeapTupleIsValid(tuple)))
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		if (strcmp(NameStr(mgr_node->nodesync), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
		{
			namestrcpy(&(mgr_node->nodesync), sync_state_tab[SYNC_STATE_SYNC].name);
		}
		mgr_node->nodemasternameoid = slave_nodeinfo.tupleoid;
		heap_inplace_update(rel_node, tuple);
		heap_freetuple(tuple);
	}
	heap_close(rel_node, RowExclusiveLock);	
	
	/*for mgr_updateparm systbl, drop the old master param, update slave parm info in the mgr_updateparm systbl*/
	ereport(LOG, (errmsg("refresh \"param\" table in ADB Manager, delete the old master parameters, and update %s \"%s\" as master type", nodetypestr, nodenamedata.data)));
	pfree(nodetypestr);
	mgr_parm_after_gtm_failover_handle(&nodenamedata, mastertype, &nodenamedata, nodetype);

	/*set new master sync*/
	resetStringInfo(&strinfo);
	mgr_get_master_sync_string(slave_nodeinfo.tupleoid, true, InvalidOid, &strinfo);

	resetStringInfo(&infosendmsg);
	if (strinfo.len == 0)
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	else
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo.data, &infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD,
							slave_nodeinfo.nodepath,
							&infosendmsg,
							slave_nodeinfo.nodehost,
							&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
		ereport(WARNING, (errmsg("refresh synchronous_standby_names of datanode master \"%s\" fail, %s", nodenamedata.data, getAgentCmdRst.description.data)));
	
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
		ereport(ERROR, (errmsg("%s \"%s\" does not running", nodetypestr, nodenamedata.data)));
	if (GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype 
			|| GTM_TYPE_GTM_MASTER == nodetype)
		cmdtype = AGT_CMD_GTM_SLAVE_FAILOVER;
	else if (CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype
					|| CNDN_TYPE_DATANODE_MASTER == nodetype)
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
	bool master_is_exist = true;
	bool master_is_running = true;
	bool cn_is_exist = false;
	bool cn_is_running = false;
	bool getrefresh = false;
	Oid cnoid;
	NameData nodenamedata;
	AppendNodeInfo master_nodeinfo;
	AppendNodeInfo cn_nodeinfo;
	ScanKeyData key[0];
	Relation rel_node;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	GetAgentCmdRst getAgentCmdRst;
	PGconn *pg_conn;

	/*get the input variable*/
	nodetype = PG_GETARG_INT32(0);
	namestrcpy(&nodenamedata, PG_GETARG_CSTRING(1));
	/*get the new master info*/
	get_nodeinfo_byname(nodenamedata.data, nodetype, &master_is_exist, &master_is_running, &master_nodeinfo);
	if (master_is_exist)
		pfree_AppendNodeInfo(master_nodeinfo);
	if (CNDN_TYPE_DATANODE_MASTER != nodetype)
	{
		ereport(ERROR, (errmsg("the type of node \"%s\" is not datanode master",nodenamedata.data)));
	}
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
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
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

	/*pause cluster*/
	mgr_lock_cluster(&pg_conn, &cnoid);
	/*refresh pgxc_node on all coordiantors*/
	initStringInfo(&(getAgentCmdRst.description));
	getrefresh = mgr_pqexec_refresh_pgxc_node(PGXC_FAILOVER, CNDN_TYPE_DATANODE_MASTER, nodenamedata.data, &getAgentCmdRst, &pg_conn, cnoid);
	if(!getrefresh)
	{
		getAgentCmdRst.ret = getrefresh;
		ereport(WARNING, (errmsg("%s", (getAgentCmdRst.description).data)));
	}
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
	bool master_is_exist = true;
	bool master_is_running = true;
	bool slave_is_exist = true;
	bool slave_is_running = true;
	bool res = false;
	bool get = false;
	NameData nodenamedata;
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

	if (nodetype == GTM_TYPE_GTM_SLAVE || nodetype == GTM_TYPE_GTM_EXTRA)
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
	get_nodeinfo_byname(nodenamedata.data, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
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
		mgr_node->nodemasternameoid = master_nodeinfo.tupleoid;
		mgr_node->nodeinited = true;
		mgr_node->nodeincluster = true;
		if (strcmp(NameStr(mgr_node->nodesync), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0
			&& (strinfo_sync.len == 0 || strcmp(strinfo_sync.len !=0 ? strinfo_sync.data:"",
				(nodetype == GTM_TYPE_GTM_SLAVE || nodetype == CNDN_TYPE_DATANODE_SLAVE)?"slave":"extra") == 0))
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
		pfree(infosendmsg.data);
		pfree(strinfo.data);
		pfree(strinfo_sync.data);
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
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
					(nodetype == GTM_TYPE_GTM_SLAVE || nodetype == CNDN_TYPE_DATANODE_SLAVE)? "slave" : "extra");

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
		if (GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
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
				appendStringInfo(&strinfo_sync, "%s",(nodetype == GTM_TYPE_GTM_SLAVE || nodetype == CNDN_TYPE_DATANODE_SLAVE)?"slave":"extra");
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo_sync.data, &infosendmsg);
			}
			else
				mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		}
		else
		{
			if (strcmp(slave_sync.data, sync_state_tab[SYNC_STATE_SYNC].name) == 0 
				|| strcmp(slave_sync.data, sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
			{
				if (strstr(strinfo_sync.data, (nodetype == GTM_TYPE_GTM_SLAVE || nodetype == CNDN_TYPE_DATANODE_SLAVE)?"slave":"extra") == NULL)
					appendStringInfo(&strinfo_sync, ",%s",(nodetype == GTM_TYPE_GTM_SLAVE || nodetype == CNDN_TYPE_DATANODE_SLAVE)?"slave":"extra");
			}
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", strinfo_sync.data, &infosendmsg);
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
	if (mgr_check_node_exist_incluster(&nodename, CNDN_TYPE_COORDINATOR_MASTER, true))
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
		if (mgr_node->nodetype == GTM_TYPE_GTM_MASTER || mgr_node->nodetype == GTM_TYPE_GTM_SLAVE 
			|| mgr_node->nodetype == GTM_TYPE_GTM_EXTRA)
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
	StringInfoData sqlsrc;
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
	Oid cnoid;

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

	if (mgr_check_node_exist_incluster(&s_nodename, CNDN_TYPE_COORDINATOR_MASTER, true))
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
	initStringInfo(&sqlsrc);

	PG_TRY();
	{
		/* lock the cluster */
		ereport(LOG, (errmsg("lock the cluster")));
		ereport(NOTICE, (errmsg("lock the cluster")));
		mgr_lock_cluster(&pg_conn, &cnoid);
		/*set xc_maintenance_mode=on  */
		res = PQexec(pg_conn, "set xc_maintenance_mode = on;");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			ereport(ERROR, (errmsg("execute \"xc_maintenance_mode=on\" on coordiantors oid=%d fail, %s"
				, cnoid, PQerrorMessage(pg_conn))));
		}
		PQclear(res);

		appendStringInfo(&sqlsrc, "CREATE NODE \"%s\" with (TYPE=COORDINATOR, HOST='%s', PORT=%d);"
			, s_coordname, dest_nodeinfo.nodeaddr, dest_nodeinfo.nodeport);
		/* check the diff xlog */
		iloop = 60;
		ereport(LOG, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iloop, m_nodename.data, s_coordname)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iloop, m_nodename.data, s_coordname)));
		resetStringInfo(&restmsg);
		appendStringInfo(&restmsg, "EXECUTE DIRECT ON (\"%s\") 'checkpoint;select now()'", m_nodename.data);
		appendStringInfo(&sqlstrmsg, "EXECUTE DIRECT ON (\"%s\") 'select pg_xlog_location_diff(pg_current_xlog_insert_location(),replay_location) < 200  from pg_stat_replication where application_name=''%s'';'"
			, m_nodename.data, s_coordname);
		while (iloop-- > 0)
		{
			/*checkponit first*/
			res = PQexec(pg_conn, restmsg.data);
			PQclear(res);
			res = NULL;
			pg_usleep(500000L);
			res = PQexec(pg_conn, sqlstrmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
				if (strcasecmp("t", PQgetvalue(res, 0, 0) != NULL ? PQgetvalue(res, 0, 0):"") == 0)
					break;
			if (iloop)
			{
				PQclear(res);
				res = NULL;
			}
			pg_usleep(500000L);
		}
		
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
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
		iloop = 60;
		ereport(LOG, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iloop, m_nodename.data, s_coordname)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check coordinator \"%s\", \"%s\" have the same xlog position"
			, iloop, m_nodename.data, s_coordname)));
		resetStringInfo(&restmsg);
		appendStringInfo(&restmsg, "EXECUTE DIRECT ON (\"%s\") 'checkpoint;select now()'", m_nodename.data);
		while (iloop-- > 0)
		{
			/*checkponit first*/
			res = PQexec(pg_conn, restmsg.data);
			PQclear(res);
			res = NULL;
			pg_usleep(500000L);
			res = PQexec(pg_conn, sqlstrmsg.data);
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
				if (strcasecmp("t", PQgetvalue(res, 0, 0) != NULL ? PQgetvalue(res, 0, 0):"") == 0)
					break;
			if (iloop)
			{
				PQclear(res);
				res = NULL;
			}
			pg_usleep(500000L);
		}
		
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
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
		tuple = mgr_get_tuple_node_from_name_type(rel_node, s_coordname, CNDN_TYPE_COORDINATOR_MASTER);
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
		pfree(sqlsrc.data);
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
	tuple = mgr_get_tuple_node_from_name_type(rel_node, s_coordname, CNDN_TYPE_COORDINATOR_MASTER);
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	mgr_node->nodeinited = true;
	mgr_node->nodeincluster = true;
	heap_inplace_update(rel_node, tuple);
	heap_freetuple(tuple);
	heap_close(rel_node, RowExclusiveLock);

	pfree(sqlsrc.data);
	pfree(sqlstrmsg.data);
	pfree(restmsg.data);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	pfree_AppendNodeInfo(dest_nodeinfo);

	/* unlock the cluster */
	ereport(NOTICE, (errmsg("unlock the cluster")));
	ereport(LOG, (errmsg("unlock the cluster")));
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
* datanode switchover, command format: switchover datanode slave|extra datanode_name
* condition: the datanode slave|extra is sync with master
*/

Datum mgr_switchover_func(PG_FUNCTION_ARGS)
{
	char nodeType;
	char *typestr;
	char *cndnPath;
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
	int iloop = 60;
	int iMax = iloop;
	HeapTuple tuple;
	HeapTuple tupResult;
	HeapTuple tupleS;
	NameData nodeNameData;
	NameData nodeTypeStrData;
	NameData oldMSyncData;
	NameData syncStateData;
	AppendNodeInfo nodeInfoS;
	AppendNodeInfo nodeInfoM;
	Form_mgr_node mgr_node;
	PGconn *pgConn;
	StringInfoData restmsg;
	StringInfoData infosendmsg;
	StringInfoData strerr;
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
	
	/* check the type */
	if (CNDN_TYPE_DATANODE_MASTER == nodeType)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("it is the datanode master, no need switchover")));
	}
	
	if (CNDN_TYPE_DATANODE_SLAVE != nodeType && CNDN_TYPE_DATANODE_EXTRA != nodeType)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			,errmsg("unknown the datanode type : %c", nodeType)));
	}
	
	/* check the slave node exist */
	namestrcpy(&nodeTypeStrData, nodeType == CNDN_TYPE_DATANODE_SLAVE ? "datanode slave":"datanode extra");
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
			ereport(ERROR, (errmsg("%s \"%s\" does not running normal", nodeTypeStrData.data, nodeNameData.data)));
		}
	}
	PG_CATCH();
	{
		pfree_AppendNodeInfo(nodeInfoS);
		PG_RE_THROW();
	}PG_END_TRY();

	/* check the datanode master */
	PG_TRY();
	{
		get_nodeinfo_byname(nodeNameData.data, CNDN_TYPE_DATANODE_MASTER, &isExistM, &isRunningM, &nodeInfoM);
		if (false == isExistM)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("datanode master \"%s\" does not exist", nodeNameData.data)));
		}
		if (false == isRunningM)
		{
			ereport(ERROR, (errmsg("datanode master \"%s\" does not running normal", nodeNameData.data)));
		}
	}
	PG_CATCH();
	{
		pfree_AppendNodeInfo(nodeInfoS);
		pfree_AppendNodeInfo(nodeInfoM);

		PG_RE_THROW();
	}PG_END_TRY();	
	
	initStringInfo(&restmsg);
	mgr_get_master_sync_string(nodeInfoM.tupleoid, true, InvalidOid, &restmsg);
	namestrcpy(&syncStateData, restmsg.len == 0 ? "''":restmsg.data);
	
	/* lock the cluster */
	mgr_lock_cluster(&pgConn, &cnOid);
	
	/* check the xlog diff */
	PG_TRY();
	{
		resetStringInfo(&restmsg);
		initStringInfo(&infosendmsg);
		initStringInfo(&(getAgentCmdRst.description));
		ereport(LOG, (errmsg("wait max %d seconds to check datanode master \"%s\", %s \"%s\" have the same xlog position"
				, iloop, nodeNameData.data,  nodeTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("wait max %d seconds to check datanode master \"%s\", %s \"%s\" have the same xlog position"
				, iloop, nodeNameData.data,  nodeTypeStrData.data, nodeNameData.data)));

		res = mgr_maxtime_check_xlog_diff(nodeType, nodeNameData.data, &nodeInfoM, iMax);
		if (res <= 0)
		{
			ereport(ERROR, (errmsg("wait max %d seconds to check datanode master \"%s\", %s \"%s\" have the same xlog position fail"
					, iMax, nodeNameData.data,  nodeTypeStrData.data, nodeNameData.data)));
		}
		
		/* stop datanode master mode i*/
		bStopOldMaster = true;
		appendStringInfo(&infosendmsg, " stop -D %s -m i -o -i -w -c", nodeInfoM.nodepath);
		res = mgr_ma_send_cmd(AGT_CMD_DN_STOP, infosendmsg.data, nodeInfoM.nodehost, &restmsg);
		if (!res)
				ereport(ERROR, (errmsg("stop datanode master \"%s\" fail %s", nodeNameData.data, restmsg.data)));

		bRefreshParam = true;
		/* set parameters the given slave node in postgresql.conf */
		resetStringInfo(&infosendmsg);
		ereport(LOG, (errmsg("on %s \"%s\", set hot_standby=off, synchronous_standby_names=''", nodeTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("on %s \"%s\", set hot_standby=off, synchronous_standby_names=''", nodeTypeStrData.data, nodeNameData.data)));
		mgr_add_parm(nodeNameData.data, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "off", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodeInfoS.nodepath, &infosendmsg
				, nodeInfoS.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetAgentCmdRst = true;
			ereport(ERROR, (errmsg("on %s \"%s\", set hot_standby=off, synchronous_standby_names='' fail, %s"
				, nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		}
		
		/* set the given slave node pg_hba.conf for streaming replication*/
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_get_hba_replication_info(nodeNameData.data, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, nodeInfoS.nodepath, &infosendmsg
				, nodeInfoS.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetAgentCmdRst = true;
			ereport(ERROR, (errmsg("on %s \"%s\", refresh pg_bha.conf fail, %s"
				, nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		}

		/* promote the given slave node */
		resetStringInfo(&restmsg);
		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, " promote -D %s -w", nodeInfoS.nodepath);
		res = mgr_ma_send_cmd(AGT_CMD_DN_FAILOVER, infosendmsg.data, nodeInfoS.nodehost, &restmsg);
		if (!res)
		{
			brestmsg = true;
			ereport(ERROR, (errmsg("promote %s \"%s\" fail, %s", nodeTypeStrData.data, nodeNameData.data, restmsg.data)));
		}
		/*check recovery finish*/
		ereport(LOG, (errmsg("waiting for the new datanode master \"%s\" can accept connections...", nodeNameData.data)));
		ereport(NOTICE, (errmsg("waiting for the new datanode master \"%s\" can accept connections...", nodeNameData.data)));
		mgr_check_node_connect(nodeType, nodeInfoS.nodehost, nodeInfoS.nodeport);

	}
	PG_CATCH();
	{
		ereport(LOG, (errmsg("rollback start:")));
		ereport(NOTICE, (errmsg("rollback start:")));

		if (bStopOldMaster)
		{
			ereport(WARNING, (errmsg("make %s \"%s\" as datanode master fail, use \"monitor all\", \"monitor ha\" to check nodes status, !!!you may need use \"rewind %s %s\" to make the original %s \"%s\" to run normal !!!",
			nodeTypeStrData.data, nodeNameData.data, nodeTypeStrData.data
			, nodeNameData.data, nodeTypeStrData.data, nodeNameData.data)));
			/* start the old master node */
			resetStringInfo(&(getAgentCmdRst.description));
			nodeRel = heap_open(NodeRelationId, AccessShareLock);
			tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data, CNDN_TYPE_DATANODE_MASTER);
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
			mgr_add_parm(nodeNameData.data, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
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
			tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data, nodeType);
			mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
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
		ereport(LOG, (errmsg("rollback end")));
		ereport(NOTICE, (errmsg("rollback end")));
		PG_RE_THROW();
	}PG_END_TRY();
	
	pfree(restmsg.data);

	initStringInfo(&strerr);
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
	/*unlock cluster*/
	mgr_unlock_cluster(&pgConn);

	PG_TRY();
	{
		ereport(LOG, (errmsg("exchange the node type for datanode master \"%s\" and %s \"%s\" in node table", nodeNameData.data, nodeTypeStrData.data, nodeNameData.data)));
		ereport(NOTICE, (errmsg("exchange the node type for datanode master \"%s\" and %s \"%s\" in node table", nodeNameData.data, nodeTypeStrData.data, nodeNameData.data)));
		
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
			ereport(ERROR, (errmsg("get original datanode master \"%s\" tuple information in node table error",  nodeNameData.data)));
		}

		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		namestrcpy(&oldMSyncData, NameStr(mgr_node->nodesync));
		mgr_node->nodetype = CNDN_TYPE_DATANODE_MASTER;
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

		ereport(WARNING, (errmsg("exchange the node type for datanode master \"%s\" and %s \"%s\" in node table fail, use \"monitor all\", \"monitor ha\" to check nodes status, if you need restore the original %s \"%s\", 1. restore the original datanode master information in pgxc_node on all coordinators, 2.check the original datanode master is running normal, use the command \"rewind %s %s\" !!!",  nodeNameData.data, nodeTypeStrData.data, nodeNameData.data, nodeTypeStrData.data, nodeNameData.data, nodeTypeStrData.data, nodeNameData.data)));

		/* set parameters the given master node in postgresql.conf */
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		ereport(LOG, (errmsg("on original datanode master \"%s\", set hot_standby=off, synchronous_standby_names=%s", nodeNameData.data, syncStateData.data)));
		ereport(NOTICE, (errmsg("on original datanode master \"%s\", set hot_standby=off, synchronous_standby_names=%s", nodeNameData.data, syncStateData.data)));
		mgr_append_pgconf_paras_str_str("hot_standby", "off", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", syncStateData.data, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodeInfoM.nodepath, &infosendmsg
				, nodeInfoM.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetAgentCmdRst = true;
			ereport(WARNING, (errmsg("on original datanode master \"%s\", set hot_standby=off fail, %s", nodeNameData.data, getAgentCmdRst.description.data)));
		}

		/* rm old master recovery.conf */
		resetStringInfo(&infosendmsg);
		resetStringInfo(&restmsg);
		appendStringInfo(&infosendmsg, "%s/recovery.conf", nodeInfoM.nodepath);
		res = mgr_ma_send_cmd(AGT_CMD_RM, infosendmsg.data, nodeInfoM.nodehost, &restmsg);

		/*restart the old master node*/
		resetStringInfo(&(getAgentCmdRst.description));
		nodeRel = heap_open(NodeRelationId, AccessShareLock);
		tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data, CNDN_TYPE_DATANODE_MASTER);
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
		heap_freetuple(tuple);
		heap_close(nodeRel, AccessShareLock);
		if(!getAgentCmdRst.ret)
		{
			ereport(WARNING, (errmsg("restart original datanode master \"%s\" fail, %s", nodeNameData.data, getAgentCmdRst.description.data)));
		}
		
		pfree(strerr.data);
		pfree(infosendmsg.data);
		pfree(getAgentCmdRst.description.data);
		pfree_AppendNodeInfo(nodeInfoS);
		pfree_AppendNodeInfo(nodeInfoM);

		ereport(LOG, (errmsg("rollback end")));
		ereport(NOTICE, (errmsg("rollback end")));
		PG_RE_THROW();
	}PG_END_TRY();

	/* update new slave postgresql.conf */
	resetStringInfo(&infosendmsg);
	resetStringInfo(&(getAgentCmdRst.description));
	ereport(LOG, (errmsg("on new %s \"%s\", set hot_standby=on, synchronous_standby_names=''", nodeTypeStrData.data, nodeNameData.data)));
	ereport(NOTICE, (errmsg("on new %s \"%s\", set hot_standby=on, synchronous_standby_names=''", nodeTypeStrData.data, nodeNameData.data)));
	mgr_add_parm(nodeNameData.data, nodeType, &infosendmsg);
	mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodeInfoM.nodepath, &infosendmsg
			, nodeInfoM.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("on new %s \"%s\", set hot_standby=on, synchronous_standby_names='' fail, %s"
			, nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "on new %s \"%s\", set hot_standby=on, synchronous_standby_names='' fail, %s\n", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data);
	}	
	
	/* update new slave recovery.conf */
	ereport(LOG, (errmsg("on new %s \"%s\" refresh recovery.conf", nodeTypeStrData.data, nodeNameData.data)));
	ereport(NOTICE, (errmsg("on new %s \"%s\" refresh recovery.conf", nodeTypeStrData.data, nodeNameData.data)));
	resetStringInfo(&(getAgentCmdRst.description));
	resetStringInfo(&infosendmsg);
	mgr_add_parameters_recoveryconf(nodeType, nodeType == CNDN_TYPE_DATANODE_SLAVE ? "slave":"extra", nodeInfoS.tupleoid, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, nodeInfoM.nodepath, &infosendmsg, nodeInfoM.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("on new %s \"%s\", refresh recovery.conf fail, %s"
			, nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "on new %s \"%s\", refresh recovery.conf fail, %s\n", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data);
	}
	
	/* start the new slave node */
	resetStringInfo(&(getAgentCmdRst.description));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(nodeRel, nodeNameData.data, nodeType);
	mgr_runmode_cndn_get_result(AGT_CMD_DN_START, &getAgentCmdRst, nodeRel, tuple, TAKEPLAPARM_N);
	heap_freetuple(tuple);
	heap_close(nodeRel, AccessShareLock);

	if(!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("start new %s \"%s\" fail, %s", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "start new %s \"%s\" fail, %s\n", nodeTypeStrData.data, nodeNameData.data, getAgentCmdRst.description.data);
	}

	/* for other slave or extra */
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
		,NameGetDatum(&nodeNameData));
	nodeRel = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_node->nodemasternameoid != nodeInfoM.tupleoid)
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
		mgr_add_parameters_recoveryconf(mgr_node->nodetype, mgr_node->nodetype == CNDN_TYPE_DATANODE_SLAVE ? "slave":"extra", nodeInfoS.tupleoid, &infosendmsg);
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
			ereport(WARNING, (errmsg("you should use \"monitor all\", \"monitor ha\" to check the node \"%s\" status, modify the mastername of datanode slave or datanode extra \"%s\" in node table"
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
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, nodeRel, tuple, SHUTDOWN_F);
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
	
	/* set new datanode master synchronous_standby_names */
	resetStringInfo(&infosendmsg);
	resetStringInfo(&(getAgentCmdRst.description));

	ereport(LOG, (errmsg("on new datanode master \"%s\", set synchronous_standby_names=%s", nodeNameData.data, syncStateData.data)));
	ereport(NOTICE, (errmsg("on new datanode master \"%s\", set synchronous_standby_names=%s", nodeNameData.data, syncStateData.data)));

	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", syncStateData.data, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodeInfoS.nodepath, &infosendmsg
			, nodeInfoS.nodehost, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		rest = false;
		ereport(WARNING, (errmsg("on new datanode master \"%s\", set synchronous_standby_names=%s fail %s", nodeNameData.data, syncStateData.data, getAgentCmdRst.description.data)));
		appendStringInfo(&strerr, "on new datanode master \"%s\", set synchronous_standby_names=%s fail %s", nodeNameData.data, syncStateData.data, getAgentCmdRst.description.data);
	}

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


static void mgr_get_hba_replication_info(char *nodename, StringInfo infosendmsg)
{
	ScanKeyData key[3];
	Relation nodeRel;
	HeapScanDesc relScan;
	HeapTuple tuple;
	NameData nodeNameData;
	Form_mgr_node mgr_node;
	char *hostAddr;
	char *userName;
	
	Assert(nodename);

	namestrcpy(&nodeNameData, nodename);
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
		,NameGetDatum(&nodeNameData));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(nodeRel, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		userName = get_hostuser_from_hostoid(mgr_node->nodehost);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", userName, hostAddr, 32, "trust", infosendmsg);
		pfree(hostAddr);
		pfree(userName);
	}
	
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);
}


static bool mgr_maxtime_check_xlog_diff(const char nodeType, const char *nodeName, AppendNodeInfo *nodeInfoM, const int maxSecond)
{
	char *appName;
	int iloop = 0;
	int agentPortM;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	Form_mgr_host mgr_host;
	HeapTuple hostTupleM;
	
	Assert(CNDN_TYPE_DATANODE_SLAVE == nodeType || CNDN_TYPE_DATANODE_EXTRA == nodeType || GTM_TYPE_GTM_SLAVE == nodeType || GTM_TYPE_GTM_EXTRA == nodeType);
	Assert(nodeName);
	Assert(nodeInfoM);

	if (CNDN_TYPE_DATANODE_SLAVE == nodeType || GTM_TYPE_GTM_SLAVE == nodeType)
		appName = "slave";
	else
		appName = "extra";
	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);
	appendStringInfo(&infosendmsg, "select pg_xlog_location_diff(pg_current_xlog_insert_location(),replay_location) < 200  from pg_stat_replication where application_name='%s';", appName);
	
	hostTupleM = SearchSysCache1(HOSTHOSTOID, nodeInfoM->nodehost);
	if(!(HeapTupleIsValid(hostTupleM)))
	{
		ereport(ERROR, (errmsg("get the datanode master \"%s\" information in node table fail", nodeName)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(hostTupleM);
	Assert(mgr_host);
	agentPortM = mgr_host->hostagentport;
	ReleaseSysCache(hostTupleM);	
	
	iloop = maxSecond;
	while (iloop-- > 0)
	{
		/*checkponit first*/
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPortM, "checkpoint;select now();"
			, nodeInfoM->nodeusername, nodeInfoM->nodeaddr, nodeInfoM->nodeport, DEFAULT_DB, &restmsg);
		pg_usleep(500000L);
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPortM, infosendmsg.data
			, nodeInfoM->nodeusername, nodeInfoM->nodeaddr, nodeInfoM->nodeport, DEFAULT_DB, &restmsg);
		if (restmsg.len != 0)
		{
			if (strcmp(restmsg.data, "t") == 0)
				break;
		}
		pg_usleep(500000L);
	}
	
	pfree(infosendmsg.data);
	pfree(restmsg.data);
	
	if (iloop <= 0)
		return false;
	else
		return true;
}
