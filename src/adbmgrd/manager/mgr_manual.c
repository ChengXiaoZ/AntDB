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
			ereport(ERROR, (errmsg("coordinator \"%s\" does not running normal", NameStr(mgr_node->nodename))));
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
