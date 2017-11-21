/*
 * commands of zone
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
#include "access/xlog.h"
#include "nodes/nodes.h"


char *mgr_zone;

static bool mgr_zone_modify_conf_agtm_host_port(const char *zone);
static bool mgr_zone_has_node(const char *zonename, char nodetype);
/*
* mgr_zone_promote
* make one zone as sub-center, all of nodes in sub-center as slave nodes, when we promote the sub-center to
* master conter, we should promote the nodes to master which whose master nodename not in the given zone
*/
Datum mgr_zone_promote(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapTuple tuple;
	HeapTuple tupResult;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	Datum datumPath;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData resultmsg;
	ScanKeyData key[2];
	NameData name;
	bool isNull;
	bool bres = true;
	char *nodePath;
	char *hostAddr;
	char *userName;
	char *currentZone;
	char nodePortBuf[10];
	int i = 0;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);

	if (strcmp(currentZone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));
	
	ereport(LOG, (errmsg("in zone \"%s\"", currentZone)));
	ereport(NOTICE, (errmsg("in zone \"%s\"", currentZone)));
	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);
	initStringInfo(&resultmsg);

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(currentZone));

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 2, key);
	PG_TRY();
	{
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/* check the node's master in the same zone */
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			/* change slave type to master */
			mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
			heap_inplace_update(relNode, tuple);
			/* get node path */
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull)
			{
				heap_endscan(relScan);
				heap_close(relNode, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column nodepath is null")));
			}
			nodePath = TextDatumGetCString(datumPath);
			resetStringInfo(&infosendmsg);
			resetStringInfo(&restmsg);
			appendStringInfo(&infosendmsg, " promote -D %s", nodePath);
			mgr_ma_send_cmd_get_original_result(AGT_CMD_DN_FAILOVER, infosendmsg.data, mgr_node->nodehost, &restmsg, true);
			if (restmsg.len != 0 && strcmp("server promoting\n", restmsg.data) == 0)
			{
				ereport(LOG, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.data)));
			}
			else
			{
				bres = false;
				ereport(WARNING, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.len != 0 ? restmsg.data:"fail")));
				appendStringInfo(&resultmsg, "promote \"%s\" fail %s\n", NameStr(mgr_node->nodename), restmsg.data);
			}
		}
		
		/* check the node has allow connect */
		heap_endscan(relScan);
		relScan = heap_beginscan(relNode, SnapshotNow, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/* check the node's master in the same zone */
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			i = 0;
			while(i++<3)
			{
				if (mgr_check_node_recovery_finish(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, hostAddr))
					break;
				pg_usleep(1 * 1000000L);
			}
			if (mgr_check_node_recovery_finish(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, hostAddr))
			{
				memset(nodePortBuf, 0, 10);
				sprintf(nodePortBuf, "%d", mgr_node->nodeport);
				if (mgr_node->nodetype != GTM_TYPE_GTM_MASTER && mgr_node->nodetype != GTM_TYPE_GTM_SLAVE)
					userName = get_hostuser_from_hostoid(mgr_node->nodehost);
				else
					userName = NULL;
				i = 0;
				while(i++<3)
				{
					if (pingNode_user(hostAddr, nodePortBuf, userName == NULL ? AGTM_USER : userName) == 0)
						break;
					pg_usleep(1 * 1000000L);
				}
				
				if (pingNode_user(hostAddr, nodePortBuf, userName == NULL ? AGTM_USER : userName) != 0)
				{
					bres = false;
					ereport(WARNING, (errmsg("the node \"%s\" is master type, but not running normal", NameStr(mgr_node->nodename))));
					appendStringInfo(&resultmsg, "the node \"%s\" is master type, but not running normal\n", NameStr(mgr_node->nodename));
				}
			}
			else
			{
				bres = false;
				ereport(WARNING, (errmsg("the node \"%s\" is not master type, execute \"select pg_is_in_recovery()\" on the node to check", NameStr(mgr_node->nodename))));
				appendStringInfo(&resultmsg, "the node \"%s\" is not master type, execute \"select pg_is_in_recovery()\" on the node to check\n", NameStr(mgr_node->nodename));
			}
			
			pfree(hostAddr);
		}
	}PG_CATCH();
	{
		pfree(infosendmsg.data);
		pfree(restmsg.data);
		pfree(resultmsg.data);	
		PG_RE_THROW();
	}PG_END_TRY();

	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);

	pfree(infosendmsg.data);
	pfree(restmsg.data);

	ereport(LOG, (errmsg("the command of \"ZONE PROMOTE %s\" result is %s, description is %s", currentZone
		, bres ? "true":"false", resultmsg.len == 0 ? "success":resultmsg.data)));

	namestrcpy(&name, "promote master node");
	tupResult = build_common_command_tuple(&name, bres, resultmsg.len == 0 ? "success":resultmsg.data);
	pfree(resultmsg.data);
	
	return HeapTupleGetDatum(tupResult);
}

/*
* mgr_zone_config_all
*
* refresh pgxc_node on all coordinators in the given zone 
*
*/
Datum mgr_zone_config_all(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapTuple tuple;
	HeapTuple masterTuple;
	HeapTuple tupResult;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodeM;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData resultmsg;
	ScanKeyData key[3];
	NameData name;
	char *hostAddr;
	char *userName;
	char *hostAddress;
	char *currentZone;
	char *pgxc_reload = "select pgxc_pool_reload()";
	int i = 0;
	int agentPort;
	bool bres = true;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);

	if (strcmp(currentZone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));
	if (!mgr_zone_has_node(currentZone, GTM_TYPE_GTM_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not GTM MASTER in cluster", currentZone)));
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_COORDINATOR_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not COORDINATOR MASTER in cluster", currentZone)));
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_DATANODE_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not DATANODE MASTER in cluster", currentZone)));

	ereport(LOG, (errmsg("in zone \"%s\"", currentZone)));
	ereport(NOTICE, (errmsg("in zone \"%s\"", currentZone)));

	/* refresh agtm_host, agtm_port */
	ereport(LOG, (errmsg("refresh agtm_host, agtm_port in zone \"%s\"", currentZone)));
	ereport(NOTICE, (errmsg("refresh agtm_host, agtm_port in zone \"%s\"", currentZone)));
	if (!mgr_zone_modify_conf_agtm_host_port(currentZone))
		ereport(ERROR, (errmsg("refresh agtm_host, agtm_port in zone \"%s\" fail", currentZone)));

	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);
	initStringInfo(&resultmsg);

	ereport(LOG, (errmsg("refresh pgxc_node on all coordinators in zone \"%s\"", currentZone)));
	ereport(NOTICE, (errmsg("refresh pgxc_node on all coordinators in zone \"%s\"", currentZone)));
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(currentZone));

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 2, key);
	PG_TRY();
	{
		/* get the sql string which used to refresh pgxc_node table */
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/* check the node's master in the same zone */
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);

			if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype)
				appendStringInfo(&infosendmsg, "select pg_alter_node('%s', '%s', '%s', %d, false);"
				,NameStr(mgr_node->nodename), NameStr(mgr_node->nodename), hostAddr, mgr_node->nodeport);
			else if (CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype)
			{
				masterTuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
				if(!HeapTupleIsValid(masterTuple))
				{
					heap_endscan(relScan);
					heap_close(relNode, RowExclusiveLock);
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\"", NameStr(mgr_node->nodename), currentZone)));
				}
				mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
				appendStringInfo(&infosendmsg, "select pg_alter_node('%s', '%s', '%s', %d, false);"
					,NameStr(mgr_nodeM->nodename), NameStr(mgr_node->nodename), hostAddr, mgr_node->nodeport);
				ReleaseSysCache(masterTuple);
			}
			pfree(hostAddr);
		}

		/* refresh pgxc_node on all coordinators */
		heap_endscan(relScan);
		ScanKeyInit(&key[0]
					,Anum_mgr_node_nodeincluster
					,BTEqualStrategyNumber
					,F_BOOLEQ
					,BoolGetDatum(true));
		ScanKeyInit(&key[1]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(currentZone));	
		ScanKeyInit(&key[2]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
		relScan = heap_beginscan(relNode, SnapshotNow, 3, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/* check the node's master in the same zone */
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\" in zone \"%s\"", NameStr(mgr_node->nodename), infosendmsg.data, currentZone)));
			ereport(NOTICE, (errmsg("on coordinator \"%s\" execute \"%s\" in zone \"%s\"", NameStr(mgr_node->nodename), infosendmsg.data, currentZone)));
			i = 0;
			userName = get_hostuser_from_hostoid(mgr_node->nodehost);
			hostAddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
			agentPort = get_agentPort_from_hostoid(mgr_node->nodehost);
			while(i++<3)
			{
				resetStringInfo(&restmsg);
				monitor_get_stringvalues(AGT_CMD_GET_EXPLAIN_STRINGVALUES, agentPort, infosendmsg.data, userName, hostAddress, mgr_node->nodeport, AGTM_DBNAME, &restmsg);
				if ((restmsg.len != 0) && (strcasecmp(restmsg.data, "t\n") ==0))
					break;
			}
			pfree(userName);
			pfree(hostAddress);

			if ((restmsg.len == 0) || ((restmsg.len !=0) && (strcasecmp(restmsg.data, "t\n") !=0)))
			{
				bres = false;
				ereport(WARNING, (errmsg("on coordinator %s execute %s fail %s in zone \"%s\"", NameStr(mgr_node->nodename), infosendmsg.data, restmsg.len == 0 ? "":restmsg.data, currentZone)));
				appendStringInfo(&resultmsg, "on coordinator %s execute %s fail %s in zone \"%s\"\n", NameStr(mgr_node->nodename), infosendmsg.data, restmsg.len == 0 ? "":restmsg.data, currentZone);
			}
				
		}

		/* select pgxc_pool_reload() */
		heap_endscan(relScan);
		relScan = heap_beginscan(relNode, SnapshotNow, 3, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/* check the node's master in the same zone */
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;	
			ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\" in zone \"%s\"", NameStr(mgr_node->nodename), pgxc_reload, currentZone)));
			ereport(NOTICE, (errmsg("on coordinator \"%s\" execute \"%s\" in zone \"%s\"", NameStr(mgr_node->nodename), pgxc_reload, currentZone)));
			i = 0;
			userName = get_hostuser_from_hostoid(mgr_node->nodehost);
			hostAddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
			agentPort = get_agentPort_from_hostoid(mgr_node->nodehost);
			while(i++<3)
			{
				resetStringInfo(&restmsg);
				monitor_get_stringvalues(AGT_CMD_GET_EXPLAIN_STRINGVALUES, agentPort, pgxc_reload, userName, hostAddress, mgr_node->nodeport, AGTM_DBNAME, &restmsg);
				if ((restmsg.len != 0) && (strcasecmp(restmsg.data, "t\n") ==0))
					break;
			}
			pfree(userName);
			pfree(hostAddress);

			if ((restmsg.len == 0) || ((restmsg.len !=0) && (strcasecmp(restmsg.data, "t\n") !=0)))
			{
				bres = false;
				ereport(WARNING, (errmsg("on coordinator %s execute %s fail %s in zone \"%s\"", NameStr(mgr_node->nodename), pgxc_reload, restmsg.len == 0 ? "":restmsg.data, currentZone)));
				appendStringInfo(&resultmsg, "on coordinator %s execute %s fail %s in zone \"%s\"\n", NameStr(mgr_node->nodename), pgxc_reload, restmsg.len == 0 ? "":restmsg.data, currentZone);
			}
		}
		
		/* clean the node info which not in the given zone */
		if (bres)
		{
				heap_endscan(relScan);
				ereport(LOG, (errmsg("make the node of master type clear: the column mastername is null, sync_stat is null in zone \"%s\"", currentZone)));
				ereport(NOTICE, (errmsg("make the node of master type clear: the column mastername is null, sync_stat is null in zone \"%s\"", currentZone)));
				/* change coordinator's mastername and sync_stat */
				ScanKeyInit(&key[0]
					,Anum_mgr_node_nodezone
					,BTEqualStrategyNumber
					,F_NAMEEQ
					,CStringGetDatum(currentZone));
				relScan = heap_beginscan(relNode, SnapshotNow, 1, key);
				while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
				{
					mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
					Assert(mgr_node);
					if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype || CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype
							|| GTM_TYPE_GTM_MASTER == mgr_node->nodetype)
					{
						namestrcpy(&(mgr_node->nodesync), "");
						mgr_node->nodemasternameoid = 0;
						heap_inplace_update(relNode, tuple);
					}
				}
				
				heap_endscan(relScan);
				ereport(LOG, (errmsg("on node table, drop the node which is not in zone \"%s\"", currentZone)));
				ereport(NOTICE, (errmsg("on node table, drop the node which is not in zone \"%s\"", currentZone)));
				relScan = heap_beginscan(relNode, SnapshotNow, 0, NULL);
				while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
				{
					mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
					Assert(mgr_node);
					if (strcasecmp(NameStr(mgr_node->nodezone), currentZone) == 0)
						continue;
					simple_heap_delete(relNode, &(tuple->t_self));
					CatalogUpdateIndexes(relNode, tuple);
				}
		}
	}PG_CATCH();
	{
		pfree(infosendmsg.data);
		pfree(restmsg.data);
		pfree(resultmsg.data);	
		PG_RE_THROW();
	}PG_END_TRY();

	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);
	
	pfree(infosendmsg.data);
	pfree(restmsg.data);

	ereport(LOG, (errmsg("the command of \"ZONE CONFIG %s\" result is %s, description is %s", currentZone
		,bres ? "true":"false", resultmsg.len == 0 ? "success":resultmsg.data)));
	namestrcpy(&name, "config all");
	tupResult = build_common_command_tuple(&name, bres, resultmsg.len == 0 ? "success":resultmsg.data);
	pfree(resultmsg.data);
	
	return HeapTupleGetDatum(tupResult);
}

/*
* mgr_checknode_in_currentzone
* 
* check given tuple oid, if tuple is in the current zone return true, else return false;
*
*/
bool mgr_checknode_in_currentzone(const char *zone, const Oid TupleOid)
{
	Relation relNode;
	HeapTuple tuple;
	HeapScanDesc relScan;
	ScanKeyData key[1];
	bool res = false;
	
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));

	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 1, key);		
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		if (TupleOid == HeapTupleGetOid(tuple))
		{
			res = true;
			break;
		}
		
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);	
	
	return res;
}

/*
* mgr_zone_modify_conf_agtm_host_port
* 
* refresh agtm_port, agtm_host in all the node in zone
*
*/
static bool mgr_zone_modify_conf_agtm_host_port(const char *zone)
{
	Relation relNode;
	HeapTuple tuple;
	HeapScanDesc relScan;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumPath;
	ScanKeyData key[2];
	Form_mgr_node mgr_node;
	bool isNull;
	bool res = true;
	char *cndnPath;

	initStringInfo(&infosendmsg);
	/* get agtm_host, agtm_port */
	mgr_get_gtm_host_port(&infosendmsg);
	
	initStringInfo(&(getAgentCmdRst.description));
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype)
			continue;
		resetStringInfo(&(getAgentCmdRst.description));
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
		if(isNull)
		{
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			heap_endscan(relScan);
			heap_close(relNode, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		cndnPath = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			res = false;
			ereport(WARNING, (errmsg("on %s set agtm_host, agtm_port fail %s", NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
		}
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	
	return res;
}

/*
* mgr_get_nodetuple_by_name_zone
*
* get the tuple of node according to nodename and zone
*/
HeapTuple mgr_get_nodetuple_by_name_zone(Relation rel, char *nodename, char *nodezone)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	HeapTuple tupleret = NULL;
	NameData nodenamedata;
	NameData nodezonedata;

	Assert(nodename);
	namestrcpy(&nodenamedata, nodename);
	namestrcpy(&nodezonedata, nodezone);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&nodenamedata));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodezonedata));
	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	tupleret = heap_copytuple(tuple);
	heap_endscan(rel_scan);
	return tupleret;
}


/*
* mgr_zone_clear
*
* clear the tuple which is not in the current zone
*/

Datum mgr_zone_clear(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapScanDesc relScan;
	ScanKeyData key[2];
	HeapTuple tuple =NULL;
	Form_mgr_node mgr_node;
	char *zone;
	char *nodetypestr;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	zone  = PG_GETARG_CSTRING(0);
	Assert(zone);
	
	if (strcmp(zone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", zone, mgr_zone)));

	ereport(LOG, (errmsg("make the special node as master type and set its master name is null, sync_state is null on node table in zone \"%s\"", zone)));
	ereport(NOTICE, (errmsg("make the special node as master type and set its master name is null, sync_state is null on node table in zone \"%s\"", zone)));
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(zone));

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		/* check the node's master in the same zone */
		if (mgr_checknode_in_currentzone(zone, mgr_node->nodemasternameoid))
			continue;
		ereport(LOG, (errmsg("make the node \"%s\" as master type on node table in zone \"%s\"", NameStr(mgr_node->nodename), zone)));
		ereport(NOTICE, (errmsg("make the node \"%s\" as master type on node table in zone \"%s\"", NameStr(mgr_node->nodename), zone)));
		/* change slave type to master */
		mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
		namestrcpy(&(mgr_node->nodesync), "");
		mgr_node->nodemasternameoid = 0;
		heap_inplace_update(relNode, tuple);
	}
	heap_endscan(relScan);

	ereport(LOG, (errmsg("on node table, drop the node which is not in zone \"%s\"", zone)));
	ereport(NOTICE, (errmsg("on node table, drop the node which is not in zone \"%s\"", zone)));
	relScan = heap_beginscan(relNode, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (strcmp(NameStr(mgr_node->nodezone), zone) == 0)
			continue;
		nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
		ereport(LOG, (errmsg("drop %s \"%s\" on node table in zone \"%s\"", nodetypestr, NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone))));
		ereport(NOTICE, (errmsg("drop %s \"%s\" on node table in zone \"%s\"", nodetypestr, NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone))));
		pfree(nodetypestr);
		simple_heap_delete(relNode, &(tuple->t_self));
		CatalogUpdateIndexes(relNode, tuple);
	}

	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}

/*
* mgr_node_has_slave_inzone
*
* check the oid has been used by slave in given zone
*/
bool mgr_node_has_slave_inzone(Relation rel, char *zone, Oid mastertupleoid)
{
	ScanKeyData key[2];
	HeapTuple tuple;
	HeapScanDesc scan;

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameOid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupleoid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));
	scan = heap_beginscan(rel, SnapshotNow, 2, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		heap_endscan(scan);
		return true;
	}
	heap_endscan(scan);
	return false;
}

/*
* mgr_zone_has_node
*
* check the zone has given the type of node in cluster
*/

static bool mgr_zone_has_node(const char *zonename, char nodetype)
{
	bool bres = false;
	Relation relNode;
	HeapScanDesc relScan;
	ScanKeyData key[3];
	HeapTuple tuple =NULL;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(zonename));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{	
		bres = true;
		break;
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);
	
	return bres;
}

