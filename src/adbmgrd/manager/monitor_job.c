/*
 * monitor_job.c
 *
 * ADB Integrated Monitor Daemon
 *
 * The ADB monitor dynamic item, uses two catalog table to record the job content:
 * job table and job table. Jobitem table used to record monitor item name,
 * batch absoulte path with filename and its description. Job table used to record 
 * jobname, next_runtime, interval, status, command(SQL format) and description.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2017 ADB Development Group
 *
 * IDENTIFICATION
 *	  src/adbmgrd/manager/monitor_job.c
 */

#include "postgres.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/skey.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "lib/ilist.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/adbmonitor.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "access/heapam.h"
#include "catalog/monitor_jobitem.h"
#include "catalog/monitor_job.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "parser/mgr_node.h"
#include "mgr/mgr_cmds.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "utils/formatting.h"
#include "postmaster/adbmonitor.h"
#include "mgr/mgr_msg_type.h"
#include "executor/spi.h"
#include "funcapi.h"


/*
* GUC parameters
*/
int	adbmonitor_naptime;

static HeapTuple montiot_job_get_item_tuple(Relation rel_job, Name jobname);
static bool mgr_element_in_array(Oid tupleOid, int array[], int count);
static int64 mgr_database_run_time(char *database);

/*
* ADD ITEM jobname(jobname, filepath, desc)
*/
void monitor_job_add(MonitorJobAdd *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall3(monitor_job_add_func,
									BoolGetDatum(node->if_not_exists),
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

Datum monitor_job_add_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple newtuple;
	HeapTuple checktuple;
	ListCell *lc;
	DefElem *def;
	NameData jobnamedata;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	bool if_not_exists = false;
	char *str;
	char *jobname;
	List *options;
	int32 interval;
	bool status;
	Datum datumtime;
	TimestampTz current_time = 0;

	if_not_exists = PG_GETARG_BOOL(0);
	jobname = PG_GETARG_CSTRING(1);
	options = (List *)PG_GETARG_POINTER(2);

	Assert(jobname);
	namestrcpy(&jobnamedata, jobname);
	rel = heap_open(MjobRelationId, AccessShareLock);
	/* check exists */
	checktuple = montiot_job_get_item_tuple(rel, &jobnamedata);
	if (HeapTupleIsValid(checktuple))
	{
		heap_freetuple(checktuple);
		if(if_not_exists)
		{
			ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("\"%s\" already exists, skipping", jobname)));
			PG_RETURN_BOOL(false);
		}
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("\"%s\" already exists", jobname)));
	}
	heap_close(rel, AccessShareLock);
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_monitor_job_name-1] = NameGetDatum(&jobnamedata);
	foreach(lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));

		if (strcmp(def->defname, "nexttime") == 0)
		{
			if(got[Anum_monitor_job_nexttime-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));

			str = defGetString(def);
			datumtime = DirectFunctionCall2(to_timestamp,
																			 PointerGetDatum(cstring_to_text(str)),
																			 PointerGetDatum(cstring_to_text("yyyy-mm-dd hh24:mi:ss")));
			datum[Anum_monitor_job_nexttime-1] = datumtime;
			got[Anum_monitor_job_nexttime-1] = true;
		}
		else if (strcmp(def->defname, "interval") == 0)
		{
			if(got[Anum_monitor_job_interval-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			interval = defGetInt32(def);
			if (interval <= 0)
				ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
					,errmsg("interval is out of range 1 ~ %d", INT_MAX)));
			datum[Anum_monitor_job_interval-1] = Int32GetDatum(interval);
			got[Anum_monitor_job_interval-1] = true;
		}
		else if (strcmp(def->defname, "status") == 0)
		{
			if(got[Anum_monitor_job_status-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			status = defGetBoolean(def);
			datum[Anum_monitor_job_status-1] = BoolGetDatum(status);
			got[Anum_monitor_job_status-1] = true;
		}
		else if (strcmp(def->defname, "command") == 0)
		{
			if(got[Anum_monitor_job_command-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_command-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_command-1] = true;
		}
		else if (strcmp(def->defname, "desc") == 0)
		{
			if(got[Anum_monitor_job_desc-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_desc-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_desc-1] = true;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is nexttime, interval, status, command, desc")));
		}
	}
	/* if not give, set to default */
	if (false == got[Anum_monitor_job_nexttime-1])
	{
		current_time = GetCurrentTimestamp();
		datum[Anum_monitor_job_nexttime-1] = TimestampTzGetDatum(current_time);;
	}
	if (false == got[Anum_monitor_job_interval-1])
	{
		datum[Anum_monitor_job_interval-1] = Int32GetDatum(adbmonitor_naptime);
	}
	if (false == got[Anum_monitor_job_status-1])
	{
		datum[Anum_monitor_job_status-1] = BoolGetDatum(true);
	}
	if (false == got[Anum_monitor_job_command-1])
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"command\" must be given")));
	}
	if (false == got[Anum_monitor_job_desc-1])
	{
		datum[Anum_monitor_job_desc-1] = PointerGetDatum(cstring_to_text(""));
	}
	/* now, we can insert record */
	rel = heap_open(MjobRelationId, RowExclusiveLock);
	newtuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	simple_heap_insert(rel, newtuple);
	CatalogUpdateIndexes(rel, newtuple);
	heap_freetuple(newtuple);
	/*close relation */
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}


void monitor_job_alter(MonitorJobAlter *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall2(monitor_job_alter_func,
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

Datum monitor_job_alter_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple newtuple;
	HeapTuple checktuple;
	ListCell *lc;
	DefElem *def;
	NameData jobnamedata;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	char *str;
	char *jobname;
	List *options;
	TupleDesc job_dsc;
	int32 interval;
	bool status;
	Datum datumtime;

	jobname = PG_GETARG_CSTRING(0);
	options = (List *)PG_GETARG_POINTER(1);

	Assert(jobname);
	namestrcpy(&jobnamedata, jobname);
	rel = heap_open(MjobRelationId, RowExclusiveLock);
	/* check exists */
	checktuple = montiot_job_get_item_tuple(rel, &jobnamedata);
	if (!HeapTupleIsValid(checktuple))
	{
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("\"%s\" does not exist", jobname)));
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_monitor_job_name-1] = NameGetDatum(&jobnamedata);
	foreach(lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));

		if (strcmp(def->defname, "nexttime") == 0)
		{
			if(got[Anum_monitor_job_name-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));

			str = defGetString(def);
			datumtime = DirectFunctionCall2(to_timestamp,
																			 PointerGetDatum(cstring_to_text(str)),
																			 PointerGetDatum(cstring_to_text("yyyy-mm-dd hh24:mi:ss")));
			datum[Anum_monitor_job_nexttime-1] = datumtime;
			got[Anum_monitor_job_nexttime-1] = true;
		}
		else if (strcmp(def->defname, "interval") == 0)
		{
			if(got[Anum_monitor_job_interval-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			interval = defGetInt32(def);
			if (interval <= 0)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("interval is out of range 1 ~ %d", INT_MAX)));
			datum[Anum_monitor_job_interval-1] = Int32GetDatum(interval);
			got[Anum_monitor_job_interval-1] = true;
		}
		else if (strcmp(def->defname, "status") == 0)
		{
			if(got[Anum_monitor_job_status-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			status = defGetBoolean(def);
			datum[Anum_monitor_job_status-1] = BoolGetDatum(status);
			got[Anum_monitor_job_status-1] = true;
		}
		else if (strcmp(def->defname, "command") == 0)
		{
			if(got[Anum_monitor_job_command-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_command-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_command-1] = true;
		}
		else if (strcmp(def->defname, "desc") == 0)
		{
			if(got[Anum_monitor_job_desc-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_desc-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_desc-1] = true;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is nexttime, interval, status, command, desc")));
		}
	}
	job_dsc = RelationGetDescr(rel);
	newtuple = heap_modify_tuple(checktuple, job_dsc, datum,isnull, got);
	simple_heap_update(rel, &checktuple->t_self, newtuple);
	CatalogUpdateIndexes(rel, newtuple);	
		
	heap_freetuple(checktuple);
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}


void monitor_job_drop(MonitorJobDrop *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall2(monitor_job_drop_func,
									BoolGetDatum(node->if_exists),
									PointerGetDatum(node->namelist));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum monitor_job_drop_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	NameData name;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	bool if_exists = false;
	MemoryContext context, old_context;
	List *name_list;

	if_exists = PG_GETARG_BOOL(0);
	name_list = (List *)PG_GETARG_POINTER(1);
	Assert(name_list);
	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP JOB"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(MjobRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, name_list)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = montiot_job_get_item_tuple(rel, &name);
		if(!HeapTupleIsValid(tuple))
		{
			if(if_exists)
			{
				ereport(NOTICE,  (errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("\"%s\" does not exist, skipping", NameStr(name))));
				continue;
			}
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("\"%s\" does not exist", NameStr(name))));
		}
		heap_freetuple(tuple);
	}

	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	foreach(lc, name_list)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = montiot_job_get_item_tuple(rel, &name);
		if(HeapTupleIsValid(tuple))
		{
			simple_heap_delete(rel, &(tuple->t_self));
			CatalogUpdateIndexes(rel, tuple);
			heap_freetuple(tuple);
		}
	}
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
	PG_RETURN_BOOL(true);
}


static HeapTuple montiot_job_get_item_tuple(Relation rel_job, Name jobname)
{
	ScanKeyData key[1];
	HeapTuple tupleret = NULL;
	HeapTuple tuple = NULL;
	HeapScanDesc rel_scan;

	ScanKeyInit(&key[0]
				,Anum_monitor_job_name
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(jobname));
	rel_scan = heap_beginscan(rel_job, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		tupleret = heap_copytuple(tuple);
		break;
	}
	heap_endscan(rel_scan);

	return tupleret;
}


/*
* drop the coordinator which is not running normal
*
*/
Datum monitor_handle_coordinator(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tuple = NULL;
	ScanKeyData key[3];
	Relation relNode;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData strerr;
	NameData s_nodename;
	HeapTuple tup_result;
	PGconn* conn;
	PGresult *res;
	StringInfoData constr;
	int nodePort;
	int agentPort;
	int iloop = 0;
	int count = 0;
	int coordNum = 0;
	int64 runTime;
	int checkTime = 300;
	int dropCoordOidArray[1000];
	char *address;
	char *userName;
	char portBuf[10];
	Oid tupleOid;
	bool result = true;
	bool rest;
	

	/*check ADBMGR run time*/
	runTime = mgr_database_run_time("postgres");
	namestrcpy(&s_nodename, "coordinator");
	initStringInfo(&infosendmsg);
	if (runTime < checkTime)
	{
		appendStringInfo(&infosendmsg, "the ADBMGR running time less then %d seconds, not check coordinators's status now", checkTime);
		ereport(LOG, (errmsg("%s", infosendmsg.data)));
		ereport(NOTICE, (errmsg("%s", infosendmsg.data)));
		tup_result = build_common_command_tuple(&s_nodename, false, infosendmsg.data);
		pfree(infosendmsg.data);
		return HeapTupleGetDatum(tup_result);
	}
	/* check the status of all coordinator in cluster */
	memset(dropCoordOidArray, 0, 1000);
	memset(portBuf, 0, sizeof(portBuf));
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
	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan(relNode, SnapshotNow, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		coordNum++;
		nodePort = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		rest = port_occupancy_test(address, nodePort);
		if (!rest)
		{
			/*check it two times again*/
			pg_usleep(1000000L);
			rest = port_occupancy_test(address, nodePort);
			if (!rest)
			{
				pg_usleep(1000000L);
				rest = port_occupancy_test(address, nodePort);
			}
		}
		/*record drop coordinator*/
		if (!rest)
		{
			dropCoordOidArray[iloop] = HeapTupleGetOid(tuple);
			iloop++;
			appendStringInfo(&infosendmsg, "drop node \"%s\"; ", NameStr(mgr_node->nodename));
		}
		
		pfree(address);
	}
	count = iloop;

	if (coordNum == count)
	{
		heap_endscan(relScan);
		heap_close(relNode, RowExclusiveLock);
		pfree(infosendmsg.data);
		ereport(ERROR, (errmsg("all coordinators in cluster are not running normal!!!")));
	}

	if (0 == count)
	{
		heap_endscan(relScan);
		heap_close(relNode, RowExclusiveLock);
		pfree(infosendmsg.data);
		tup_result = build_common_command_tuple(&s_nodename, true, "all coordinators in cluster are running normal");
		return HeapTupleGetDatum(tup_result);
	}

	/*remove coordinator out of cluster*/
	initStringInfo(&constr);
	initStringInfo(&restmsg);
	initStringInfo(&strerr);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&strerr, "%s\n", infosendmsg.data);

	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		userName = get_hostuser_from_hostoid(mgr_node->nodehost);
		tupleOid = HeapTupleGetOid(tuple);
		rest = mgr_element_in_array(tupleOid, dropCoordOidArray, count);
		memset(portBuf, 0, sizeof(portBuf));
		snprintf(portBuf, sizeof(portBuf), "%d", mgr_node->nodeport);
		if (!rest)
		{
			resetStringInfo(&constr);
			appendStringInfo(&constr, "host='%s' port=%d user=%s connect_timeout=5"
				, address, mgr_node->nodeport, userName);
			rest = PQping(constr.data);
			if (rest == PQPING_OK)
			{
				PG_TRY();
				{
					/*drop the coordinator*/
					resetStringInfo(&constr);
					appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", userName, address, mgr_node->nodeport, AGTM_DBNAME);
					appendStringInfoCharMacro(&constr, '\0');
					conn = PQconnectdb(constr.data);
					if (PQstatus(conn) != CONNECTION_OK)
					{
						result = false;
						ereport(WARNING, (errmsg("on ADBMGR cannot connect coordinator \"%s\"", NameStr(mgr_node->nodename))));
						appendStringInfo(&strerr, "on ADBMGR cannot connect coordinator \"%s\", you need do \"%s\" and \"select pgxc_pool_reload()\" on coordinator \"%s\" by yourself!!!\n"
								, NameStr(mgr_node->nodename), infosendmsg.data, NameStr(mgr_node->nodename));
					}
					else
					{
						res = PQexec(conn, infosendmsg.data);
						if(PQresultStatus(res) == PGRES_COMMAND_OK)
						{
							ereport(LOG, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,infosendmsg.data, PQcmdStatus(res))));
							ereport(NOTICE, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,infosendmsg.data, PQcmdStatus(res))));
						}
						else
						{
							result = false;
							ereport(WARNING, (errmsg("from ADBMGR, on coordinator \"%s\", execute \"%s\" fail: %s" 
								, NameStr(mgr_node->nodename), infosendmsg.data, PQresultErrorMessage(res))));
							appendStringInfo(&strerr, "from ADBMGR, on coordinator \"%s\" execute \"%s\" fail: %s\n"
							, NameStr(mgr_node->nodename), infosendmsg.data, PQresultErrorMessage(res));
						}
						PQclear(res);

						res = PQexec(conn, "select pgxc_pool_reload()");
						if(PQresultStatus(res) == PGRES_TUPLES_OK)
						{
							ereport(LOG, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,"select pgxc_pool_reload()", "t")));
							ereport(NOTICE, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,"select pgxc_pool_reload()", "t")));
						}
						else
						{
							result = false;
							ereport(WARNING, (errmsg("from ADBMGR, on coordinator \"%s\", execute \"%s\" fail: %s" 
								, NameStr(mgr_node->nodename), "select pgxc_pool_reload()", PQresultErrorMessage(res))));
							appendStringInfo(&strerr, "from ADBMGR, on coordinator \"%s\" execute \"%s\" fail: %s\n"
								, NameStr(mgr_node->nodename), "select pgxc_pool_reload()", PQresultErrorMessage(res));
						}
						PQclear(res);
					}
					PQfinish(conn);
					
				}PG_CATCH();
				{
					ereport(WARNING, (errmsg("the command result : %s", strerr.data)));
					pfree(address);
					pfree(userName);
					heap_endscan(relScan);
					heap_close(relNode, RowExclusiveLock);

					pfree(constr.data);
					pfree(restmsg.data);
					pfree(strerr.data);
					pfree(infosendmsg.data);
					pfree(getAgentCmdRst.description.data);
					PG_RE_THROW();
				}PG_END_TRY();
			}
			else
			{
				agentPort = get_agentPort_from_hostoid(mgr_node->nodehost);
				rest = port_occupancy_test(address, agentPort);
				/* agent running normal */
				if (rest)
				{
					resetStringInfo(&restmsg);
					monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES_COMMAND, agentPort, infosendmsg.data
					,userName, address, mgr_node->nodeport, DEFAULT_DB, &restmsg);
					ereport(LOG, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, infosendmsg.data, restmsg.len == 0 ? "f":restmsg.data)));
					ereport(NOTICE, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, infosendmsg.data, restmsg.len == 0 ? "f":restmsg.data)));
					if (restmsg.len == 0)
					{
						result = false;
						appendStringInfo(&strerr, "from agent, on coordinator \"%s\" execute \"%s\" fail\n"
							, NameStr(mgr_node->nodename), infosendmsg.data);
					}

					resetStringInfo(&restmsg);
					monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPort, "select pgxc_pool_reload()"
					,userName, address, mgr_node->nodeport, DEFAULT_DB, &restmsg);
					ereport(LOG, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, "select pgxc_pool_reload()", restmsg.data)));
					ereport(NOTICE, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, "select pgxc_pool_reload()", restmsg.data)));
					if (restmsg.len != 0)
					{
						if (strcasecmp(restmsg.data, "t") != 0)
						{
							result = false;
							appendStringInfo(&strerr, "from agent, on coordinator \"%s\" execute \"%s\" fail: %s\n"
								, NameStr(mgr_node->nodename), "select pgxc_pool_reload()", restmsg.data);
						}
					}
				}
				else
				{
					result = false;
					ereport(WARNING, (errmsg("on address \"%s\" the agent is not running normal; you need execute \"%s\" and \"select pgxc_pool_reload()\" on coordinator \"%s\" by yourself !!!", address, infosendmsg.data, NameStr(mgr_node->nodename))));
					appendStringInfo(&strerr,"on address \"%s\" the agent is not running normal; you need execute \"%s\" and \"select pgxc_pool_reload()\" on coordinator \"%s\" by yourself !!!", address, infosendmsg.data, NameStr(mgr_node->nodename));
				}
			}
		}
		else
		{
			mgr_node->nodeinited = false;
			mgr_node->nodeincluster = false;
			heap_inplace_update(relNode, tuple);
		}
		
		pfree(address);
		pfree(userName);
	}
	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);
	
	pfree(restmsg.data);
	pfree(constr.data);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	if (strerr.len > 2)
	{
		if (strerr.data[strerr.len-2] == '\n')
			strerr.data[strerr.len-2] = '\0';
		if (strerr.data[strerr.len-1] == '\n')
			strerr.data[strerr.len-1] = '\0';
	}
	ereport(LOG, (errmsg("monitor handle coordinator result : %s, description : %s"
		,result==true?"true":"false", strerr.data)));
	tup_result = build_common_command_tuple(&s_nodename, result, strerr.data);
	pfree(strerr.data);
	return HeapTupleGetDatum(tup_result);
}


static bool mgr_element_in_array(Oid tupleOid, int array[], int count)
{
	int iloop = 0;

	while(iloop < count)
	{
		if(array[iloop] == tupleOid)
			return true;
		iloop++;
	}
	return false;
}


static int64 mgr_database_run_time(char *database)
{
	int64 totalTime = 0;
	StringInfoData stringinfomsg;
	NameData restdata;
	int ret;
	
	Assert(database);
	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));

	initStringInfo(&stringinfomsg);
	appendStringInfo(&stringinfomsg, "select case when  stats_reset IS NULL then  0 else  round(abs(extract(epoch from now())- extract(epoch from  stats_reset))) end from pg_stat_database where datname = '%s';", database);

	ret = SPI_execute(stringinfomsg.data, false, 0);
	pfree(stringinfomsg.data);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("ADB Manager SPI_execute failed: error code %d", ret)));
	if (SPI_processed > 0 && SPI_tuptable != NULL)
	{
		namestrcpy(&restdata, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
		totalTime = atoi(restdata.data);
		pg_lltoa(totalTime, restdata.data);
	}

	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	return totalTime;
}