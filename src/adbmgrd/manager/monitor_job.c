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

static HeapTuple montiot_job_get_item_tuple(Relation rel_job, Name jobname);


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

	if_not_exists = PG_GETARG_BOOL(0);
	jobname = PG_GETARG_CSTRING(1);
	options = (List *)PG_GETARG_POINTER(2);

	Assert(jobname);
	namestrcpy(&jobnamedata, jobname);
	rel = heap_open(MjobRelationId, RowExclusiveLock);
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
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("\"%s\" already exists", jobname)));
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
			datum[Anum_monitor_job_interval-1] = Int32GetDatum(interval);
			got[Anum_monitor_job_interval-1] = true;
		}
		else if (strcmp(def->defname, "status") == 0)
		{
			if(got[Anum_monitor_job_status-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			status = defGetBoolean(def);
			datum[Anum_monitor_job_status-1] = BoolGetDatum(interval);
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
	/* now, we can insert record */
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
			datum[Anum_monitor_job_interval-1] = Int32GetDatum(interval);
			got[Anum_monitor_job_interval-1] = true;
		}
		else if (strcmp(def->defname, "status") == 0)
		{
			if(got[Anum_monitor_job_status-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			status = defGetBoolean(def);
			datum[Anum_monitor_job_status-1] = BoolGetDatum(interval);
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

	if_exists = PG_GETARG_BOOL(0);
	List *name_list = (List *)PG_GETARG_POINTER(1);
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