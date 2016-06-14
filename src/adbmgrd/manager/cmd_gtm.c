/*
 * commands of gtm
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

#define MAX_PREPARED_TRANSACTIONS_DEFAULT	100
#define shutdown_s  "smart"
#define shutdown_f  "fast"
#define shutdown_i  "immediate"
#define takeplaparm_n  "none"

typedef struct InitGtmInfo
{
	Relation		rel_gtm;
	HeapScanDesc	rel_scan;
	ListCell		**lcp;
}InitGtmInfo;

#if (Natts_mgr_gtm != 6)
#error "need change code"
#endif

/* systbl: gtm */
void mgr_add_gtm(MGRAddGtm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	NameData name;
	Datum datum[Natts_mgr_gtm];
	bool isnull[Natts_mgr_gtm];
	bool got[Natts_mgr_gtm];
	ObjectAddress myself;
	ObjectAddress host;
	Oid gtm_oid;
	Assert(node && node->name);

	rel = heap_open(GtmRelationId, RowExclusiveLock);
	namestrcpy(&name, node->name);
	/* check exists */
	if(SearchSysCacheExists1(GTMGTMNAME, NameGetDatum(&name)))
	{
		if(node->if_not_exists)
		{
			heap_close(rel, RowExclusiveLock);
			return;
		}
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("host \"%s\" already exists", NameStr(name))));
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_gtm_gtmname-1] = NameGetDatum(&name);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));

		if(strcmp(def->defname, "host") == 0)
		{
			NameData hostname;
			if(got[Anum_mgr_gtm_gtmhost-1])
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
			datum[Anum_mgr_gtm_gtmhost-1] = ObjectIdGetDatum(HeapTupleGetOid(tuple));
			got[Anum_mgr_gtm_gtmhost-1] = true;
			ReleaseSysCache(tuple);
		}else if(strcmp(def->defname, "type") == 0)
		{
			if(got[Anum_mgr_gtm_gtmtype-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			/* str should be gtm/gtm_standby/gtm_proxy */
			if(strcmp(str, "gtm") == 0)
				datum[Anum_mgr_gtm_gtmtype-1] = CharGetDatum(GTM_TYPE_GTM);
			else if(strcmp(str, "gtm_standby") == 0)
				datum[Anum_mgr_gtm_gtmtype-1] = CharGetDatum(GTM_TYPE_STANDBY);
			else
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("gtm \"%s\" not recognized", str)
					, errhint("option type is gtm or gtm_standby")));
			got[Anum_mgr_gtm_gtmtype-1] = true;
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_gtm_gtmport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_gtm_gtmport-1] = Int32GetDatum(port);
			got[Anum_mgr_gtm_gtmport-1] = true;
		}else if(strcmp(def->defname, "path") == 0)
		{
			if(got[Anum_mgr_gtm_gtmpath-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_gtm_gtmpath-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_gtm_gtmpath-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is host, type, port and path")));
		}
		
	}

	/* if not give, set to default */
	if(got[Anum_mgr_gtm_gtmtype-1] == false)
	{
		datum[Anum_mgr_gtm_gtmtype-1] = CharGetDatum(GTM_TYPE_GTM);
	}
	if(got[Anum_mgr_gtm_gtmport-1] == false)
	{
		datum[Anum_mgr_gtm_gtmport-1] = Int32GetDatum(GTM_DEFAULT_PORT);
	}
	if(got[Anum_mgr_gtm_gtmpath-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"path\" must give")));
	}
	if(got[Anum_mgr_gtm_gtmhost-1] == false)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"host\" must give")));
	}

	/* now, gtm is not initialized, need "INIT GTM ..." command */
	datum[Anum_mgr_gtm_gtminited-1] = BoolGetDatum(false);

	/* now, we can insert record */
	tuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	gtm_oid = simple_heap_insert(rel, tuple);
	CatalogUpdateIndexes(rel, tuple);
	heap_freetuple(tuple);

	/*close relation */
	heap_close(rel, RowExclusiveLock);

	/* Record dependencies on host */
	myself.classId = GtmRelationId;
	myself.objectId = gtm_oid;
	myself.objectSubId = 0;

	host.classId = HostRelationId;
	host.objectId = DatumGetObjectId(datum[Anum_mgr_gtm_gtmhost-1]);
	host.objectSubId = 0;
	recordDependencyOn(&myself, &host, DEPENDENCY_NORMAL);
}

void mgr_alter_gtm(MGRAlterGtm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple,
	          new_tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	NameData name;
	Datum datum[Natts_mgr_gtm];
	bool isnull[Natts_mgr_gtm];
	bool got[Natts_mgr_gtm];
	HeapTuple searchHostTuple;	
	TupleDesc gtm_dsc;
	NameData hostname;
			
	Assert(node && node->name);
	rel = heap_open(GtmRelationId, RowExclusiveLock);
	gtm_dsc = RelationGetDescr(rel);
	namestrcpy(&name, node->name);
	/* check exists */
	tuple = SearchSysCache1(GTMGTMNAME, NameGetDatum(&name));
	if(!SearchSysCacheExists1(GTMGTMNAME, NameGetDatum(&name)))
	{
		if(node->if_not_exists)
		{
			heap_close(rel, RowExclusiveLock);
			return;
		}

		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				,errmsg("gtm \"%s\" doesnot exists", NameStr(name))));
	}

	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_gtm_gtmname-1] = NameGetDatum(&name);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "host") == 0)
		{		
			if(got[Anum_mgr_gtm_gtmhost-1])
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
			datum[Anum_mgr_gtm_gtmhost-1] = ObjectIdGetDatum(HeapTupleGetOid(searchHostTuple));
			got[Anum_mgr_gtm_gtmhost-1] = true;
			ReleaseSysCache(searchHostTuple);
		}else if(strcmp(def->defname, "type") == 0)
		{
			if(got[Anum_mgr_gtm_gtmtype-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_gtm_gtmtype-1] = true;
			str = defGetString(def);
			/* str should be gtm/gtm_standby/gtm_proxy */
			if(strcmp(str, "gtm") == 0)
				datum[Anum_mgr_gtm_gtmtype-1] = CharGetDatum(GTM_TYPE_GTM);
			else if(strcmp(str, "gtm_standby") == 0)
				datum[Anum_mgr_gtm_gtmtype-1] = CharGetDatum(GTM_TYPE_STANDBY);
			else
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("gtm \"%s\" not recognized", str)
					, errhint("option type is gtm or gtm_standby")));

		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_gtm_gtmport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_gtm_gtmport-1] = Int32GetDatum(port);
			got[Anum_mgr_gtm_gtmport-1] = true;
		}else if(strcmp(def->defname, "path") == 0)
		{
			if(got[Anum_mgr_gtm_gtmpath-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_gtm_gtmpath-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_gtm_gtmpath-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is host, type, port and path")));
		}
		
	}
	
	new_tuple = heap_modify_tuple(tuple, gtm_dsc, datum,isnull, got);
	simple_heap_update(rel, &tuple->t_self, new_tuple);
	CatalogUpdateIndexes(rel, new_tuple);
	ReleaseSysCache(tuple);
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
}

void mgr_drop_gtm(MGRDropGtm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	MemoryContext context, old_context;
	NameData name;

	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP GTM"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(GtmRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, node->hosts)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = SearchSysCache1(GTMGTMNAME, NameGetDatum(&name));
		if(!HeapTupleIsValid(tuple))
		{
			if(node->if_exists)
				continue;
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("gtm \"%s\" dose not exists", NameStr(name))));
		}
		/* todo chech used by other */
		ReleaseSysCache(tuple);
	}

	/* now we can delete gtm(s) */
	foreach(lc, node->hosts)
	{
		val = lfirst(lc);
		Assert(val  && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = SearchSysCache1(GTMGTMNAME, NameGetDatum(&name));
		if(HeapTupleIsValid(tuple))
		{
			simple_heap_delete(rel, &(tuple->t_self));
			ReleaseSysCache(tuple);
		}
	}

	heap_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
}

/*
* execute init gtm master, send infomation to agent to init gtm master 
*/
Datum 
mgr_init_gtm(PG_FUNCTION_ARGS)
{
	return mgr_runmode_gtm(GTM_TYPE_GTM, AGT_CMD_GTM_INIT, fcinfo, takeplaparm_n);
}

/*
*	execute init gtm all, send infomation to agent to init gtm all 
*/
Datum 
mgr_init_gtm_all(PG_FUNCTION_ARGS)
{
	InitGtmInfo *info;
	GetAgentCmdRst		getAgentCmdRst;
	FuncCallContext *funcctx;
	HeapTuple tuple
			,tup_result;
	Form_mgr_gtm mgr_gtm;

	/*output the exec result: col1 hostname,col2 SUCCESS(t/f),col3 description*/	
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc(sizeof(*info));
		info->rel_gtm = heap_open(GtmRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan(info->rel_gtm, SnapshotNow, 0, NULL);
				/* save info */
		funcctx->user_fctx = info;	
		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;
	Assert(info);
	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);

	if (tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_gtm, AccessShareLock);
		if (funcctx->call_cntr <1)
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
				, errmsg("the need infomation does not in system table of gtm, use \"list gtm\" to check")));
		}
		pfree(info);
		SRF_RETURN_DONE(funcctx);

	}
	initStringInfo(&(getAgentCmdRst.description));
	mgr_gtm = (Form_mgr_gtm)GETSTRUCT(tuple);
	Assert(mgr_gtm);
	if (mgr_gtm->gtmtype == GTM_TYPE_GTM)
	{
		mgr_runmode_gtm_get_result(AGT_CMD_GTM_INIT, &getAgentCmdRst, info->rel_gtm, tuple, takeplaparm_n);
		tup_result = build_common_command_tuple(
			&(getAgentCmdRst.nodename)
			, getAgentCmdRst.ret
			, getAgentCmdRst.description.data);
		pfree(getAgentCmdRst.description.data);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}
	else /* (mgr_gtm->gtmtype == GTM_TYPE_STANDBY) */
	{
		mgr_runmode_gtm_get_result(AGT_CMD_GTM_INIT, &getAgentCmdRst, info->rel_gtm, tuple, takeplaparm_n);
		tup_result = build_common_command_tuple(
			&(getAgentCmdRst.nodename)
			, getAgentCmdRst.ret
			, getAgentCmdRst.description.data);
		pfree(getAgentCmdRst.description.data);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}
}

/*
* start gtm master
*/
Datum mgr_start_gtm(PG_FUNCTION_ARGS)
{
	return mgr_runmode_gtm(GTM_TYPE_GTM, AGT_CMD_GTM_START_MASTER, fcinfo, takeplaparm_n);
}

/*
* stop gtm master
*/
Datum mgr_stop_gtm(PG_FUNCTION_ARGS)
{
	const char *shutdown_mode = PG_GETARG_CSTRING(0);
	char *stop_mode;
	if(strcmp(shutdown_mode, "smart") == 0)
	{
		stop_mode = shutdown_s;
	}
	else if (strcmp(shutdown_mode, "fast") == 0)
	{
		stop_mode = shutdown_f;
	}
	else /* if (strcmp(shutdown_mode, "immediate") == 0) */
	{
		stop_mode = shutdown_i;
	}
	return mgr_runmode_gtm(GTM_TYPE_GTM, AGT_CMD_GTM_STOP_MASTER, fcinfo, stop_mode);
}

/*
* runmode include:start/stop/init gtm
*/
Datum mgr_runmode_gtm(const char gtmtype, const char cmdtype, PG_FUNCTION_ARGS, const char *shutdown_mode)
{
	GetAgentCmdRst getAgentCmdRst;
	FuncCallContext *funcctx;
	HeapTuple tuple;
	HeapTuple aimtuple;
	Relation rel_gtm;
	HeapScanDesc scan;
	Form_mgr_gtm mgr_gtm;
	ScanKeyData key[1];
	bool gettuple = false;

	/*output the exec result: col1 hostname,col2 SUCCESS(t/f),col3 description*/
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		/* for now, we have only one master gtm, so we return in first time */
		ScanKeyInit(&key[0],
			Anum_mgr_gtm_gtmtype
			,BTEqualStrategyNumber, F_CHAREQ
			,CharGetDatum(gtmtype));
		rel_gtm = heap_open(GtmRelationId, RowExclusiveLock);
		scan = heap_beginscan(rel_gtm, SnapshotNow, 1, key);
		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			mgr_gtm = (Form_mgr_gtm)GETSTRUCT(tuple);
			Assert(mgr_gtm);
			if (mgr_gtm->gtmtype == gtmtype)
			{
				aimtuple = tuple;
				gettuple = true;
				break;
			}
		}
		if (gettuple == false)
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
				, errmsg("the need infomation does not in system table of gtm, use \"list gtm\" to check")));
		}
		initStringInfo(&(getAgentCmdRst.description));
		mgr_runmode_gtm_get_result(cmdtype, &getAgentCmdRst, rel_gtm, aimtuple, shutdown_mode);
		tuple = build_common_command_tuple(
			&(getAgentCmdRst.nodename)
			, getAgentCmdRst.ret
			, getAgentCmdRst.description.data);
		pfree(getAgentCmdRst.description.data);
		heap_endscan(scan);
		heap_close(rel_gtm, RowExclusiveLock);
		MemoryContextSwitchTo(oldcontext);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	/* we have only one master gtm, returnd at first time */
	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	SRF_RETURN_DONE(funcctx);
}

/*
* get the execute result: init/start/stop gtm
*/
void mgr_runmode_gtm_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation gtmrel, HeapTuple aimtuple, const char *shutdown_mode)
{
	/*get gtm path from adbmgr.gtm, which row info type is gtm*/
	Datum datumPath;
	char *gtmPath;
	char *gtmnametmp;
	char *hostaddress;
	Oid hostOid;
	StringInfoData buf;
	StringInfoData infosendmsg;
	ManagerAgent *ma;
	Form_mgr_gtm mgr_gtm;
	char gtmtype;
	bool done = false;
	bool isNull = false;
	char *runmode;
	int gtmport;

	initStringInfo(&infosendmsg);
	/*get column values from aimtuple*/	
	mgr_gtm = (Form_mgr_gtm)GETSTRUCT(aimtuple);
	Assert(mgr_gtm);
	gtmtype = mgr_gtm->gtmtype;
	gtmnametmp = NameStr(mgr_gtm->gtmname);
	hostOid = mgr_gtm->gtmhost;
	gtmport = mgr_gtm->gtmport;
	/*get host address*/
	hostaddress = get_hostname_from_hostoid(hostOid);
	Assert(hostaddress);
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), gtmnametmp);
	pfree(hostaddress);
	/*when start/stop, check gtm init or not*/
	if ( (AGT_CMD_GTM_START_MASTER == cmdtype || AGT_CMD_GTM_STOP_MASTER == cmdtype) \
					&& !mgr_gtm->gtminited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "the gtm \"%s\" has not inited", GTM_TYPE_GTM ==  gtmtype ? "master": (GTM_TYPE_STANDBY == gtmtype ? "slave":gtmnametmp));
		getAgentCmdRst->ret = false;
		return;
	}
	/*when initd ,check initd or not*/
	if (cmdtype == AGT_CMD_GTM_INIT && mgr_gtm->gtminited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "the gtm \"%s\" has inited", GTM_TYPE_GTM ==  gtmtype ? "master": (GTM_TYPE_STANDBY == gtmtype ? "slave":gtmnametmp));
		getAgentCmdRst->ret = false;
		return;
	}
	/*get gtmPath from aimtuple*/
	datumPath = heap_getattr(aimtuple, Anum_mgr_gtm_gtmpath, RelationGetDescr(gtmrel), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_gtm")
			, errmsg("column gtmpath is null")));
	}
	gtmPath = TextDatumGetCString(datumPath);
	
	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		return;
	}
	switch(cmdtype)
	{
		case AGT_CMD_GTM_INIT:
				runmode = "";
				break;
		case AGT_CMD_GTM_START_MASTER:
		case AGT_CMD_GTM_START_SLAVE:
				runmode = "start";
				break;
		case AGT_CMD_GTM_STOP_MASTER:
		case AGT_CMD_GTM_STOP_SLAVE:
				runmode = "stop";
				break;
		default:
				/*never come here*/
				runmode = "none";
				break;
	}
	if (AGT_CMD_GTM_INIT == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", runmode, gtmPath);
	}
	else if (AGT_CMD_GTM_STOP_MASTER == cmdtype || AGT_CMD_GTM_STOP_SLAVE == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s -m %s -o -i -w -c -l %s/logfile", runmode, gtmPath, shutdown_mode, gtmPath);
	}
	else
	{
		appendStringInfo(&infosendmsg, " %s -D %s -o -i -w -c -l %s/logfile", runmode, gtmPath, gtmPath);
	}
	/* send message */
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
	/*check the receive msg */
	done = mgr_recv_msg(ma, getAgentCmdRst);
	ma_close(ma);
	/*when init, 1. update gtm system table's column to set initial is true 2. refresh postgresql.conf*/
	if (done && AGT_CMD_GTM_INIT == cmdtype)
	{
		/*update node system table's column to set initial is true when cmd is init*/
		mgr_gtm->gtminited = true;
		heap_inplace_update(gtmrel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", gtmport, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("listen_addresses", "*", &infosendmsg);
		mgr_append_pgconf_paras_str_int("max_prepared_transactions", MAX_PREPARED_TRANSACTIONS_DEFAULT, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("log_destination", "stderr", &infosendmsg);
		mgr_append_pgconf_paras_str_str("logging_collector", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, gtmPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, gtmPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	pfree(infosendmsg.data);
}
