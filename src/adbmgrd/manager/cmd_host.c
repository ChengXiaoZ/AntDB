/*
 * commands of host
 */

#include "postgres.h"

#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ifaddrs.h>

#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "libpq/ip.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "pgtar.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/fmgroids.h"    /* For F_NAMEEQ	*/


typedef struct StartAgentInfo
{
	Relation		rel_host;
	HeapScanDesc	rel_scan;
}StartAgentInfo;

#if (Natts_mgr_host != 7)
#error "need change code"
#endif

static FILE* make_tar_package(void);
static void append_file_to_tar(FILE *tar, const char *path, const char *name);
static bool host_is_localhost(const char *name);
static bool deploy_to_host(FILE *tar, TupleDesc desc, HeapTuple tup, StringInfo msg, const char *password);
static void get_pghome(char *pghome);

void mgr_add_host(MGRAddHost *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	NameData name;
	NameData user;
	Datum datum[Natts_mgr_host];
	bool isnull[Natts_mgr_host];
	bool got[Natts_mgr_host];
	char pghome[MAXPGPATH]={0};
	Assert(node && node->name);

	rel = heap_open(HostRelationId, RowExclusiveLock);
	namestrcpy(&name, node->name);
	/* check exists */
	if(SearchSysCacheExists1(HOSTHOSTNAME, NameGetDatum(&name)))
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
	datum[Anum_mgr_host_hostname-1] = NameGetDatum(&name);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "user") == 0)
		{
			if(got[Anum_mgr_host_hostuser-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_host_hostuser-1] = true;
			str = defGetString(def);
			namestrcpy(&user, str);
			datum[Anum_mgr_host_hostuser-1] = NameGetDatum(&user);
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostport-1] = true;
		}else if(strcmp(def->defname, "protocol") == 0)
		{
			if(got[Anum_mgr_host_hostproto-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(strcmp(str, "telnet") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_TELNET);
			}else if(strcmp(str, "ssh") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_SSH);
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"protocol\": \"%s\", must be \"telnet\" or \"ssh\"", str)));
			}
			got[Anum_mgr_host_hostproto-1] = true;
		}else if(strcmp(def->defname, "address") == 0)
		{
			if(got[Anum_mgr_host_hostaddr-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"%s\"", "address")));
			datum[Anum_mgr_host_hostaddr-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostaddr-1] = true;
		}else if(strcmp(def->defname, "pghome") == 0)
		{
			if(got[Anum_mgr_host_hostpghome-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_host_hostpghome-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostpghome-1] = true;
		}else if(strcmp(def->defname, "agentport") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostagentport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostagentport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostagentport-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is user,port,protocol,agentport,address and pghome")));
		}
	}

	/* if not give, set to default */
	if(got[Anum_mgr_host_hostuser-1] == false)
	{
		namestrcpy(&user, GetUserNameFromId(GetUserId()));
		datum[Anum_mgr_host_hostuser-1] = NameGetDatum(&user);
	}
	if(got[Anum_mgr_host_hostproto-1] == false)
	{
		datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_SSH);
	}
	if(got[Anum_mgr_host_hostport-1] == false)
	{
		if(DatumGetChar(datum[Anum_mgr_host_hostproto-1]) == HOST_PROTOCOL_SSH)
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(22);
		else if(DatumGetChar(datum[Anum_mgr_host_hostproto-1]) == HOST_PROTOCOL_TELNET)
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(23);
		else
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				,errmsg("unknown protocol type %d", DatumGetChar(datum[Anum_mgr_host_hostproto-1]))));
	}
	if(got[Anum_mgr_host_hostaddr-1] == false)
	{
		datum[Anum_mgr_host_hostaddr-1] = PointerGetDatum(cstring_to_text(node->name));
	}
	if(got[Anum_mgr_host_hostpghome-1] == false)
	{
		get_pghome(pghome);
		datum[Anum_mgr_host_hostpghome-1] = PointerGetDatum(cstring_to_text(pghome));
	}
	if(got[Anum_mgr_host_hostagentport-1] == false)
	{
		datum[Anum_mgr_host_hostagentport-1] = Int32GetDatum(AGENTDEFAULTPORT);
	}
	/* now, we can insert record */
	tuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	simple_heap_insert(rel, tuple);
	CatalogUpdateIndexes(rel, tuple);
	heap_freetuple(tuple);

	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
}

void mgr_drop_host(MGRDropHost *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	MemoryContext context, old_context;
	NameData name;

	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP HOST"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(HostRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, node->hosts)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
		if(!HeapTupleIsValid(tuple))
		{
			if(node->if_exists)
				continue;
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("host \"%s\" dose not exists", NameStr(name))));
		}
		/*check the tuple has been used or not*/
		if(mgr_check_host_in_use(HeapTupleGetOid(tuple)))
		{
			ReleaseSysCache(tuple);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("\"%s\" has been used, cannot be dropped", NameStr(name))));
		}
		/* todo chech used by other */
		ReleaseSysCache(tuple);
	}

	/* now we can delete host(s) */
	foreach(lc, node->hosts)
	{
		val = lfirst(lc);
		Assert(val  && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
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

void mgr_alter_host(MGRAlterHost *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple,
	          new_tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	NameData name;
	NameData user;
	Datum datum[Natts_mgr_host];
	bool isnull[Natts_mgr_host];
	bool got[Natts_mgr_host];
	
	TupleDesc host_dsc;
	
	Assert(node && node->name);
	rel = heap_open(HostRelationId, RowExclusiveLock);
	host_dsc = RelationGetDescr(rel);
	namestrcpy(&name, node->name);
	/* check whether exists */
	tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	if(!SearchSysCacheExists1(HOSTHOSTNAME, NameGetDatum(&name)))
	{
		if(node->if_not_exists)
		{
			heap_close(rel, RowExclusiveLock);
			return;
		}
                
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("host \"%s\" doesnot exists", NameStr(name))));
	}
	/*check the tuple has been used or not*/
	if(mgr_check_host_in_use(HeapTupleGetOid(tuple)))
	{
		ReleaseSysCache(tuple);
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("\"%s\" has been used, cannot be changed", NameStr(name))));
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_host_hostname-1] = NameGetDatum(&name);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "user") == 0)
		{
			if(got[Anum_mgr_host_hostuser-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_host_hostuser-1] = true;
			str = defGetString(def);
			namestrcpy(&user, str);
			datum[Anum_mgr_host_hostuser-1] = NameGetDatum(&user);
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostport-1] = true;
		}else if(strcmp(def->defname, "protocol") == 0)
		{
			if(got[Anum_mgr_host_hostproto-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(strcmp(str, "telnet") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_TELNET);
			}else if(strcmp(str, "ssh") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_SSH);
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"protocol\": \"%s\", must be \"telnet\" or \"ssh\"", str)));
			}
			got[Anum_mgr_host_hostproto-1] = true;
		}else if(strcmp(def->defname, "address") == 0)
		{
			if(got[Anum_mgr_host_hostaddr-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"%s\"", "address")));
			datum[Anum_mgr_host_hostaddr-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostaddr-1] = true;
		}else if(strcmp(def->defname, "pghome") == 0)
		{
			if(got[Anum_mgr_host_hostpghome-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_host_hostpghome-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostpghome-1] = true;
		}else if(strcmp(def->defname, "agentport") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostagentport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostagentport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostagentport-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is user,port,protocol,agentport, address and pghome")));
		}
	}

	new_tuple = heap_modify_tuple(tuple, host_dsc, datum,isnull, got);
	simple_heap_update(rel, &tuple->t_self, new_tuple);
	CatalogUpdateIndexes(rel, new_tuple);
	ReleaseSysCache(tuple);
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
}

Datum
mgr_start_agent(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	StartAgentInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	ManagerAgent *ma;
	ScanKeyData	 key[1];

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_host = heap_open(HostRelationId, AccessShareLock);
		
		if(PG_ARGISNULL(1)) /* no argument, start all */
			info->rel_scan = heap_beginscan(info->rel_host, SnapshotNow, 0, NULL);
		else
		{
			Datum host_name = DirectFunctionCall1(namein, PG_GETARG_DATUM(1));
			ScanKeyInit(&key[0]
						,Anum_mgr_host_hostname
						,BTEqualStrategyNumber
						,F_NAMEEQ
						,host_name);
			info->rel_scan = heap_beginscan(info->rel_host, SnapshotNow, 1, key);
		}
		
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
		heap_close(info->rel_host, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_host = (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);

	/* test is running ? */
	ma = ma_connect_hostoid(HeapTupleGetOid(tup));
	if(ma_isconnected(ma))
	{
		tup_result = build_common_command_tuple(&(mgr_host->hostname)
			, true, _("running"));
	}else
	{
		StringInfoData exec_path;
		StringInfoData message;
		Datum datum;
		char *host_addr;
		int ret;
		bool isNull;

		/* get exec path */
		datum = heap_getattr(tup, Anum_mgr_host_hostpghome, RelationGetDescr(info->rel_host), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostpghome is null")));
		}
		initStringInfo(&exec_path);
		appendStringInfoString(&exec_path, TextDatumGetCString(datum));
		if(exec_path.data[exec_path.len] != '/')
			appendStringInfoChar(&exec_path, '/');
		appendStringInfoString(&exec_path, "bin/agent");
		/* append argument */
		appendStringInfo(&exec_path, " -b -P %u", mgr_host->hostagentport);

		/* get host address */
		datum = heap_getattr(tup, Anum_mgr_host_hostaddr, RelationGetDescr(info->rel_host), &isNull);
		if(isNull)
			host_addr = NameStr(mgr_host->hostname);
		else
			host_addr = TextDatumGetCString(datum);

		/* exec start */
		initStringInfo(&message);
		if(mgr_host->hostproto == HOST_PROTOCOL_TELNET)
		{
			appendStringInfoString(&message, _("telnet not support yet"));
			ret = 1;
		}else if(mgr_host->hostproto == HOST_PROTOCOL_SSH)
		{
			ret = ssh2_start_agent(host_addr
				, mgr_host->hostport
				, NameStr(mgr_host->hostuser)
				, PG_ARGISNULL(0) ? "" : PG_GETARG_CSTRING(0) /* password for libssh2*/
				, exec_path.data
				, &message);
		}else
		{
			appendStringInfo(&message, _("unknown protocol '%d'"), mgr_host->hostproto);
			ret = 1;
		}
		tup_result = build_common_command_tuple(
			  &(mgr_host->hostname)
			, ret == 0 ? true:false
			, message.data);
		pfree(message.data);
		pfree(exec_path.data);
	}

	ma_close(ma);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

void mgr_deplory(MGRDeplory *node, ParamListInfo params, DestReceiver *dest)
{
	FILE volatile *tar = NULL;
	HeapTuple tuple;
	HeapTuple out;
	TupleTableSlot *slot;
	Form_mgr_host host;
	TupleDesc desc;
	MemoryContext context;
	MemoryContext oldcontext;
	Relation rel;
	StringInfoData buf;
	bool success;
	HeapScanDesc scan;
	AssertArg(node && dest);

	context = AllocSetContextCreate(CurrentMemoryContext, "deplory"
					, ALLOCSET_DEFAULT_MINSIZE
					, ALLOCSET_DEFAULT_INITSIZE
					, ALLOCSET_DEFAULT_MAXSIZE);
	desc = get_common_command_tuple_desc();
	(*dest->rStartup)(dest, CMD_UTILITY, desc);
	slot = MakeSingleTupleTableSlot(desc);
	initStringInfo(&buf);
	oldcontext = CurrentMemoryContext;
	PG_TRY();
	{
		if(node->hosts == NIL)
		{
			rel = heap_open(HostRelationId, AccessShareLock);
			scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
			while((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
			{
				host = (Form_mgr_host)GETSTRUCT(tuple);
				if(tar == NULL)
					tar = make_tar_package();
				resetStringInfo(&buf);
				MemoryContextSwitchTo(context);
				MemoryContextResetAndDeleteChildren(context);
				success = deploy_to_host((FILE*)tar, RelationGetDescr(rel), tuple, &buf, node->password);
				out = build_common_command_tuple(&host->hostname, success, buf.data);
				ExecClearTuple(slot);
				ExecStoreTuple(out, slot, InvalidBuffer, false);
				MemoryContextSwitchTo(oldcontext);
				(*dest->receiveSlot)(slot, dest);
			}
			heap_endscan(scan);
			heap_close(rel, AccessShareLock);
		}else
		{
			ListCell *lc;
			Value *value;
			TupleDesc host_desc;
			NameData name;
			rel = heap_open(HostRelationId, AccessShareLock);
			host_desc = CreateTupleDescCopy(RelationGetDescr(rel));
			heap_close(rel, AccessShareLock);
			foreach(lc, node->hosts)
			{
				value = lfirst(lc);
				Assert(value && IsA(value, String));
				namestrcpy(&name, strVal(value));
				tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
				resetStringInfo(&buf);
				if(HeapTupleIsValid(tuple))
				{
					if(tar == NULL)
						tar = make_tar_package();
					success = deploy_to_host((FILE*)tar, host_desc, tuple, &buf, node->password);
					ReleaseSysCache(tuple);
				}else
				{
					success = false;
					appendStringInfoString(&buf, "host not exists");
				}
				MemoryContextSwitchTo(context);
				MemoryContextResetAndDeleteChildren(context);
				out = build_common_command_tuple(&name, success, buf.data);
				ExecClearTuple(slot);
				ExecStoreTuple(out, slot, InvalidBuffer, false);
				MemoryContextSwitchTo(oldcontext);
				(*dest->receiveSlot)(slot, dest);
			}
			FreeTupleDesc(host_desc);
		}
	}PG_CATCH();
	{
		if(tar != NULL)
			fclose((FILE*)tar);
		PG_RE_THROW();
	}PG_END_TRY();
	(*dest->rShutdown)(dest);
	if(tar)
		fclose((FILE*)tar);
}

static FILE* make_tar_package(void)
{
	FILE volatile *fd;
	DIR volatile *dir;
	struct dirent *item;
	char pghome[MAXPGPATH];

	fd = NULL;
	dir = NULL;
	PG_TRY();
	{
		/* create an temp file */
		fd = tmpfile();

		/* get package directory */
		get_pghome(pghome);

		/* enum dirent */
		dir = opendir(pghome);
		if(dir == NULL)
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("Can not open directory \"%s\" for read", pghome)));
		}
		while((item = readdir((DIR*)dir)) != NULL)
		{
			if(strcmp(item->d_name, "..") != 0
				&& strcmp(item->d_name, ".") != 0)
			{
				append_file_to_tar((FILE*)fd, pghome, item->d_name);
			}
		}
	}PG_CATCH();
	{
		fclose((FILE*)fd);
		if(dir != NULL)
			closedir((DIR*)dir);
		PG_RE_THROW();
	}PG_END_TRY();

	closedir((DIR*)dir);
	return (FILE*)fd;
}

static void append_file_to_tar(FILE *tar, const char *path, const char *name)
{
	DIR volatile* dir;
	FILE volatile *fd;
	struct dirent *item;
	StringInfoData buf;
	StringInfoData full;
	struct stat st;
	char head[512];
	ssize_t ret;
	AssertArg(tar && path);

	dir = NULL;
	fd = NULL;
	initStringInfo(&buf);
	initStringInfo(&full);
	appendStringInfo(&full, "%s/%s", path, name);
	PG_TRY();
	{
		ret = lstat(full.data, &st);
		if(ret != 0)
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("Can not lstat \"%s\":%m", full.data)));
		}
		if(S_ISLNK(st.st_mode))
		{
			for(;;)
			{
				ret = readlink(full.data, buf.data, buf.maxlen-1);
				if(ret == buf.maxlen-1)
				{
					enlargeStringInfo(&buf, buf.maxlen + 1024);
				}else
				{
					break;
				}
			}
			if(ret < 0)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not readlink \"%s\":%m", full.data)));
			}
			Assert(ret < buf.maxlen);
			buf.len = ret;
			buf.data[buf.len] = '\0';
		}
		tarCreateHeader(head, name, S_ISLNK(st.st_mode) ? buf.data : NULL
			, S_ISREG(st.st_mode) ? st.st_size : 0, st.st_mode
			, st.st_uid, st.st_gid, st.st_mtime);
		ret = fwrite(head, 1, sizeof(head), tar);
		if(ret != sizeof(head))
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("Can not append data to tar file:%m")));
		}
		if(S_ISREG(st.st_mode))
		{
			size_t cnt;
			size_t pad;
			fd = fopen(full.data, "rb");
			if(fd == NULL)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not open file \"%s\" for read:%m", full.data)));
			}
			cnt = 0;
			enlargeStringInfo(&buf, 32*1024);
			while((ret = fread(buf.data, 1, buf.maxlen, (FILE*)fd)) > 0)
			{
				if(fwrite(buf.data, 1, ret, tar) != ret)
				{
					ereport(ERROR, (errcode_for_file_access(),
						errmsg("Can not append data to tar file:%m")));
				}
				cnt += ret;
			}
			if(ret < 0)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not read file \"%s\":%m", full.data)));
			}else if(cnt != st.st_size)
			{
				ereport(ERROR, (errmsg("file size changed when reading")));
			}
			pad = ((cnt + 511) & ~511) - cnt;
			enlargeStringInfo(&buf, pad);
			memset(buf.data, 0, pad);
			if(fwrite(buf.data, 1, pad, tar) != pad)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not append data to tar file:%m")));
			}
		}else if(S_ISDIR(st.st_mode))
		{
			dir = opendir(full.data);
			if(dir == NULL)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not open directory \"%s\" for read", full.data)));
			}
			while((item = readdir((DIR*)dir)) != NULL)
			{
				if(strcmp(item->d_name, "..") == 0
					|| strcmp(item->d_name, ".") == 0)
				{
					continue;
				}
				resetStringInfo(&buf);
				appendStringInfo(&buf, "%s/%s", name, item->d_name);
				append_file_to_tar(tar, path, buf.data);
			}
		}
	}PG_CATCH();
	{
		if(dir != NULL)
			closedir((DIR*)dir);
		if(fd != NULL)
			fclose((FILE*)fd);
		PG_RE_THROW();
	}PG_END_TRY();
	if(dir != NULL)
		closedir((DIR*)dir);
	if(fd != NULL)
		fclose((FILE*)fd);
	pfree(buf.data);
	pfree(full.data);
}

static bool host_is_localhost(const char *name)
{
	struct ifaddrs *ifaddr, *ifa;
	struct addrinfo *addr;
	struct addrinfo *addrs;
	struct addrinfo hint;
	static const char tmp_port[3] = {"22"};
	bool is_localhost;

	if(getifaddrs(&ifaddr) == -1)
		ereport(ERROR, (errmsg("getifaddrs failed:%m")));
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;
	hint.ai_flags = AI_PASSIVE;
	if(pg_getaddrinfo_all(name, tmp_port, &hint, &addrs) != 0)
	{
		freeifaddrs(ifaddr);
		ereport(ERROR, (errmsg("could not resolve \"%s\"", name)));
	}

	is_localhost = false;
	for(ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next)
	{
		if(ifa->ifa_addr == NULL)
			continue;
		if(ifa->ifa_addr->sa_family != AF_INET
#ifdef HAVE_IPV6
			&& ifa->ifa_addr->sa_family != AF_INET6
#endif /* HAVE_IPV6 */
			)
		{
			continue;
		}

		for(addr = addrs; addr != NULL; addr = addr->ai_next)
		{
			if(ifa->ifa_addr->sa_family != addr->ai_family)
				continue;
			if(addr->ai_family == AF_INET)
			{
				struct sockaddr_in *l = (struct sockaddr_in*)ifa->ifa_addr;
				struct sockaddr_in *r = (struct sockaddr_in*)addr->ai_addr;
				if(memcmp(&(l->sin_addr) , &(r->sin_addr), sizeof(l->sin_addr)) == 0)
				{
					is_localhost = true;
					break;
				}
			}
#ifdef HAVE_IPV6
			else if(addr->ai_family == AF_INET6)
			{
				struct sockaddr_in6 *l = (struct sockaddr_in6*)ifa->ifa_addr;
				struct sockaddr_in6 *r = (struct sockaddr_in6*)addr->ai_addr;
				if(memcmp(&(l->sin6_addr), &(r->sin6_addr), sizeof(l->sin6_addr)) == 0)
				{
					is_localhost = true;
					break;
				}
			}
#endif /* HAVE_IPV6 */
		}
		if(is_localhost)
			break;
	}

	pg_freeaddrinfo_all(AF_UNSPEC, addrs);
	freeifaddrs(ifaddr);

	return is_localhost;
}

static bool deploy_to_host(FILE *tar, TupleDesc desc, HeapTuple tup, StringInfo msg, const char *password)
{
	Form_mgr_host host;
	Datum datum;
	char *str_path;
	char *str_addr;
	bool isnull;
	AssertArg(tar && desc && tup && msg);

	host = (Form_mgr_host)GETSTRUCT(tup);
	datum = heap_getattr(tup, Anum_mgr_host_hostaddr, desc, &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	}
	str_addr = TextDatumGetCString(datum);
	datum = heap_getattr(tup, Anum_mgr_host_hostpghome, desc, &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column _hostpghome is null")));
	}
	str_path = TextDatumGetCString(datum);

	if(host_is_localhost(str_addr))
	{
		char pghome[MAXPGPATH];
		get_pghome(pghome);
		if(strcmp(pghome, str_path) == 0)
		{
			appendStringInfoString(msg, "skip localhost");
			return true;
		}
	}

	if(host->hostproto != HOST_PROTOCOL_SSH)
	{
		appendStringInfoString(msg, "deplory support ssh only for now");
		return false;
	}

	if(password == NULL)
		password = "";

	return ssh2_deplory_tar(str_addr, host->hostport
			, NameStr(host->hostuser), password, str_path
			, tar, msg);
}

static void get_pghome(char *pghome)
{
	if(my_exec_path[0] == '\0')
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			,errmsg("can't get the pghome path")));
	}
	strcpy(pghome, my_exec_path);
	get_parent_directory(pghome);
	get_parent_directory(pghome);
}

Datum mgr_stop_agent(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	StartAgentInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	ManagerAgent *ma;
	ScanKeyData	 key[1];
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData message;
	Datum host_name;
	char cmdtype = AGT_CMD_STOP_AGENT;
	int retry = 0;
	const int retrymax = 10;
	
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_host = heap_open(HostRelationId, AccessShareLock);
		
		if(PG_ARGISNULL(0))
			info->rel_scan = heap_beginscan(info->rel_host, SnapshotNow, 0, NULL);
		else
		{
			host_name = DirectFunctionCall1(namein, PG_GETARG_DATUM(0));
			ScanKeyInit(&key[0]
				,Anum_mgr_host_hostname
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,host_name);
			info->rel_scan = heap_beginscan(info->rel_host, SnapshotNow, 1, key);
		}
		
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
		heap_close(info->rel_host, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_host = (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);

	/* test is running ? */
	ma = ma_connect_hostoid(HeapTupleGetOid(tup));
	if(!ma_isconnected(ma))
	{
		tup_result = build_common_command_tuple(&(mgr_host->hostname)
			, true, _("not running"));
		ma_close(ma);
	}else
	{
		initStringInfo(&message);
		initStringInfo(&(getAgentCmdRst.description));
		/*send cmd*/
		ma_beginmessage(&message, AGT_MSG_COMMAND);
		ma_sendbyte(&message, cmdtype);
		ma_sendstring(&message, "stop agent");
		ma_endmessage(&message, ma);
		if (!ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);
			tup_result = build_common_command_tuple(&(mgr_host->hostname)
			, getAgentCmdRst.ret, getAgentCmdRst.description.data);
		}
		else
		{
			mgr_recv_msg(ma, &getAgentCmdRst);
			ma_close(ma);
			/*check stop agent result*/
			retry = 0;
			while (retry++ < retrymax)
			{
				/*sleep 0.2s, wait the agent process to be killed, max try retrymax times*/
				usleep(200000);
				ma = ma_connect_hostoid(HeapTupleGetOid(tup));
				if(!ma_isconnected(ma))
				{
					getAgentCmdRst.ret = 1;
					resetStringInfo(&(getAgentCmdRst.description));
					appendStringInfoString(&(getAgentCmdRst.description), run_success);
					ma_close(ma);
					break;
				}
				else
				{
					getAgentCmdRst.ret = 0;
					resetStringInfo(&(getAgentCmdRst.description));
					appendStringInfoString(&(getAgentCmdRst.description), "stop agent fail");
					ma_close(ma);
				}
			}
			tup_result = build_common_command_tuple(
				&(mgr_host->hostname)
				, getAgentCmdRst.ret == 0 ? false:true
				, getAgentCmdRst.description.data);
			if(getAgentCmdRst.description.data)
				pfree(getAgentCmdRst.description.data);
		}
	}

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}
