/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_database.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/adb_ha_sync_log.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "pgxc/pgxc.h"
#include "storage/bufmgr.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

extern bool enable_adb_ha_sync;
extern bool enable_adb_ha_sync_select;
extern char *adb_ha_param_delimiter;

static MemoryContext AdbHaSyncLogContext = NULL;

static int time2tm(TimeADT time, struct pg_tm * tm, fsec_t *fsec);
static int timetz2tm(TimeTzADT *time, struct pg_tm * tm, fsec_t *fsec, int *tzp);
static void deparsePgTmOutString(struct pg_tm *tm, fsec_t fsec, StringInfo buf);
static char* deparseParamOutString(ParamExternData *prm);
static void deparseParamListInfo(ParamListInfo params, StringInfo buf);
static bool WalkerAlterSeqValFunc(Node *node, void *context);
static bool WalkerSelectPortal(Portal portal);
static bool WalkerMultiQueryPortal(Portal portal);

#define IsNormalDatabase()	(MyDatabaseId > TemplateDbOid)

#define CanAdbHaSync() \
	(enable_adb_ha_sync && \
	IS_PGXC_COORDINATOR && \
	!IsConnFromCoord() && \
	IsNormalDatabase() )

void
AddAdbHaSyncLog(TimestampTz create_time,
				ParseGrammar sql_gram,
				char sql_kind,
				const char *query_sql,
				ParamListInfo params)
{
	Relation	adbharel;
	HeapTuple	htup;
	char		grammar = ADB_SQL_GRAM_DEFAULT;
	Datum		values[Natts_adb_ha_sync_log];
	bool		nulls[Natts_adb_ha_sync_log];
	CommandId	cmdid;
	TransactionId gxid;
	char		*schemaname;
	MemoryContext oldContext;

	if (!CanAdbHaSync())
		return ;

	if (TransactionBlockStatusCode() == 'E')
		return ;

	Assert(IsValidAdbSqlKind(sql_kind));

	switch (sql_gram)
	{
		case PARSE_GRAM_ORACLE:
			grammar = ADB_SQL_GRAM_ORACLE;
			break;
		default:
			break;
	}

	if (AdbHaSyncLogContext == NULL)
	{
		AdbHaSyncLogContext = AllocSetContextCreate(TopMemoryContext,
													"AdbHaSyncLogContext",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);
	} else
	{
		MemoryContextResetAndDeleteChildren(AdbHaSyncLogContext);
	}

	oldContext = MemoryContextSwitchTo(AdbHaSyncLogContext);
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

	gxid = GetCurrentTransactionId();
	cmdid = GetCurrentCommandId(false);
	values[Anum_adb_ha_sync_log_gxid - 1] = Int64GetDatum(gxid);
	values[Anum_adb_ha_sync_log_cmdid - 1] = CommandIdGetDatum(cmdid);
	values[Anum_adb_ha_sync_log_create_time - 1] = TimestampTzGetDatum(create_time);
	nulls[Anum_adb_ha_sync_log_finish_time - 1] = true;
	values[Anum_adb_ha_sync_log_sql_gram - 1] = CharGetDatum(grammar);	
	values[Anum_adb_ha_sync_log_sql_kind - 1] = CharGetDatum(sql_kind);

	schemaname = get_current_schema();
	if (schemaname == NULL)
	{
		nulls[Anum_adb_ha_sync_log_sql_schema - 1] = true;
	} else
	{
		NameData	schemadata;
		namestrcpy(&schemadata, (const char *) schemaname);
		values[Anum_adb_ha_sync_log_sql_schema - 1] = NameGetDatum(&schemadata);
	}

	if (query_sql)
		values[Anum_adb_ha_sync_log_query_sql - 1] = CStringGetTextDatum(query_sql);
	else
		nulls[Anum_adb_ha_sync_log_query_sql - 1] = true;

	if (params)
	{
		StringInfoData buf;
		initStringInfo(&buf);
		deparseParamListInfo(params, &buf);
		if (buf.len != 0)
			values[Anum_adb_ha_sync_log_params - 1] = CStringGetTextDatum(buf.data);
		else
			nulls[Anum_adb_ha_sync_log_params - 1] = true;
		pfree(buf.data);
	} else
	{
		nulls[Anum_adb_ha_sync_log_params - 1] = true;
	}

	adbharel = heap_open(AdbHaSyncLogRelationId, RowExclusiveLock);

	htup = heap_form_tuple(RelationGetDescr(adbharel), values, nulls);

	(void) simple_heap_insert(adbharel, htup);

	CatalogUpdateIndexes(adbharel, htup);

	heap_close(adbharel, RowExclusiveLock);

	(void)MemoryContextSwitchTo(oldContext);

#ifdef DEBUG_ADB
	MemoryContextStats(AdbHaSyncLogContext);
#endif
	MemoryContextResetAndDeleteChildren(AdbHaSyncLogContext);
}

static int
time2tm(TimeADT time, struct pg_tm * tm, fsec_t *fsec)
{
#ifdef HAVE_INT64_TIMESTAMP
	tm->tm_hour = time / USECS_PER_HOUR;
	time -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = time / USECS_PER_MINUTE;
	time -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = time / USECS_PER_SEC;
	time -= tm->tm_sec * USECS_PER_SEC;
	*fsec = time;
#else
	double		trem;

recalc:
	trem = time;
	TMODULO(trem, tm->tm_hour, (double) SECS_PER_HOUR);
	TMODULO(trem, tm->tm_min, (double) SECS_PER_MINUTE);
	TMODULO(trem, tm->tm_sec, 1.0);
	trem = TIMEROUND(trem);
	/* roundoff may need to propagate to higher-order fields */
	if (trem >= 1.0)
	{
		time = ceil(time);
		goto recalc;
	}
	*fsec = trem;
#endif

	return 0;
}

static int
timetz2tm(TimeTzADT *time, struct pg_tm * tm, fsec_t *fsec, int *tzp)
{
	TimeOffset	trem = time->time;

#ifdef HAVE_INT64_TIMESTAMP
	tm->tm_hour = trem / USECS_PER_HOUR;
	trem -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = trem / USECS_PER_MINUTE;
	trem -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = trem / USECS_PER_SEC;
	*fsec = trem - tm->tm_sec * USECS_PER_SEC;
#else
recalc:
	TMODULO(trem, tm->tm_hour, (double) SECS_PER_HOUR);
	TMODULO(trem, tm->tm_min, (double) SECS_PER_MINUTE);
	TMODULO(trem, tm->tm_sec, 1.0);
	trem = TIMEROUND(trem);
	/* roundoff may need to propagate to higher-order fields */
	if (trem >= 1.0)
	{
		trem = ceil(time->time);
		goto recalc;
	}
	*fsec = trem;
#endif

	if (tzp != NULL)
		*tzp = time->zone;

	return 0;
}

static void
deparsePgTmOutString(struct pg_tm *tm, fsec_t fsec, StringInfo buf)
{
	Assert(tm && buf);

	/* year | month | day | hour | minute | second | nano-second | gmtoff */
	appendStringInfo(buf, "%d|%d|%d|%d|%d|%d|%ld|%ld",
						  tm->tm_year,
						  tm->tm_mon,
						  tm->tm_mday,
						  tm->tm_hour,
						  tm->tm_min,
						  tm->tm_sec,
#ifdef HAVE_INT64_TIMESTAMP
						  (long int)(fsec * 1000),
#else
						  (long int)(fsec * 1000000000),
#endif
						  tm->tm_gmtoff);
}

static char*
deparseParamOutString(ParamExternData *prm)
{
	Oid				typoutput;
	bool			typisvarlena;
	char			*pstring = NULL;
	char			*p = NULL;
	StringInfoData	buf;
	struct pg_tm	tm = {0,0,0,0,0,0,0,0,0,0,NULL};
	fsec_t			fsec = 0;
	int				tz;

	Assert(prm);

	if (prm->isnull || !OidIsValid(prm->ptype))
		return pstrdup("NULL");

	initStringInfo(&buf);

	appendStringInfoCharMacro(&buf, '\'');
	switch (prm->ptype)
	{
		case DATEOID:
			{
				DateADT		date = DatumGetDateADT(prm->value);
				if (DATE_NOT_FINITE(date))
				{
					/* TODO: how to display infinity */
				} else
				{
					j2date(date + POSTGRES_EPOCH_JDATE,
						&(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
					deparsePgTmOutString(&tm, 0, &buf);
				}
			}
			break;
		case ORADATEOID:
		case TIMESTAMPOID:
			{
				Timestamp	timestamp = DatumGetTimestamp(prm->value);
				if (TIMESTAMP_NOT_FINITE(timestamp))
				{
					/* TODO: how to display infinity */
				} else
				{
					timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
					deparsePgTmOutString(&tm, fsec, &buf);
				}
			}
			break;
		case TIMESTAMPTZOID:
			{
				TimestampTz	dt = DatumGetTimestampTz(prm->value);
				if (TIMESTAMP_NOT_FINITE(dt))
				{
					/* TODO: how to display infinity */
				} else
				{
					timestamp2tm(dt, &tz, &tm, &fsec, NULL, NULL);
					deparsePgTmOutString(&tm, fsec, &buf);
				}
			}
			break;
		case INTERVALOID:
			{
				Interval   *span = DatumGetIntervalP(prm->value);
				interval2tm(*span, &tm, &fsec);
				deparsePgTmOutString(&tm, fsec, &buf);
			}
			break;
		case TIMEOID:
			{
				TimeADT		time = DatumGetTimeADT(prm->value);
				time2tm(time, &tm, &fsec);
				deparsePgTmOutString(&tm, fsec, &buf);
			}
			break;
		case TIMETZOID:
			{
				TimeTzADT  *time = DatumGetTimeTzADTP(prm->value);
				timetz2tm(time, &tm, &fsec, &tz);
				deparsePgTmOutString(&tm, fsec, &buf);
			}
			break;
		default:
			{
				getTypeOutputInfo(prm->ptype, &typoutput, &typisvarlena);
				pstring = OidOutputFunctionCall(typoutput, prm->value);

				for (p = pstring; *p; p++)
				{
					if (*p == '\'') /* double single quotes */
						appendStringInfoCharMacro(&buf, *p);
					appendStringInfoCharMacro(&buf, *p);
				}
				pfree(pstring);
			}
			break;
	}
	appendStringInfoCharMacro(&buf, '\'');

	return buf.data;
}

static void
deparseParamListInfo(ParamListInfo params, StringInfo buf)
{
	/* We mustn't call user-defined I/O functions when in an aborted xact */
	if (params && params->numParams > 0 &&
		!IsAbortedTransactionBlockState())
	{
		int			paramno;

		for (paramno = 0; paramno < params->numParams; paramno++)
		{
			ParamExternData *prm = &params->params[paramno];
			char			*pstring = NULL;

			appendStringInfo(buf, "%s$%d",
							 paramno > 0 ? adb_ha_param_delimiter : "",
							 paramno + 1);

			switch (prm->ptype)
			{
				case DATEOID:
				case ORADATEOID:
					appendStringInfo(buf, "(DATE) = ");
					break;
				case TIMESTAMPOID:
					appendStringInfo(buf, "(TIMESTAMP WITHOUT TIME ZONE) = ");
					break;
				case TIMESTAMPTZOID:
					appendStringInfo(buf, "(TIMESTAMP WITH TIME ZONE) = ");
					break;
				case INTERVALOID:
					appendStringInfo(buf, "(INTERVAL) = ");
					break;
				case TIMEOID:
					appendStringInfo(buf, "(TIME WITHOUT TIME ZONE) = ");
					break;
				case TIMETZOID:
					appendStringInfo(buf, "(TIME WITH TIME ZONE) = ");
					break;
				default:
					appendStringInfo(buf, " = ");
					break;
			}

			pstring = deparseParamOutString(prm);
			appendStringInfoString(buf, pstring);
			pfree(pstring);
		}
	}
}

static bool
WalkerAlterSeqValFunc(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, TargetEntry))
	{
		TargetEntry *tle = (TargetEntry *)node;
		if (IsA(tle->expr, FuncExpr))
		{
			FuncExpr *fexpr = (FuncExpr *)(tle->expr);
			if (fexpr->funcid == FUNC_NEXTVAL_OID ||
				fexpr->funcid == FUNC_SETVAL2_OID ||
				fexpr->funcid == FUNC_SETVAL3_OID)
			return true;
		}
	}

	return node_tree_walker(node, WalkerAlterSeqValFunc, context);
}

static bool
WalkerSelectPortal(Portal portal)
{
	ListCell	*lc = NULL;
	Node		*stmt = NULL;

	Assert(portal);

	foreach (lc, portal->stmts)
	{
		stmt = (Node *)lfirst(lc);
		if (WalkerAlterSeqValFunc(stmt, NULL))
			return true;
	}

	return false;
}

static bool
WalkerMultiQueryPortal(Portal portal)
{
	bool result = true;

	Assert(portal);
	if (list_length(portal->stmts) == 1)
	{
		Node *node = (Node *) linitial(portal->stmts);
		if (IsA(node, PlannedStmt))
		{
			PlannedStmt *stmt = (PlannedStmt *) node;
			switch(stmt->commandType)
			{
				case CMD_UPDATE:
				case CMD_INSERT:
				case CMD_DELETE:
					if (list_length(stmt->rtable) == 1)
					{
						RangeTblEntry *rte = (RangeTblEntry*)linitial(stmt->rtable);
						if (rte->relid == AdbHaSyncLogRelationId)
							result = false;
					}
					break;
				default:
					result = true;
					break;
			}
		} else
		if (IsA(node, TransactionStmt))
		{
			TransactionStmt *stmt = (TransactionStmt *) node;
			switch(stmt->kind)
			{
				case TRANS_STMT_ROLLBACK_PREPARED:
					result = false;
					break;
				default:
					result = true;
					break;
			}
		} else
		if (IsA(node, VariableSetStmt))
		{
			VariableSetStmt *stmt = (VariableSetStmt *) node;
			if (stmt->name != NULL && pg_strcasecmp(stmt->name, "search_path") == 0)
				result = false;
		}
	}

	return result;
}

bool
AdbHaSyncLogWalkerPortal(Portal portal)
{
	bool result = false;
	Assert(portal);

	if (!CanAdbHaSync())
		return false;

	switch (portal->strategy)
	{
		case PORTAL_ONE_SELECT:
			if (enable_adb_ha_sync_select)
				result = true;
			else
				result = WalkerSelectPortal(portal);
			break;
		case PORTAL_ONE_RETURNING:
		case PORTAL_ONE_MOD_WITH:
		case PORTAL_UTIL_SELECT:
			result = true;
			break;
		case PORTAL_MULTI_QUERY:
			result = WalkerMultiQueryPortal(portal);
			break;
		default:
			elog(ERROR,
				"unrecognized portal strategy: %d",
				(int) portal->strategy);
			break;
	}
	return result;
}

