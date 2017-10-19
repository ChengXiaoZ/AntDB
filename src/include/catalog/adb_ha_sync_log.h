/*-------------------------------------------------------------------------
 *
 * adb_ha_sync_log.h
 *	  definition of the system "adb_ha_sync_log" relation (adb_ha_sync_log)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2016 ADB Development Group
 *
 * src/include/catalog/adb_ha_sync_log.h
 *-------------------------------------------------------------------------
 */
#ifndef ADB_HA_SYNC_LOG_H
#define ADB_HA_SYNC_LOG_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"
#define timestamptz int
#endif /* BUILD_BKI */

#define AdbHaSyncLogRelationId 5005

CATALOG(adb_ha_sync_log,5005) BKI_SHARED_RELATION
{
	int64			gxid;
	int32			cmdid;
	timestamptz		create_time;		/* the create time of current record */
	timestamptz		finish_time;		/* the sync time of current record */
	char			sql_gram;			/* the grammar of the record */
	char			sql_kind;			/* see below ADB_SQL_* */
	NameData		sql_schema;			/* current schema of the sql */
#ifdef CATALOG_VARLEN
	text			query_sql;			/* the sql string of current record */
	text			params;				/* the params of current sql */
#endif
} FormData_adb_ha_sync_log;

#ifndef BUILD_BKI
#undef timestamptz
#endif

typedef FormData_adb_ha_sync_log *Form_adb_ha_sync_log;

#define ADB_SQL_GRAM_DEFAULT		'P'			/* default is pg grammar */
#define ADB_SQL_GRAM_ORACLE			'O'			/* oracle grammar */

#define ADB_SQL_KIND_SIMPLE			'S'			/* simple sql via exec_simple_query */
#define ADB_SQL_KIND_EXECUTE		'E'			/* extension sql via exec_execute_message */

#define IsValidAdbSqlKind(kind) \
	((kind) == ADB_SQL_KIND_SIMPLE || \
	 (kind) == ADB_SQL_KIND_EXECUTE)

/* ----------------
 *		compiler constants for adb_ha_sync_log
 * ----------------
 */
#define Natts_adb_ha_sync_log				9
#define Anum_adb_ha_sync_log_gxid			1
#define Anum_adb_ha_sync_log_cmdid			2
#define Anum_adb_ha_sync_log_create_time	3
#define Anum_adb_ha_sync_log_finish_time	4
#define Anum_adb_ha_sync_log_sql_gram		5
#define Anum_adb_ha_sync_log_sql_kind		6
#define Anum_adb_ha_sync_log_sql_schema		7
#define Anum_adb_ha_sync_log_query_sql		8
#define Anum_adb_ha_sync_log_params			9

extern void AddAdbHaSyncLog(TimestampTz create_time,
							ParseGrammar sql_gram,
							char sql_kind,
							const char *query_sql,
							ParamListInfo params);

extern bool AdbHaSyncLogWalkerPortal(Portal portal);

#endif /* ADB_HA_SYNC_LOG_H */

