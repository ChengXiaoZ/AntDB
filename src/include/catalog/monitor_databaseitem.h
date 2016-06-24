
#ifndef MONITOR_MULTEITEM_H
#define MONITOR_MULTEITEM_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#define timestamptz int
#endif /* BUILD_BKI */

#define MdatabaseitemRelationId 4952


CATALOG(monitor_databaseitem,4952)
{
	timestamptz		monitor_databaseitem_time;		/* monitor timestamp */
	NameData		monitor_databaseitem_dbname;
	int32			monitor_databaseitem_dbsize;
	bool			monitor_databaseitem_archivemode;
	bool			monitor_databaseitem_autovacuum;
	float4			monitor_databaseitem_heaphitrate;
	float4			monitor_databaseitem_commitrate;
	int32			monitor_databaseitem_dbage;
	int32			monitor_databaseitem_connectnum;
	int32			monitor_databaseitem_standbydelay;
	int32			monitor_databaseitem_locksnum;
	int32			monitor_databaseitem_longtransnum;
	int32			monitor_databaseitem_idletransnum;
	int32			monitor_databaseitem_preparenum;	
	int32			monitor_databaseitem_unusedindexnum;
	int32			monitor_databaseitem_indexsize;
} FormData_monitor_databaseitem;

/* ----------------
 *		Form_monitor_databaseitem corresponds to a pointer to a tuple with
 *		the format of monitor_databaseitem relation.
 * ----------------
 */
typedef FormData_monitor_databaseitem *Form_monitor_databaseitem;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 * compiler constants for monitor_databaseitem
 * ----------------
 */
#define Natts_monitor_databaseitem								16
#define Anum_monitor_databaseitem_time							1
#define Anum_monitor_databaseitem_dbname						2
#define Anum_monitor_databaseitem_dbsize						3
#define Anum_monitor_databaseitem_archivemode					4
#define Anum_monitor_databaseitem_autovacuum					5
#define Anum_monitor_databaseitem_heaphitrate					6
#define Anum_monitor_databaseitem_commitrate					7
#define Anum_monitor_databaseitem_dbage							8
#define Anum_monitor_databaseitem_connectnum					9
#define Anum_monitor_databaseitem_standbydelay					10
#define Anum_monitor_databaseitem_locksnum						11
#define Anum_monitor_databaseitem_longtransnum					12
#define Anum_monitor_databaseitem_idletransnum					13
#define Anum_monitor_databaseitem_preparenum					14
#define Anum_monitor_databaseitem_unusedindexnum				15
#define Anum_monitor_databaseitem_indexsize						16

#endif /* MONITOR_MULTEITEM_H */
