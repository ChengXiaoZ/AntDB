
#ifndef MONITOR_SLOWLOG_H
#define MONITOR_SLOWLOG_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#define timestamptz int
#endif /* BUILD_BKI */

#define MslowlogRelationId 4954

CATALOG(monitor_slowlog,4954)
{
	NameData				slowlogdbname;				/*the database name*/
	NameData				slowloguser;
	float4					slowlogsingletime;			/*single query one time*/
	int32					slowlogtotalnum;			/*how many num the query has runned*/
	timestamptz				slowlogtime;				/* monitor tps timestamp */
#ifdef CATALOG_VARLEN
	text						slowlogquery;			/*the query*/
	text						slowlogqueryplan;		/*plan for the query*/
#endif /* CATALOG_VARLEN */	
} FormData_monitor_slowlog;

/* ----------------
 *		Form_monitor_slowlog corresponds to a pointer to a tuple with
 *		the format of monitor_slowlog relation.
 * ----------------
 */
typedef FormData_monitor_slowlog *Form_monitor_slowlog;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 *		compiler constants for monitor_slowlog
 * ----------------
 */
#define Natts_monitor_slowlog									7
#define Anum_monitor_slowlog_dbname								1
#define Anum_monitor_slowlog_user								2
#define Anum_monitor_slowlog_singletime							3
#define Anum_monitor_slowlog_totalnum							4
#define Anum_monitor_slowlog_time								5
#define Anum_monitor_slowlog_query								6
#define Anum_monitor_slowlog_queryplan							7

#endif /* MONITOR_SLOWLOG_H */
