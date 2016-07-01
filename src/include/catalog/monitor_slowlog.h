
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
	timestamptz			monitor_slowlog_time;					/* monitor tps timestamp */
	NameData				monitor_slowlog_dbname;	      /*the database name*/
	NameData				monitor_slowlog_user;			
	float4					monitor_slowlog_singletime;   /*single query one time*/
	int32						monitor_slowlog_totalnum;			/*how many num the query has runned*/
#ifdef CATALOG_VARLEN
	text						monitor_slowlog_query;				/*the query*/
	text						monitor_slowlog_queryplan;    /*plan for the query*/
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
#define Anum_monitor_slowlog_time							1
#define Anum_monitor_slowlog_dbname						2
#define Anum_monitor_slowlog_user							3
#define Anum_monitor_slowlog_singletime				4
#define Anum_monitor_slowlog_totalnum					5
#define Anum_monitor_slowlog_query						6
#define Anum_monitor_slowlog_queryplan				7

#endif /* MONITOR_SLOWLOG_H */
