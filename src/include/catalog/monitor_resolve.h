
#ifndef MONITOR_RESOLVE_H
#define MONITOR_RESOLVE_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "catalog/genbki.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"
#define timestamptz int
#endif /* BUILD_BKI */

#define MonitorResolveRelationId 5021

CATALOG(monitor_resolve,5021)
{
	Oid             mr_alarm_oid;           /* table monitor alarm oid */
	timestamptz     mr_resolve_timetz;      /* alarm resolve time:timestamp with timezone */
#ifdef CATALOG_VARLEN
	text            mr_solution;            /* alarm solution */
#endif /* CATALOG_VARLEN */
} FormData_monitor_resolve;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
* FormData_monitor_resolve corresponds to a pointer to a tuple with
* the format of Form_monitor_resolve relation.
* ----------------
*/
typedef FormData_monitor_resolve *Form_monitor_resolve;

/* ----------------
* compiler constants for Form_monitor_resolve
* ----------------
*/
#define Natts_monitor_resolve                          3
#define Anum_monitor_resolve_mr_alarm_oid              1
#define Anum_monitor_resolve_mr_resolve_timetz         2
#define Anum_monitor_resolve_mr_solution               3

#endif /* MONITOR_RESOLVE_H */
