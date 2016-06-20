
#ifndef MONITOR_HOST_H
#define MONITOR_HOST_H

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

#define MonitorHostRelationId 4921

CATALOG(monitor_host,4921)
{
	Oid 			host_oid;				/* host name */
	int16 			mh_run_state;			/* host run state */
	timestamptz 	mh_begin_run_time;		/* host begin run time */
	int16			mh_cpu_core_total;		/* host cpu total cores */
	int16			mh_cpu_core_available;	/* host cpu available cores */
	
#ifdef CATALOG_VARLEN
	text			mh_system;				/* host system */
	text			mh_platform_type;		/* host plateform type */
#endif /* CATALOG_VARLEN */
} FormData_monitor_host;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 *		Form_mgr_host corresponds to a pointer to a tuple with
 *		the format of mgr_host relation.
 * ----------------
 */
typedef FormData_monitor_host *Form_monitor_host;

/* ----------------
 *		compiler constants for monitor_host
 * ----------------
 */
#define Natts_monitor_host							7
#define Anum_monitor_host_host_oid					1
#define Anum_monitor_host_mh_run_state				2
#define Anum_monitor_host_mh_begin_run_time			3
#define Anum_monitor_host_mh_cpu_core_total			4
#define Anum_monitor_host_mh_cpu_core_available		5
#define Anum_monitor_host_mh_system					6
#define Anum_monitor_host_mh_platform_type			7


#endif /* MONITOR_HOST_H */
