
#ifndef MONITOR_HOST_H
#define MONITOR_HOST_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define MonitorHostRelationId 4921

CATALOG(monitor_host,4921)
{
	Oid 			host_oid;				/* host name */
	inet			mh_ip_addr;				/* host ip address */
	int16 			mh_run_state;			/* host run state */
	timestamp 	mh_begin_run_time;		/* host begin run time */
	
#ifdef CATALOG_VARLEN
	text			mh_platform_type;		/* host plateform type */
	text			mh_cpu_type;			/* host cpu type */
#endif /* CATALOG_VARLEN */
} FormData_monitor_host;

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
#define Natts_monitor_host							6
#define Anum_monitor_host_host_oid					1
#define Anum_monitor_host_mh_ip_addr				2
#define Anum_monitor_host_mh_run_state				3
#define Anum_monitor_host_mh_begin_run_time			4
#define Anum_monitor_host_mh_platform_type			5
#define Anum_monitor_host_mh_cpu_type				6

#endif /* MONITOR_HOST_H */
