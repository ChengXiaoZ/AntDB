
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
    NameData       	hostname;               /* host name */
    int16           mh_run_state;           /* host run state */
    timestamptz     mh_current_time;        /* host cuttent time */
    int64           mh_seconds_since_boot;  /* host seconds since boot */
    int16           mh_cpu_core_total;      /* host cpu total cores */
    int16           mh_cpu_core_available;  /* host cpu available cores */
    
#ifdef CATALOG_VARLEN
    text            mh_system;              /* host system */
    text            mh_platform_type;       /* host plateform type */
#endif /* CATALOG_VARLEN */
} FormData_monitor_host;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 *      Form_mgr_host corresponds to a pointer to a tuple with
 *      the format of mgr_host relation.
 * ----------------
 */
typedef FormData_monitor_host *Form_monitor_host;

/* ----------------
 *      compiler constants for monitor_host
 * ----------------
 */
#define Natts_monitor_host                          8
#define Anum_monitor_host_host_name                 1
#define Anum_monitor_host_mh_run_state              2
#define Anum_monitor_host_mh_current_time           3
#define Anum_monitor_host_mh_seconds_since_boot     4
#define Anum_monitor_host_mh_cpu_core_total         5
#define Anum_monitor_host_mh_cpu_core_available     6
#define Anum_monitor_host_mh_system                 7
#define Anum_monitor_host_mh_platform_type          8


#endif /* MONITOR_HOST_H */
