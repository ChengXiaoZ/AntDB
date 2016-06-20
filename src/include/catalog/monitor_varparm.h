
#ifndef MONITOR_VARPARM_H
#define MONITOR_VARPARM_H

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

#define MonitorVarParmRelationId 4926

CATALOG(monitor_varparm,4926)
{
    int16       mv_cpu_threshold;   /* CPU threshold,More than this value will alarm */
    int16       mv_mem_threshold;   /* memory threshold,More than this value will alarm */
    int16       mv_disk_threshold;  /* disk threshold,More than this value will alarm */
} FormData_monitor_varparm;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 *      Form_monitor_varparm corresponds to a pointer to a tuple with
 *      the format of monitor_varparm relation.
 * ----------------
 */
typedef FormData_monitor_varparm *Form_monitor_varparm;

/* ----------------
 *      compiler constants for monitor_varparm
 * ----------------
 */
#define Natts_monitor_varparm                            3
#define Anum_monitor_varparm_mv_cpu_threshold            1
#define Anum_monitor_varparm_mv_mem_threshold            2
#define Anum_monitor_varparm_mv_disk_threshold           3


#endif /* MONITOR_VARPARM_H */
