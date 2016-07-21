
#ifndef MONITOR_HOST_THRESHOLD_H
#define MONITOR_HOST_THRESHOLD_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"
#define timestamptz int
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define MonitorHostThresholdRelationId 4927

CATALOG(monitor_host_threshold,4927)
{
    int16       mt_type;                /* host alarm type */
    int16       mt_direction;              /*0 is '<', 1 is '>'*/
    int16       mt_warning_threshold;   /* warning threshold, More than this value will alarm */
    int16       mt_critical_threshold;  /* critical threshold, More than this value will alarm */
    int16       mt_emergency_threshold; /* emergency threshold, More than this value will alarm */
} FormData_monitor_host_threshold;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 * FormData_monitor_host_threshold corresponds to a pointer to a tuple with
 * the format of Form_monitor_host_threshold relation.
 * ----------------
 */
typedef FormData_monitor_host_threshold *Form_monitor_host_threshold;

/* ----------------
 * compiler constants for monitor_host_threshold
 * ----------------
 */
#define Natts_monitor_host_threshold                           5
#define Anum_monitor_host_threshold_mt_type                    1
#define Anum_monitor_host_threshold_mt_direction               2
#define Anum_monitor_host_threshold_mt_warning_threshold       3
#define Anum_monitor_host_threshold_mt_critical_threshold      4
#define Anum_monitor_host_threshold_mt_emergency_threshold     5

#endif /* MONITOR_HOST_THRESHOLD_H */
