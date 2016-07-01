
#ifndef MONITOR_HOST_THRESHLOD_H
#define MONITOR_HOST_THRESHLOD_H

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

#define MonitorHostThreshlodRelationId 4927

CATALOG(monitor_host_threshlod,4927)
{
    int16       mt_type;                /* host alarm type */ 
    int16       mt_warning_threshold;   /* warning threshold, More than this value will alarm */
    int16       mt_critical_threshold;  /* critical threshold, More than this value will alarm */
    int16       mt_emergency_threshold; /* emergency threshold, More than this value will alarm */
} FormData_monitor_host_threshlod;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 * FormData_monitor_host_threshlod corresponds to a pointer to a tuple with
 * the format of Form_monitor_host_threshlod relation.
 * ----------------
 */
typedef FormData_monitor_host_threshlod *Form_monitor_host_threshlod;

/* ----------------
 * compiler constants for monitor_host_threshlod
 * ----------------
 */
#define Natts_monitor_host_threshlod                           4
#define Anum_monitor_host_threshlod_mt_type                    1
#define Anum_monitor_host_threshlod_mt_warning_threshold       2
#define Anum_monitor_host_threshlod_mt_critical_threshold      3
#define Anum_monitor_host_threshlod_mt_emergency_threshold     4

#endif /* MONITOR_HOST_THRESHLOD_H */
