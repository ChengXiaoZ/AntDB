
#ifndef MONITOR_CPU_H
#define MONITOR_CPU_H

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

#define MonitorCpuRelationId 4922

CATALOG(monitor_cpu,4922)
{
    NameData    hostname;           /* host name */
    timestamptz mc_timestamptz;     /* monitor cpu timestamptz */
    float4      mc_cpu_usage;       /* monitor cpu usage */

#ifdef CATALOG_VARLEN
    text        mc_cpu_freq;        /* monitor cpu frequency */
#endif /* CATALOG_VARLEN */
} FormData_monitor_cpu;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 *      Form_monitor_cpu corresponds to a pointer to a tuple with
 *      the format of moniotr_cpu relation.
 * ----------------
 */
typedef FormData_monitor_cpu *Form_monitor_cpu;

/* ----------------
 *      compiler constants for monitor_cpu
 * ----------------
 */
#define Natts_monitor_cpu                           4
#define Anum_monitor_cpu_host_name                  1
#define Anum_monitor_cpu_mc_timestamptz             2
#define Anum_monitor_cpu_mc_usage                   3
#define Anum_monitor_cpu_mc_freq                    4

#endif /* MONITOR_CPU_H */
