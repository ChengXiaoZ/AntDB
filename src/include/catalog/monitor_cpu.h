
#ifndef MONITOR_CPU_H
#define MONITOR_CPU_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define MonitorCpuRelationId 4922

CATALOG(monitor_cpu,4922)
{
	Oid 		host_oid;			/* host oid */
	Timestamp	mc_timestamp;		/* monitor cpu timestamp */
	float4		mc_cpu_usage;		/* monitor cpu usage */
} FormData_monitor_cpu;

/* ----------------
 *		Form_monitor_cpu corresponds to a pointer to a tuple with
 *		the format of moniotr_cpu relation.
 * ----------------
 */
typedef FormData_monitor_cpu *Form_monitor_cpu;

/* ----------------
 *		compiler constants for monitor_cpu
 * ----------------
 */
#define Natts_monitor_cpu							3
#define Anum_monitor_cpu_host_oid					1
#define Anum_monitor_cpu_mc_timestamp				2
#define Anum_monitor_cpu_mc_usage					3

#endif /* MONITOR_CPU_H */
