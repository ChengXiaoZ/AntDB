
#ifndef MONITOR_MEM_H
#define MONITOR_MEM_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define MonitorMemRelationId 4923

CATALOG(monitor_mem,4923)
{
	Oid 		host_oid;			/* host oid */
	timestamp	mm_timestamp;		/* monitor memory timestamp */
	int64		mm_total;			/* monitor memory total */
	int64		mm_used;			/* monitor memory used */
} FormData_monitor_mem;

/* ----------------
 *		Form_monitor_mem corresponds to a pointer to a tuple with
 *		the format of moniotr_mem relation.
 * ----------------
 */
typedef FormData_monitor_mem *Form_monitor_mem;

/* ----------------
 *		compiler constants for monitor_mem
 * ----------------
 */
#define Natts_monitor_mem						4
#define Anum_monitor_mem_host_oid				1
#define Anum_monitor_mem_mm_timestamp			2
#define Anum_monitor_mem_mm_total				3
#define Anum_monitor_mem_mm_used				4

#endif /* MONITOR_MEM_H */
