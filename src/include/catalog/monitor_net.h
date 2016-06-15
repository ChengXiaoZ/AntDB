
#ifndef MONITOR_NET_H
#define MONITOR_NET_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define MonitorNetRelationId 4924

CATALOG(monitor_net,4924)
{
	Oid 		host_oid;			/* host oid */
	Timestamp	mn_timestamp;		/* monitor network timestamp */
	int64		mn_sent;			/* monitor network sent speed */
	int64		mn_recv;			/* monitor network recv speed */
} FormData_monitor_net;


/* ----------------
 *		Form_monitor_net corresponds to a pointer to a tuple with
 *		the format of moniotr_net relation.
 * ----------------
 */
typedef FormData_monitor_net *Form_monitor_net;

/* ----------------
 *		compiler constants for monitor_net
 * ----------------
 */
#define Natts_monitor_net						4
#define Anum_monitor_net_host_oid				1
#define Anum_monitor_net_mn_timestamp			2
#define Anum_monitor_net_mn_sent				3
#define Anum_monitor_net_mn_recv				4

#endif /* MONITOR_NET_H */
