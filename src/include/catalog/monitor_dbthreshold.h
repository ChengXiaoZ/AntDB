
#ifndef MONITOR_DBTHRESHOLD_H
#define MONITOR_DBTHRESHOLD_H

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

#define MonitorDbThresholdRelationId 4963

CATALOG(monitor_dbthreshold,4963)
{
		int16				dbthresholdtype;                			 /* host alarm type */ 
		int32				dbthresholdnodewarning;  	 /* warning threshold, More than this value will alarm */
		int32				dbthresholdnodecritical;  	/* critical threshold, More than this value will alarm */
		int32				dbthresholdnodeemergency; 	/* emergency threshold, More than this value will alarm */
		int32				dbthresholdclusterwarning;   /* warning threshold, More than this value will alarm */
		int32				dbthresholdclustercritical;  /* critical threshold, More than this value will alarm */
		int32				dbthresholdclusteremergency; /* emergency threshold, More than this value will alarm */
} FormData_monitor_dbthreshold;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 * FormData_monitor_dbthreshold corresponds to a pointer to a tuple with
 * the format of Form_monitor_dbthreshold relation.
 * ----------------
 */
typedef FormData_monitor_dbthreshold *Form_monitor_dbthreshold;

/* ----------------
 * compiler constants for monitor_dbthreshold
 * ----------------
 */
#define Natts_monitor_dbthreshold											4
#define Anum_monitor_dbthreshold_type									1
#define Anum_monitor_dbthreshold_nodewarning					2
#define Anum_monitor_dbthreshold_nodecritical					3
#define Anum_monitor_dbthreshold_nodeemergency				4
#define Anum_monitor_dbthreshold_clusterwarning				5
#define Anum_monitor_dbthreshold_clustercritical			6
#define Anum_monitor_dbthreshold_clusteremergency			7

#endif /* MONITOR_DBTHRESHOLD_H */
