
#ifndef MONITOR_JOBITEM_H
#define MONITOR_JOBITEM_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#endif /* BUILD_BKI */

#define MjobitemRelationId 4920
CATALOG(monitor_jobitem,4920) BKI_WITHOUT_OIDS
{
	NameData				jobitem_itemname;
#ifdef CATALOG_VARLEN
	text						jobitem_path;
	text						jobitem_desc;
#endif
} FormData_monitor_jobitemitem;

/* ----------------
 *		Form_monitor_jobitemitem corresponds to a pointer to a tuple with
 *		the format of monitor_jobitem relation.
 * ----------------
 */
typedef FormData_monitor_jobitemitem *Form_monitor_jobitemitem;

/* ----------------
 *		compiler constants for monitor_jobitem
 * ----------------
 */
#define Natts_monitor_jobitem							3
#define Anum_monitor_jobitem_itemname					1
#define Anum_monitor_jobitem_path					2
#define Anum_monitor_jobitem_desc					3

#endif /* MONITOR_JOBITEM_H */
