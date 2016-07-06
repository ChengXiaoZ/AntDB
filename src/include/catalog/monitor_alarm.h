
#ifndef MONITOR_ALARM_H
#define MONITOR_ALARM_H

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

#define MonitorAlarmRelationId 5209

CATALOG(monitor_alarm,5209)
{
	int16           ma_alarm_level;       /* alarm level:1(warning),2(critical),3(emergency) */
	int16           ma_alarm_type;        /* alarm type:1(host),2(database) */
	timestamptz     ma_alarm_timetz;      /* alarm time:timestamp with timezone */
	int16           ma_alarm_status;      /* alarm status: 1(unsolved),2(resolved) */
#ifdef CATALOG_VARLEN
	text            ma_alarm_source;      /* alarm source: ip addr or other string */
	text            ma_alarm_text;        /* alarm text */
#endif /* CATALOG_VARLEN */
} FormData_monitor_alarm;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
* Form_mgr_alarm corresponds to a pointer to a tuple with
* the format of FormData_monitor_alarm relation.
* ----------------
*/
typedef FormData_monitor_alarm *Form_monitor_alarm;

/* ----------------
* compiler constants for Form_monitor_alarm
* ----------------
*/
#define Natts_monitor_alarm                          6
#define Anum_monitor_alarm_ma_alarm_level            1
#define Anum_monitor_alarm_ma_alarm_type             2
#define Anum_monitor_alarm_ma_alarm_timetz           3
#define Anum_monitor_alarm_ma_alarm_status           4
#define Anum_monitor_alarm_ma_alarm_source           5
#define Anum_monitor_alarm_ma_alarm_text             6


#endif /* MONITOR_ALARM_H */
