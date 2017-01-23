
#ifndef MONITOR_JOB_H
#define MONITOR_JOB_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#define timestamptz int
#endif /* BUILD_BKI */

/* #ifdef HAVE_INT64_TIMESTAMP
#define timestamptz int64
#else
#define timestamptz int32
#endif */

#define MjobRelationId 4918
CATALOG(monitor_job,4918)
{
	NameData				name;
	timestamptz			next_time;
	int32						interval;
	bool						status;
#ifdef CATALOG_VARLEN
	text						command;
	text						desc;
#endif
} FormData_monitor_job;

/* ----------------
 *		Form_monitor_job corresponds to a pointer to a tuple with
 *		the format of monitor_job relation.
 * ----------------
 */
typedef FormData_monitor_job *Form_monitor_job;

/* ----------------
 *		compiler constants for monitor_job
 * ----------------
 */
#define Natts_monitor_job									6
#define Anum_monitor_job_name							1
#define Anum_monitor_job_nexttime					2
#define Anum_monitor_job_interval					3
#define Anum_monitor_job_status						4
#define Anum_monitor_job_command					5
#define Anum_monitor_job_desc							6

#endif /* MONITOR_JOB_H */
