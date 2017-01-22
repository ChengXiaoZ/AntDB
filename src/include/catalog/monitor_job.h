
#ifndef MONITOR_JOB_H
#define MONITOR_JOB_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#endif /* BUILD_BKI */

#ifdef HAVE_INT64_TIMESTAMP
#define timestamptz int64
#else
#define timestamptz int32
#endif

#define MjobRelationId 4918
CATALOG(monitor_job,4918)
{
	NameData				job_name;
	timestamptz			next_time;
	int32						interval_time;
	bool						job_status;
#ifdef CATALOG_VARLEN
	text						job_command;
	text						job_desc;
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
#define Natts_monitor_job												6
#define Anum_monitor_job_job_name								1
#define Anum_monitor_job_next_time							2
#define Anum_monitor_job_interval_time					3
#define Anum_monitor_job_job_status							4
#define Anum_monitor_job_job_job_command				5
#define Anum_monitor_job_job_desc								6

#endif /* MONITOR_JOB_H */
