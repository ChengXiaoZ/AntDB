
#ifndef MONITOR_JOB_H
#define MONITOR_JOB_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#define timestamptz int64
#endif /* BUILD_BKI */

#define MjobRelationId 4918


CATALOG(monitor_job,4918)
{
	NameData			job_name;
	NameData			host_name;
	NameData			node_name;
	timestamptz		start_time;
	timestamptz		next_time;
	int32					interval_time;
	bool					job_status;
#ifdef CATALOG_VARLEN
	text					job_path;
	text					job_desc;
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
#define Natts_monitor_job								9
#define Anum_monitor_job_job_name				1
#define Anum_monitor_job_host_name			2
#define Anum_monitor_job_node_name			3
#define Anum_monitor_job_start_time			4
#define Anum_monitor_job_next_time			5
#define Anum_monitor_job_interval_time	6
#define Anum_monitor_job_job_status			7
#define Anum_monitor_job_job_path				8
#define Anum_monitor_job_job_desc				9


#endif /* MONITOR_JOB_H */
