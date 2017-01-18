/*-------------------------------------------------------------------------
 *
 * adbmonitor.h
 *	  header file for integrated adb monitor daemon
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2010-2017 ADB Development Group
 *
 * src/adbmgrd/include/postmaster/adbmonitor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADB_MONITOR_H
#define ADB_MONITOR_H

/* GUC variables */
extern bool adbmonitor_start_daemon;
extern int adbmonitor_max_workers;
extern int adbmonitor_naptime;

/* adb monitor launcher PID, only valid when worker is shutting down */
extern int AdbMonitorLauncherPid;

/* Status inquiry functions */
extern bool AdbMonitoringActive(void);
extern bool IsAdbMonitorLauncherProcess(void);
extern bool IsAdbMonitorWorkerProcess(void);

#define IsAnyAdbMonitorProcess() \
	(IsAdbMonitorLauncherProcess() || IsAdbMonitorWorkerProcess())

/* Functions to start adb monitor process, called from postmaster */
extern void adbmonitor_init(void);
extern int	StartAdbMntLauncher(void);
extern int	StartAdbMntWorker(void);

/* called from postmaster when a worker could not be forked */
extern void AdbMntWorkerFailed(void);

/* shared memory stuff */
extern Size AdbMonitorShmemSize(void);
extern void AdbMonitorShmemInit(void);

#endif /* ADB_MONITOR_H */

