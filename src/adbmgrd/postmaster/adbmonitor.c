/*
 * adbmonitor.c
 *
 * ADB Integrated Monitor Daemon
 *
 * The ADB monitor system is structured in two different kind of processes: the
 * monitor launcher and the monitor worker. The launcher is an always-running
 * process, started by postmaster when the monitor GUC parameter is set. The
 * launcher schedules monitor workers to be started when appropriate. The workers
 * are the processes which execute the actual monitor job determined in the
 * launcher.
 *
 * The monitor launcher cannot start the worker processes by itself, because
 * doing so would cause robustness issues (namely, failure to shut
 * them down on exceptional conditions, and also, since the launcher is
 * connected to shared memory and is thus subject to corruption there, it is
 * not as robust as the postmaster).  So it leaves that task to the postmaster.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2017 ADB Development Group
 *
 * IDENTIFICATION
 *	  src/adbmgrd/postmaster/adbmonitor.c
 */
#include "postgres.h"
	 
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/xact.h"
#include "lib/ilist.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/adbmonitor.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "access/heapam.h"
#include "catalog/monitor_job.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "storage/spin.h"

/* Default database */
#define DEFAULT_DATABASE	"postgres"

/*
 * GUC parameters
 */
bool	adbmonitor_start_daemon = false;
#if defined(ADB_MONITOR_POOL)
int		adbmonitor_max_workers;
#else
int		adbmonitor_probable_workers;
#endif
int		adbmonitor_naptime;

/* Flags to tell if we are in an adb monitor process */
static bool am_adbmonitor_launcher = false;
static bool am_adbmonitor_worker = false;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGUSR2 = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* struct to keep track of monitor job in launcher */
typedef struct AmlJobData
{
	Oid				amj_id;					/* job oid */
	TimestampTz		amj_next_worker_tm;		/* next worker time */
} AmlJobData;

typedef struct AmlJobData *AmlJob;

/*-------------
 * This struct holds information about a single worker's whereabouts.  We keep
 * one in shared memory.
 *
 * wi_job		monitor job done by this 
 * wi_proc		pointer to PGPROC of the running worker, NULL if not started
 * wi_launchtime Time at which this worker was launched
 *
 * All fields are protected by AdbmonitorLock.
 *-------------
 */
typedef struct WorkerInfoData
{
#if defined(ADB_MONITOR_POOL)
	dlist_node	wi_links;
#endif
	Oid			wi_job;
	PGPROC	   *wi_proc;
	pid_t		wi_launcherpid;
	TimestampTz wi_launchtime;
} WorkerInfoData;

typedef struct WorkerInfoData *WorkerInfo;

/*
 * Possible signals received by the launcher from remote processes.  These are
 * stored atomically in shared memory so that other processes can set them
 * without locking.
 */
typedef enum
{
	AdbMntForkFailed,			/* failed trying to start a worker */
	AdbMntNumSignals			/* must be last */
}	AdbMonitorSignal;

/*-------------
 * The main adb monitor shmem struct.  On shared memory we store this main
 * struct. This struct keeps:
 *
 * am_signal		set by other processes to indicate various conditions
 * am_launcherpid	the PID of the adb monitor launcher
 * am_startingWorker pointer to WorkerInfo currently being started (cleared by
 *					the worker itself as soon as it's up and running)
 *
 * This struct is protected by AdbmonitorLock, except for am_signal.
 *-------------
 */
typedef struct
{
	sig_atomic_t am_signal[AdbMntNumSignals];
	pid_t		am_launcherpid;
#if defined(ADB_MONITOR_POOL)
	dlist_head	am_freeWorkers;
#endif
	WorkerInfo	am_startingWorker;
} AdbMonitorShmemStruct;

static AdbMonitorShmemStruct *AdbMonitorShmem;

/* Memory context for long-lived data */
static MemoryContext AdbMntMemCxt;

/* Current monitor job */
static AmlJobData CurrentAmlJobData = {0, 0};

/* Pointer to my own WorkerInfo, valid on each worker */
#if !defined(ADB_MONITOR_POOL)
static WorkerInfoData MyWorkerInfoData = {InvalidOid, NULL, 0, 0};
#endif
static WorkerInfo MyWorkerInfo = NULL;

/* PID of launcher, valid only in worker while shutting down */
int AdbMonitorLauncherPid = 0;

NON_EXEC_STATIC void AdbMntLauncherMain(int argc, char *argv[]) __attribute__((noreturn));
NON_EXEC_STATIC void AdbMntWorkerMain(int argc, char *argv[]) __attribute__((noreturn));

static void launcher_determine_sleep(bool canlaunch, struct timeval * nap);
static AmlJob launcher_obtain_amljob(void);
static void launch_worker(TimestampTz now, AmlJob amljob);
static void rebuild_job_htab(void);
static void aml_sighup_handler(SIGNAL_ARGS);
static void aml_sigusr2_handler(SIGNAL_ARGS);
static void aml_sigterm_handler(SIGNAL_ARGS);
static void FreeWorkerInfo(int code, Datum arg);
static void do_monitor_job(Oid jobid);
static bool IsMonitorJobRunning(Oid jobid);
static bool monitor_job_get_last_timetz(TimestampTz *time);
static void print_workers(void);
#ifdef DEBUG_ADB
static void OutputMonitorJob(void);
#endif

/*
 * Main entry point for adb monitor launcher process, to be called from the
 * postmaster.
 */
int
StartAdbMntLauncher(void)
{
	pid_t		AdbMonitorPID;

	switch ((AdbMonitorPID = fork_process()))
	{
		case -1:
			ereport(LOG,
				 (errmsg("could not fork adb monitor launcher process: %m")));
			return 0;

		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Lose the postmaster's on-exit routines */
			on_exit_reset();

			AdbMntLauncherMain(0, NULL);
			break;

		default:
			return (int) AdbMonitorPID;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * Main loop for the adb monitor launcher process.
 */
NON_EXEC_STATIC void
AdbMntLauncherMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	const char *dbname = DEFAULT_DATABASE;

	/* we are a postmaster subprocess now */
	IsUnderPostmaster = true;
	am_adbmonitor_launcher = true;

	/* reset MyProcPid */
	MyProcPid = getpid();

	/* record Start Time for logging */
	MyStartTime = time(NULL);

	/* Identify myself via ps */
	init_ps_display("adb monitor launcher process", "", "", "");

	ereport(LOG,
			(errmsg("adb monitor launcher started")));

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	SetProcessingMode(InitProcessing);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.  (adb monitor probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, aml_sighup_handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, aml_sigterm_handler);

	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, aml_sigusr2_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	InitPostgres(dbname, InvalidOid, NULL, NULL);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	AdbMntMemCxt = AllocSetContextCreate(TopMemoryContext,
										  "ADB Monitor Launcher",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(AdbMntMemCxt);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		QueryCancelPending = false;		/* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(AdbMntMemCxt);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(AdbMntMemCxt);

		/* don't leave dangling pointers to freed memory */
		//DatabaseListCxt = NULL;
		//dlist_init(&DatabaseList);

		/*
		 * Make sure pgstat also considers our stat data as gone.
		 */
		pgstat_clear_snapshot();

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* if in shutdown mode, no need for anything further; just go away */
		if (got_SIGTERM)
			goto shutdown;

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild_job_htab */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Force zero_damaged_pages OFF in the adb monitor process, even if it is set
	 * in postgresql.conf.  We don't really want such a dangerous option being
	 * applied non-interactively.
	 */
	SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force statement_timeout and lock_timeout to zero to avoid letting these
	 * settings prevent regular maintenance from being executed.
	 */
	SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force default_transaction_isolation to READ COMMITTED.  We don't want
	 * to pay the overhead of serializable mode, nor add any risk of causing
	 * deadlocks or delaying other transactions.
	 */
	SetConfigOption("default_transaction_isolation", "read committed",
					PGC_SUSET, PGC_S_OVERRIDE);

	if (!AdbMonitoringActive())
		proc_exit(0);			/* done */

	AdbMonitorShmem->am_launcherpid = MyProcPid;

	/*
	 * Create the initial job hash table
	 */
	rebuild_job_htab();

	/* loop until shutdown request */
	while (!got_SIGTERM)
	{
		struct timeval nap;
		TimestampTz current_time = 0;
		bool		can_launch;
		int			rc;
		AmlJob		amljobtmp;

		/*
		 * This loop is a bit different from the normal use of WaitLatch,
		 * because we'd like to sleep before the first launch of a child
		 * process.  So it's WaitLatch, then ResetLatch, then check for
		 * wakening conditions.
		 */
#if defined(ADB_MONITOR_POOL)
		launcher_determine_sleep(!dlist_is_empty(&AdbMonitorShmem->am_freeWorkers),
								 &nap);
#else
		launcher_determine_sleep(true, &nap);
#endif
		LWLockAcquire(AdbmonitorLock, LW_EXCLUSIVE);
		amljobtmp = launcher_obtain_amljob();
		LWLockRelease(AdbmonitorLock);
		/* Allow singal catchup interrupts while sleeping */
		EnableCatchupInterrupt();
		if (!amljobtmp)
			continue;
		/*
		 * Wait until naptime expires or we get some type of signal (all the
		 * signal handlers will wake us by calling SetLatch).
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   (nap.tv_sec * 1000L) + (nap.tv_usec / 1000L));
		ResetLatch(&MyProc->procLatch);

		DisableCatchupInterrupt();

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* the normal shutdown case */
		if (got_SIGTERM)
			break;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* shutdown requested in config file? */
			if (!AdbMonitoringActive())
				break;

			/* rebuild the job hash table in case the naptime changed */
			rebuild_job_htab();
		}

		/*
		 * a worker finished, or postmaster signalled failure to start a
		 * worker
		 */
		if (got_SIGUSR2)
		{
			got_SIGUSR2 = false;

			if (AdbMonitorShmem->am_signal[AdbMntForkFailed])
			{
				/*
				 * If the postmaster failed to start a new worker, we sleep
				 * for a little while and resend the signal.  The new worker's
				 * state is still in memory, so this is sufficient.  After
				 * that, we restart the main loop.
				 *
				 * XXX should we put a limit to the number of times we retry?
				 * I don't think it makes much sense, because a future start
				 * of a worker will continue to fail in the same way.
				 */
				AdbMonitorShmem->am_signal[AdbMntForkFailed] = false;
				pg_usleep(1000000L);	/* 1s */
				SendPostmasterSignal(PMSIGNAL_START_ADBMNT_WORKER);
				continue;
			}
		}

		current_time = GetCurrentTimestamp();

		LWLockAcquire(AdbmonitorLock, LW_SHARED);

#if defined(ADB_MONITOR_POOL)
		can_launch = !dlist_is_empty(&AdbMonitorShmem->am_freeWorkers);
#else
		can_launch = true;
#endif
		if (AdbMonitorShmem->am_startingWorker != NULL)
		{
			int			waittime;
			WorkerInfo	worker = AdbMonitorShmem->am_startingWorker;
			/*
			 * We can't launch another worker when another one is still
			 * starting up (or failed while doing so), so just sleep for a bit
			 * more; that worker will wake us up again as soon as it's ready.
			 * We will only wait adbmonitor_naptime seconds (up to a maximum
			 * of 60 seconds) for this to happen however.  Note that failure
			 * to connect to a particular database is not a problem here,
			 * because the worker removes itself from the startingWorker
			 * pointer before trying to connect.  Problems detected by the
			 * postmaster (like fork() failure) are also reported and handled
			 * differently.  The only problems that may cause this code to
			 * fire are errors in the earlier sections of AdbMntWorkerMain,
			 * before the worker removes the WorkerInfo from the
			 * startingWorker pointer.
			 */
			waittime = Min(adbmonitor_naptime, 60) * 1000;
			if (TimestampDifferenceExceeds(worker->wi_launchtime, current_time,
										   waittime))
			{
				LWLockRelease(AdbmonitorLock);
				LWLockAcquire(AdbmonitorLock, LW_EXCLUSIVE);

				/*
				 * No other process can put a worker in starting mode, so if
				 * startingWorker is still INVALID after exchanging our lock,
				 * we assume it's the same one we saw above (so we don't
				 * recheck the launch time).
				 */
				if (AdbMonitorShmem->am_startingWorker != NULL)
				{
					worker = AdbMonitorShmem->am_startingWorker;
					worker->wi_job = InvalidOid;
					worker->wi_proc = NULL;
					worker->wi_launcherpid = 0;
					worker->wi_launchtime = 0;
#if defined(ADB_MONITOR_POOL)
					dlist_push_head(&AdbMonitorShmem->am_freeWorkers,
									&worker->wi_links);
#endif
					AdbMonitorShmem->am_startingWorker = NULL;
					elog(WARNING, "worker took too long to start; canceled");
				}
			}
			else
				can_launch = false;
		}
		LWLockRelease(AdbmonitorLock);	/* either shared or exclusive */

		/* if we can't do anything, just go back to sleep */
		if (!can_launch)
			continue;

		/*
		 * We're OK to start a new worker
		 */
		launch_worker(current_time, amljobtmp);
	}

	/* Normal exit from the adb monitor launcher is here */
shutdown:
	ereport(LOG,
			(errmsg("adb monitor launcher shutting down")));
	AdbMonitorShmem->am_launcherpid = 0;

	proc_exit(0);				/* done */
}

/*
 * Determine the time to sleep, based on the jobs.
 */
static void
launcher_determine_sleep(bool canlaunch, struct timeval * nap)
{
	print_workers();

	if (!canlaunch)
	{
		nap->tv_sec = adbmonitor_naptime;
		nap->tv_usec = 0;
	}
	/*
	 * TODO:
	 * find the job which next work time is the smallest.
	 */
	else if (!dlist_is_empty(&AdbMonitorShmem->am_freeWorkers))
	{
		long		secs;
		int			usecs;
		TimestampTz current_time = GetCurrentTimestamp();
		TimestampTz next_wakeup = GetCurrentTimestamp();
		if (monitor_job_get_last_timetz(&next_wakeup))
		{
			TimestampDifference(current_time, next_wakeup, &secs, &usecs);
			nap->tv_sec = secs;
			nap->tv_usec = usecs;
		}
		else
		{
			nap->tv_sec = adbmonitor_naptime;
			nap->tv_usec = 0;
		}
	}
	else
	{
		// job item is empty, sleep for whole adbmonitor_naptime seconds 
		nap->tv_sec = adbmonitor_naptime;
		nap->tv_usec = 0;
	}
}

static AmlJob
launcher_obtain_amljob(void)
{
	/*
	 * TODO:
	 * Return an monitor job will be launched now or
	 * NULL if there is no appropriate monitor job.
	 */
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tup;
	Form_monitor_job monitor_job;
	TimestampTz timetz;
	TimestampTz current_time;
	TimestampTz timetzMin = 0;
	Oid amjobOid = InvalidOid;
	
	print_workers();
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel_node = heap_open(MjobRelationId, AccessExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
	while ((tup = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		monitor_job = (Form_monitor_job) GETSTRUCT(tup);
		Assert(monitor_job);
				
		/* Job is invalid */
		if (!monitor_job->job_status)
			continue;
		current_time = GetCurrentTimestamp();
		/* Get the last time which need execute */
		if (current_time < monitor_job->next_time)
			continue;
		/* The job is running? */
		if (IsMonitorJobRunning(HeapTupleGetOid(tup)))
			continue;
		/* Find the latest monitor job */
		timetz = monitor_job->next_time;
		if (!timetzMin)
		{
			timetzMin = timetz;
			amjobOid = HeapTupleGetOid(tup);
		} else if (timetz < timetzMin)
		{
			timetzMin = timetz;
			amjobOid = HeapTupleGetOid(tup);
		}
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, AccessExclusiveLock);
	CommitTransactionCommand();

	if (OidIsValid(amjobOid))
	{
		CurrentAmlJobData.amj_id = amjobOid;
		CurrentAmlJobData.amj_next_worker_tm = timetzMin;
		return &CurrentAmlJobData;
	}

	return NULL;
}

static void
launch_worker(TimestampTz now, AmlJob amljob)
{
	WorkerInfo	worker;
#if defined(ADB_MONITOR_POOL)
	/*
	 * Check for free worker. 
	 */
	LWLockAcquire(AdbmonitorLock, LW_SHARED);
	if (dlist_is_empty(&AdbMonitorShmem->am_freeWorkers))
	{
		LWLockRelease(AdbmonitorLock);
		return ;
	}
	LWLockRelease(AdbmonitorLock);
#endif

	if (amljob)
	{
#if defined(ADB_MONITOR_POOL)
		dlist_node *wptr;
#endif
		AssertArg(OidIsValid(amljob->amj_id));

		LWLockAcquire(AdbmonitorLock, LW_EXCLUSIVE);

#if defined(ADB_MONITOR_POOL)
		wptr = dlist_pop_head_node(&AdbMonitorShmem->am_freeWorkers);

		worker = dlist_container(WorkerInfoData, wi_links, wptr);
#else
		worker = (WorkerInfo) ((char *) AdbMonitorShmem +
								MAXALIGN(sizeof(AdbMonitorShmemStruct)));
#endif
		worker->wi_job = amljob->amj_id;
		worker->wi_proc = NULL;
		worker->wi_launchtime = GetCurrentTimestamp();

		AdbMonitorShmem->am_startingWorker = worker;
		LWLockRelease(AdbmonitorLock);

		SendPostmasterSignal(PMSIGNAL_START_ADBMNT_WORKER);
	}
}

static void
rebuild_job_htab(void)
{
	/*
	 * TODO:
	 * Scan job catalog then build job hash table,
	 * be care about job's next work tiemstamptz.
	 */
}

/*
 * Called from postmaster to signal a failure to fork a process to become
 * worker.  The postmaster should kill(SIGUSR2) the launcher shortly
 * after calling this function.
 */
void
AdbMntWorkerFailed(void)
{
	AdbMonitorShmem->am_signal[AdbMntForkFailed] = true;
}


/* SIGHUP: set flag to re-read config file at next convenient time */
static void
aml_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void
aml_sigusr2_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGTERM: time to die */
static void
aml_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * AdbMonitoringActive
 *		Check GUC vars and report whether the adb monitor process should be
 *		running.
 */
bool
AdbMonitoringActive(void)
{
	return adbmonitor_start_daemon;
}

/*
 * IsAdbMonitor functions
 *		Return whether this is either a launcher adb monitor process or a worker
 *		process.
 */
bool
IsAdbMonitorLauncherProcess(void)
{
	return am_adbmonitor_launcher;
}

bool
IsAdbMonitorWorkerProcess(void)
{
	return am_adbmonitor_worker;
}


/*
 * AdbMonitorShmemSize
 *		Compute space needed for adbmonitor-related shared memory
 */
Size
AdbMonitorShmemSize(void)
{
	Size		size;

	/*
	 * Need the fixed struct and one WorkerInfoData.
	 */
	size = sizeof(AdbMonitorShmemStruct);
	size = MAXALIGN(size);
#if defined(ADB_MONITOR_POOL)
	size = add_size(size, mul_size(adbmonitor_max_workers,
								   sizeof(WorkerInfoData)));
#else
	size = add_size(size, sizeof(WorkerInfoData));
#endif
	return size;
}

/*
 * AdbMonitorShmemInit
 *		Allocate and initialize adbmonitor-related shared memory
 */
void
AdbMonitorShmemInit(void)
{
	bool		found;

	AdbMonitorShmem = (AdbMonitorShmemStruct *)
		ShmemInitStruct("AdbMonitor Data",
						AdbMonitorShmemSize(),
						&found);

	if (!IsUnderPostmaster)
	{
		WorkerInfo	worker;
#if defined(ADB_MONITOR_POOL)
		int i;
#endif

		Assert(!found);

		AdbMonitorShmem->am_launcherpid = 0;
#if defined(ADB_MONITOR_POOL)
		dlist_init(&AdbMonitorShmem->am_freeWorkers);
#endif
		AdbMonitorShmem->am_startingWorker = NULL;

		worker = (WorkerInfo) ((char *) AdbMonitorShmem +
							   MAXALIGN(sizeof(AdbMonitorShmemStruct)));
#if defined(ADB_MONITOR_POOL)
		/* initialize the WorkerInfo free list */
		for (i = 0; i < adbmonitor_max_workers; i++)
			dlist_push_head(&AdbMonitorShmem->am_freeWorkers,
							&worker[i].wi_links);
#else
		worker->wi_job = 0;
		worker->wi_proc = NULL;
		worker->wi_launcherpid = 0;
		worker->wi_launchtime = 0;
#endif
	}
	else
		Assert(found);
}

/********************************************************************
 *					  ADB MONITOR WORKER CODE
 ********************************************************************/
int
StartAdbMntWorker(void)
{
	pid_t		worker_pid;

	switch ((worker_pid = fork_process()))
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork adb monitor worker process: %m")));
			return 0;

		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Lose the postmaster's on-exit routines */
			on_exit_reset();

			AdbMntWorkerMain(0, NULL);
			break;

		default:
			return (int) worker_pid;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * AdbMntWorkerMain
 */
NON_EXEC_STATIC void
AdbMntWorkerMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	const char *dbname = DEFAULT_DATABASE;
	Oid			jobid;

	/* we are a postmaster subprocess now */
	IsUnderPostmaster = true;
	am_adbmonitor_worker = true;

	/* reset MyProcPid */
	MyProcPid = getpid();

	/* record Start Time for logging */
	MyStartTime = time(NULL);

	/* Identify myself via ps */
	init_ps_display("adb monitor worker process", "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.  (adn monitor probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 *
	 * Currently, we don't pay attention to postgresql.conf changes that
	 * happen during a single daemon iteration, so we can ignore SIGHUP.
	 */
	pqsignal(SIGHUP, SIG_IGN);

	/*
	 * SIGINT is used to signal canceling the current monitor job, SIGTERM
	 * means abort and exit cleanly, and SIGQUIT means abandon ship.
	 */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.  Note that because we called InitProcess, a
		 * callback was registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/*
	 * Force zero_damaged_pages OFF in the adb monitor process, even if it is set
	 * in postgresql.conf.  We don't really want such a dangerous option being
	 * applied non-interactively.
	 */
	SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force statement_timeout and lock_timeout to zero to avoid letting these
	 * settings prevent regular maintenance from being executed.
	 */
	SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force default_transaction_isolation to READ COMMITTED.  We don't want
	 * to pay the overhead of serializable mode, nor add any risk of causing
	 * deadlocks or delaying other transactions.
	 */
	SetConfigOption("default_transaction_isolation", "read committed",
					PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force synchronous replication off to allow regular maintenance even if
	 * we are waiting for standbys to connect. This is important to ensure we
	 * aren't blocked from performing anti-wraparound tasks.
	 */
	if (synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
		SetConfigOption("synchronous_commit", "local",
						PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Get the info about the monitor job we're going to work on.
	 */
	LWLockAcquire(AdbmonitorLock, LW_EXCLUSIVE);

	/*
	 * beware of startingWorker being INVALID; this should normally not
	 * happen, but if a worker fails after forking and before this, the
	 * launcher might have decided to remove it from the queue and start
	 * again.
	 */
	if (AdbMonitorShmem->am_startingWorker != NULL)
	{
#if defined(ADB_MONITOR_POOL)
		MyWorkerInfo = AdbMonitorShmem->am_startingWorker;
		jobid = MyWorkerInfo->wi_job;
#else
		WorkerInfo ShmemWorker = AdbMonitorShmem->am_startingWorker;
		MyWorkerInfo = &MyWorkerInfoData;
		MyWorkerInfo->wi_job = jobid = ShmemWorker->wi_job;
		MyWorkerInfo->wi_launchtime = ShmemWorker->wi_launchtime;
#endif
		MyWorkerInfo->wi_proc = MyProc;
		MyWorkerInfo->wi_launcherpid = AdbMonitorShmem->am_launcherpid;

		/*
		 * remove from the "starting" pointer, so that the launcher can start
		 * a new worker if required
		 */
		AdbMonitorShmem->am_startingWorker = NULL;
		LWLockRelease(AdbmonitorLock);

		on_shmem_exit(FreeWorkerInfo, 0);

		/* wake up the launcher */
		if (AdbMonitorShmem->am_launcherpid != 0)
			kill(AdbMonitorShmem->am_launcherpid, SIGUSR2);
	}
	else
	{
		/* no worker entry for me, go away */
		elog(WARNING, "adb monitor worker started without a worker entry");
		jobid = InvalidOid;
		LWLockRelease(AdbmonitorLock);
	}

	if (OidIsValid(jobid))
	{
		char jobstr[16] = {0};
		snprintf(jobstr, sizeof(jobstr), "job %u", jobid);

		InitPostgres(dbname, InvalidOid, NULL, NULL);
		SetProcessingMode(NormalProcessing);
		set_ps_display(jobstr, false);
		ereport(DEBUG1,
				(errmsg("adb monitor is processing job \"%u\"", jobid)));

		if (PostAuthDelay)
			pg_usleep(PostAuthDelay * 1000000L);

		do_monitor_job(jobid);
	}

	/*
	 * The launcher will be notified of my death in ProcKill, *if* we managed
	 * to get a worker slot at all
	 */

	/* All done, go away */
	proc_exit(0);
}

/*
 * Return a WorkerInfo to the free list
 */
static void
FreeWorkerInfo(int code, Datum arg)
{
	if (MyWorkerInfo != NULL)
	{
#if defined(ADB_MONITOR_POOL)
		LWLockAcquire(AdbmonitorLock, LW_EXCLUSIVE);

		AdbMonitorLauncherPid = AdbMonitorShmem->am_launcherpid;

		dlist_delete(&MyWorkerInfo->wi_links);
#else
		AdbMonitorLauncherPid = MyWorkerInfo->wi_launcherpid;
#endif

		MyWorkerInfo->wi_job = InvalidOid;
		MyWorkerInfo->wi_proc = NULL;
		MyWorkerInfo->wi_launcherpid = 0;
		MyWorkerInfo->wi_launchtime = 0;
#if defined(ADB_MONITOR_POOL)
		dlist_push_head(&AdbMonitorShmem->am_freeWorkers,
						&MyWorkerInfo->wi_links);
#endif
		/* not mine anymore */
		MyWorkerInfo = NULL;

#if defined(ADB_MONITOR_POOL)
		LWLockRelease(AdbmonitorLock);
#endif
	}
}

static void
do_monitor_job(Oid jobid)
{
#ifdef DEBUG_ADB
	OutputMonitorJob();
#endif

	/*
	 * TODO
	 */
	/*refresh next_time of job table*/
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tup;
	HeapTuple newtuple;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	TupleDesc tupledsc;
	TimestampTz current_time;
	TimestampTz next_time;
	Form_monitor_job monitor_job;

	pg_usleep(15 * 1000000L);
	
	StartTransactionCommand();
	(void) GetTransactionSnapshot();	
	rel_node = heap_open(MjobRelationId, AccessShareLock);
	tupledsc = RelationGetDescr(rel_node);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
	while((tup = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (HeapTupleGetOid(tup) != jobid)
			continue;

		monitor_job = (Form_monitor_job) GETSTRUCT(tup);
		Assert(monitor_job);
		MemSet(datum, 0, sizeof(datum));
		MemSet(isnull, 0, sizeof(isnull));
		MemSet(got, 0, sizeof(got));
		current_time = GetCurrentTimestamp();
#ifdef HAVE_INT64_TIMESTAMP
		next_time = current_time + (monitor_job->interval_time)*1000000L;
#else
		next_time = current_time + monitor_job->interval_time;
#endif
		datum[Anum_monitor_job_next_time-1] = TimestampTzGetDatum(next_time);
		got[Anum_monitor_job_next_time-1] = true;
		newtuple = heap_modify_tuple(tup, tupledsc, datum,isnull, got);	
		simple_heap_update(rel_node, &(tup->t_self), newtuple);
		CatalogUpdateIndexes(rel_node, newtuple);

		break;
	}

	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);
	CommitTransactionCommand();
}

#ifdef DEBUG_ADB
static void
OutputMonitorJob(void)
{
	if (MyWorkerInfo != NULL)
	{
		StringInfoData	buf;
		Oid jobid = MyWorkerInfo->wi_job;

		initStringInfo(&buf);
		appendStringInfo(&buf, "Monitor job: %u", jobid);
		appendStringInfo(&buf, " Launchtime: %s", timestamptz_to_str(MyWorkerInfo->wi_launchtime));

		/*
		 * TODO:
		 * Print something about the PGPROC
		 */
		
		/*
		 * TODO:
		 * Print something about the job
		 */

		ereport(LOG,
			(errmsg("%s", buf.data)));

		pfree(buf.data);
	}
}
#endif

static bool IsMonitorJobRunning(Oid jobid)
{
	WorkerInfo	workers;
	int			i;

	workers = (WorkerInfo) ((char *) AdbMonitorShmem +
							   MAXALIGN(sizeof(AdbMonitorShmemStruct)));
	for (i = 0; i < adbmonitor_max_workers; i++)
	{
		if (workers[i].wi_job == jobid)
			return true;
	}

	return false;
}

/*get the last  tiem to run*/
static bool monitor_job_get_last_timetz(TimestampTz *time)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tup;
	Form_monitor_job monitor_job;
	TimestampTz current_time = GetCurrentTimestamp();
	TimestampTz timetzMin = GetCurrentTimestamp()<<1;
	TimestampTz timetz = current_time;
	bool bget = false;
	
	StartTransactionCommand();
	(void) GetTransactionSnapshot();
	rel_node = heap_open(MjobRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 0, NULL);
	while((tup = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		monitor_job = (Form_monitor_job)GETSTRUCT(tup);
		Assert(monitor_job);
		/*get the least time*/
		if (!monitor_job->job_status)
			continue;
		if (IsMonitorJobRunning(HeapTupleGetOid(tup)))
		{
			continue;
		}
		if(current_time >= monitor_job->start_time)
			timetz = monitor_job->next_time;
		if (timetz < timetzMin)
				timetzMin = timetz;
		bget = true;
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	CommitTransactionCommand();

	if (bget)
		*time = timetzMin;
	return bget;
}

static void print_workers(void)
{
	WorkerInfo	workers;
	int			i;

	workers = (WorkerInfo) ((char *) AdbMonitorShmem +
							MAXALIGN(sizeof(AdbMonitorShmemStruct)));
	for (i = 0; i < adbmonitor_max_workers; i++)
		elog(DEBUG1, "workers[%d] %d is running", i, workers[i].wi_job);
}
