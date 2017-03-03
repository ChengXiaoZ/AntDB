/*-------------------------------------------------------------------------
 *
 * pause.c
 *
 *	 Cluster Pause/Unpause handling
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifdef ADB

#include "postgres.h"
#include "pgxc/execRemote.h"
#include "pgxc/pause.h"
#include "pgxc/pgxc.h"
#include "storage/spin.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolmgr.h"
#include "pgxc/nodemgr.h"
#include "nodes/makefuncs.h"

/* globals */
bool cluster_lock_held;
bool cluster_ex_lock_held;
char *pause_cluster_str = "SELECT PG_PAUSE_CLUSTER()";
char *unpause_cluster_str = "SELECT PG_UNPAUSE_CLUSTER()";

static void HandleClusterPause(bool pause, bool initiator);
static void ProcessClusterPauseRequest(bool pause);

ClusterLockInfo *ClustLinfo = NULL;

/*
 * ProcessClusterPauseRequest:
 *
 * Carry out select pg_pause_cluster()/select pg_unpause_cluster() request on a coordinator node
 */
static void
ProcessClusterPauseRequest(bool pause)
{
	char *action = pause? pause_cluster_str:unpause_cluster_str;

	if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The \"%s\" message is expected to "
						"arrive at a coordinator from another coordinator",
						action)));

	elog(DEBUG2, "Received \"%s\" from a coordinator", action);

	/*
	 * If calling unpause, ensure that the cluster lock has already been held
	 * in exclusive mode
	 */
	if (!pause && !cluster_ex_lock_held)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Received an \"%s\" request when cluster not \"%s\"!", unpause_cluster_str, pause_cluster_str)));

	/*
	 * Enable/Disable local queries. We need to release the lock first
	 *
	 * TODO: Think of some timeout mechanism here, if the locking takes too
	 * much time...
	 */
	ReleaseClusterLock(pause? false:true);
	AcquireClusterLock(pause? true:false);

	if (pause)
		cluster_ex_lock_held = true;
	else
		cluster_ex_lock_held = false;

	elog(DEBUG2, "%s queries at the coordinator", pause? "Paused":"Resumed");

	return;
}

/*
 * HandleClusterPause:
 *
 * Any errors will be reported via ereport.
 */
static void
HandleClusterPause(bool pause, bool initiator)
{
	PGXCNodeAllHandles *coord_handles;
	int conn;
	int response;
	char *action = pause? pause_cluster_str:unpause_cluster_str;

	elog(DEBUG2, "Preparing coordinators for \"%s\"", action);

	if (pause && cluster_ex_lock_held)
	{
		ereport(NOTICE, (errmsg("cluster already paused")));

		/* Nothing to do */
		return;
	}

	if (!pause && !cluster_ex_lock_held)
	{
		ereport(NOTICE, (errmsg("Issue \"%s\" before calling \"%s\"", pause_cluster_str, unpause_cluster_str)));

		/* Nothing to do */
		return;
	}

	/*
	 * If we are one of the participating coordinators, just do the action
	 * locally and return
	 */
	if (!initiator)
	{
		ProcessClusterPauseRequest(pause);
		return;
	}
	if(pause)
		PoolManagerSetCommand(POOL_CMD_TEMP, NULL);

	/*
	 * Send SELECT PG_PAUSE_CLUSTER()/SELECT PG_UNPAUSE_CLUSTER() message to all the coordinators. We should send an
	 * asyncronous request, update the local ClusterLock and then wait for the remote
	 * coordinators to respond back
	 */

	coord_handles = get_handles(NIL, GetAllCoordNodes(), true);

	for (conn = 0; conn < coord_handles->co_conn_count; conn++)
	{
		PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

		if (pgxc_node_send_query(handle, pause ? pause_cluster_str : unpause_cluster_str) != 0)
			ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send \"%s\" request to some coordinator nodes",action)));
	}

	/*
	 * Disable/Enable local queries. We need to release the SHARED mode first
	 *
	 * TODO: Start a timer to cancel the request in case of a timeout
	 */
	ReleaseClusterLock(pause? false:true);
	AcquireClusterLock(pause? true:false);

	if (pause)
		cluster_ex_lock_held = true;
	else
		cluster_ex_lock_held = false;


	elog(DEBUG2, "%s queries at the driving coordinator", pause? "Paused":"Resumed");

	/*
	 * Local queries are paused/enabled. Check status of the remote coordinators
	 * now. We need a TRY/CATCH block here, so that if one of the coordinator
	 * fails for some reason, we can try best-effort to salvage the situation
	 * at others
	 *
	 * We hope that errors in the earlier loop generally do not occur (out of
	 * memory and improper handles..) or we can have a similar TRY/CATCH block
	 * there too
	 *
	 * To repeat: All the salvaging is best effort really...
	 */
	PG_TRY();
	{
		RemoteQueryState 	*combiner;
		combiner = CreateResponseCombiner(coord_handles->co_conn_count, COMBINE_TYPE_NONE);
		for (conn = 0; conn < coord_handles->co_conn_count; conn++)
		{
			PGXCNodeHandle *handle;

			handle = coord_handles->coord_handles[conn];

			while (true)
			{
				if (pgxc_node_receive(1, &handle, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to receive a response from the remote coordinator node")));

				response = handle_response(handle, combiner);
				if (response == RESPONSE_EOF)
					continue;
				else if (response == RESPONSE_COMPLETE)
					break;
				else if (response == RESPONSE_TUPDESC)
					continue;
				else if (response == RESPONSE_DATAROW)
					continue;
 				else
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("\"%s\" command failed "
									"with error %s", action, handle->error)));
			}
			/* throw away message */
			if (combiner->currentRow.msg)
			{
				pfree(combiner->currentRow.msg);
				combiner->currentRow.msg = NULL;
			}
		}

 		if (!combiner->errorMessage.data)
		{
			char *code = combiner->errorCode;
			if (combiner->errorDetail != NULL)
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", combiner->errorMessage.data), errdetail("%s", combiner->errorDetail) ));
			else
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", combiner->errorMessage.data)));
		}
		
		CloseCombiner(combiner);

	}
	PG_CATCH();
	{
		/*
		 * If "SELECT PG_PAUSE_CLUSTER()", issue "SELECT PG_UNPAUSE_CLUSTER()" on the reachable nodes. For failure
		 * in cases of unpause, might need manual intervention at the offending
		 * coordinator node (maybe do a pg_cancel_backend() on the backend
		 * that's holding the exclusive lock or something..)
		 */
		if (!pause)
			ereport(WARNING,
				 (errmsg("\"%s\" command failed on one or more coordinator nodes."
						" Manual intervention may be required!", unpause_cluster_str)));
		else
			ereport(WARNING,
				 (errmsg("\"%s\" command failed on one or more coordinator nodes."
						" Trying to \"%s\" reachable nodes now", pause_cluster_str, unpause_cluster_str)));

		for (conn = 0; conn < coord_handles->co_conn_count && pause; conn++)
		{
			PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

			(void) pgxc_node_send_query(handle, unpause_cluster_str);

			/*
			 * The incoming data should hopefully be discarded as part of
			 * cleanup..
			 */
		}

		/* cleanup locally.. */
		ReleaseClusterLock(true);
		AcquireClusterLock(false);
		cluster_ex_lock_held = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(DEBUG2, "Successfully completed \"%s\" command on "
				 "all coordinator nodes", action);

	return;
}

Datum
pg_pause_cluster(PG_FUNCTION_ARGS)
{
	bool pause = true;
	char	*action = pause ? pause_cluster_str : unpause_cluster_str;
	bool	 initiator = true;

	elog(DEBUG2, "\"%s\" request received", action);
	/* Only a superuser can perform this activity on a cluster */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("\"%s\" command: must be a superuser", action)));

	/* Ensure that we are a coordinator */
	if (!IS_PGXC_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("\"%s\" command must be sent to a coordinator", action)));

	/*
	 * Did the command come directly to this coordinator or via another
	 * coordinator?
	 */
	if (IsConnFromCoord())
		initiator = false;
	HandleClusterPause(pause, initiator);

	PG_RETURN_BOOL(true);
}

Datum
pg_unpause_cluster(PG_FUNCTION_ARGS)
{
	bool pause = false;
	char	*action = pause ? pause_cluster_str : unpause_cluster_str;
	bool	 initiator = true;

	elog(DEBUG2, "\"%s\" request received", action);

	/* Only a superuser can perform this activity on a cluster */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("\"%s\" command: must be a superuser", action)));

	/* Ensure that we are a coordinator */
	if (!IS_PGXC_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("\"%s\" command must be sent to a coordinator", action)));

	/*
	 * Did the command come directly to this coordinator or via another
	 * coordinator?
	 */
	if (IsConnFromCoord())
		initiator = false;

	HandleClusterPause(pause, initiator);

	PG_RETURN_BOOL(true);
}

/*
 * If the backend is shutting down, cleanup the PAUSE cluster lock
 * appropriately. We do this before shutting down shmem, because this needs
 * LWLock and stuff
 */
void
PGXCCleanClusterLock(int code, Datum arg)
{
	PGXCNodeAllHandles *coord_handles;
	int conn;

	cluster_lock_held = false;

	/* Do nothing if cluster lock not held */
	if (!cluster_ex_lock_held)
		return;
	/* Do nothing if we are not the initiator */
	if (IsConnFromCoord())
	{
		if (cluster_ex_lock_held)
			ReleaseClusterLock(true);
		return;
	}

	coord_handles = get_handles(NIL, GetAllCoordNodes(), true);
	/* Try best-effort to UNPAUSE other coordinators now */
	for (conn = 0; conn < coord_handles->co_conn_count; conn++)
	{
		PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

		/* No error checking here... */
		(void)pgxc_node_send_query(handle, unpause_cluster_str);
			
		
	}

	/* Release locally too. We do not want a dangling value in cl_holder_pid! */
	ReleaseClusterLock(true);
	cluster_ex_lock_held = false;
}

/* Report shared memory space needed by ClusterLockShmemInit */
Size
ClusterLockShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(ClusterLockInfo));

	return size;
}

/* Allocate and initialize cluster locking related shared memory */
void
ClusterLockShmemInit(void)
{
	bool		found;

	ClustLinfo = (ClusterLockInfo *)
		ShmemInitStruct("Cluster Lock Info", ClusterLockShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(ClustLinfo, 0, ClusterLockShmemSize());
		SpinLockInit(&ClustLinfo->cl_mutex);
	}
}

/*
 * AcquireClusterLock
 *
 *  Based on the argument passed in, try to update the shared memory
 *  appropriately. In case the conditions cannot be satisfied immediately this
 *  function resorts to a simple sleep. We don't envision PAUSE CLUSTER to
 *  occur that frequently so most of the calls will come out immediately here
 *  without any sleeps at all
 *
 *  We could have used a semaphore to allow the processes to sleep while the
 *  cluster lock is held. But again we are really not worried about performance
 *  and immediate wakeups around PAUSE CLUSTER functionality. Using the sleep
 *  in an infinite loop keeps things simple yet correct
 */
void
AcquireClusterLock(bool exclusive)
{
	volatile ClusterLockInfo *clinfo = ClustLinfo;

	if (exclusive && cluster_ex_lock_held)
	{
		return;
	}

	/*
	 * In the normal case, none of the backends will ask for exclusive lock, so
	 * they will just update the cl_process_count value and exit immediately
	 * from the below loop
	 */
	for (;;)
	{
		bool wait = false;

		SpinLockAcquire(&clinfo->cl_mutex);

		if (!exclusive)
		{
			if (clinfo->cl_holder_pid != 0)
				wait = true;
		}
		else /* pause cluster handling */
		{
			if (clinfo->cl_holder_pid != 0)
			{
				SpinLockRelease(&clinfo->cl_mutex);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("pause cluster already in progress")));
			}

			/*
			 * There should be no other process
			 * holding the lock including ourself
			 */
				clinfo->cl_holder_pid = MyProcPid;
		}
		SpinLockRelease(&clinfo->cl_mutex);

		/*
		 * We use a simple sleep mechanism. If pause cluster has been invoked,
		 * we are not worried about immediate performance characteristics..
		 */
		if (wait)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(100000L);
		}
		else /* Got the proper semantic read/write lock.. */
			break;
	}
}

/*
 * ReleaseClusterLock
 *
 * 		Update the shared memory appropriately across the release call. We
 * 		really do not need the bool argument, but it's there for some
 * 		additional sanity checking
 */
void
ReleaseClusterLock(bool exclusive)
{
	volatile ClusterLockInfo *clinfo = ClustLinfo;

	if (!exclusive)
		return;
	SpinLockAcquire(&clinfo->cl_mutex);
	if (exclusive)
	{
		/*
		 * Reset the holder pid. Any waiters in AcquireClusterLock will
		 * eventually come out of their sleep and notice this new value and
		 * move ahead
		 */
		clinfo->cl_holder_pid = 0;
	}
	SpinLockRelease(&clinfo->cl_mutex);
}

Datum pg_alter_node(PG_FUNCTION_ARGS)
{
	/*name, port, host, primary, preferred*/
	char *node_host;
	int		node_port;
	bool	node_preferred;
	AlterNodeStmt *nodestmt;

	node_host = PG_GETARG_CSTRING(1);
	node_port = PG_GETARG_INT32(2);
	node_preferred = PG_GETARG_BOOL(3);
	nodestmt = makeNode(AlterNodeStmt);
	nodestmt->node_name = PG_GETARG_CSTRING(0);
	nodestmt->options = lappend(nodestmt->options, makeDefElem("port", (Node *)makeInteger(node_port)));
	nodestmt->options = lappend(nodestmt->options, makeDefElem("host", (Node *)makeString(node_host)));
	nodestmt->options = lappend(nodestmt->options, makeDefElem("preferred", (Node *)makeInteger(node_preferred)));
	
	PgxcNodeAlter(nodestmt);
	
	PG_RETURN_BOOL(true);
}

#endif
