/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remote_xact.h"
#include "access/rxact_mgr.h"
#include "access/twophase.h"
#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"

static void CommitPreparedRxact(const char *gid,
								int nnodes,
								Oid *nodeIds,
								bool isMissingOK);
static void AbortPreparedRxact(const char *gid,
							   int nnodes,
							   Oid *nodeIds,
							   bool missing_ok);

void
RemoteXactCommit(int nnodes, Oid *nodeIds)
{
	if (!IsUnderRemoteXact())
		return ;

	if (nnodes > 0)
		PreCommit_Remote(NULL, false);
	agtm_CommitTransaction(NULL, false);
}

void
RemoteXactAbort(int nnodes, Oid *nodeIds, bool normal)
{
	if (!IsUnderRemoteXact())
		return ;

	if (normal)
	{
		if (nnodes > 0)
			PreAbort_Remote(NULL, false);
		agtm_AbortTransaction(NULL, false);
	} else
	{
		AbnormalAbort_Remote();
		PG_TRY_HOLD();
		{
			agtm_AbortTransaction(NULL, false);
		} PG_CATCH_HOLD();
		{
			errdump();
			agtm_Close();
		} PG_END_TRY_HOLD();
	}
}

/*
 * StartFinishPreparedRxact
 *
 * It is used to record log in remote xact manager.
 */
void
StartFinishPreparedRxact(const char *gid,
						 int nnodes,
						 Oid *nodeIds,
						 bool isImplicit,
						 bool isCommit)
{
	if (!IsUnderRemoteXact())
		return ;

	AssertArg(gid && gid[0]);

	/*
	 * Record rollback prepared transaction log
	 */
	if (!isCommit)
	{
		RecordRemoteXact(gid, nodeIds, nnodes, RX_ROLLBACK);
		return ;
	}

	/*
	 * Remote xact manager has already recorded log
	 * if it is implicit commit prepared transaction.
	 *
	 * See EndRemoteXactPrepare.
	 */
	if (!isImplicit)
		RecordRemoteXact(gid, nodeIds, nnodes, RX_COMMIT);
}

/*
 * EndFinishPreparedRxact
 *
 * Here we trully commit/rollback prepare remote xact.
 */
void
EndFinishPreparedRxact(const char *gid,
					   int nnodes,
					   Oid *nodeIds,
					   bool isMissingOK,
					   bool isCommit)
{
	if (!IsUnderRemoteXact())
		return ;

	AssertArg(gid && gid[0]);

	if (isCommit)
		CommitPreparedRxact(gid, nnodes, nodeIds, isMissingOK);
	else
		AbortPreparedRxact(gid, nnodes, nodeIds, isMissingOK);
}

static void
CommitPreparedRxact(const char *gid,
					int nnodes,
					Oid *nodeIds,
					bool isMissingOK)
{
	volatile bool fail_to_commit = false;

	PG_TRY_HOLD();
	{
		/* Commit prepared on remote nodes */
		if (nnodes > 0)
			PreCommit_Remote(gid, isMissingOK);

		/* Commit prepared on AGTM */
		agtm_CommitTransaction(gid, isMissingOK);
	} PG_CATCH_HOLD();
	{
		AtAbort_Twophase();
		/* Record failed log */
		RecordRemoteXactFailed(gid, RX_COMMIT);
		/* Discard error data */
		errdump();
		fail_to_commit = true;
	} PG_END_TRY_HOLD();

	/* Return if success */
	if (!fail_to_commit)
	{
		/* Record success log */
		RecordRemoteXactSuccess(gid, RX_COMMIT);
		return ;
	}

	PG_TRY();
	{
		/* Fail to commit then wait for rxact to finish it */
		RxactWaitGID(gid);
	} PG_CATCH();
	{
		/*
		 * Fail again and exit current process
		 *
		 * ADBQ:
		 *		Does it cause crash of other processes?
		 *		Are there some resources not be released?
		 */
		proc_exit(1);
	} PG_END_TRY();
}

static void
AbortPreparedRxact(const char *gid,
				   int nnodes,
				   Oid *nodeIds,
				   bool isMissingOK)
{
	PG_TRY();
	{
		/* rollback prepared on remote nodes */
		if (nnodes > 0)
			PreAbort_Remote(gid, isMissingOK);

		/* rollback prepared on AGTM */
		agtm_AbortTransaction(gid, isMissingOK);
	} PG_CATCH();
	{
		/* Record FAILED log */
		RecordRemoteXactFailed(gid, RX_ROLLBACK);
		PG_RE_THROW();
	} PG_END_TRY();

	RecordRemoteXactSuccess(gid, RX_ROLLBACK);
}

