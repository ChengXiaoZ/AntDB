/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remote_xact.h"
#include "access/rxact_mgr.h"
#include "agtm/agtm.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"

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
RemoteXactAbort(int nnodes, Oid *nodeIds)
{
	if (!IsUnderRemoteXact())
		return ;

	if (nnodes > 0)
		PreAbort_Remote(NULL, false);
	agtm_AbortTransaction(NULL, false);
}

/*
 * RecordRemoteXactPrepare
 */
void
RecordRemoteXactPrepare(const char *gid,
						int nnodes,
						Oid *nodeIds,
						bool implicit)
{
	if (!IsUnderRemoteXact())
		return ;

	AssertArg(gid && gid[0]);

	/* Record PREPARE log */
	RecordRemoteXact(gid, nodeIds, nnodes, RX_PREPARE);

	PG_TRY();
	{
		/* Prepare on remote nodes */
		if (nnodes > 0)
			PrePrepare_Remote(gid);

		/* Prepare on AGTM */
		agtm_PrepareTransaction(gid);
	} PG_CATCH();
	{
		/* Record FAILED log */
		RecordRemoteXactFailed(gid, RX_PREPARE);
		PG_RE_THROW();
	} PG_END_TRY();

	/* Mark the remote xact will COMMIT or SUCCESS PREPARE */
	if (implicit)
		RecordRemoteXactChange(gid, RX_COMMIT);
	else
		RecordRemoteXactSuccess(gid, RX_PREPARE);
}

/*
 * RecordRemoteXactCommitPrepared
 */
void
RecordRemoteXactCommitPrepared(const char *gid,
							   int nnodes,
							   Oid *nodeIds,
							   bool missing_ok,
							   bool implicit)
{
	if (!IsUnderRemoteXact())
		return ;

	AssertArg(gid && gid[0]);

	/* Record COMMIT log */
	if (!implicit)
		RecordRemoteXact(gid, nodeIds, nnodes, RX_COMMIT);

	PG_TRY();
	{
		/* commit prepared on remote nodes */
		if (nnodes > 0)
			PreCommit_Remote(gid, missing_ok);

		/* commit prepared on AGTM */
		agtm_CommitTransaction(gid, missing_ok);
	} PG_CATCH();
	{
		/* Record FAILED log */
		RecordRemoteXactFailed(gid, RX_COMMIT);
		PG_RE_THROW();
	} PG_END_TRY();
}

/*
 * RecordRemoteXactAbortPrepared
 */
void
RecordRemoteXactAbortPrepared(const char *gid,
							  int nnodes,
							  Oid *nodeIds,
							  bool missing_ok)
{
	if (!IsUnderRemoteXact())
		return ;

	AssertArg(gid && gid[0]);

	/* Record ROLLBACK log */
	RecordRemoteXact(gid, nodeIds, nnodes, RX_ROLLBACK);

	PG_TRY();
	{
		/* rollback prepared on remote nodes */
		if (nnodes > 0)
			PreAbort_Remote(gid, missing_ok);

		/* rollback prepared on AGTM */
		agtm_AbortTransaction(gid, missing_ok);
	} PG_CATCH();
	{
		/* Record FAILED log */
		RecordRemoteXactFailed(gid, RX_ROLLBACK);
		PG_RE_THROW();
	} PG_END_TRY();
}

