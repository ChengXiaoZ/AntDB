/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>
#include <unistd.h>

#include "access/multixact.h"
#include "access/remote_xact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "libpq/be-fsstubs.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "utils/catcache.h"
#include "utils/combocid.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "pg_trace.h"

#ifdef ADB
#include "agtm/agtm.h"
#endif

#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#endif

#define Min2Xid(a, b)		(TransactionIdPrecedes((a), (b)) ? (a) : (b))
#define Min3Xid(a, b, c)	(Min2Xid(Min2Xid((a), (b)), (c)))

static List *prepared_rxact = NIL;
static List *commit_prepared_rxact = NIL;
static List *abort_prepared_rxact = NIL;

static void RecordRemoteXactInternal(uint8 info,
									 TransactionId xid,
									 TimestampTz xact_time,
									 bool isimplicit,
									 bool missing_ok,
									 const char *gid,
									 int nnodes,
									 Oid *nodeIds);

void
RecordRemoteXactCommit(int nnodes, Oid *nodeIds)
{
	TransactionId xid = GetCurrentTransactionIdIfAny();
	TimestampTz xact_time = GetCurrentTimestamp();

	RecordRemoteXactInternal(XLOG_RXACT_COMMIT,
							 xid,
							 xact_time,
							 false,
							 false,
							 NULL,
							 nnodes,
							 nodeIds);
}

void
RecordRemoteXactAbort(int nnodes, Oid *nodeIds)
{
	TransactionId xid = GetCurrentTransactionIdIfAny();
	TimestampTz xact_time = GetCurrentTimestamp();

	RecordRemoteXactInternal(XLOG_RXACT_ABORT,
							 xid,
							 xact_time,
							 false,
							 false,
							 NULL,
							 nnodes,
							 nodeIds);
}

void
RecordRemoteXactPrepare(TransactionId xid,
						TimestampTz prepared_at,
						bool isimplicit,
						const char *gid,
						int nnodes,
						Oid *nodeIds)
{
	RecordRemoteXactInternal(XLOG_RXACT_PREPARE,
							 xid,
							 prepared_at,
							 isimplicit,
							 false,
							 gid,
							 nnodes,
							 nodeIds);
}

void
RecordRemoteXactCommitPrepared(TransactionId xid,
							   bool isimplicit,
							   bool missing_ok,
							   const char *gid,
							   int nnodes,
							   Oid *nodeIds)
{
	RecordRemoteXactInternal(XLOG_RXACT_COMMIT_PREPARED,
							 xid,
							 GetCurrentTimestamp(),
							 isimplicit,
							 missing_ok,
							 gid,
							 nnodes,
							 nodeIds);
}

void
RecordRemoteXactAbortPrepared(TransactionId xid,
							  bool isimplicit,
							  bool missing_ok,
							  const char *gid,
							  int nnodes,
							  Oid *nodeIds)
{

	RecordRemoteXactInternal(XLOG_RXACT_ABORT_PREPARED,
							 xid,
							 GetCurrentTimestamp(),
							 isimplicit,
							 missing_ok,
							 gid,
							 nnodes,
							 nodeIds);
}

static void
MakeUpXLRemoteXact(xl_remote_xact *xlrec,
				   uint8 info,
				   TransactionId xid,
				   TimestampTz xact_time,
				   bool isimplicit,
				   bool missing_ok,
				   const char *dbname,
				   const char *gid,
				   int nnodes)
{
	xlrec->xid = xid;
	xlrec->xact_time = xact_time;
	xlrec->xinfo = info;
	xlrec->implicit = isimplicit;
	xlrec->missing_ok = missing_ok;
	xlrec->nnodes = nnodes;

	/* database name */
	MemSet(xlrec->dbname, 0, NAMEDATALEN);
	if (dbname && dbname[0])
		StrNCpy(xlrec->dbname, dbname, NAMEDATALEN);

	/* gid */
	MemSet(xlrec->gid, 0, GIDSIZE);
	if (gid && gid[0])
		StrNCpy(xlrec->gid, gid, GIDSIZE);
}

static void
RecordRemoteXactSuccess(uint8 info, xl_remote_xact *xlrec)
{
	XLogRecData rdata[1];
	XLogRecPtr	recptr;

	Assert(IS_PGXC_COORDINATOR && !IsConnFromCoord());

	rdata[0].data = (char *) xlrec;
	rdata[0].len = MinSizeOfRemoteXact;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = NULL;

	recptr = XLogInsert(RM_XACT_ID, info, rdata);

	/* Always flush, since we're about to remove the 2PC state file */
	XLogFlush(recptr);

	XactLastRecEnd = 0;
}

static void
RecordRemoteXactInternal(uint8 info,
						 TransactionId xid,
						 TimestampTz xact_time,
						 bool isimplicit,
						 bool missing_ok,
						 const char *gid,
						 int nnodes,
						 Oid *nodeIds)
{
	char *dbname = NULL;

	if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
		return ;

	if (nnodes > 0)
	{
		XLogRecData rdata[2];
		int			lastrdata = 0;
		xl_remote_xact xlrec;
		XLogRecPtr	recptr;

		AssertArg(nodeIds);

		START_CRIT_SECTION();

		/* Emit the remote XLOG record */
		dbname = get_database_name(MyDatabaseId);
		MakeUpXLRemoteXact(&xlrec, info, xid, xact_time, isimplicit,
						   missing_ok, dbname, gid, nnodes);

		rdata[0].data = (char *) (&xlrec);
		rdata[0].len = MinSizeOfRemoteXact;
		rdata[0].buffer = InvalidBuffer;
		/* dump involved nodes */
		if (nnodes > 0)
		{
			rdata[0].next = &(rdata[1]);
			rdata[1].data = (char *) nodeIds;
			rdata[1].len = nnodes * sizeof(Oid);
			rdata[1].buffer = InvalidBuffer;
			lastrdata = 1;
		}
		rdata[lastrdata].next = NULL;

		recptr = XLogInsert(RM_XACT_ID, info, rdata);

		/* Always flush, since we're about to remove the 2PC state file */
		XLogFlush(recptr);

		XactLastRecEnd = 0;

		switch (info)
		{
			case XLOG_RXACT_PREPARE:
				{
					START_FATAL_SECTION();
					PrePrepare_Remote(gid);
					agtm_PrepareTransaction(gid);
					END_FATAL_SECTION();
					RecordRemoteXactSuccess(XLOG_RXACT_PREPARE_SUCCESS,
											&xlrec);
				}
				break;
			case XLOG_RXACT_COMMIT:
			case XLOG_RXACT_COMMIT_PREPARED:
				{
					START_FATAL_SECTION();
					PreCommit_Remote(gid, missing_ok);
					agtm_CommitTransaction(gid, missing_ok);
					END_FATAL_SECTION();

					if (info == XLOG_RXACT_COMMIT_PREPARED)
					{
						RecordRemoteXactSuccess(XLOG_RXACT_COMMIT_PREPARED_SUCCESS,
												&xlrec);
					}
				}
				break;
			case XLOG_RXACT_ABORT:
			case XLOG_RXACT_ABORT_PREPARED:
				{
					START_FATAL_SECTION();
					PreAbort_Remote(gid, missing_ok);
					agtm_AbortTransaction(gid, missing_ok);
					END_FATAL_SECTION();

					if (info == XLOG_RXACT_ABORT_PREPARED)
					{
						RecordRemoteXactSuccess(XLOG_RXACT_ABORT_PREPARED_SUCCESS,
												&xlrec);
					}
				}
				break;
			case XLOG_RXACT_PREPARE_SUCCESS:
			case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
			case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
			default:
				Assert(0);
				break;
		}

		END_CRIT_SECTION();

		/*
		 * Wait for synchronous replication, if required.
		 *
		 * Note that at this stage we have marked clog, but still show as running
		 * in the procarray and continue to hold locks.
		 */
		SyncRepWaitForLSN(recptr);
	} else
	{
		switch (info)
		{
			case XLOG_RXACT_PREPARE:
				START_FATAL_SECTION();
				agtm_PrepareTransaction(gid);
				END_FATAL_SECTION();
				break;
			case XLOG_RXACT_COMMIT:
			case XLOG_RXACT_COMMIT_PREPARED:
				START_FATAL_SECTION();
				agtm_CommitTransaction(gid, true);
				END_FATAL_SECTION();
				break;
			case XLOG_RXACT_ABORT:
			case XLOG_RXACT_ABORT_PREPARED:
				START_FATAL_SECTION();
				agtm_AbortTransaction(gid, true);
				END_FATAL_SECTION();
				break;
			case XLOG_RXACT_PREPARE_SUCCESS:
			case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
			case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
			default:
				Assert(0);
				break;
		}
	}
}

/*
 * Keep xl_remote_xact in proper list and sort asc by xid
 */
static void
PushXlogRemoteXact(uint8 info, xl_remote_xact *xlrec)
{
	List **result = NULL;
	xl_remote_xact *last_xlrec = NULL;

	AssertArg(xlrec);

	switch (info)
	{
		case XLOG_RXACT_PREPARE:
			result = &prepared_rxact;
			break;
		case XLOG_RXACT_COMMIT:
			return ;
		case XLOG_RXACT_COMMIT_PREPARED:
			result = &commit_prepared_rxact;
			break;
		case XLOG_RXACT_ABORT:
			return ;
		case XLOG_RXACT_ABORT_PREPARED:
			result = &abort_prepared_rxact;
			break;
		default:
			Assert(0);
			break;
	}

	Assert(TransactionIdIsValid(xlrec->xid));

	/* empty list */
	if (*result == NIL)
	{
		*result = lappend(NIL, (void *) xlrec);
		return ;
	}

	/* must have last element */
	last_xlrec = (xl_remote_xact *) llast(*result);

	/* if new xid is bigger, then append it */
	if (TransactionIdFollowsOrEquals(xlrec->xid, last_xlrec->xid))
	{
		*result = lappend(*result, (void *) xlrec);
	}
	/* now new xid is smaller than the last one */
	else
	{
		ListCell		*lc = NULL;
		ListCell		*prev_lc = NULL;
		xl_remote_xact	*each_xlrec = NULL;

		foreach (lc, *result)
		{
			each_xlrec = (xl_remote_xact *)lfirst(lc);
			if (TransactionIdFollowsOrEquals(xlrec->xid, each_xlrec->xid))
			{
				prev_lc = lc;
				continue;
			}

			/* keep it append prev_lc */
			if (prev_lc)
				(void) lappend_cell(*result, prev_lc, (void *) xlrec);
			/* keep it prepend the list */
			else
				*result = lcons((void *) xlrec, *result);

			break;
		}
	}
}

static void
PopXlogRemoteXact(uint8 info, xl_remote_xact *xlrec)
{
	List		  **result = NULL;
	TransactionId	xid = xlrec->xid;
	const char	   *gid = xlrec->gid;
	ListCell	   *cel = NULL;
	xl_remote_xact *rxact = NULL;
	int				slen1,
					slen2;

	AssertArg(xlrec);
	Assert(TransactionIdIsValid(xlrec->xid));

	switch (info)
	{
		case XLOG_RXACT_PREPARE_SUCCESS:
			result = &prepared_rxact;
			break;
		case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
			result = &commit_prepared_rxact;
			break;
		case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
			result = &abort_prepared_rxact;
			break;
		case XLOG_RXACT_PREPARE:
		case XLOG_RXACT_COMMIT:
		case XLOG_RXACT_COMMIT_PREPARED:
		case XLOG_RXACT_ABORT:
		case XLOG_RXACT_ABORT_PREPARED:
		default:
			Assert(0);
			break;
	}

	slen1 = strlen(gid);
	foreach (cel, *result)
	{
		rxact = (xl_remote_xact *) lfirst(cel);
		if (xid == rxact->xid)
		{
			slen2 = strlen(rxact->gid);
			Assert(slen1 == slen2);
			Assert(strncmp(gid, rxact->gid, slen1) == 0);
			*result = list_delete_ptr(*result, (void *) rxact);
			break;
		}
	}
}

void
remote_xact_redo(uint8 xl_info, xl_remote_xact *xlrec)
{
	AssertArg(xlrec);

	switch (xl_info)
	{
		case XLOG_RXACT_PREPARE:
		case XLOG_RXACT_COMMIT:
		case XLOG_RXACT_COMMIT_PREPARED:
		case XLOG_RXACT_ABORT:
		case XLOG_RXACT_ABORT_PREPARED:
			PushXlogRemoteXact(xl_info, xlrec);
			break;
		case XLOG_RXACT_PREPARE_SUCCESS:
		case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
		case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
			PopXlogRemoteXact(xl_info, xlrec);
			break;
		default:
			Assert(0);
			break;
	}
}

static ListCell *
MinRemoteXact(ListCell *lc1, ListCell *lc2, ListCell *lc3)
{
	xl_remote_xact 	*xlrec1 = NULL;
	xl_remote_xact 	*xlrec2 = NULL;
	xl_remote_xact 	*xlrec3 = NULL;
	TransactionId 	minxid;

	xlrec1 = lc1 ? (xl_remote_xact *) lfirst(lc1) : NULL;
	xlrec2 = lc2 ? (xl_remote_xact *) lfirst(lc2) : NULL;
	xlrec3 = lc3 ? (xl_remote_xact *) lfirst(lc3) : NULL;

	Assert (xlrec1 || xlrec2 || xlrec3);

	/* xlrec1 NULL NULL */
	if (xlrec1 && !xlrec2 && !xlrec3)
		return lc1;

	/* NULL xlrec2 NULL */
	if (!xlrec1 && xlrec2 && !xlrec3)
		return lc2;

	/* NULL NULL xlrec3 */
	if (!xlrec1 && !xlrec2 && xlrec3)
		return lc3;

	/* NULL xlrec2 xlrec3 */
	if (!xlrec1 && xlrec2 && xlrec3)
	{
		if (TransactionIdPrecedes(xlrec2->xid, xlrec3->xid))
			return lc2;
		else
			return lc3;
	}

	/* xlrec1 NULL xlrec3 */
	if (xlrec1 && !xlrec2 && xlrec3)
	{
		if (TransactionIdPrecedes(xlrec1->xid, xlrec3->xid))
			return lc1;
		else
			return lc3;
	}

	/* xlrec1 xlrec2 NULL */
	if (xlrec1 && xlrec2 && !xlrec3)
	{
		if (TransactionIdPrecedes(xlrec1->xid, xlrec2->xid))
			return lc1;
		else
			return lc2;
	}

	/* xlrec1 xlrec2 xlrec3 */
	if (xlrec1 && xlrec2 && xlrec3)
	{
		minxid = Min3Xid(xlrec1->xid, xlrec2->xid, xlrec3->xid);
		if (minxid == xlrec1->xid)
			return lc1;
		else
		if (minxid == xlrec2->xid)
			return lc2;
		else
			return lc3;
	}

	return NULL;	/* Never reach here */
}

void
ReplayRemoteXact(void)
{
	ListCell 		*lc1 = NULL;
	ListCell 		*lc2 = NULL;
	ListCell 		*lc3 = NULL;
	ListCell		*lc = NULL;
	xl_remote_xact 	*xlrec = NULL;

	for (lc1 = list_head(prepared_rxact),
		 lc2 = list_head(commit_prepared_rxact),
		 lc3 = list_head(abort_prepared_rxact);
		 lc1 != NULL || lc2 != NULL || lc3 != NULL;)
	{
		lc = MinRemoteXact(lc1, lc2, lc3);
		xlrec = (xl_remote_xact *) lfirst(lc);

		switch (xlrec->xinfo)
		{
			case XLOG_RXACT_PREPARE:
				{
					init_RemoteXactStateByNodes(xlrec->nnodes, xlrec->nodeIds, false);
					PreAbort_Remote(xlrec->gid, true);
					agtm_AbortTransaction_ByDBname(xlrec->gid, true, xlrec->dbname);
				}
				break;
			case XLOG_RXACT_COMMIT_PREPARED:
				{
					init_RemoteXactStateByNodes(xlrec->nnodes, xlrec->nodeIds, true);
					PreCommit_Remote(xlrec->gid, true);
					agtm_CommitTransaction_ByDBname(xlrec->gid, true, xlrec->dbname);
				}
				break;
			case XLOG_RXACT_ABORT_PREPARED:
				{
					init_RemoteXactStateByNodes(xlrec->nnodes, xlrec->nodeIds, true);
					PreAbort_Remote(xlrec->gid, true);
					agtm_AbortTransaction_ByDBname(xlrec->gid, true, xlrec->dbname);
				}
				break;
			default:
				Assert(0);
				break;
		}

		if (lc == lc1)
			lc1 = lnext(lc1);
		else
		if (lc == lc2)
			lc2 = lnext(lc2);
		else
			lc3 = lnext(lc3);
	}

	list_free(prepared_rxact);
	list_free(commit_prepared_rxact);
	list_free(abort_prepared_rxact);

	prepared_rxact = NIL;
	commit_prepared_rxact = NIL;
	abort_prepared_rxact = NIL;
}
