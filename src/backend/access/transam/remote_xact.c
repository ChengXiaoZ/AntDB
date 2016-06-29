/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>
#include <unistd.h>

#include "access/remote_xact.h"
#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-fe.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#define Min2Xid(a, b)		(TransactionIdPrecedes((a), (b)) ? (a) : (b))
#define Min3Xid(a, b, c)	(Min2Xid(Min2Xid((a), (b)), (c)))
#define AGTMOID				((Oid) 0)

typedef struct RemoteConnKey
{
	RemoteNode	rnode;
	char		dbname[NAMEDATALEN];
	char		user[NAMEDATALEN];
} RemoteConnKey;

typedef struct RemoteConnEnt
{
	RemoteConnKey	 key;
	PGconn			*conn;
} RemoteConnEnt;

extern char	*AGtmHost;
extern int 	 AGtmPort;

static HTAB *RemoteConnHashTab = NULL;
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
				   const char *gid,
				   int nnodes)
{
	char	*dbname = NULL;
	char	*user = NULL;

	AssertArg(xlrec);

	dbname = get_database_name(MyDatabaseId);
	user = GetUserNameFromId(GetUserId());

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

	/* user */
	MemSet(xlrec->user, 0, NAMEDATALEN);
	if (user && user[0])
		StrNCpy(xlrec->user, user, NAMEDATALEN);

	/* gid */
	MemSet(xlrec->gid, 0, GIDSIZE);
	if (gid && gid[0])
		StrNCpy(xlrec->gid, gid, GIDSIZE);
}

static RemoteNode *
MakeUpRemoteNodeInfo(int nnodes, Oid *nodeIds, int *rlen)
{
	RemoteNode	*rnodes = NULL;
	Oid 	 	 nodeId = InvalidOid;
	char		*nodeHost = NULL;
	int 	 	 nodePort = -1;
	int			 i;

	AssertArg(nnodes > 0);
	AssertArg(nodeIds);

	*rlen = nnodes * sizeof(RemoteNode);

	rnodes = (RemoteNode *)palloc0(*rlen);
	for (i = 0; i < nnodes; i++)
	{
		nodeId = nodeIds[i];
		rnodes[i].nodeId = nodeId;
		nodeHost = get_pgxc_nodehost(nodeId);
		Assert(nodeHost);
		StrNCpy(rnodes[i].nodeHost, nodeHost, NAMEDATALEN);
		nodePort = get_pgxc_nodeport(nodeId);
		Assert(nodePort > 0);
		rnodes[i].nodePort = nodePort;
	}

	return rnodes;
}

static void
RecordRemoteXactSuccess(uint8 info, xl_remote_xact *xlrec)
{
	XLogRecData rdata[1];
	XLogRecPtr	recptr;

	Assert(IS_PGXC_COORDINATOR && !IsConnFromCoord());

	START_CRIT_SECTION();

	rdata[0].data = (char *) xlrec;
	rdata[0].len = MinSizeOfRemoteXact;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = NULL;

	recptr = XLogInsert(RM_XACT_ID, info, rdata);

	/* Always flush, since we're about to remove the 2PC state file */
	XLogFlush(recptr);

	XactLastRecEnd = 0;

	END_CRIT_SECTION();
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
	if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
		return ;

	if (nnodes > 0)
	{
		XLogRecData		rdata[2];
		int				lastrdata = 0;
		xl_remote_xact	xlrec;
		XLogRecPtr		recptr;
		RemoteNode	   *rnodes = NULL;
		int			 	rlen = 0;

		AssertArg(nodeIds);

		START_CRIT_SECTION();

		/* Emit the remote XLOG record */
		MakeUpXLRemoteXact(&xlrec, info, xid, xact_time, isimplicit,
						   missing_ok, gid, nnodes);

		rdata[0].data = (char *) (&xlrec);
		rdata[0].len = MinSizeOfRemoteXact;
		rdata[0].buffer = InvalidBuffer;

		/* dump involved nodes */
		rnodes = MakeUpRemoteNodeInfo(nnodes, nodeIds, &rlen);			
		rdata[0].next = &(rdata[1]);
		rdata[1].data = (char *) rnodes;
		rdata[1].len = rlen;
		rdata[1].buffer = InvalidBuffer;
		lastrdata = 1;
		rdata[lastrdata].next = NULL;

		recptr = XLogInsert(RM_XACT_ID, info, rdata);

		/* Always flush, since we're about to remove the 2PC state file */
		XLogFlush(recptr);

		pfree(rnodes);

		XactLastRecEnd = 0;

		END_CRIT_SECTION();

		switch (info)
		{
			case XLOG_RXACT_PREPARE:
				{
					START_CRIT_SECTION();
					PrePrepare_Remote(gid);
					agtm_PrepareTransaction(gid);
					END_CRIT_SECTION();
					RecordRemoteXactSuccess(XLOG_RXACT_PREPARE_SUCCESS,
											&xlrec);
				}
				break;
			case XLOG_RXACT_COMMIT:
			case XLOG_RXACT_COMMIT_PREPARED:
				{
					START_CRIT_SECTION();
					PreCommit_Remote(gid, missing_ok);
					agtm_CommitTransaction(gid, missing_ok);
					END_CRIT_SECTION();

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
					START_CRIT_SECTION();
					PreAbort_Remote(gid, missing_ok);
					agtm_AbortTransaction(gid, missing_ok);
					END_CRIT_SECTION();

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
				START_CRIT_SECTION();
				agtm_PrepareTransaction(gid);
				END_CRIT_SECTION();
				break;
			case XLOG_RXACT_COMMIT:
			case XLOG_RXACT_COMMIT_PREPARED:
				agtm_CommitTransaction(gid, missing_ok);
				break;
			case XLOG_RXACT_ABORT:
			case XLOG_RXACT_ABORT_PREPARED:
				agtm_AbortTransaction(gid, missing_ok);
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

static xl_remote_xact *
CopyXLRemoteXact(xl_remote_xact *from)
{
	xl_remote_xact  *to = NULL;

	if (from)
	{
		int nnodes = from->nnodes;

		to = (xl_remote_xact *)palloc0(MinSizeOfRemoteXact + nnodes * sizeof(RemoteNode));
		memcpy(to, from, MinSizeOfRemoteXact);
		memcpy(to->rnodes, from->rnodes, nnodes * sizeof(RemoteNode));
	}

	return to;
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
		*result = lappend(NIL, (void *) CopyXLRemoteXact(xlrec));
		return ;
	}

	/* must have last element */
	last_xlrec = (xl_remote_xact *) llast(*result);

	/* if new xid is bigger, then append it */
	if (TransactionIdFollowsOrEquals(xlrec->xid, last_xlrec->xid))
	{
		*result = lappend(*result, (void *) CopyXLRemoteXact(xlrec));
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
				(void) lappend_cell(*result, prev_lc, (void *) CopyXLRemoteXact(xlrec));
			/* keep it prepend the list */
			else
				*result = lcons((void *) CopyXLRemoteXact(xlrec), *result);

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
			pfree(rxact);
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

static void
DestroyRemoteConnHashTab(void)
{
	if (RemoteConnHashTab)
	{
		HASH_SEQ_STATUS	 status;
		RemoteConnEnt	*item = NULL;

		hash_seq_init(&status, RemoteConnHashTab);
		while ((item = (RemoteConnEnt *) hash_seq_search(&status)) != NULL)
		{
			PQfinish(item->conn);
		}

		hash_destroy(RemoteConnHashTab);
	}

	RemoteConnHashTab = NULL;
}

static PGconn *
ObtainValidConnection(RemoteConnKey *key)
{
	RemoteConnEnt	*rce = NULL;
	bool			 found = false;
	char			*nodePortStr = NULL;

	if (!key)
		return NULL;

	/* initialize remote connection hash table */
	if (RemoteConnHashTab == NULL)
	{
		HASHCTL hctl;

		/* Initialize temporary hash table */
		MemSet(&hctl, 0, sizeof(hctl));
		hctl.keysize = sizeof(RemoteConnKey);
		hctl.entrysize = sizeof(RemoteConnEnt);
		hctl.hash = tag_hash;
		hctl.hcxt = CurrentMemoryContext;

		RemoteConnHashTab = hash_create("RemoteConnHashTab",
										32,
										&hctl,
										HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	rce = (RemoteConnEnt *)hash_search(RemoteConnHashTab,
									   (const void *) &key,
									   HASH_ENTER,
									   &found);

	if (!found)
	{
		nodePortStr = psprintf("%d", key->rnode.nodePort);
		rce->conn = PQsetdbLogin(key->rnode.nodeHost,
								 nodePortStr,
								 NULL,
								 NULL,
								 key->dbname,
								 key->user,
								 NULL);
		if (PQstatus(rce->conn) == CONNECTION_BAD)
		{
			char *msg = pstrdup(PQerrorMessage(rce->conn));
			PQfinish(rce->conn);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not establish connection with (%s:%s)",
					 	key->rnode.nodeHost, nodePortStr),
					 errdetail_internal("%s", msg)));
		}
		pfree(nodePortStr);
	}

	return rce->conn;
}

static void
ReplayRemoteCommand(PGconn *conn,
					const char *command,
					const char *host,
					const int port)
{
	PGresult	*result = NULL;

	if (!conn || !command)
		return ;

	/* execute command */
	result = PQexec(conn, command);

	/* check result */
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		char *msg = pstrdup(PQresultErrorMessage(result));
		PQclear(result);
		ereport(ERROR,
			(errmsg("Fail to redo command: %s on remote(%s:%d)",
				command, host, port),
			errdetail_internal("%s", msg)));
	}

	PQclear(result);
}

static void
MakeUpRemoteConnKey(RemoteConnKey *key,
					Oid nodeId,
					char *nodeHost,
					int nodePort,
					char *dbname,
					char *user)
{
	if (!key)
		return ;

	MemSet(key, 0, sizeof(RemoteConnKey));
	key->rnode.nodeId = nodeId;
	key->rnode.nodePort = nodePort;
	StrNCpy(key->rnode.nodeHost, (const char *) nodeHost, NAMEDATALEN);
	StrNCpy(key->dbname, (const char *) dbname, NAMEDATALEN);
	StrNCpy(key->user, (const char *) user, NAMEDATALEN);
}

static void
ReplayRemoteXactAGTM(xl_remote_xact *xlrec, const char *command)
{
	RemoteConnKey	 key;
	PGconn			*agtm_conn = NULL;

	/* make up key */
	MakeUpRemoteConnKey(&key, AGTMOID, AGtmHost, AGtmPort,
						xlrec->dbname, xlrec->user);

	/* search one or add new one */
	agtm_conn = ObtainValidConnection(&key);

	/* execure command */
	ReplayRemoteCommand(agtm_conn, command, AGtmHost, AGtmPort);
}

static void
ReplayRemoteXactOnce(xl_remote_xact *xlrec)
{
	StringInfoData	 command;

	/* sanity check */
	AssertArg(xlrec);
	AssertArg(xlrec->nnodes > 0);
	AssertArg(xlrec->dbname && xlrec->user);
	AssertArg(xlrec->gid && xlrec->rnodes);

	/* do remote command */
	initStringInfo(&command);
	switch (xlrec->xinfo)
	{
		case XLOG_RXACT_PREPARE:
		case XLOG_RXACT_ABORT_PREPARED:
			appendStringInfo(&command, "ROLLBACK PREPARED IF EXISTS '%s'", xlrec->gid);
			break;
		case XLOG_RXACT_COMMIT_PREPARED:
			appendStringInfo(&command, "COMMIT PREPARED IF EXISTS '%s'", xlrec->gid);
			break;
		default:
			Assert(0);
			break;
	}

	PG_TRY();
	{
		RemoteConnKey 	 key;
		int				 nodeCnt = 0;
		int				 nodeIdx = 0;
		RemoteNode		*rnodes = NULL;
		PGconn			*rconn = NULL;
		
		/* do command on remote node (not include AGTM) */
		rnodes = xlrec->rnodes;
		nodeCnt = xlrec->nnodes;
		for (nodeIdx = 0; nodeIdx < nodeCnt; nodeIdx++)
		{
			/* make up key */
			MakeUpRemoteConnKey(&key, rnodes[nodeIdx].nodeId,
								rnodes[nodeIdx].nodeHost,
								rnodes[nodeIdx].nodePort,
								xlrec->dbname, xlrec->user);

			/* search one or add new one */
			rconn = ObtainValidConnection(&key);

			/* execure command */
			ReplayRemoteCommand(rconn, command.data,
								rnodes[nodeIdx].nodeHost,
								rnodes[nodeIdx].nodePort);
		}

		/* do command on AGMT */
		ReplayRemoteXactAGTM(xlrec, command.data);

	} PG_CATCH();
	{
		pfree(command.data);
		DestroyRemoteConnHashTab();
		PG_RE_THROW();
	} PG_END_TRY();

	pfree(command.data);
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

		ReplayRemoteXactOnce(xlrec);

		if (lc == lc1)
			lc1 = lnext(lc1);
		else
		if (lc == lc2)
			lc2 = lnext(lc2);
		else
			lc3 = lnext(lc3);
	}

	DestroyRemoteConnHashTab();
	list_free_deep(prepared_rxact);
	list_free_deep(commit_prepared_rxact);
	list_free_deep(abort_prepared_rxact);
	prepared_rxact = NIL;
	commit_prepared_rxact = NIL;
	abort_prepared_rxact = NIL;
}

