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
#include "storage/sinval.h"
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
	if (dbname && dbname[0])
		StrNCpy(xlrec->dbname, dbname, NAMEDATALEN);

	/* user */
	if (user && user[0])
		StrNCpy(xlrec->user, user, NAMEDATALEN);

	/* gid */
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
RecordRemoteXactSuccess(uint8 info, xl_remote_success *xlres)
{
	XLogRecData rdata[1];
	XLogRecPtr	recptr;

	Assert(IsUnderRemoteXact());

	START_CRIT_SECTION();

	rdata[0].data = (char *) xlres;
	rdata[0].len = MinSizeOfRemoteSuccess;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = NULL;

	recptr = XLogInsert(RM_XACT_ID, info, rdata);

	/* Always flush, since we're about to remove the 2PC state file */
	XLogFlush(recptr);

	XactLastRecEnd = 0;

	END_CRIT_SECTION();
}

void
MakeUpRemoteXactBuffer(StringInfo buf,
					   uint8 info,
					   TransactionId xid,
					   TimestampTz xact_time,
					   bool isimplicit,
					   bool missing_ok,
					   const char *gid,
					   int nnodes,
					   Oid *nodeIds)
{
	char			*nodeHost;
	int				 nodePort;
	Oid				 nodeId;
	int				 i, len;
	xl_remote_xact	*xlrec = NULL;

	AssertArg(buf);

	len = MinSizeOfRemoteXact + nnodes * sizeof(RemoteNode);
	xlrec = (xl_remote_xact *) palloc0(len);

	MakeUpXLRemoteXact(xlrec, info, xid, xact_time,
					   isimplicit, missing_ok, gid, nnodes);

	for (i = 0; i < nnodes; i++)
	{
		nodeId = nodeIds[i];
		nodePort = get_pgxc_nodeport(nodeId);
		Assert(nodePort > 0);
		nodeHost = get_pgxc_nodehost(nodeId);
		Assert(nodeHost);
		xlrec->rnodes[i].nodeId = nodeId;
		xlrec->rnodes[i].nodePort = nodePort;
		StrNCpy(xlrec->rnodes[i].nodeHost, nodeHost, NAMEDATALEN);
	}

	initStringInfo(buf);
	appendBinaryStringInfo(buf, (const char *) xlrec, len);
	pfree(xlrec);
}

/*
 * Record remote prepare log and then prepare xact on remote nodes.
 *
 * If fail to prepare xact on remote nodes, ADB will step into recovery mode.
 * it is correct to do "ROLLBACK PREPARED IF EXISTS 'gid'". because it don't
 * prepare on local node.
 *
 * If number of remote nodes are not bigger than 0(current transaction is local)
 * just prepare at AGTM and will not record remote prepare log.
 */
void
RecordRemoteXactPrepare(TransactionId xid,
						TimestampTz prepared_at,
						bool isimplicit,
						const char *gid,
						int nnodes,
						Oid *nodeIds)
{
	xl_remote_success	 xlres;
	XLogRecData			 rdata[2];
	int					 lastrdata = 0;
	xl_remote_xact		 xlrec;
	XLogRecPtr			 recptr;
	RemoteNode			*rnodes = NULL;
	int					 rlen = 0;

	if (!IsUnderRemoteXact())
		return ;

	Assert(gid && gid[0]);

	START_CRIT_SECTION();

	/* Emit the remote XLOG record */
	MemSet(&xlrec, 0, MinSizeOfRemoteXact);
	MakeUpXLRemoteXact(&xlrec, XLOG_RXACT_PREPARE,
					   xid, prepared_at, isimplicit,
					   false, gid, nnodes);
	rdata[0].data = (char *) (&xlrec);
	rdata[0].len = MinSizeOfRemoteXact;
	rdata[0].buffer = InvalidBuffer;

	/* dump involved nodes */
	if (nnodes > 0)
	{
		AssertArg(nodeIds);
		rnodes = MakeUpRemoteNodeInfo(nnodes, nodeIds, &rlen);
		rdata[0].next = &(rdata[1]);
		rdata[1].data = (char *) rnodes;
		rdata[1].len = rlen;
		rdata[1].buffer = InvalidBuffer;
		lastrdata = 1;
	}
	rdata[lastrdata].next = NULL;

	recptr = XLogInsert(RM_XACT_ID, XLOG_RXACT_PREPARE, rdata);

	/* Always flush, since we're about to remove the 2PC state file */
	XLogFlush(recptr);

	/* Prepare at remote nodes */
	if (nnodes > 0)
	{
		pfree(rnodes);
		PrePrepare_Remote(gid);
	}
	agtm_PrepareTransaction(gid);

	END_CRIT_SECTION();

	/* Record SUCCESS log for remote prepare */
	MemSet(&xlres, 0, MinSizeOfRemoteSuccess);
	xlres.xid = xid;
	StrNCpy(xlres.gid, gid, GIDSIZE);
	RecordRemoteXactSuccess(XLOG_RXACT_PREPARE_SUCCESS, &xlres);
}

/*
 * Commit prepared gid on remote nodes and AGMT if "nnodes" > 0,
 * then record SUCCESS log.
 *
 * Otherwise, commit prepared gid only on AGTM.
 *
 * The function will called in a critical section to force a PANIC
 * if we are unable to complete remote commit prepared transaction
 * then, WAL replay should repair the inconsistency.
 *
 * Note: we never record REMOTE XLOG, because it has already done.
 * see RecordTransactionCommitPrepared
 */
void
RemoteXactCommitPrepared(TransactionId xid,
						 bool isimplicit,
						 bool missing_ok,
						 const char *gid,
						 int nnodes,
						 Oid *nodeIds)
{
	xl_remote_success xlres;

	if (!IsUnderRemoteXact())
		return ;

	Assert(gid && gid[0]);

	if (nnodes > 0)
		PreCommit_Remote(gid, missing_ok);

	agtm_CommitTransaction(gid, missing_ok);

	/*
	 * We record SUCCESS XLOG with xlrec. it is used to judge whether the remote
	 * transaction needs to be redo.
	 *
	 * so we just care about xid and gid. see PopXlogRemoteXact
	 */
	MemSet(&xlres, 0, MinSizeOfRemoteSuccess);
	xlres.xid = xid;
	StrNCpy(xlres.gid, gid, GIDSIZE);
	RecordRemoteXactSuccess(XLOG_RXACT_COMMIT_PREPARED_SUCCESS, &xlres);
}

/*
 * Rollback prepared gid on remote nodes and AGMT if "nnodes" > 0,
 * then record SUCCESS log.
 *
 * Otherwise, Rollback prepared gid only on AGTM.
 *
 * The function will called in a critical section to force a PANIC
 * if we are unable to complete remote rollback prepared transaction
 * then, WAL replay should repair the inconsistency.
 *
 * Note: we never record REMOTE XLOG, because it has already been done.
 * see RecordTransactionAbortPrepared
 */

void
RemoteXactAbortPrepared(TransactionId xid,
						bool isimplicit,
						bool missing_ok,
						const char *gid,
						int nnodes,
						Oid *nodeIds)
{
	xl_remote_success xlres;

	if (!IsUnderRemoteXact())
		return ;

	Assert(gid && gid[0]);

	if (nnodes > 0)
		PreAbort_Remote(gid, missing_ok);
	agtm_AbortTransaction(gid, missing_ok);

	/*
	 * We record SUCCESS XLOG with xlrec. it is used to judge whether the remote
	 * transaction needs to be redo.
	 *
	 * so we just care about xid and gid. see PopXlogRemoteXact
	 */
	MemSet(&xlres, 0, MinSizeOfRemoteSuccess);
	xlres.xid = xid;
	StrNCpy(xlres.gid, gid, GIDSIZE);
	RecordRemoteXactSuccess(XLOG_RXACT_ABORT_PREPARED_SUCCESS, &xlres);
}

/* -------------------- remote xact redo interface ---------------------- */
static xl_remote_xact *
CopyXLRemoteXact(xl_remote_xact *from)
{
	xl_remote_xact  *to = NULL;

	if (from)
	{
		int nnodes = from->nnodes;

		to = (xl_remote_xact *) palloc0(MinSizeOfRemoteXact +
										nnodes * sizeof(RemoteNode));
		memcpy(to, from, MinSizeOfRemoteXact);
		memcpy(to->rnodes, from->rnodes, nnodes * sizeof(RemoteNode));
	}

	return to;
}

#ifdef WAL_DEBUG
static void
rxact_debug(uint8 info, TransactionId xid, const char *gid)
{
	bool push;
	char *kind;

	if (XLOG_DEBUG)
	{
		switch (info)
		{
			case XLOG_RXACT_PREPARE:
			case XLOG_RXACT_PREPARE_SUCCESS:
				kind = "REMOTE PREPARE";
				break;
			case XLOG_RXACT_COMMIT_PREPARED:
			case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
				kind = "COMMIT REMOTE PREPARED";
				break;
			case XLOG_RXACT_ABORT_PREPARED:
			case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
				kind = "ABORT REMOTE PREPARED";
				break;
			default:
				return ;
		}
		push = (info < XLOG_RXACT_PREPARE_SUCCESS);
		elog(LOG, "%s @ %s: xid: %u; gid: %s",
			push ? "PUSH" : "POP",
			kind, xid, gid);
	}
}
#endif

/*
 * Keep xl_remote_xact in proper list and sort asc by xid
 */
static void
PushXlogRemoteXact(uint8 info, xl_remote_xact *xlrec)
{
	List **result = NULL;
	xl_remote_xact *last_xlrec = NULL;

	AssertArg(xlrec);
	Assert(IsUnderRemoteXact());
	Assert(TransactionIdIsValid(xlrec->xid));

	switch (info)
	{
		case XLOG_RXACT_PREPARE:
			result = &prepared_rxact;
			break;
		case XLOG_RXACT_COMMIT_PREPARED:
			result = &commit_prepared_rxact;
			break;
		case XLOG_RXACT_ABORT_PREPARED:
			result = &abort_prepared_rxact;
			break;
		default:
			Assert(0);
			break;
	}

#ifdef WAL_DEBUG
	rxact_debug(info, xlrec->xid, xlrec->gid);
#endif

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
PopXlogRemoteXact(uint8 info, xl_remote_success *xlres)
{
	List		  **result = NULL;
	TransactionId	xid;
	const char	   *gid;
	ListCell	   *cell = NULL;
	xl_remote_xact *rxact = NULL;
	int				slen1,
					slen2;

	AssertArg(xlres);
	Assert(TransactionIdIsValid(xlres->xid));
	Assert(IsUnderRemoteXact());

	xid = xlres->xid;
	gid = xlres->gid;
	slen1 = strlen(gid);

	switch (info)
	{
		case XLOG_RXACT_PREPARE_SUCCESS:
			{
				result = &prepared_rxact;
				foreach (cell, *result)
				{
					rxact = (xl_remote_xact *) lfirst(cell);
					if (xid == rxact->xid)
					{
						slen2 = strlen(rxact->gid);
						Assert(slen1 == slen2);
						Assert(strncmp(gid, rxact->gid, slen1) == 0);
						Assert(rxact->xinfo == XLOG_RXACT_PREPARE);
						break;
					}
				}
			}
			break;
		case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
			{
				XLogRecPtr		lsn;
				xl_xact_commit	*xlrec;
				char			*bufptr;

				result = &commit_prepared_rxact;
				foreach (cell, *result)
				{
					rxact = (xl_remote_xact *) lfirst(cell);
					if (xid == rxact->xid)
					{
						slen2 = strlen(rxact->gid);
						Assert(slen1 == slen2);
						Assert(strncmp(gid, rxact->gid, slen1) == 0);
						Assert(rxact->xinfo == XLOG_RXACT_COMMIT_PREPARED);

						bufptr = (char *) &(rxact->rnodes[rxact->nnodes]);
						memcpy((void *) &lsn, (const void *) bufptr, sizeof(lsn));
						bufptr += sizeof(lsn);
						xlrec = (xl_xact_commit *) bufptr;
						xact_redo_commit_prepared(xlrec, xid, lsn);
						break;
					}
				}
			}
			break;
		case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
			{
				xl_xact_abort	*xlrec;

				result = &abort_prepared_rxact;
				foreach (cell, *result)
				{
					rxact = (xl_remote_xact *) lfirst(cell);
					if (xid == rxact->xid)
					{
						slen2 = strlen(rxact->gid);
						Assert(slen1 == slen2);
						Assert(strncmp(gid, rxact->gid, slen1) == 0);
						Assert(rxact->xinfo == XLOG_RXACT_ABORT_PREPARED);

						xlrec = (xl_xact_abort *) &(rxact->rnodes[rxact->nnodes]);
						xact_redo_abort_prepared(xlrec, xid);
						break;
					}
				}
			}
			break;
		default:
			Assert(0);
			break;
	}


#ifdef WAL_DEBUG
	rxact_debug(info, xlres->xid, xlres->gid);
#endif

	if (rxact)
	{
		*result = list_delete_ptr(*result, (void *) rxact);
		pfree(rxact);
	}
}

static ListCell *
MinRemoteXact(ListCell *lc1, ListCell *lc2, ListCell *lc3)
{
	xl_remote_xact 	*xlrec1 = NULL;
	xl_remote_xact 	*xlrec2 = NULL;
	xl_remote_xact 	*xlrec3 = NULL;
	TransactionId 	minxid;

	Assert(IsUnderRemoteXact());

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
	Assert(IsUnderRemoteXact());

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

	Assert(IsUnderRemoteXact());

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

	Assert(IsUnderRemoteXact());

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

	Assert(IsUnderRemoteXact());

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

	Assert(IsUnderRemoteXact());
	AssertArg(xlrec);
	AssertArg(command && command[0]);

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
	Assert(IsUnderRemoteXact());
	AssertArg(xlrec);
	AssertArg(xlrec->dbname && xlrec->user);
	AssertArg(xlrec->gid);

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

		/* redo local xact */
		switch (xlrec->xinfo)
		{
			case XLOG_RXACT_PREPARE:
				break;
			case XLOG_RXACT_ABORT_PREPARED:
				{
					xl_xact_abort *abort_xlrec;

					abort_xlrec = (xl_xact_abort *) &(xlrec->rnodes[xlrec->nnodes]);
					xact_redo_abort_prepared(abort_xlrec, xlrec->xid);
				}
				break;
			case XLOG_RXACT_COMMIT_PREPARED:
				{
					char			*bufptr;
					xl_xact_commit	*commit_xlrec;
					XLogRecPtr		 lsn;

					bufptr = (char *) &(xlrec->rnodes[xlrec->nnodes]);
					memcpy((void *) &lsn, (const void *) bufptr, sizeof(lsn));
					bufptr += sizeof(lsn);
					commit_xlrec = (xl_xact_commit *) bufptr;

					xact_redo_commit_prepared(commit_xlrec, xlrec->xid, lsn);
				}
				break;
			default:
				Assert(0);
				break;
		}

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

	if (!IsUnderRemoteXact())
		return ;

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

void
rxact_redo_commit_prepared(xl_xact_commit *xlrec, TransactionId xid, XLogRecPtr lsn)
{
	TransactionId 				*subxacts;
	SharedInvalidationMessage 	*inval_msgs;
	xl_remote_xact 				*remote_xlrec, *push_xlrec;
	char						*bufptr;
	int 						 len1, len2;

	AssertArg(xlrec);

	if (!xlrec->can_redo_rxact)
		return ;

	/* subxid array follows relfilenodes */
	subxacts = (TransactionId *) &(xlrec->xnodes[xlrec->nrels]);

	/* invalidation messages array follows subxids */
	inval_msgs = (SharedInvalidationMessage *) &(subxacts[xlrec->nsubxacts]);

	/* remote xact info */
	remote_xlrec = (xl_remote_xact *) &(inval_msgs[xlrec->nmsgs]);

	/* true size of xl_xact_commit */
	len1 = (char *) remote_xlrec - (char *) xlrec;

	/* true size of xl_remote_xact */
	len2 = MinSizeOfRemoteXact + remote_xlrec->nnodes * sizeof(RemoteNode);

	/* create xl_remote_xact which push to list */
	push_xlrec = (xl_remote_xact *) palloc0(len2 + sizeof(lsn) + len1);

	bufptr = (char *) push_xlrec;
	/* copy from remote_xlrec */
	memcpy((void *) bufptr, (const void *) remote_xlrec, len2);
	bufptr += len2;

	/* copy from lsn */
	memcpy((void *) bufptr, (const void *) &lsn, sizeof(lsn));
	bufptr += sizeof(lsn);

	/* copy from xlrec */
	memcpy((void *) bufptr, (const void *) xlrec, len1);

	PushXlogRemoteXact(XLOG_RXACT_COMMIT_PREPARED, push_xlrec);
}

void
rxact_redo_abort_prepared(xl_xact_abort *xlrec, TransactionId xid)
{
	TransactionId 				*sub_xids;
	xl_remote_xact 				*remote_xlrec, *push_xlrec;
	char						*bufptr;
	int 						 len1, len2;

	if (!IsUnderRemoteXact())
		return ;

	AssertArg(xlrec);

	/* subxid array follows relfilenodes */
	sub_xids = (TransactionId *) &(xlrec->xnodes[xlrec->nrels]);

	/* remote xact info */
	remote_xlrec = (xl_remote_xact *) &(sub_xids[xlrec->nsubxacts]);

	/* true size of xl_xact_abort */
	len1 = (char *) remote_xlrec - (char *) xlrec;

	/* true size of xl_remote_xact */
	len2 = MinSizeOfRemoteXact + remote_xlrec->nnodes * sizeof(RemoteNode);

	/* create xl_remote_xact which push to list */
	push_xlrec = (xl_remote_xact *) palloc0(len2 + len1);

	bufptr = (char *) push_xlrec;
	/* copy from xl_remote_xact */
	memcpy((void *) bufptr, (const void *) remote_xlrec, len2);
	bufptr += len2;

	/* copy from xl_xact_abort */
	memcpy((void *) bufptr, (const void *) xlrec, len1);

	PushXlogRemoteXact(XLOG_RXACT_ABORT_PREPARED, push_xlrec);
}

void
rxact_redo_prepare(xl_remote_xact *xlrec)
{
	Assert(IsUnderRemoteXact());
	PushXlogRemoteXact(XLOG_RXACT_PREPARE, CopyXLRemoteXact(xlrec));
}

void
rxact_redo_success(uint8 info, xl_remote_success *xlrec)
{
	Assert(IsUnderRemoteXact());
	PopXlogRemoteXact(info, xlrec);
}
