/*-------------------------------------------------------------------------
 *
 * xactdesc.c
 *	  rmgr descriptor routines for access/transam/xact.c
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/xactdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/catalog.h"
#include "common/relpath.h"
#include "storage/sinval.h"
#include "utils/timestamp.h"

#ifdef ADB
#include "access/remote_xact.h"

static void
remote_xact_desc_prepare(StringInfo buf, xl_remote_binary *rbinary)
{
	TransactionId	xid;
	TimestampTz		xact_time;
	uint8			xinfo;
	bool			implicit;
	bool			missing_ok;
	int				nnodes;
	Oid				nodeId;
	int				nodePort;
	int				i;
	char			*nodeHost;
	char			*gid;
	char			*dbname;
	char			*user;

	/* xid */
	xid = *(TransactionId *) rbinary;
	rbinary += sizeof(xid);
	/* gid */
	gid = (char *) rbinary;
	rbinary += strlen(gid) + 1;
	/* nnodes */
	nnodes = *(int *) rbinary;
	rbinary += sizeof(nnodes);
	/* xinfo */
	xinfo = *(uint8 *) rbinary;
	rbinary += sizeof(xinfo);
	/* implicit */
	implicit = *(bool *) rbinary;
	rbinary += sizeof(implicit);
	/* missing_ok */
	missing_ok = *(bool *) rbinary;
	rbinary += sizeof(missing_ok);
	/* xact_time */
	xact_time = *(TimestampTz *) rbinary;
	rbinary += sizeof(xact_time);
	/* dbname */
	dbname = (char *) rbinary;
	rbinary += strlen(dbname) + 1;
	/* user */
	user = (char *) rbinary;
	rbinary += strlen(user) + 1;

	appendStringInfo(buf, "remote prepare: xid: %u", xid);
	appendStringInfo(buf, "; prepared gid: '%s'", gid);
	appendStringInfo(buf, "; implicit: %s", implicit ? "yes" : "no");
	appendStringInfo(buf, "; missing ok: %s", missing_ok? "yes" : "no");
	appendStringInfo(buf, "; xact time: %s", timestamptz_to_str(xact_time));
	appendStringInfo(buf, "; database: %s", dbname);
	appendStringInfo(buf, "; user: %s", user);
	appendStringInfo(buf, "; involved remote nodes: %d", nnodes);
	/* rnodes */
	for (i = 0; i < nnodes; i++)
	{
		/* nodeId */
		nodeId = *(Oid *) rbinary;
		rbinary += sizeof(nodeId);
		/* nodePort */
		nodePort = *(int *) rbinary;
		rbinary += sizeof(nodePort);
		/* nodeHost */
		nodeHost = (char *) rbinary;
		rbinary += strlen(nodeHost) + 1;

		appendStringInfo(buf, " {%u@%s:%d}", nodeId, nodeHost, nodePort);
	}
}

static void
remote_xact_desc_success(StringInfo buf, uint8 xl_info, xl_remote_success *xlres)
{
	switch (xl_info)
	{
		case XLOG_RXACT_PREPARE_SUCCESS:
			appendStringInfo(buf, "remote prepare success");
			break;
		case XLOG_RXACT_COMMIT_PREPARED_SUCCESS:
			appendStringInfo(buf, "remote commit prepared success");
			break;
		case XLOG_RXACT_ABORT_PREPARED_SUCCESS:
			appendStringInfo(buf, "remote abort prepared success");
			break;
		default:
			Assert(0);
			break;
	}
	appendStringInfo(buf, ": xid: %u", xlres->xid);
	appendStringInfo(buf, "; prepared gid: '%s'", xlres->gid);
}
#endif

static void
xact_desc_commit(StringInfo buf, xl_xact_commit *xlrec)
{
	int			i;
	TransactionId *subxacts;

	subxacts = (TransactionId *) &xlrec->xnodes[xlrec->nrels];

	appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));

	if (xlrec->nrels > 0)
	{
		appendStringInfo(buf, "; rels:");
		for (i = 0; i < xlrec->nrels; i++)
		{
			char	   *path = relpathperm(xlrec->xnodes[i], MAIN_FORKNUM);

			appendStringInfo(buf, " %s", path);
			pfree(path);
		}
	}
	if (xlrec->nsubxacts > 0)
	{
		appendStringInfo(buf, "; subxacts:");
		for (i = 0; i < xlrec->nsubxacts; i++)
			appendStringInfo(buf, " %u", subxacts[i]);
	}
	if (xlrec->nmsgs > 0)
	{
		SharedInvalidationMessage *msgs;

		msgs = (SharedInvalidationMessage *) &subxacts[xlrec->nsubxacts];

		if (XactCompletionRelcacheInitFileInval(xlrec->xinfo))
			appendStringInfo(buf, "; relcache init file inval dbid %u tsid %u",
							 xlrec->dbId, xlrec->tsId);

		appendStringInfo(buf, "; inval msgs:");
		for (i = 0; i < xlrec->nmsgs; i++)
		{
			SharedInvalidationMessage *msg = &msgs[i];

			if (msg->id >= 0)
				appendStringInfo(buf, " catcache %d", msg->id);
			else if (msg->id == SHAREDINVALCATALOG_ID)
				appendStringInfo(buf, " catalog %u", msg->cat.catId);
			else if (msg->id == SHAREDINVALRELCACHE_ID)
				appendStringInfo(buf, " relcache %u", msg->rc.relId);
			/* remaining cases not expected, but print something anyway */
			else if (msg->id == SHAREDINVALSMGR_ID)
				appendStringInfo(buf, " smgr");
			else if (msg->id == SHAREDINVALRELMAP_ID)
				appendStringInfo(buf, " relmap");
			else
				appendStringInfo(buf, " unknown id %d", msg->id);
		}
	}
}

static void
xact_desc_commit_compact(StringInfo buf, xl_xact_commit_compact *xlrec)
{
	int			i;

	appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));

	if (xlrec->nsubxacts > 0)
	{
		appendStringInfo(buf, "; subxacts:");
		for (i = 0; i < xlrec->nsubxacts; i++)
			appendStringInfo(buf, " %u", xlrec->subxacts[i]);
	}
}

static void
xact_desc_abort(StringInfo buf, xl_xact_abort *xlrec)
{
	int			i;

	appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));
	if (xlrec->nrels > 0)
	{
		appendStringInfo(buf, "; rels:");
		for (i = 0; i < xlrec->nrels; i++)
		{
			char	   *path = relpathperm(xlrec->xnodes[i], MAIN_FORKNUM);

			appendStringInfo(buf, " %s", path);
			pfree(path);
		}
	}
	if (xlrec->nsubxacts > 0)
	{
		TransactionId *xacts = (TransactionId *)
		&xlrec->xnodes[xlrec->nrels];

		appendStringInfo(buf, "; subxacts:");
		for (i = 0; i < xlrec->nsubxacts; i++)
			appendStringInfo(buf, " %u", xacts[i]);
	}
}

static void
xact_desc_assignment(StringInfo buf, xl_xact_assignment *xlrec)
{
	int			i;

	appendStringInfo(buf, "subxacts:");

	for (i = 0; i < xlrec->nsubxacts; i++)
		appendStringInfo(buf, " %u", xlrec->xsub[i]);
}

void
xact_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	if (info == XLOG_XACT_COMMIT_COMPACT)
	{
		xl_xact_commit_compact *xlrec = (xl_xact_commit_compact *) rec;

		appendStringInfo(buf, "commit: ");
		xact_desc_commit_compact(buf, xlrec);
	}
	else if (info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) rec;

		appendStringInfo(buf, "commit: ");
		xact_desc_commit(buf, xlrec);
	}
	else if (info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) rec;

		appendStringInfo(buf, "abort: ");
		xact_desc_abort(buf, xlrec);
	}
	else if (info == XLOG_XACT_PREPARE)
	{
		appendStringInfo(buf, "prepare");
	}
	else if (info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit_prepared *xlrec = (xl_xact_commit_prepared *) rec;

		appendStringInfo(buf, "commit prepared %u: ", xlrec->xid);
		xact_desc_commit(buf, &xlrec->crec);
	}
	else if (info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort_prepared *xlrec = (xl_xact_abort_prepared *) rec;

		appendStringInfo(buf, "abort prepared %u: ", xlrec->xid);
		xact_desc_abort(buf, &xlrec->arec);
	}
	else if (info == XLOG_XACT_ASSIGNMENT)
	{
		xl_xact_assignment *xlrec = (xl_xact_assignment *) rec;

		/*
		 * Note that we ignore the WAL record's xid, since we're more
		 * interested in the top-level xid that issued the record and which
		 * xids are being reported here.
		 */
		appendStringInfo(buf, "xid assignment xtop %u: ", xlrec->xtop);
		xact_desc_assignment(buf, xlrec);
	}
#ifdef ADB
	else if (info == XLOG_RXACT_PREPARE)
	{
		xl_remote_binary *rbinary = (xl_remote_binary *) rec;
		remote_xact_desc_prepare(buf, rbinary);
	}
	else if (info == XLOG_RXACT_PREPARE_SUCCESS ||
			 info == XLOG_RXACT_COMMIT_PREPARED_SUCCESS ||
			 info == XLOG_RXACT_ABORT_PREPARED_SUCCESS)
	{
		xl_remote_success *xlres = (xl_remote_success *) rec;
		remote_xact_desc_success(buf, info, xlres);
	}
#endif
	else
		appendStringInfo(buf, "UNKNOWN");
}
