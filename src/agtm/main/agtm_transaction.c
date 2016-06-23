#include "postgres.h"

#include "access/clog.h"
#include "access/hash.h"
#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_protocol.h"
#include "agtm/agtm_transaction.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "storage/lock.h"
#include "utils/snapmgr.h"

StringInfo ProcessGetGXIDCommand(StringInfo message, StringInfo output)
{
	TransactionId	xid;
	bool			isSubXact;

	isSubXact = pq_getmsgbyte(message);
	pq_getmsgend(message);
	if (IsTransactionState())
	{
		xid = GetCurrentTransactionId();
		elog(LOG, "AGTM return current xid %u.", xid);
	} else
	{
		xid = GetNewTransactionId(isSubXact);
		elog(LOG, "AGTM return new xid %u.", xid);
	}

	/* Respond to the client */
	pq_sendint(output, AGTM_GET_GXID_RESULT, 4);
	pq_sendbytes(output, (char *)&xid, sizeof(xid));

	return output;
}

StringInfo ProcessGetTimestamp(StringInfo message, StringInfo output)
{
	Timestamp timestamp;

	pq_getmsgend(message);
	timestamp = GetCurrentTransactionStartTimestamp();

	/* Respond to the client */
	pq_sendint(output, AGTM_GET_TIMESTAMP_RESULT, 4);
	pq_sendbytes(output, (char *)&timestamp, sizeof(timestamp));

	return output;
}

StringInfo ProcessGetSnapshot(StringInfo message, StringInfo output)
{
	Snapshot snapshot;

	pq_getmsgend(message);
	snapshot = GetTransactionSnapshot();

	/* Respond to the client */
	pq_sendint(output, AGTM_SNAPSHOT_GET_RESULT, 4);

	pq_sendbytes(output, (char *)&snapshot->xmin, sizeof (TransactionId));
	pq_sendbytes(output, (char *)&snapshot->xmax, sizeof (TransactionId));

	pq_sendint(output, snapshot->xcnt, sizeof (int));
	pq_sendbytes(output, (char *)snapshot->xip,
				 sizeof(TransactionId) * snapshot->xcnt);

	pq_sendint(output, snapshot->subxcnt, sizeof (int));
	pq_sendbytes(output, (char *)snapshot->subxip,
				 sizeof(TransactionId) * snapshot->subxcnt);

	pq_sendbytes(output, (char *)&snapshot->suboverflowed, sizeof(snapshot->suboverflowed));
	pq_sendbytes(output, (char *)&snapshot->takenDuringRecovery, sizeof(snapshot->takenDuringRecovery));
	/*pq_sendbytes(output, (char *)&snapshot->copied, sizeof(snapshot->copied));*/
	pq_sendbytes(output, (char *)&snapshot->curcid, sizeof(snapshot->curcid));
	pq_sendbytes(output, (char *)&snapshot->active_count, sizeof(snapshot->active_count));
	pq_sendbytes(output, (char *)&snapshot->regd_count, sizeof(snapshot->regd_count));

	return output;
}

StringInfo ProcessGetXactStatus(StringInfo message, StringInfo output)
{
	TransactionId	xid;
	XidStatus		xid_status;
	XLogRecPtr		xid_lsn;

	xid = pq_getmsgint(message, sizeof(xid));
	pq_getmsgend(message);

	xid_status = TransactionIdGetStatus(xid, &xid_lsn);

	/* Respond to the client */
	pq_sendint(output, AGTM_GET_XACT_STATUS_RESULT, 4);
	pq_sendbytes(output, (char *)&xid_status, sizeof(XidStatus));
	pq_sendbytes(output, (char *)&xid_lsn, sizeof(XLogRecPtr));

	return output;
}
