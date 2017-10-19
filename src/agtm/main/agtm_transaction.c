#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_protocol.h"
#include "agtm/agtm_transaction.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/snapmgr.h"

void ProcessGetGXIDCommand(StringInfo message)
{
	StringInfoData	buf;
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
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, AGTM_GET_GXID_RESULT, 4);
	pq_sendbytes(&buf, (char *)&xid, sizeof(xid));
	pq_endmessage(&buf);
	pq_flush();
}

void ProcessGetTimestamp(StringInfo message)
{
	StringInfoData buf;
	Timestamp timestamp;

	pq_getmsgend(message);
	timestamp = GetCurrentTransactionStartTimestamp();

	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, AGTM_GET_TIMESTAMP_RESULT, 4);
	pq_sendbytes(&buf, (char *)&timestamp, sizeof(timestamp));
	pq_endmessage(&buf);
	pq_flush();
}

void ProcessGetSnapshot(StringInfo message)
{
	StringInfoData buf;
	GlobalSnapshot snapshot;

	pq_getmsgend(message);
	snapshot = (GlobalSnapshot)GetTransactionSnapshot();

	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, AGTM_SNAPSHOT_GET_RESULT, 4);

	pq_sendbytes(&buf, (char *)&snapshot->xmin, sizeof (TransactionId));
	pq_sendbytes(&buf, (char *)&snapshot->xmax, sizeof (TransactionId));

	pq_sendint(&buf, snapshot->xcnt, sizeof (int));
	pq_sendbytes(&buf, (char *)snapshot->xip,
				 sizeof(TransactionId) * snapshot->xcnt);

	pq_sendint(&buf, snapshot->subxcnt, sizeof (int));
	pq_sendbytes(&buf, (char *)snapshot->subxip,
				 sizeof(TransactionId) * snapshot->subxcnt);

	pq_sendbytes(&buf, (char *)&snapshot->suboverflowed, sizeof(snapshot->suboverflowed));
	pq_sendbytes(&buf, (char *)&snapshot->takenDuringRecovery, sizeof(snapshot->takenDuringRecovery));
	pq_sendbytes(&buf, (char *)&snapshot->copied, sizeof(snapshot->copied));
	pq_sendbytes(&buf, (char *)&snapshot->curcid, sizeof(snapshot->curcid));
	pq_sendbytes(&buf, (char *)&snapshot->active_count, sizeof(snapshot->active_count));
	pq_sendbytes(&buf, (char *)&snapshot->regd_count, sizeof(snapshot->regd_count));

	pq_endmessage(&buf);
	pq_flush();

}

