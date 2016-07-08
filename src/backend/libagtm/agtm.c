#include "postgres.h"

#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_utils.h"
#include "agtm/agtm_client.h"
#include "agtm/agtm_transaction.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxc.h"
#include "storage/procarray.h"

#include <unistd.h>

static AGTM_Sequence agtm_DealSequence(const char *seqname, AGTM_MessageType type, AGTM_ResultType rtype);
static PGresult* agtm_get_result(AGTM_MessageType msg_type);
static void agtm_send_message(AGTM_MessageType msg, const char *fmt, ...)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

TransactionId
agtm_GetGlobalTransactionId(bool isSubXact)
{
	PGresult 		*res;
	StringInfoData	buf;
	GlobalTransactionId gxid;

	if(!IsUnderAGTM())
		return InvalidGlobalTransactionId;

	agtm_send_message(AGTM_MSG_GET_GXID, "%c", isSubXact);
	res = agtm_get_result(AGTM_MSG_GET_GXID);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_GXID_RESULT);
	pq_copymsgbytes(&buf, (char*)&gxid, sizeof(TransactionId));

	ereport(LOG, 
		(errmsg("get global xid: %d from agtm", gxid)));

	agtm_use_result_end(&buf);
	return gxid;
}

Timestamp
agtm_GetTimestamptz(void)
{
	PGresult		*res;
	StringInfoData	buf;
	Timestamp		timestamp;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetTimestamptz function must under AGTM")));

	agtm_send_message(AGTM_MSG_GET_TIMESTAMP, "");
	res = agtm_get_result(AGTM_MSG_GET_TIMESTAMP);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_TIMESTAMP_RESULT);
	pq_copymsgbytes(&buf, (char*)&timestamp, sizeof(timestamp));

	ereport(DEBUG1,
		(errmsg("get timestamp: %ld from agtm", timestamp)));

	agtm_use_result_end(&buf);
	return timestamp;
}

Snapshot
agtm_GetGlobalSnapShot(Snapshot snapshot)
{
	PGresult 	*res;
	const char *str;
	StringInfoData	buf;
	AssertArg(snapshot && snapshot->xip && snapshot->subxip);

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetGlobalSnapShot function must under AGTM")));

	agtm_send_message(AGTM_MSG_SNAPSHOT_GET, "");
	res = agtm_get_result(AGTM_MSG_SNAPSHOT_GET);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SNAPSHOT_GET_RESULT);

	pq_copymsgbytes(&buf, (char*)&(snapshot->xmin), sizeof(snapshot->xmin));
	pq_copymsgbytes(&buf, (char*)&(snapshot->xmax), sizeof(snapshot->xmax));
	snapshot->xcnt = pq_getmsgint(&buf, sizeof(snapshot->xcnt));
	if(snapshot->xcnt > GetMaxSnapshotXidCount())
		ereport(ERROR, (errmsg("too many transaction")));
	pq_copymsgbytes(&buf, (char*)(snapshot->xip)
		, sizeof(snapshot->xip[0]) * (snapshot->xcnt));
	snapshot->subxcnt = pq_getmsgint(&buf, sizeof(snapshot->subxcnt));
	str = pq_getmsgbytes(&buf, snapshot->subxcnt * sizeof(snapshot->subxip[0]));
	snapshot->suboverflowed = pq_getmsgbyte(&buf);
	if(snapshot->subxcnt > GetMaxSnapshotXidCount())
	{
		snapshot->subxcnt = GetMaxSnapshotXidCount();
		snapshot->suboverflowed = true;
	}
	memcpy(snapshot->subxip, str, sizeof(snapshot->subxip[0]) * snapshot->subxcnt);
	snapshot->takenDuringRecovery = pq_getmsgbyte(&buf);
	pq_copymsgbytes(&buf, (char*)&(snapshot->curcid), sizeof(snapshot->curcid));
	pq_copymsgbytes(&buf, (char*)&(snapshot->active_count), sizeof(snapshot->active_count));
	pq_copymsgbytes(&buf, (char*)&(snapshot->regd_count), sizeof(snapshot->regd_count));

	agtm_use_result_end(&buf);
	return snapshot;
}

XidStatus
agtm_TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	PGresult		*res;
	StringInfoData	buf;
	XidStatus		xid_status;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_TransactionIdGetStatus function must under AGTM")));

	agtm_send_message(AGTM_MSG_GET_XACT_STATUS, "%d%d", (int)xid, (int)sizeof(xid));
	res = agtm_get_result(AGTM_MSG_GET_XACT_STATUS);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_XACT_STATUS_RESULT);
	pq_copymsgbytes(&buf, (char*)&xid_status, sizeof(xid_status));
	pq_copymsgbytes(&buf, (char*)lsn, sizeof(XLogRecPtr));

	ereport(DEBUG1,
		(errmsg("get xid %u status %d", xid, xid_status)));

	agtm_use_result_end(&buf);
	return xid_status;
}

static AGTM_Sequence 
agtm_DealSequence(const char *seqname, AGTM_MessageType type, AGTM_ResultType rtype)
{
	PGresult		*res;
	StringInfoData	buf;
	int				seq_len;
	AGTM_Sequence	seq;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_DealSequence function must under AGTM")));

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = (%s), parameter seqname is null", gtm_util_message_name(type))));

	seq_len = strlen(seqname);
	agtm_send_message(type, "%d%d %p%d", seq_len, 4, seqname, seq_len);
	res = agtm_get_result(type);
	Assert(res);
	agtm_use_result_type(res, &buf, rtype);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(&buf);
	return seq;
}

AGTM_Sequence 
agtm_GetSeqNextVal(const char *seqname)
{
	return agtm_DealSequence(seqname, AGTM_MSG_SEQUENCE_GET_NEXT
			, AGTM_SEQUENCE_GET_NEXT_RESULT);
}

AGTM_Sequence 
agtm_GetSeqCurrVal(const char *seqname)
{
	return agtm_DealSequence(seqname, AGTM_MSG_SEQUENCE_GET_CUR
			, AGTM_MSG_SEQUENCE_GET_CUR_RESULT);
}

AGTM_Sequence
agtm_GetSeqLastVal(const char *seqname)
{
	return agtm_DealSequence(seqname, AGTM_MSG_SEQUENCE_GET_LAST
			, AGTM_SEQUENCE_GET_LAST_RESULT);
}


AGTM_Sequence
agtm_SetSeqVal(const char *seqname, AGTM_Sequence nextval)
{
	return (agtm_SetSeqValCalled(seqname, nextval, true));
}

AGTM_Sequence
agtm_SetSeqValCalled(const char *seqname, AGTM_Sequence nextval, bool iscalled)
{
	PGresult		*res;
	StringInfoData	buf;
	int				len;
	AGTM_Sequence	seq;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_SetSeqValCalled function must under AGTM")));

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = %s, parameter seqname is null",
			"AGTM_MSG_SEQUENCE_SET_VAL")));

	len = strlen(seqname);
	agtm_send_message(AGTM_MSG_SEQUENCE_SET_VAL, "%d%d %p%d" INT64_FORMAT "%c"
		, len, 4, seqname, len, nextval, iscalled);
	res = agtm_get_result(AGTM_MSG_SEQUENCE_SET_VAL);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SEQUENCE_SET_VAL_RESULT);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(&buf);
	return seq;
}

/*
 * call pqPutMsgStart ... pqPutMsgEnd
 * only support:
 *   %d%d: first is value, second is length
 *   %p%d: first is binary point, second is binary length
 *   %s: string, include '\0'
 *   %c: one char
 *   space: skip it
 */
static void agtm_send_message(AGTM_MessageType msg, const char *fmt, ...)
{
	va_list args;
	PGconn *conn;
	void *p;
	int len;
	char c;
	AssertArg(msg < AGTM_MSG_TYPE_COUNT && fmt);

	/* get connection */
	conn = getAgtmConnection();

	/* start message */
	if(PQsendQueryStart(conn) == false
		|| pqPutMsgStart('A', true, conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR, (errmsg("Start message for agtm failed:%s", PQerrorMessage(conn))));
	}

	va_start(args, fmt);
	/* put AGTM message type */
	if(pqPutInt(msg, 4, conn) < 0)
		goto put_error_;

	while(*fmt)
	{
		if(isspace(fmt[0]))
		{
			/* skip space */
			++fmt;
			continue;
		}else if(fmt[0] != '%')
		{
			goto format_error_;
		}
		++fmt;

		c = *fmt;
		++fmt;

		if(c == 's')
		{
			/* %s for string */
			p = va_arg(args, char *);
			len = strlen(p);
			++len; /* include '\0' */
			if(pqPutnchar(p, len, conn) < 0)
				goto put_error_;
		}else if(c == 'c')
		{
			/* %c for char */
			c = (char)va_arg(args, int);
			if(pqPutc(c, conn) < 0)
				goto put_error_;
		}else if(c == 'p')
		{
			/* %p for binary */
			p = va_arg(args, void *);
			/* and need other "%d" for value binary length */
			if(fmt[0] != '%' || fmt[1] != 'd')
				goto format_error_;
			fmt += 2;
			len = va_arg(args, int);
			if(pqPutnchar(p, len, conn) < 0)
				goto put_error_;
		}else if(c == 'd')
		{
			/* %d for int */
			int val = va_arg(args, int);
			/* and need other "%d" for value binary length */
			if(fmt[0] != '%' || fmt[1] != 'd')
				goto format_error_;
			fmt += 2;
			len = va_arg(args, int);
			if(pqPutInt(val, len, conn) < 0)
				goto put_error_;
		}else if(c == 'l')
		{
			if(fmt[0] == 'd')
			{
				long val = va_arg(args, long);
				fmt += 1;
				if(pqPutnchar((char*)&val, sizeof(val), conn) < 0)
					goto put_error_;
			}
			else if(fmt[0] == 'l' && fmt[1] == 'd')
			{
				long long val = va_arg(args, long long);
				fmt += 2;
				if(pqPutnchar((char*)&val, sizeof(val), conn) < 0)
					goto put_error_;
			}
			else
			{
				goto put_error_;
			}
		}
		else
		{
			goto format_error_;
		}
	}
	va_end(args);

	if(pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR, (errmsg("End message for agtm failed:%s", PQerrorMessage(conn))));
	}

	conn->asyncStatus = PGASYNC_BUSY;
	return;

format_error_:
	va_end(args);
	pqHandleSendFailure(conn);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
		, errmsg("format message error for agtm_send_message")));
	return;

put_error_:
	va_end(args);
	pqHandleSendFailure(conn);
	ereport(ERROR, (errmsg("put message to AGTM error:%s", PQerrorMessage(conn))));
	return;
}

/*
 * call pqFlush, pqWait, pqReadData and return agtm_GetResult
 */
static PGresult* agtm_get_result(AGTM_MessageType msg_type)
{
	PGconn *conn;
	PGresult *result;
	ExecStatusType state;
	int res;

	conn = getAgtmConnection();

	while((res=pqFlush(conn)) > 0)
		; /* nothing todo */
	if(res < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("flush message to AGTM error:%s, message type:%s",
			PQerrorMessage(conn), gtm_util_message_name(msg_type))));
	}

	result = NULL;
	if(pqWait(true, false, conn) != 0
		|| pqReadData(conn) < 0
		|| (result = PQexecFinish(conn)) == NULL)
	{
		ereport(ERROR,
			(errmsg("read message from AGTM error:%s, message type:%s",
			PQerrorMessage(conn), gtm_util_message_name(msg_type))));
	}

	state = PQresultStatus(result);
	if(state == PGRES_FATAL_ERROR)
	{
		ereport(ERROR, (errmsg("got error message from AGTM %s", PQresultErrorMessage(result))));
	}else if(state != PGRES_TUPLES_OK && state != PGRES_COMMAND_OK)
	{
		ereport(ERROR, (errmsg("AGTM result a \"%s\" message", PQresStatus(state))));
	}

	return result;
}
