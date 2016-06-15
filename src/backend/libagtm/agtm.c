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

static AGTM_Sequence agtm_DealSequence(const char *seqname, AGTM_MessageType type);
static AGTM_Result* agtm_get_result(AGTM_MessageType msg_type);
static void agtm_send_message(AGTM_MessageType msg, const char *fmt, ...)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

TransactionId
agtm_GetGlobalTransactionId(bool isSubXact)
{
	AGTM_Result 		*res;
	GlobalTransactionId gxid;
	PGconn				*conn = NULL;

	if(!IsUnderAGTM())
		return InvalidGlobalTransactionId;

	conn = getAgtmConnection();

	/* send message*/
	if (pqPutMsgStart('A',true,conn) < 0 ||
		pqPutInt(AGTM_MSG_GET_GXID,4,conn) < 0 ||
		pqPutc((char)isSubXact, conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error, %s, message type = AGTM_MSG_GET_GXID",
			PQerrorMessage(conn))));
	}
	res = agtm_get_result(AGTM_MSG_GET_GXID);
	if (res->gr_status != AGTM_RESULT_OK)
	{
		return InvalidGlobalTransactionId;
	}

	gxid = (GlobalTransactionId)res->gr_resdata.grd_gxid;

	ereport(LOG, 
		(errmsg("get global xid: %d from agtm", gxid)));

	return gxid;

}

Timestamp
agtm_GetTimestamptz(void)
{
	AGTM_Result		*res;
	Timestamp		timestamp;
	PGconn 			*conn = NULL;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetTimestamptz function must under AGTM")));

	conn = getAgtmConnection();

	/* send message*/
	if(pqPutMsgStart('A',true,conn) < 0 ||
		pqPutInt(AGTM_MSG_GET_TIMESTAMP,4,conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error, %s, message type = AGTM_MSG_GET_TIMESTAMP",
			PQerrorMessage(conn))));
	}
	res = agtm_get_result(AGTM_MSG_GET_TIMESTAMP);
	if (res->gr_status != AGTM_RESULT_OK)
		ereport(ERROR,
			(errmsg("agtm_GetResult result not ok, message type = AGTM_MSG_GET_TIMESTAMP, %s",
			PQerrorMessage(conn))));

	timestamp = res->gr_resdata.grd_timestamp;

	ereport(DEBUG1,
		(errmsg("get timestamp: %ld from agtm", timestamp)));

	return timestamp;
}

GlobalSnapshot
agtm_GetSnapShot(GlobalSnapshot snapshot)
{
	AGTM_Result 	*res;
	PGconn			*conn = NULL;
	AssertArg(snapshot && snapshot->xip && snapshot->subxip);

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetSnapShot function must under AGTM")));

	conn = getAgtmConnection();

	/* send message*/
	if(pqPutMsgStart('A',true,conn) < 0 ||
		pqPutInt(AGTM_MSG_SNAPSHOT_GET,4,conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error, %s, message type = AGTM_MSG_SNAPSHOT_GET",
			PQerrorMessage(conn))));
	}
	res = agtm_get_result(AGTM_MSG_SNAPSHOT_GET);
	if (res->gr_status != AGTM_RESULT_OK)
		ereport(ERROR,
			(errmsg("agtm_GetResult result not ok, message type = AGTM_MSG_SNAPSHOT_GET, %s",
			PQerrorMessage(conn))));

	snapshot->xmin = res->gr_resdata.snapshot->xmin;
	snapshot->xmax = res->gr_resdata.snapshot->xmax;
	snapshot->xcnt = res->gr_resdata.snapshot->xcnt;
	if(snapshot->xcnt > GetMaxSnapshotXidCount())
		ereport(ERROR, (errmsg("too many transaction")));

	memcpy(snapshot->xip,res->gr_resdata.snapshot->xip,
		sizeof(TransactionId) * snapshot->xcnt);

	snapshot->suboverflowed = res->gr_resdata.snapshot->suboverflowed;
	snapshot->subxcnt = res->gr_resdata.snapshot->subxcnt;
	if(snapshot->subxcnt > GetMaxSnapshotXidCount())
	{
		snapshot->suboverflowed = true;
		snapshot->subxcnt = GetMaxSnapshotXidCount();
	}

	memcpy(snapshot->subxip,res->gr_resdata.snapshot->subxip,
		sizeof(TransactionId) * snapshot->subxcnt);

	snapshot->takenDuringRecovery = res->gr_resdata.snapshot->takenDuringRecovery;
	/*snapshot->copied = res->gr_resdata.snapshot->copied;*/
	snapshot->curcid = res->gr_resdata.snapshot->curcid;
	snapshot->active_count = res->gr_resdata.snapshot->active_count;
	snapshot->regd_count = res->gr_resdata.snapshot->regd_count;

	return snapshot;
}

static AGTM_Sequence 
agtm_DealSequence(const char *seqname, AGTM_MessageType type)
{
	AGTM_Result		*res;
	PGconn			*conn = NULL;
	StringInfoData 	seq_key;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_DealSequence function must under AGTM")));

	conn = getAgtmConnection();

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = (%s), parameter seqname is null", gtm_util_message_name(type))));

	initStringInfo(&seq_key);
	appendStringInfoString(&seq_key,seqname);
	Assert(seq_key.len > 0);

	/* send message*/
	if(pqPutMsgStart('A', true, conn) < 0 ||
		pqPutInt(type, 4, conn) < 0 ||
		pqPutInt(seq_key.len, 4, conn) ||
		pqPutnchar(seq_key.data, seq_key.len, conn) ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error, %s, message type = %s",
			PQerrorMessage(conn), gtm_util_message_name(type))));
	}
	res = agtm_get_result(type);
	if (res->gr_status != AGTM_RESULT_OK)
		ereport(ERROR,
			(errmsg("result status is not ok, %s, message type = %s",
			PQerrorMessage(conn), gtm_util_message_name(type))));
	pfree(seq_key.data);
	return (res->gr_resdata.gsq_val);
	
}

AGTM_Sequence 
agtm_GetSeqNextVal(const char *seqname)
{
	return agtm_DealSequence(seqname, AGTM_MSG_SEQUENCE_GET_NEXT);
}

AGTM_Sequence 
agtm_GetSeqCurrVal(const char *seqname)
{
	return agtm_DealSequence(seqname, AGTM_MSG_SEQUENCE_GET_CUR);
}

AGTM_Sequence
agtm_GetSeqLastVal(const char *seqname)
{
	return agtm_DealSequence(seqname, AGTM_MSG_SEQUENCE_GET_LAST);
}


AGTM_Sequence

agtm_SetSeqVal(const char *seqname, AGTM_Sequence nextval)
{
	return (agtm_SetSeqValCalled(seqname, nextval, true));
}

AGTM_Sequence

agtm_SetSeqValCalled(const char *seqname, AGTM_Sequence nextval, bool iscalled)
{
	AGTM_Result 	*res;
	PGconn 			*conn = NULL;
	StringInfoData 	seq_key;
	
	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_SetSeqValCalled function must under AGTM")));

	conn = getAgtmConnection();

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = %s, parameter seqname is null",
			"AGTM_MSG_SEQUENCE_SET_VAL")));

	initStringInfo(&seq_key);
	appendStringInfoString(&seq_key,seqname);
	Assert(seq_key.len > 0);

	/* send message*/
	if(pqPutMsgStart('A', true, conn) < 0 ||
		pqPutInt(AGTM_MSG_SEQUENCE_SET_VAL, 4, conn) < 0 ||
		pqPutInt(seq_key.len, 4, conn) ||
		pqPutnchar(seq_key.data, seq_key.len, conn) ||
		pqPutnchar((char*)&nextval, sizeof(nextval), conn) ||
		pqPutc(iscalled, conn) ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error, %s, message type = %s",
			PQerrorMessage(conn), "AGTM_MSG_SEQUENCE_SET_VAL")));
	}
	res = agtm_get_result(AGTM_MSG_SEQUENCE_SET_VAL);
	if (res->gr_status != AGTM_RESULT_OK)
		ereport(ERROR,
			(errmsg("agtm_GetResult result not ok, message type = %s, %s",
			"AGTM_MSG_SEQUENCE_SET_VAL", PQerrorMessage(conn))));

	pfree(seq_key.data);
	return (res->gr_resdata.gsq_val);
}

void agtm_XactLockTableWait(TransactionId xid)
{
	AGTM_Result *result;
	StringInfoData buf;

	if(!IsUnderAGTM())
		return;

	initStringInfo(&buf);
	for(;;)
	{
		Assert(TransactionIdIsValid(xid));
		Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));
		pq_sendint(&buf, xid, sizeof(xid));
		xid = SubTransGetParent(xid);
		if(!TransactionIdIsValid(xid))
			break;
	}
	pq_sendint(&buf, InvalidTransactionId, sizeof(TransactionId));

	agtm_send_message(AGTM_MSG_XACT_LOCK_TABLE_WAIT, "%p%d", buf.data, buf.len);
	pfree(buf.data);

	result = agtm_get_result(AGTM_MSG_XACT_LOCK_TABLE_WAIT);
	if(result == NULL || result->gr_status != AGTM_RESULT_OK)
	{
		ereport(ERROR,
			(errmsg("agtm_XactLockTableWait failed:%s", PQerrorMessage(getAgtmConnection()))));
	}
}

void agtm_LockTransactionId(TransactionId xid, char lock_type, bool is_lock)
{
	AGTM_Result *result;

	if(!IsUnderAGTM())
		return;

	agtm_send_message(AGTM_MSG_LOCK_TRANSACTION, "%d%d %c %c", xid, (int)sizeof(xid), lock_type, is_lock);
	result = agtm_get_result(AGTM_MSG_LOCK_TRANSACTION);
	if(result == NULL || result->gr_status != AGTM_RESULT_OK)
	{
		ereport(ERROR,
			(errmsg("agtm_LockTransactionId failed:%s", PQerrorMessage(getAgtmConnection()))));
	}
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
	if(pqPutMsgStart('A', true, conn) < 0)
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
		}else
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
static AGTM_Result* agtm_get_result(AGTM_MessageType msg_type)
{
	PGconn *conn;
	AGTM_Result *result;
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

	if(pqWait(true, false, conn) != 0
		|| pqReadData(conn) < 0
		|| (result = agtm_GetResult()) == NULL)
	{
		ereport(ERROR,
			(errmsg("flush message to AGTM error:%s, message type:%s",
			PQerrorMessage(conn), gtm_util_message_name(msg_type))));
	}

	return result;
}
