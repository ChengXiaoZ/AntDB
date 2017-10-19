#include "postgres.h"

#include "access/transam.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_utils.h"
#include "agtm/agtm_client.h"
#include "agtm/agtm_transaction.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxc.h"

#include <unistd.h>

static AGTM_Sequence agtm_DealSequence(const char *seqname, AGTM_MessageType type);

TransactionId
agtm_GetGlobalTransactionId(bool isSubXact)
{
	time_t				finish_time;
	AGTM_Result 		*res;
	GlobalTransactionId gxid;
	PGconn				*conn = NULL;

	if(!IsUnderAGTM())
		return InvalidGlobalTransactionId;

	conn = get_AgtmConnect();

	/* send message*/
	if (pqPutMsgStart('A',true,conn) < 0 ||
		pqPutInt(AGTM_MSG_GET_GXID,4,conn) < 0 ||
		pqPutc((char)isSubXact, conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error,message type : AGTM_MSG_GET_GXID")));
	}

	if(pqFlush(conn))
	{
		pqHandleSendFailure(conn);
		ereport(ERROR, 
			(errmsg("send message to PGconn error,message type : AGTM_MSG_GET_GXID")));
	}

	finish_time = time(NULL) + CLIENT_AGTM_TIMEOUT;
	if (pqWaitTimed(true, false, conn, finish_time) ||
			pqReadData(conn) < 0)
	{
		return InvalidGlobalTransactionId;
	}

	if ( NULL == (res = agtm_GetResult()) )
	{
		return InvalidGlobalTransactionId;
	}

	if (res->gr_status != AGTM_RESULT_OK)
		return InvalidGlobalTransactionId;

	gxid = (GlobalTransactionId)res->gr_resdata.grd_gxid;

	ereport(LOG, 
		(errmsg("get global xid: %d from agtm", gxid)));

	return gxid;

}

Timestamp
agtm_GetTimestamptz(void)
{
	time_t			finish_time;
	AGTM_Result		*res;
	Timestamp		timestamp;
	PGconn 			*conn = NULL;

	if(!IsUnderAGTM())
		return 0;

	conn = get_AgtmConnect();

	/* send message*/
	if(pqPutMsgStart('A',true,conn) < 0 ||
		pqPutInt(AGTM_MSG_GET_TIMESTAMP,4,conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error,message type : AGTM_MSG_GET_TIMESTAMP")));
	}

	if(pqFlush(conn))
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("send message to PGconn error,message type : AGTM_MSG_GET_TIMESTAMP")));
	}

	finish_time = time(NULL) + CLIENT_AGTM_TIMEOUT;
	if (pqWaitTimed(true, false, conn, finish_time) ||
			pqReadData(conn) < 0)
	{
		return 0;
	}

	if ( NULL == (res = agtm_GetResult()) )
	{
		return 0;
	}

	if (res->gr_status != AGTM_RESULT_OK)
		return 0;

	timestamp = res->gr_resdata.grd_timestamp;

	ereport(DEBUG1,
		(errmsg("get timestamp: %ld from agtm", timestamp)));

	return timestamp;
}

GlobalSnapshot
agtm_GetSnapShot(GlobalSnapshot snapshot)
{
	time_t			finish_time;
	AGTM_Result 	*res;
	PGconn			*conn = NULL;

	if(!IsUnderAGTM())
		return NULL;

	conn = get_AgtmConnect();

	/* send message*/
	if(pqPutMsgStart('A',true,conn) < 0 ||
		pqPutInt(AGTM_MSG_SNAPSHOT_GET,4,conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("put message to PGconn error,message type : AGTM_MSG_SNAPSHOT_GET")));
	}

	if(pqFlush(conn))
	{
		pqHandleSendFailure(conn);
		ereport(ERROR, 
			(errmsg("send message to PGconn error,message type : AGTM_MSG_SNAPSHOT_GET")));
	}

	finish_time = time(NULL) + CLIENT_AGTM_TIMEOUT;
	if(pqWaitTimed(true, false, conn, finish_time) ||
			pqReadData(conn) < 0)
	{
		return NULL;
	}

	if ( NULL == (res = agtm_GetResult()) )
	{
		return NULL;
	}

	if (res->gr_status != AGTM_RESULT_OK)
		return NULL;

	snapshot->xmin = res->gr_resdata.snapshot->xmin;
	snapshot->xmax = res->gr_resdata.snapshot->xmax;
	snapshot->xcnt = res->gr_resdata.snapshot->xcnt;
	snapshot->xip = (TransactionId*)malloc(sizeof(TransactionId) * snapshot->xcnt);
	if(snapshot->xip == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("agtm_GetSnapShot malloc memory failed")));
	}

	memcpy(snapshot->xip,res->gr_resdata.snapshot->xip,
		sizeof(TransactionId) * snapshot->xcnt);

	snapshot->subxcnt = res->gr_resdata.snapshot->subxcnt;
	snapshot->subxip = (TransactionId*)malloc(sizeof(TransactionId) * snapshot->subxcnt);
	if(snapshot->subxip == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("agtm_GetSnapShot malloc memory failed")));
	}

	memcpy(snapshot->subxip,res->gr_resdata.snapshot->subxip,
		sizeof(TransactionId) * snapshot->subxcnt);

	snapshot->suboverflowed = res->gr_resdata.snapshot->suboverflowed;
	snapshot->takenDuringRecovery = res->gr_resdata.snapshot->takenDuringRecovery;
	snapshot->copied = res->gr_resdata.snapshot->copied;
	snapshot->curcid = res->gr_resdata.snapshot->curcid;
	snapshot->active_count = res->gr_resdata.snapshot->active_count;
	snapshot->regd_count = res->gr_resdata.snapshot->regd_count;

	return snapshot;
}

static AGTM_Sequence 
agtm_DealSequence(const char *seqname, AGTM_MessageType type)
{
	time_t			finish_time;
	AGTM_Result		*res;
	PGconn			*conn = NULL;
	StringInfoData 	seq_key;
	
	if(!IsUnderAGTM())
		return 0;
	
	conn = get_AgtmConnect();

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = (%d), parameter seqname is null",
			(int)type)));

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
			(errmsg("put message to PGconn error,message type = (%d)",
			(int)type)));
	}
	
	if(pqFlush(conn))
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("send message to PGconn error,message type = (%d)",
			(int)type)));
	}
	
	finish_time = time(NULL) + CLIENT_AGTM_TIMEOUT;
	if(pqWaitTimed(true, false, conn, finish_time) ||
		pqReadData(conn) < 0)
			ereport(ERROR,
				(errmsg("wait agtm return result timeout or read message from PGconn error"),
				 errhint("message type = (%d)",
				 (int)type)));
	
	
	if ( NULL == (res = agtm_GetResult()) )
		ereport(ERROR,
			(errmsg("parse message from AGTM_Result error,message type = (%d)",
			(int)type)));
	
	if (res->gr_status != AGTM_RESULT_OK)
		ereport(ERROR,
			(errmsg("result status is not ok ,message type = (%d)",
			(int)type)));

	pfree(seq_key.data);
	return (res->gr_resdata.gsq_val);
	
}

AGTM_Sequence 
agtm_GetSeqNextVal(const char *seqname)
{
	return (agtm_DealSequence(seqname,AGTM_MSG_SEQUENCE_GET_NEXT));	
}

AGTM_Sequence 
agtm_GetSeqCurrVal(const char *seqname)
{
	return (agtm_DealSequence(seqname,AGTM_MSG_SEQUENCE_GET_CUR));
}

AGTM_Sequence
agtm_GetSeqLastVal(const char *seqname)
{
	return (agtm_DealSequence(seqname,AGTM_MSG_SEQUENCE_GET_LAST));
}


AGTM_Sequence

agtm_SetSeqVal(const char *seqname, AGTM_Sequence nextval)
{
	return (agtm_SetSeqValCalled(seqname, nextval, true));
}

AGTM_Sequence

agtm_SetSeqValCalled(const char *seqname, AGTM_Sequence nextval, bool iscalled)
{
	time_t			finish_time;
	AGTM_Result 	*res;
	PGconn 			*conn = NULL;
	StringInfoData 	seq_key;
	
	if(!IsUnderAGTM())
		return 0;
	
	conn = get_AgtmConnect();

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = %s (%d), parameter seqname is null",
			"AGTM_MSG_SEQUENCE_SET_VAL",
			(int)AGTM_MSG_SEQUENCE_SET_VAL)));
	
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
			(errmsg("put message to PGconn error,message type = %s (%d)",
			"AGTM_MSG_SEQUENCE_SET_VAL",
			(int)AGTM_MSG_SEQUENCE_SET_VAL)));
	}
	
	if(pqFlush(conn))
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("send message to PGconn error,message type = %s (%d)",
			"AGTM_MSG_SEQUENCE_SET_VAL",
			(int)AGTM_MSG_SEQUENCE_SET_VAL)));
	}
	
	finish_time = time(NULL) + CLIENT_AGTM_TIMEOUT;
	if(pqWaitTimed(true, false, conn, finish_time) ||
		pqReadData(conn) < 0)
			ereport(ERROR,
				(errmsg("wait agtm return result timeout or read message from PGconn error"),
				 errhint("message type = %s (%d)",
				 "AGTM_MSG_SEQUENCE_SET_VAL",
				 (int)AGTM_MSG_SEQUENCE_SET_VAL)));
	
	
	if ( NULL == (res = agtm_GetResult()) )
		ereport(ERROR,
			(errmsg("parse message from AGTM_Result error,message type = %s (%d)",
			"AGTM_MSG_SEQUENCE_SET_VAL",
			(int)AGTM_MSG_SEQUENCE_SET_VAL)));
	
	if (res->gr_status != AGTM_RESULT_OK)
		ereport(ERROR,
			(errmsg("result status is not ok ,message type = %s (%d)",
			"AGTM_MSG_SEQUENCE_SET_VAL",
			(int)AGTM_MSG_SEQUENCE_SET_VAL)));

	pfree(seq_key.data);
	return (res->gr_resdata.gsq_val);
}

