#include "postgres.h"

#include "access/htup_details.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_utils.h"
#include "agtm/agtm_client.h"
#include "agtm/agtm_transaction.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "nodes/parsenodes.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

static AGTM_Sequence agtm_DealSequence(const char *seqname, const char * database,
								const char * schema, AGTM_MessageType type, AGTM_ResultType rtype);
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

	ereport(DEBUG1, 
		(errmsg("get global xid: %d from agtm", gxid)));

	agtm_use_result_end(&buf);
	PQclear(res);
	return gxid;
}

void
agtm_CreateSequence(const char * seqName, const char * database,
						const char * schema , List * seqOptions)
{
	PGresult 		*res;

	int				nameSize;
	int				databaseSize;
	int				schemaSize;
	StringInfoData	buf;
	StringInfoData	strOption;

	Assert(seqName != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		return;

	initStringInfo(&strOption);
	parse_seqOption_to_string(seqOptions, &strOption);

	nameSize = strlen(seqName);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_INIT, 
					  "%d%d %p%d %d%d %p%d %d%d %p%d %p%d",
					  nameSize, 4,
					  seqName, nameSize,
					  databaseSize, 4,
					  database, databaseSize,
					  schemaSize, 4,
					  schema, schemaSize,
					  strOption.data, strOption.len);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_INIT);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_INIT_RESULT);

	agtm_use_result_end(&buf);
	pfree(strOption.data);
	PQclear(res);
	ereport(DEBUG1,
		(errmsg("create sequence on agtm :%s", seqName)));
}

void
agtm_AlterSequence(const char * seqName, const char * database,
						const char * schema ,  List * seqOptions)
{
	PGresult 		*res;

	int				nameSize;
	int				databaseSize;
	int				schemaSize;
	StringInfoData	buf;
	StringInfoData	strOption;

	Assert(seqName != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		return;

	initStringInfo(&strOption);
	parse_seqOption_to_string(seqOptions, &strOption);

	nameSize = strlen(seqName);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_ALTER,
					  "%d%d %p%d %d%d %p%d %d%d %p%d %p%d",
					  nameSize, 4,
					  seqName, nameSize,
					  databaseSize, 4,
					  database, databaseSize,
					  schemaSize, 4, schema,
					  schemaSize, strOption.data,
					  strOption.len);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_ALTER);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_ALTER_RESULT);
	
	agtm_use_result_end(&buf);
	pfree(strOption.data);
	PQclear(res);
	ereport(DEBUG1,
		(errmsg("alter sequence on agtm :%s", seqName)));
}

void
agtm_DropSequence(const char * seqName, const char * database, const char * schema)
{
	int				seqNameSize;
	int				dbNameSize;
	int				schemaNameSize;
	StringInfoData	buf;

	PGresult 		*res;

	Assert(seqName != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		return;

	seqNameSize = strlen(seqName);
	dbNameSize = strlen(database);
	schemaNameSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_DROP,
					 "%d%d %p%d %d%d %p%d %d%d %p%d",
					 seqNameSize, 4,
					 seqName, seqNameSize,
					 dbNameSize, 4,
					 database, dbNameSize,
					 schemaNameSize, 4,
					 schema, schemaNameSize);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_DROP);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_DROP_RESULT);

	agtm_use_result_end(&buf);
	PQclear(res);
	ereport(DEBUG1,
		(errmsg("drop sequence on agtm :%s", seqName)));
}

void 
agtms_DropSequenceByDataBase(const char * database)
{
	int				dbNameSize;
	StringInfoData	buf;

	PGresult 		*res;

	Assert(database != NULL);

	if(!IsUnderAGTM())
		return;

	dbNameSize = strlen(database);
	agtm_send_message(AGTM_MSG_SEQUENCE_DROP_BYDB, "%d%d %p%d", dbNameSize, 4, database, dbNameSize);
	res = agtm_get_result(AGTM_MSG_SEQUENCE_DROP_BYDB);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_DROP_BYDB_RESULT);

	agtm_use_result_end(&buf);
	PQclear(res);
	ereport(DEBUG1,
		(errmsg("drop sequence on agtm by database :%s", database)));
}

void agtm_RenameSequence(const char * seqName, const char * database,
							const char * schema, const char* newName, SequenceRenameType type)
{
	int	seqNameSize;
	int	dbNameSize;
	int	schemaNameSize;
	int	newNameSize;	

	PGresult 		*res;
	StringInfoData	buf;
	Assert(seqName != NULL && database != NULL && schema != NULL && newName != NULL);
	if(!IsUnderAGTM())
		return;

	seqNameSize = strlen(seqName);
	dbNameSize = strlen(database);
	schemaNameSize = strlen(schema);
	newNameSize = strlen(newName);

	agtm_send_message(AGTM_MSG_SEQUENCE_RENAME,
					  "%d%d %p%d %d%d %p%d %d%d %p%d %d%d %p%d %d%d",
					  seqNameSize, 4,
					  seqName, seqNameSize,
					  dbNameSize, 4,
					  database, dbNameSize,
					  schemaNameSize, 4,
					  schema, schemaNameSize,
					  newNameSize, 4,
					  newName, newNameSize,
					  type, 4);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_RENAME);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_RENAME_RESULT);

	agtm_use_result_end(&buf);
	PQclear(res);
	ereport(DEBUG1,
		(errmsg("rename sequence %s rename to %s", seqName, newName)));
}

extern void
agtm_RenameSeuqneceByDataBase(const char * oldDatabase,
											const char * newDatabase)
{
	int				oldNameSize;
	int				newNameSize;
	StringInfoData	buf;

	PGresult 		*res;

	Assert(oldDatabase != NULL && newDatabase != NULL);

	if(!IsUnderAGTM())
		return;

	oldNameSize = strlen(oldDatabase);
	newNameSize = strlen(newDatabase);
	agtm_send_message(AGTM_MSG_SEQUENCE_RENAME_BYDB,
					"%d%d %p%d %d%d %p%d",
					oldNameSize, 4,
					oldDatabase, oldNameSize,
					newNameSize, 4,
					newDatabase, newNameSize);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_RENAME_BYDB);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_RENAME_BYDB_RESULT);

	agtm_use_result_end(&buf);
	PQclear(res);
	ereport(DEBUG1,
		(errmsg("alter sequence on agtm by database rename old name :%s, new name :%s ",
				oldDatabase , newDatabase)));
	
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

	agtm_send_message(AGTM_MSG_GET_TIMESTAMP, " ");
	res = agtm_get_result(AGTM_MSG_GET_TIMESTAMP);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_TIMESTAMP_RESULT);
	pq_copymsgbytes(&buf, (char*)&timestamp, sizeof(timestamp));

	ereport(DEBUG1,
		(errmsg("get timestamp: %ld from agtm", timestamp)));

	agtm_use_result_end(&buf);
	PQclear(res);
	return timestamp;
}

Snapshot
agtm_GetGlobalSnapShot(Snapshot snapshot)
{
	PGresult 	*res;
	const char *str;
	StringInfoData	buf;
	uint32 xcnt;
	TimestampTz	globalXactStartTimestamp;

	AssertArg(snapshot && snapshot->xip && snapshot->subxip);

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetGlobalSnapShot function must under AGTM")));

	agtm_send_message(AGTM_MSG_SNAPSHOT_GET, " ");
	res = agtm_get_result(AGTM_MSG_SNAPSHOT_GET);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SNAPSHOT_GET_RESULT);

	pq_copymsgbytes(&buf, (char*)&(globalXactStartTimestamp), sizeof(globalXactStartTimestamp));
	SetCurrentTransactionStartTimestamp(globalXactStartTimestamp);
	pq_copymsgbytes(&buf, (char*)&(RecentGlobalXmin), sizeof(RecentGlobalXmin));
	pq_copymsgbytes(&buf, (char*)&(snapshot->xmin), sizeof(snapshot->xmin));
	pq_copymsgbytes(&buf, (char*)&(snapshot->xmax), sizeof(snapshot->xmax));
	xcnt = pq_getmsgint(&buf, sizeof(snapshot->xcnt));
	EnlargeSnapshotXip(snapshot, xcnt);
	snapshot->xcnt = xcnt;
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
	PQclear(res);

	if (GetCurrentCommandId(false) > snapshot->curcid)
		snapshot->curcid = GetCurrentCommandId(false);
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
	PQclear(res);
	return xid_status;
}

void
agtm_SyncLocalNextXid(TransactionId *local_xid,		/* output */
					  TransactionId *agtm_xid)		/* output */
{
	PGresult *volatile res = NULL;
	TransactionId	lxid,	/* local xid */
					axid;	/* agtm xid */
	StringInfoData	buf;

	PG_TRY();
	{
		lxid = ReadNewTransactionId();
		agtm_send_message(AGTM_MSG_SYNC_XID, "%d%d", (int)lxid, (int)sizeof(lxid));
		res = agtm_get_result(AGTM_MSG_SYNC_XID);
		Assert(res);
		agtm_use_result_type(res, &buf, AGTM_SYNC_XID_RESULT);
		axid = (TransactionId) pq_getmsgint(&buf, 4);

		ereport(DEBUG1,
			(errmsg("Sync local xid %u with AGTM xid %u OK", lxid, axid)));

		agtm_use_result_end(&buf);
		PQclear(res);

		if (local_xid)
			*local_xid = lxid;
		if (agtm_xid)
			*agtm_xid = axid;
	} PG_CATCH();
	{
		PQclear(res);
		PG_RE_THROW();
	} PG_END_TRY();
}

Datum sync_agtm_xid(PG_FUNCTION_ARGS)
{
	TransactionId	lxid,	/* local xid */
					axid;	/* agtm xid */
	TupleDesc		tupdesc;
	Datum			values[3];
	bool			isnull[3];
	NameData		nodename;

	agtm_SyncLocalNextXid(&lxid, &axid);

	tupdesc = CreateTemplateTupleDesc(3, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "node",
					   NAMEOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "local",
					   XIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "agtm",
					   XIDOID, -1, 0);

	BlessTupleDesc(tupdesc);

	memset(isnull, 0, sizeof(isnull));

	if (PGXCNodeName && PGXCNodeName[0])
	{
		namestrcpy(&nodename, PGXCNodeName);
		values[0] = NameGetDatum(&nodename);
	} else
	{
		isnull[0] = true;
	}

	values[1] = TransactionIdGetDatum(lxid);
	values[2] = TransactionIdGetDatum(axid);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

static AGTM_Sequence 
agtm_DealSequence(const char *seqname, const char * database,
								const char * schema, AGTM_MessageType type, AGTM_ResultType rtype)
{
	PGresult		*res;
	StringInfoData	buf;
	int				seqNameSize;
	int 			databaseSize;
	int				schemaSize;
	AGTM_Sequence	seq;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_DealSequence function must under AGTM")));

	if(seqname[0] == '\0' || database[0] == '\0' || schema[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = (%s), parameter seqname is null", gtm_util_message_name(type))));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);
	agtm_send_message(type, 
					"%d%d %p%d %d%d %p%d %d%d %p%d",
					seqNameSize, 4,
					seqname, seqNameSize,
					databaseSize, 4,
					database, databaseSize,
					schemaSize, 4,
					schema, schemaSize);

	res = agtm_get_result(type);
	Assert(res);
	agtm_use_result_type(res, &buf, rtype);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(&buf);
	PQclear(res);
	return seq;
}

AGTM_Sequence 
agtm_GetSeqNextVal(const char *seqname, const char * database,	const char * schema)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);

	return agtm_DealSequence(seqname, database, schema, AGTM_MSG_SEQUENCE_GET_NEXT
			, AGTM_SEQUENCE_GET_NEXT_RESULT);
}

AGTM_Sequence 
agtm_GetSeqCurrVal(const char *seqname, const char * database,	const char * schema)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);
	
	return agtm_DealSequence(seqname, database, schema, AGTM_MSG_SEQUENCE_GET_CUR
			, AGTM_MSG_SEQUENCE_GET_CUR_RESULT);
}

AGTM_Sequence
agtm_GetSeqLastVal(const char *seqname, const char * database,	const char * schema)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);
	
	return agtm_DealSequence(seqname, database, schema, AGTM_MSG_SEQUENCE_GET_LAST
			, AGTM_SEQUENCE_GET_LAST_RESULT);
}

AGTM_Sequence
agtm_SetSeqVal(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextval)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);

	return (agtm_SetSeqValCalled(seqname, database, schema, nextval, true));
}

AGTM_Sequence
agtm_SetSeqValCalled(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextval, bool iscalled)
{
	PGresult		*res;
	StringInfoData	buf;
	AGTM_Sequence	seq;

	int				seqNameSize;
	int 			databaseSize;
	int				schemaSize;

	Assert(seqname != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_SetSeqValCalled function must under AGTM")));

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = %s, parameter seqname is null",
			"AGTM_MSG_SEQUENCE_SET_VAL")));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_SET_VAL,
					"%d%d %p%d %d%d %p%d %d%d %p%d" INT64_FORMAT "%c",
					seqNameSize, 4,
					seqname, seqNameSize,
					databaseSize, 4,
					database, databaseSize,
					schemaSize, 4,
					schema, schemaSize,
					nextval, iscalled);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_SET_VAL);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SEQUENCE_SET_VAL_RESULT);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(&buf);
	PQclear(res);
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


void
parse_seqOption_to_string(List * seqOptions, StringInfo strOption)
{
	ListCell   *option;
	int listSize = list_length(seqOptions);

	appendBinaryStringInfo(strOption, (const char *) &listSize, sizeof(listSize));

	if(listSize == 0)
		return;

	foreach(option, seqOptions)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if(defel->defnamespace)
		{
			int defnamespaceSize = strlen(defel->defnamespace);
			appendBinaryStringInfo(strOption, (const char *)&defnamespaceSize, sizeof(defnamespaceSize));
			appendStringInfo(strOption, "%s", defel->defnamespace);
		}
		else
		{
			int defnamespaceSize = 0;
			appendBinaryStringInfo(strOption, (const char *)&defnamespaceSize, sizeof(defnamespaceSize));
		}

		if (defel->defname)
		{
			int defnameSize = strlen(defel->defname);
			appendBinaryStringInfo(strOption, (const char *)&defnameSize, sizeof(defnameSize));
			appendStringInfo(strOption, "%s", defel->defname);
		}
		else
		{
			int defnameSize = 0;
			appendBinaryStringInfo(strOption, (const char *)&defnameSize, sizeof(defnameSize));
		}

		if (defel->arg)
		{
			AgtmNodeTag type = T_AgtmInvalid;

			switch(nodeTag(defel->arg))
			{
				case T_Integer:
				{
					long ival = intVal(defel->arg);
					type = T_AgtmInteger;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char *)&ival, sizeof(ival));
					break;
				}
				case T_Float:
				{
					char *str = strVal(defel->arg);
					int strSize = strlen(str);
					type = T_AgtmFloat;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char *)&strSize, sizeof(strSize));
					appendStringInfo(strOption, "%s", str);
					break;
				}
				case T_String:
				{
					char *str = strVal(defel->arg);
					int strSize = strlen(str);
					type = T_AgtmString;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char *)&strSize, sizeof(strSize));
					appendStringInfo(strOption, "%s", str);
					break;
				}
				case T_BitString:
				{
					ereport(ERROR,
						(errmsg("T_BitString is not support")));
					break;
				}
				case T_Null:
				{
					type = T_AgtmNull;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					break;
				}
				case T_List:
				{
					if (strcmp(defel->defname, "owned_by") == 0)
						listSize = listSize -1;

					if (listSize == 0)
					{
						initStringInfo(strOption);
						appendBinaryStringInfo(strOption, (const char *) &listSize, sizeof(listSize));
						return;
					}
					break;
				}
				default:
				{
					ereport(ERROR,
						(errmsg("sequence DefElem type error : %d", nodeTag(defel->arg))));
					break;
				}
			}
		}
		else
		{
			int argSize = 0;
			appendBinaryStringInfo(strOption, (const char *)&argSize, sizeof(argSize));
		}

		appendBinaryStringInfo(strOption, (const char *)&defel->defaction, sizeof(defel->defaction));
	}
}

