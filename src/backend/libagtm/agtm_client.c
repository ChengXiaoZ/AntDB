#include "postgres.h"
#include "miscadmin.h"

#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "access/transam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-int.h"
#include "libpq/fe-protocol3.c"
#include "mb/pg_wchar.h"
#include "pgxc/pgxc.h"
#include "utils/memutils.h"
#include "utils/builtins.h"

/* Configuration variables */
extern char			*AGtmHost;
extern int 			AGtmPort;
static AGTM_Conn	*agtm_conn = NULL;
#define AGTM_PORT	"agtm_port"

#define VALID_AGTM_MESSAGE_TYPE(id) \
	((id) == 'S' || (id) == 'E')

#define safe_free(p)	\
	do {				\
		if ((p))		\
		{				\
			pfree((p));	\
			(p) = NULL;	\
		}				\
	} while (0)

static void agtm_Connect(void);
static void agtm_ConnectByDBname(const char *databaseName);	
static AGTM_Result* agtm_PqParseInput(AGTM_Conn *conn);
static void agtm_PqParseSnapshot(PGconn *conn, AGTM_Result *result);
static int agtm_PqParseSuccess(PGconn *conn, AGTM_Result *result);
static void agtm_PqResetResultData(AGTM_Result* result);

static void
agtm_Connect(void)
{
	agtm_ConnectByDBname(NULL);
}

static void
agtm_ConnectByDBname(const char *databaseName)
{
	const char 		*dbname;
	Datum 			 d_name;
	char 			*userName;
	PGconn volatile	*pg_conn;
	StringInfoData 	 agtmOption;
	char			 port_buf[10];

	if (!IsUnderAGTM())
		return ;

	agtm_Close();

	if (databaseName == NULL)
		dbname = get_database_name(MyDatabaseId);
	else
		dbname = databaseName;
	
	d_name = DirectFunctionCall1(current_user, (Datum)0);
	userName = NameStr(*DatumGetName(d_name));

	sprintf(port_buf, "%d", AGtmPort);
	initStringInfo(&agtmOption);
	appendStringInfo(&agtmOption, "-c client_encoding=%s", GetDatabaseEncodingName());

	pg_conn = PQsetdbLogin(AGtmHost,
						   port_buf,
						   agtmOption.data,
						   NULL,
						   dbname,
						   userName,
						   NULL);
	pfree(agtmOption.data);

	if(pg_conn == NULL)
		ereport(ERROR,
			(errmsg("Fail to connect to AGTM(return NULL pointer)."),
			 errhint("AGTM info(host=%s port=%d dbname=%s user=%s)",
		 		AGtmHost, AGtmPort, dbname, userName)));

	PG_TRY();
	{
		if (PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to AGTM(return bad state: %d).",
					PQstatus((PGconn*)pg_conn)),
				 errhint("AGTM info(host=%s port=%d dbname=%s user=%s)",
					AGtmHost, AGtmPort, dbname, userName)));
		}

		/*
		 * Make sure agtm_conn is null pointer.
		 */
		Assert(agtm_conn == NULL);

		if(agtm_conn == NULL)
		{
			MemoryContext oldctx = NULL;

			oldctx = MemoryContextSwitchTo(TopMemoryContext);
			agtm_conn = (AGTM_Conn *)palloc0(sizeof(AGTM_Conn));
			agtm_conn->pg_Conn = (PGconn*)pg_conn;
			agtm_conn->agtm_Result = (AGTM_Result *)palloc0(sizeof(AGTM_Result));
			agtm_conn->agtm_Result->gr_resdata.snapshot = (GlobalSnapshot)palloc0(sizeof(SnapshotData));
			(void)MemoryContextSwitchTo(oldctx);
		}
	}PG_CATCH();
	{
		PQfinish((PGconn*)pg_conn);
		PG_RE_THROW();
	}PG_END_TRY();

	ereport(LOG,
		(errmsg("Connect to AGTM(host=%s port=%d dbname=%s user=%s) successfully.",
		AGtmHost, AGtmPort, dbname, userName)));
}

int
agtm_GetListenPort(void)
{
	Assert(IS_PGXC_COORDINATOR);

	return agtm_Init();
}

int agtm_Init(void)
{
	const char	*port_str = NULL;
	int			port_int;

	if (!IsUnderAGTM())
		return 0;

	port_str = PQparameterStatus(getAgtmConnection(), AGTM_PORT);
	if (port_str == NULL)
		ereport(ERROR,
			(errmsg("Can not get AGTM listen port.")));

	port_int = atoi(port_str);
	if (port_int < 1024 || port_int > 65535)
		ereport(ERROR,
			(errmsg("Invalid AGTM listen port: %d.", port_int)));

	ereport(DEBUG1,
		(errmsg("Get AGTM listen port: %d,", port_int)));

	return port_int;
}

void
agtm_SetPort(int listen_port)
{
	if(IsConnFromApp())
		ereport(ERROR, (errmsg("Can not set agtm listen port")));
	if(listen_port < 1 || listen_port > 65535)
		ereport(ERROR, (errmsg("Invalid port number %d", listen_port)));
	ereport(LOG,
		(errmsg("Get AGTM listen port: %d from coordinator,", listen_port)));

	/*
	 * Close old connection if received a new AGTM backend listen port
	 */
	if (listen_port != AGtmPort)
	{
		AGtmPort = listen_port;
		agtm_Close();
	}
}

void agtm_Close(void)
{
	if (agtm_conn)
	{
		MemoryContext oldctx = NULL;

		if (agtm_conn->pg_Conn)
		{
			PQfinish(agtm_conn->pg_Conn);
			agtm_conn->pg_Conn = NULL;
		}

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		if (agtm_conn->agtm_Result)
		{
			/* TODO: free agtm_Result */
			safe_free(agtm_conn->agtm_Result->gr_resdata.snapshot);
			safe_free(agtm_conn->agtm_Result);
		}

		pfree(agtm_conn);
		(void)MemoryContextSwitchTo(oldctx);
	}
	agtm_conn = NULL;
}

void agtm_Reset(void)
{
	agtm_Close();
	agtm_Connect();
}

void agtm_BeginMessage(StringInfo buf, char msgtype)
{

}
void agtm_SendString(StringInfo buf, const char *str)
{

}

void agtm_SendInt(StringInfo buf, int i, int b)
{

}

void agtm_Endmessage(StringInfo buf)
{

}

void agtm_Flush(void)
{

}

PGconn*
getAgtmConnection(void)
{
	return getAgtmConnectionByDBname(NULL);
}

PGconn* 
getAgtmConnectionByDBname(const char *dbname)
{
	if (agtm_conn == NULL)
	{
		agtm_ConnectByDBname(dbname);
		return agtm_conn->pg_Conn;
	}

	if (PQstatus(agtm_conn->pg_Conn) == CONNECTION_OK)
		return agtm_conn->pg_Conn;

	/*
	 * Bad AGTM connection
	 */
	if (TopXactBeginAGTM())
	{
		elog(ERROR,
			"Bad AGTM connection, status: %d", PQstatus(agtm_conn->pg_Conn));
	} else
	{
		agtm_Close();
		agtm_ConnectByDBname(dbname);
	}

	return agtm_conn->pg_Conn;
}

AGTM_Result*
agtm_GetResult(void)
{
	AGTM_Result 	*res = NULL;
	AGTM_Conn 		*conn = agtm_conn;

	if (!conn)
		return NULL;

	while ((res = agtm_PqParseInput(conn)) == NULL)
	{
		int			flushResult;

		/*
		 * If data remains unsent, send it.  Else we might be waiting for the
		 * result of a command the backend hasn't even got yet.
		 */
		while ((flushResult = pqFlush(conn->pg_Conn)) > 0)
		{
			if (pqWait(false, true, conn->pg_Conn))
			{
				flushResult = -1;
				break;
			}
		}

		/* Wait for some more data, and load it. */
		if (flushResult ||
			pqWait(true, false, conn->pg_Conn) ||
			pqReadData(conn->pg_Conn) < 0)
		{
			/*
			 * conn->errorMessage has been set by gtmpqWait or gtmpqReadData.
			 */
			return NULL;
		}
	}

	return res;
}

static AGTM_Result*
agtm_PqParseInput(AGTM_Conn *conn)
{
	char		id;
	int			msgLength;
	int			avail;
	AGTM_Result *result = NULL;

	Assert(conn);

	agtm_PqResetResultData(conn->agtm_Result);

	result = conn->agtm_Result;

	/*
	 * Try to read a message.  First get the type code and length. Return
	 * if not enough data.
	 */
	conn->pg_Conn->inCursor = conn->pg_Conn->inStart;
	if (pqGetc(&id, conn->pg_Conn))
		return NULL;

	if (pqGetInt(&msgLength, 4, conn->pg_Conn))
		return NULL;

	/*
	 * Try to validate message type/length here.  A length less than 4 is
	 * definitely broken.  Large lengths should only be believed for a few
	 * message types.
	 */
	if (msgLength < 4)
	{
		handleSyncLoss(conn->pg_Conn, id, msgLength);
		return NULL;
	}

	if (msgLength > 30000 && !VALID_AGTM_MESSAGE_TYPE(id))
	{
		handleSyncLoss(conn->pg_Conn, id, msgLength);
		return NULL;
	}

	/*
	 * Can't process if message body isn't all here yet.
	 */
	msgLength = msgLength -4;
	conn->agtm_Result->gr_msglen = msgLength;
	avail = conn->pg_Conn->inEnd - conn->pg_Conn->inCursor;

	if (avail < msgLength)
	{

		if (pqCheckInBufferSpace(conn->pg_Conn->inCursor + (size_t) msgLength,conn->pg_Conn))
		{
						/*
			 * XXX add some better recovery code... plan is to skip over
			 * the message using its length, then report an error. For the
			 * moment, just treat this like loss of sync (which indeed it
			 * might be!)
			 */
			handleSyncLoss(conn->pg_Conn, id, msgLength);
		}

		return NULL;
	}

	if(id == 'S')
	{
		if(agtm_PqParseSuccess(conn->pg_Conn, result) != 0)
			return NULL;
		/* Successfully consumed this message */
		if (conn->pg_Conn->inCursor == conn->pg_Conn->inStart + 5 + msgLength)
		{
			/* Normal case: parsing agrees with specified length */
			conn->pg_Conn->inStart = conn->pg_Conn->inCursor;
		}
		else
		{
			/* Trouble --- report it */
			printfPQExpBuffer(&conn->pg_Conn->errorMessage,
							  "message contents do not agree with length in message type \"%c\"\n",
							  id);
			/* trust the specified message length as what to skip */
			conn->pg_Conn->inStart += 5 + msgLength;
		}
	}else
	{
		/* use default parsse */
		PGresult *pgres;
		ExecStatusType est;

		conn->pg_Conn->inStart = conn->pg_Conn->inCursor;
		pgres = PQgetResult(conn->pg_Conn);
		est = PQresultStatus(pgres);
		if(est == PGRES_FATAL_ERROR)
		{
			result->gr_status = AGTM_RESULT_ERROR;
			printfPQExpBuffer(&conn->pg_Conn->errorMessage, "%s", PQresultErrorMessage(pgres));
		}
		PQclear(pgres);
	}

	return result;
}

static void
agtm_PqParseSnapshot(PGconn *conn, AGTM_Result *result)
{
	GlobalSnapshot gsnapshot = NULL;

	Assert(conn && result);

	gsnapshot = result->gr_resdata.snapshot;

	if (pqGetnchar((char *)&(gsnapshot->xmin), sizeof(TransactionId), conn) ||	/* xmin */
		pqGetnchar((char *)&(gsnapshot->xmax), sizeof(TransactionId), conn) ||	/* xmax */
		pqGetInt((int*)&(gsnapshot->xcnt), sizeof(uint32),conn))				/* xcnt */
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("get xmin/xmax/xcnt from connection buffer error"));
		result->gr_status = AGTM_RESULT_ERROR;
		return ;
	}

	Assert(result->gr_resdata.snapshot->xip == NULL);							/* xip */
	gsnapshot->xip = (TransactionId *)MemoryContextAllocZero(
						TopMemoryContext,
						sizeof(TransactionId) * gsnapshot->xcnt);
	if (pqGetnchar((char *)gsnapshot->xip, sizeof(TransactionId) * gsnapshot->xcnt, conn))
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("get xip from connection buffer error"));
		result->gr_status = AGTM_RESULT_ERROR;
		return ;
	}

	if (pqGetInt((int*)&(gsnapshot->subxcnt), sizeof(uint32), conn))			/* subxcnt */
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("get subxcnt from connection buffer error"));
		result->gr_status = AGTM_RESULT_ERROR;
		return ;
	}

	Assert(result->gr_resdata.snapshot->subxip == NULL);						/* subxip */
	gsnapshot->subxip = (TransactionId *)MemoryContextAllocZero(
						TopMemoryContext,
						sizeof(TransactionId) * gsnapshot->subxcnt);
	if (pqGetnchar((char *)gsnapshot->subxip, sizeof(TransactionId) * gsnapshot->subxcnt, conn))
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("get subxip from connection buffer error"));
		result->gr_status = AGTM_RESULT_ERROR;
		return ;
	}

	if (pqGetnchar((char *)&(gsnapshot->suboverflowed), sizeof(bool), conn) ||			/* suboverflowed */
		pqGetnchar((char *)&(gsnapshot->takenDuringRecovery), sizeof(bool), conn) ||	/* takenDuringRecovery */
		pqGetnchar((char *)&(gsnapshot->copied), sizeof(bool), conn) ||					/* copied */
		pqGetnchar((char *)&(gsnapshot->curcid), sizeof(uint32), conn) ||				/* curcid */
		pqGetnchar((char *)&(gsnapshot->active_count), sizeof(uint32), conn) ||			/* active_count */
		pqGetnchar((char *)&(gsnapshot->regd_count), sizeof(uint32), conn)) 			/* regd_count */
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("get suboverflowed/takenDuringRecovery/copied/curcid/active_count/regd_count from connection buffer error"));
		result->gr_status = AGTM_RESULT_ERROR;
		return ;
	}
}

static int
agtm_PqParseSuccess(PGconn *conn, AGTM_Result *result)
{
	Assert(conn && result);
	Assert(result->gr_resdata.snapshot);

	if (pqGetInt((int *)&result->gr_type, 4, conn))
	{
		result->gr_status = AGTM_RESULT_ERROR;
		return EOF;
	}

	result->gr_status = AGTM_RESULT_OK;
	result->gr_msglen -= 4;
	switch (result->gr_type)
	{
		case AGTM_SNAPSHOT_GET_RESULT:
			agtm_PqParseSnapshot(conn, result);
			break;

		case AGTM_GET_GXID_RESULT:
			if(pqGetnchar((char *)&result->gr_resdata.grd_gxid,sizeof (TransactionId), conn))
			{
				printfPQExpBuffer(&conn->errorMessage,
					libpq_gettext("get gxid from connection buffer error"));
				result->gr_status = AGTM_RESULT_ERROR;
			}
			break;

		case AGTM_GET_TIMESTAMP_RESULT:
			if (pqGetnchar((char *)&result->gr_resdata.grd_timestamp,sizeof (Timestamp), conn))
			{
				printfPQExpBuffer(&conn->errorMessage,
					libpq_gettext("get timestamp from connection buffer error"));
				result->gr_status = AGTM_RESULT_ERROR;
			}
			break;

		case AGTM_SEQUENCE_GET_NEXT_RESULT:
		case AGTM_MSG_SEQUENCE_GET_CUR_RESULT:
		case AGTM_SEQUENCE_GET_LAST_RESULT:
		case AGTM_SEQUENCE_SET_VAL_RESULT:
			if(pqGetnchar((char *)&result->gr_resdata.gsq_val,sizeof(AGTM_Sequence),conn))
			{
				printfPQExpBuffer(&conn->errorMessage,
					libpq_gettext("get seqval from connection buffer error"));
				result->gr_status = AGTM_RESULT_ERROR;
			}
			break;

		case AGTM_COMPLETE_RESULT:
			/* no message result */
			break;

		default:			
			ereport(ERROR,
				(errmsg("agtm result type is unknow,type : %d",
				result->gr_type)));
			break;

	}

	return (result->gr_status);
}

static void
agtm_PqResetResultData(AGTM_Result* result)
{
	Assert(result);

	switch(result->gr_type)
	{
		case AGTM_GET_GXID_RESULT:
			result->gr_resdata.grd_gxid = InvalidTransactionId;
			break;
		case AGTM_GET_TIMESTAMP_RESULT:
			result->gr_resdata.grd_timestamp = 0;
			break;
		case AGTM_SNAPSHOT_GET_RESULT:
			{
				MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

				result->gr_resdata.snapshot->xmin = InvalidTransactionId;
				result->gr_resdata.snapshot->xmax = InvalidTransactionId;
				result->gr_resdata.snapshot->xcnt = 0;
				if(result->gr_resdata.snapshot->xip != NULL)
				{
					pfree(result->gr_resdata.snapshot->xip);
					result->gr_resdata.snapshot->xip = NULL;
				}

				result->gr_resdata.snapshot->subxcnt = 0;
				if(result->gr_resdata.snapshot->subxip != NULL)
				{
					pfree(result->gr_resdata.snapshot->subxip);
					result->gr_resdata.snapshot->subxip = NULL;
				}

				result->gr_resdata.snapshot->suboverflowed = false;
				result->gr_resdata.snapshot->takenDuringRecovery = false;
				result->gr_resdata.snapshot->copied = false;

				result->gr_resdata.snapshot->curcid = 0;
				result->gr_resdata.snapshot->active_count = 0;
				result->gr_resdata.snapshot->regd_count = 0;

				(void)MemoryContextSwitchTo(oldctx);
			}
			break;

		case AGTM_SEQUENCE_GET_NEXT_RESULT:
		case AGTM_MSG_SEQUENCE_GET_CUR_RESULT:
		case AGTM_SEQUENCE_GET_LAST_RESULT:
		case AGTM_SEQUENCE_SET_VAL_RESULT:		
			result->gr_resdata.gsq_val = 0;
			break;
			
		case AGTM_NONE_RESULT:
		case AGTM_COMPLETE_RESULT:
			break;

		default:		
			ereport(ERROR,
				(errmsg("agtm result type is unknow, type : %d",
				result->gr_type)));
			break;		
	}
	result->gr_type = AGTM_NONE_RESULT;
	result->gr_msglen = 0;
	result->gr_status = AGTM_RESULT_OK;
}
