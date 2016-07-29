#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "libpq/fe-exec.c"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "pgxc/pgxc.h"
#include "utils/guc.h"

#define FAILED 0
#define SUCESS 1
#define COMMAND_SIZE 256
#define BEGIN_COMMAND_SIZE 1024

//#define ProcessResult(a) (AssertMacro(0),false)
//#define ResetCancelConn() Assert(0)

static char* agtm_generate_begin_command(void);
static bool  AcceptAgtmResult(const PGresult *result);
static bool  AgtmProcessResult(PGresult **results);
static void  agtm_node_send_query(const char *query);
static void  agtm_node_send_query_with_dbname(const char *query,const char *dbname);
static bool  CheckAgtmConnection(void);
static void  CheckAndExecOnAgtm(const char *dmlOperation);
static bool  ConnectionAgtmUp(void);

/* CheckAndExecOnAgtm
 *
 * check dml string is null and exectue on agtm
 */
static void
CheckAndExecOnAgtm(const char *dmlOperation)
{
	if (!IsUnderAGTM())
		return;

	if(!dmlOperation || dmlOperation[0] == '\0')
		return;

	agtm_node_send_query(dmlOperation);
}

/* ConnectionAgtmUp
 *
 * Returns whether our backend connection is still there.
 */
static bool
ConnectionAgtmUp(void)
{
	return PQstatus(getAgtmConnection()) != CONNECTION_BAD;
}

static bool
CheckAgtmConnection(void)
{
	bool		OK;
	
	OK = ConnectionAgtmUp();
	if (!OK)
	{
		ereport(INFO,
			(errmsg("AGTMconn status CONNECTION_BAD, try to reconnect")));

		/* try reconnect to agtm */
		agtm_Reset();	
		OK = ConnectionAgtmUp();
		if (!OK)
			ereport(INFO, (errmsg("reconnect agtm error")));
	}
	return OK;
}

static bool
AcceptAgtmResult(const PGresult *result)
{
	bool		OK;
	
	if (!result)
		OK = false;
	else
		switch (PQresultStatus(result))
		{
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
			case PGRES_EMPTY_QUERY:
			case PGRES_COPY_IN:
			case PGRES_COPY_OUT:
				/* Fine, do nothing */
				OK = true;
				break;
				
			case PGRES_BAD_RESPONSE:
			case PGRES_NONFATAL_ERROR:
			case PGRES_FATAL_ERROR:
				OK = false;
				break;

			default:
				OK = false;
				ereport(WARNING,
					(errmsg("unexpected PQresultStatus: %d",
						PQresultStatus(result))));
				break;
		}

	if (!OK)
	{
		ereport(WARNING,
			(errmsg("[From AGTM] %s", PQresultErrorMessage(result))));
		CheckAgtmConnection();			
	}	
	return OK;
}

static bool
AgtmProcessResult(PGresult **results)
{
	bool		success = true;

	for(;;)
	{
		ExecStatusType result_status;
		bool		is_copy;
		PGresult   *next_result;

		if (!AcceptAgtmResult(*results))
		{
			/*
			 * Failure at this point is always a server-side failure or a
			 * failure to submit the command string.  Either way, we're
			 * finished with this command string.
			 */
			success = false;
			break;
		}

		result_status = PQresultStatus(*results);
		switch (result_status)
		{
			case PGRES_EMPTY_QUERY:
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
				is_copy = false;
				break;

			case PGRES_COPY_OUT:
			case PGRES_COPY_IN:
				is_copy = true;
				break;
			default:
				is_copy = false;
				ereport(WARNING,(errmsg("unexpected PQresultStatus: %d", 
					result_status)));
				break;
		}

		if (is_copy)
		{
			
		}

		next_result = PQgetResult(getAgtmConnection());
		if (!next_result)
			break;
		PQclear(*results);
		*results = next_result;
	}
	return success;
}

static char*
agtm_generate_begin_command(void)
{
	static char begin_cmd[BEGIN_COMMAND_SIZE];
	const char *read_only;
	const char *isolation_level;
	TransactionId xid;

	/*
	 * First get the READ ONLY status because the next call to GetConfigOption
	 * will overwrite the return buffer
	 */
	if (strcmp(GetConfigOption("transaction_read_only", false, false), "on") == 0)
		read_only = "READ ONLY";
	else
		read_only = "READ WRITE";

	/* Now get the isolation_level for the transaction */
	isolation_level = GetConfigOption("transaction_isolation", false, false);
	if (strcmp(isolation_level, "default") == 0)
		isolation_level = GetConfigOption("default_transaction_isolation", false, false);
	

	/* Get local new xid, also is minimum xid from AGTM absolutely */
	xid = ReadNewTransactionId();

	/* Finally build a START TRANSACTION command */
	sprintf(begin_cmd,
		"START TRANSACTION ISOLATION LEVEL %s %s LEAST XID IS %u",
		isolation_level, read_only, xid);

	return begin_cmd;
}

static void
agtm_node_send_query(const char *query)
{
	agtm_node_send_query_with_dbname(query,NULL);
}

static void
agtm_node_send_query_with_dbname(const char *query,const char *dbname)

{
	PGresult 	*results = NULL;
	PGconn		*agtm_conn = NULL;
	bool		OK = false;

	agtm_conn = getAgtmConnectionByDBname(dbname);
	
	if (NULL == agtm_conn)
		ereport(ERROR,
			(errmsg("Failt to get agtm connection(return NULL pointer)!"),
			 errhint("query is: %s", query)));

	if (NULL == (results = PQexec(agtm_conn,query)))
		ereport(ERROR,
			(errmsg("Failt to PQexec command(PGresult is NULL)"),
			 errhint("query is: %s", query)));

	OK = AgtmProcessResult(&results);
	PQclear(results);
	if (!OK)
		ereport(ERROR,
			(errmsg("process command is not ok, query is: %s", query)));
}

void agtm_BeginTransaction(void)
{
	agtm_BeginTransaction_ByDBname(NULL);
}

void agtm_BeginTransaction_ByDBname(const char *dbname)
{
	char * agtm_begin_cmd = NULL;
	
	if (!IsUnderAGTM())
		return ;

	if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
		return ;

	if (TopXactBeginAGTM())
		return ;
 
	agtm_begin_cmd = agtm_generate_begin_command();

	agtm_node_send_query_with_dbname(agtm_begin_cmd, dbname);

	SetTopXactBeginAGTM(true);
}

void agtm_PrepareTransaction(const char *prepared_gid)
{
	agtm_PrepareTransaction_ByDBname(prepared_gid,NULL);
}

void agtm_PrepareTransaction_ByDBname(const char *prepared_gid, const char *dbname)
{
	StringInfoData prepare_cmd;

	if (!IsUnderAGTM())
		return ;

	if (prepared_gid == NULL || prepared_gid[0] == 0x00)
		return ;

	if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
		return ;

	if (!TopXactBeginAGTM())
		return ;

	initStringInfo(&prepare_cmd);
	appendStringInfo(&prepare_cmd, "PREPARE TRANSACTION '%s'", prepared_gid);

	PG_TRY();
	{
		agtm_node_send_query_with_dbname(prepare_cmd.data, dbname);
	} PG_CATCH();
	{
		pfree(prepare_cmd.data);
		agtm_Close();
		SetTopXactBeginAGTM(false);
		PG_RE_THROW();
	} PG_END_TRY();

	pfree(prepare_cmd.data);

	SetTopXactBeginAGTM(false);
}

void agtm_CommitTransaction(const char *prepared_gid, bool missing_ok)
{
	agtm_CommitTransaction_ByDBname(prepared_gid,missing_ok,NULL);
}

void agtm_CommitTransaction_ByDBname(const char *prepared_gid, bool missing_ok, const char *dbname)
{
	StringInfoData commit_cmd;

	if (!IsUnderAGTM())
		return ;

	if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
		return ;

	/*
	 * Return directly if prepared_gid is null and current transaction
	 * does not begin at AGTM.
	 */
	if (!TopXactBeginAGTM() && !prepared_gid)
		return ;

	initStringInfo(&commit_cmd);

	if(prepared_gid != NULL)
	{
		/*
		 * If prepared_gid is not null, assert that top transaction
		 * does not begin at AGTM.
		 */
		Assert(!TopXactBeginAGTM());
		appendStringInfo(&commit_cmd,
						 "COMMIT PREPARED%s '%s'",
						 missing_ok ? " IF EXISTS" : "",
						 prepared_gid);
	} else
	{
		appendStringInfoString(&commit_cmd, "COMMIT TRANSACTION");
	}

	PG_TRY();
	{
		agtm_node_send_query_with_dbname(commit_cmd.data, dbname);
	} PG_CATCH();
	{
		pfree(commit_cmd.data);
		if (TopXactBeginAGTM())
		{
			agtm_Close();
			SetTopXactBeginAGTM(false);
		}
		PG_RE_THROW();
	} PG_END_TRY();

	pfree(commit_cmd.data);

	SetTopXactBeginAGTM(false);
}

void agtm_AbortTransaction(const char *prepared_gid, bool missing_ok)
{
	agtm_AbortTransaction_ByDBname(prepared_gid,missing_ok,NULL);
}

void agtm_AbortTransaction_ByDBname(const char *prepared_gid, bool missing_ok, const char *dbname)
{
	StringInfoData abort_cmd;

	if (!IsUnderAGTM())
		return;

	if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
		return ;

	/*
	 * return directly if prepared_gid is null and current xact
	 * does not begin at AGTM.
	 */
	if (!TopXactBeginAGTM() && !prepared_gid)
		return ;

	initStringInfo(&abort_cmd);

	if(prepared_gid != NULL)
	{
		/*
		 * If prepared_gid is not null, assert that top transaction
		 * does not begin at AGTM.
		 */
		Assert(!TopXactBeginAGTM());
		appendStringInfo(&abort_cmd,
						 "ROLLBACK PREPARED%s '%s'",
						 missing_ok ? " IF EXISTS" : "",
						 prepared_gid);
	} else
	{
		appendStringInfoString(&abort_cmd, "ROLLBACK TRANSACTION");
	}

	PG_TRY();
	{
		agtm_node_send_query_with_dbname(abort_cmd.data, dbname);
	} PG_CATCH();
	{
		pfree(abort_cmd.data);
		if (TopXactBeginAGTM())
		{
			agtm_Close();
			SetTopXactBeginAGTM(false);
		}
		PG_RE_THROW();
	} PG_END_TRY();

	pfree(abort_cmd.data);

	SetTopXactBeginAGTM(false);
}

void
agtm_sequence(const char *dmlSeq)
{
	CheckAndExecOnAgtm(dmlSeq);
}

void
agtm_Database(const char *dmlDatabase)
{
	CheckAndExecOnAgtm(dmlDatabase);
}

void
agtm_TableSpace(const char *dmlTableSpace)
{
	CheckAndExecOnAgtm(dmlTableSpace);
}

void
agtm_Schema(const char *dmlSchema)
{
	CheckAndExecOnAgtm(dmlSchema);
}

void
agtm_User(const char *dmlUser)
{
	CheckAndExecOnAgtm(dmlUser);
}

