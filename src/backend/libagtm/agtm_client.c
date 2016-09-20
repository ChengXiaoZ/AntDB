#include "postgres.h"
#include "miscadmin.h"

#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "agtm/agtm_utils.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-int.h"
#include "libpq/fe-protocol3.c"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* Configuration variables */
extern char				*AGtmHost;
extern int 				AGtmPort;

#define AGTM_PORT		"agtm_port"
#define InvalidAGtmPort	0

static AGTM_Conn		*agtm_conn = NULL;
static char				*save_AGtmHost = NULL;
static int				save_AGtmPort = 0;
static int				save_DefaultAGtmPort = InvalidAGtmPort;
static bool				IsDefaultAGtmPortSave = false;

#define SaveDefaultAGtmPort(port)			\
	do {									\
		if (!IsDefaultAGtmPortSave)			\
			save_DefaultAGtmPort = (port);	\
		IsDefaultAGtmPortSave = true;		\
	} while(0)

static void agtm_Connect(void);

static void
agtm_Connect(void)
{
	PGconn volatile	*pg_conn;
	StringInfoData 	 agtmOption;
	char			 port_buf[10];

	if (!IsUnderAGTM())
		return ;

	agtm_Close();

	SaveDefaultAGtmPort(AGtmPort);

	sprintf(port_buf, "%d", AGtmPort);
	initStringInfo(&agtmOption);
	appendStringInfo(&agtmOption, "-c client_encoding=%s", GetDatabaseEncodingName());

	pg_conn = PQsetdbLogin(AGtmHost,
						   port_buf,
						   agtmOption.data,
						   NULL,
						   AGTM_DBNAME,
						   AGTM_USER,
						   NULL);
	pfree(agtmOption.data);

	if(pg_conn == NULL)
		ereport(ERROR,
			(errmsg("Fail to connect to AGTM(return NULL pointer)."),
			 errhint("AGTM info(host=%s port=%d dbname=%s user=%s)",
		 		AGtmHost, AGtmPort, AGTM_DBNAME, AGTM_USER)));

	PG_TRY();
	{
		if (PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to AGTM %s",
					PQerrorMessage((PGconn*)pg_conn)),
				 errhint("AGTM info(host=%s port=%d dbname=%s user=%s)",
					AGtmHost, AGtmPort, AGTM_DBNAME, AGTM_USER)));
		}

		save_AGtmHost = AGtmHost;
		save_AGtmPort = AGtmPort;

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
			(void)MemoryContextSwitchTo(oldctx);
		}
	}PG_CATCH();
	{
		PQfinish((PGconn*)pg_conn);
		save_AGtmHost = NULL;
		save_AGtmPort = 0;
		PG_RE_THROW();
	}PG_END_TRY();

	ereport(LOG,
		(errmsg("Connect to AGTM(host=%s port=%d dbname=%s user=%s) successfully.",
		AGtmHost, AGtmPort, AGTM_DBNAME, AGTM_USER)));
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

	ereport(LOG,
		(errmsg("Get AGTM listen port: %d,", port_int)));

	return port_int;
}

void
agtm_SetPort(int listen_port)
{
	if(IsConnFromApp())
		ereport(ERROR, (errmsg("Can not set agtm listen port")));
	if(listen_port < 0 || listen_port > 65535)
		ereport(ERROR, (errmsg("Invalid port number %d", listen_port)));
	ereport(LOG,
		(errmsg("Get AGTM listen port: %d from coordinator,", listen_port)));

	SaveDefaultAGtmPort(AGtmPort);
	/*
	 * Close old connection if received a new AGTM backend listen port
	 */
	if (listen_port != AGtmPort)
	{
		agtm_Close();
		AGtmPort = listen_port;
	}
}

void
agtm_SetDefaultPort(void)
{
	if (IsDefaultAGtmPortSave)
		AGtmPort = save_DefaultAGtmPort;
}

void agtm_Close(void)
{
	if (agtm_conn)
	{
		if(agtm_conn->pg_res)
		{
			PQclear(agtm_conn->pg_res);
			agtm_conn->pg_res = NULL;
		}

		if (agtm_conn->pg_Conn)
		{
			PQfinish(agtm_conn->pg_Conn);
			agtm_conn->pg_Conn = NULL;
		}

		pfree(agtm_conn);
	}
	agtm_conn = NULL;
	save_AGtmHost = NULL;
	save_AGtmPort = 0;
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
	ConnStatusType status;
	if (agtm_conn == NULL)
	{
		agtm_Connect();
		return agtm_conn->pg_Conn;
	}

	status = PQstatus(agtm_conn->pg_Conn);
	if (status == CONNECTION_OK)
		return agtm_conn->pg_Conn;

	/*
	 * Bad AGTM connection
	 */
	if (TopXactBeginAGTM())
	{
		/* Invalid AGTM connection, close and never try again. */
		agtm_Close();
		SetTopXactBeginAGTM(false);

		ereport(ERROR,
			(errmsg("Bad AGTM connection, status: %d", status)));
	} else
	{
		agtm_Close();
		agtm_Connect();
	}

	return agtm_conn->pg_Conn;
}

PGresult*
agtm_GetResult(void)
{
	AGTM_Conn 		*conn = agtm_conn;

	if (!conn)
		return NULL;

	if(conn->pg_res)
	{
		PQclear(conn->pg_res);
		conn->pg_res = NULL;
	}

	conn->pg_res = PQexecFinish(conn->pg_Conn);
	return conn->pg_res;
}

StringInfo agtm_use_result_data(const PGresult *res, StringInfo buf)
{
	AssertArg(res && buf);

	if(PQftype(res, 0) != BYTEAOID
		|| (buf->data = PQgetvalue(res, 0, 0)) == NULL)
	{
		ereport(ERROR, (errmsg("Invalid AGTM message")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	buf->cursor = 0;
	buf->len = PQgetlength(res, 0, 0);
	buf->maxlen = buf->len;
	return buf;
}

StringInfo agtm_use_result_type(const PGresult *res, StringInfo buf, AGTM_ResultType type)
{
	agtm_use_result_data(res, buf);
	agtm_check_result(buf, type);
	return buf;
}

void agtm_check_result(StringInfo buf, AGTM_ResultType type)
{
	int res = pq_getmsgint(buf, 4);
	if(res != type)
	{
		ereport(ERROR, (errmsg("need AGTM message %s, but result %s"
			, gtm_util_result_name(type), gtm_util_result_name((AGTM_ResultType)res))));
	}
}

void agtm_use_result_end(StringInfo buf)
{
	if (buf->cursor != buf->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format from AGTM")));
}

/* GUC check hook for agtm_host */
bool
check_agtm_host(char **newval, void **extra, GucSource source)
{
	if (!(IS_PGXC_COORDINATOR && !IsConnFromCoord()))
		return true;

	if (save_AGtmHost == NULL)
		return true;

	if (!newval || *newval == NULL || (*newval)[0] == '\0')
		return false;

	if (pg_strcasecmp(save_AGtmHost, *newval) == 0)
		return true;

	agtm_Close();
	SetTopXactBeginAGTM(false);
	IsDefaultAGtmPortSave = false;

	if (TopXactBeginAGTM())
	{
		elog(ERROR,
			"Abort transaction because of modifying GUC variable: agtm_host");
	}

	return true;
}

/* GUC check hook for agtm_port */
bool
check_agtm_port(int *newval, void **extra, GucSource source)
{
	if (!(IS_PGXC_COORDINATOR && !IsConnFromCoord()))
		return true;

	if (save_AGtmPort == 0)
		return true;

	if (!newval)
		return false;

	if (*newval == save_AGtmPort)
		return true;

	agtm_Close();
	SetTopXactBeginAGTM(false);
	IsDefaultAGtmPortSave = false;

	if (TopXactBeginAGTM())
	{
		elog(ERROR,
			"Abort transaction because of modifying GUC variable: agtm_port");
	}

	return true;
}

