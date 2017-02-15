/*-------------------------------------------------------------------------
 *
 * poolcomm.c
 *
 *	  Communication functions between the rxact manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/htup_details.h"
#include "access/rxact_comm.h"
#include "access/rxact_mgr.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include <unistd.h>
#include <sys/socket.h>

struct RXactLogData
{
	StringInfoData buf;
	File fd;
};

#ifdef HAVE_UNIX_SOCKETS
#include <sys/un.h>

static const char rxact_sock_path[] = {".s.PGRXACT"};

static void RxactStreamDoUnlink(int code, Datum arg);
#endif

/*
 * Open server socket on specified port to accept connection from sessions
 */
pgsocket
rxact_listen(void)
{
#ifdef HAVE_UNIX_SOCKETS
	int			fd,
				len;
	int maxconn;
	struct sockaddr_un unix_addr;

	CreateSocketLockFile(rxact_sock_path, true, "");
	unlink(rxact_sock_path);

	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return PGINVALID_SOCKET;

	/* fill in socket address structure */
	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, rxact_sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	/*
	 * bind the name to the descriptor
	 * and tell kernel we're a server
	 */
	maxconn = MaxBackends * 2;
	if(maxconn > PG_SOMAXCONN)
		maxconn = PG_SOMAXCONN;
	if (bind(fd, (struct sockaddr *) & unix_addr, len) < 0
		|| listen(fd, maxconn) < 0)
	{
		closesocket(fd);
		return PGINVALID_SOCKET;
	}

	/* Arrange to unlink the socket file at exit */
	on_proc_exit(RxactStreamDoUnlink, 0);

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("remote xact manager only supports UNIX socket")));
	return -1;
#endif
}

/* StreamDoUnlink()
 * Shutdown routine for pooler connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void
RxactStreamDoUnlink(int code, Datum arg)
{
	Assert(rxact_sock_path[0]);
	unlink(rxact_sock_path);
}
#endif   /* HAVE_UNIX_SOCKETS */

/*
 * Connect to pooler listening on specified port
 */
pgsocket
rxact_connect(void)
{
	int			fd,
				len;
	struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS
	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, rxact_sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	if (connect(fd, (struct sockaddr *) & unix_addr, len) < 0)
		return -1;

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("rxact manager only supports UNIX socket")));
	return -1;
#endif
}

const char* rxact_get_sock_path(void)
{
	return rxact_sock_path;
}

void rxact_begin_msg(StringInfo msg, char type)
{
	AssertArg(msg && type);
	initStringInfo(msg);
	rxact_reset_msg(msg, type);
}

void rxact_reset_msg(StringInfo msg, char type)
{
	AssertArg(msg && msg->data && type);
	resetStringInfo(msg);
	enlargeStringInfo(msg, 5);
	msg->len = 5;
	msg->data[4] = type;
}

void rxact_put_short(StringInfo msg, short n)
{
	AssertArg(msg);
	appendBinaryStringInfo(msg, (char*)&n, 2);
}

void rxact_put_int(StringInfo msg, int n)
{
	AssertArg(msg);
	appendBinaryStringInfo(msg, (char*)&n, 4);
}

void rxact_put_bytes(StringInfo msg, const void *s, int len)
{
	AssertArg(msg && s && len>0);
	appendBinaryStringInfo(msg, s, len);
}

void rxact_put_string(StringInfo msg, const char *s)
{
	int len;
	AssertArg(msg && s);

	len = strlen(s);
	appendBinaryStringInfo(msg, s, len+1);
}

void rxact_put_finsh(StringInfo msg)
{
	AssertArg(msg && msg->data && msg->len >= 5);

	memcpy(msg->data, &(msg->len), 4);
}

short rxact_get_short(StringInfo msg)
{
	short s;
	rxact_copy_bytes(msg, &s, 2);
	return s;
}

int rxact_get_int(StringInfo msg)
{
	int i;
	rxact_copy_bytes(msg, &i, 4);
	return i;
}

char* rxact_get_string(StringInfo msg)
{
	char *str;
	int len;
	AssertArg(msg && msg->data);

	str = (msg->data + msg->cursor);
	len = strlen(str);
	if(msg->cursor + len >= msg->len)
		ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("invalid string in message")));
	msg->cursor += (len+1);
	return str;
}

void rxact_copy_bytes(StringInfo msg, void *s, int len)
{
	AssertArg(msg && msg->data && s);

	if(len < 0 || msg->cursor + len > msg->len)
		ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("insufficient data left in message")));

	memcpy(s, msg->data + msg->cursor, len);
	msg->cursor += len;
}

void* rxact_get_bytes(StringInfo msg, int len)
{
	void *p;
	AssertArg(msg && msg->data);

	if(len < 0 || msg->cursor + len > msg->len)
		ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("insufficient data left in message")));

	p = msg->data + msg->cursor;
	msg->cursor += len;
	return p;
}

void rxact_get_msg_end(StringInfo msg)
{
	AssertArg(msg);
	if(msg->cursor != msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format")));
}

/* ---------------------------rlog--------------------------------- */
static bool rxact_log_read_internal(RXactLog rlog);

RXactLog rxact_begin_read_log(File fd)
{
	RXactLog rlog;
	AssertArg(fd != -1);

	rlog = palloc(sizeof(*rlog));
	initStringInfo(&(rlog->buf));
	rlog->fd = fd;
	return rlog;
}

void rxact_end_read_log(RXactLog rlog)
{
	AssertArg(rlog);
	if(!rxact_log_is_eof(rlog))
		rxact_report_log_error(rlog->fd, ERROR);
	pfree(rlog->buf.data);
	pfree(rlog);
}

bool rxact_log_is_eof(RXactLog rlog)
{
	AssertArg(rlog);
	Assert(rlog->buf.cursor <= rlog->buf.len);
	if(rlog->buf.cursor == rlog->buf.len)
		rxact_log_read_internal(rlog);
	return rlog->buf.cursor == rlog->buf.len;
}

int rxact_log_get_int(RXactLog rlog)
{
	int n;
	rxact_log_read_bytes(rlog, &n, sizeof(n));
	return n;
}

short rxact_log_get_short(RXactLog rlog)
{
	short n;
	rxact_log_read_bytes(rlog, &n, sizeof(n));
	return n;
}

/*
 * return a string, and maybe invalid at call next read
 */
const char* rxact_log_get_string(RXactLog rlog)
{
	char *str;
	int len;
	AssertArg(rlog);

	for(len=0;;)
	{
		Assert(rlog->buf.cursor <= rlog->buf.len);
		if(rlog->buf.cursor+len >= rlog->buf.len)
			rxact_log_read_internal(rlog);
		if(rlog->buf.cursor+len == rlog->buf.len)
			rxact_report_log_error(rlog->fd, ERROR);
		if(rlog->buf.data[rlog->buf.cursor+len++] == '\0')
			break;
	}

	str = rlog->buf.data + rlog->buf.cursor;
	rlog->buf.cursor += len;
	return str;
}

void* rxact_log_get_bytes(RXactLog rlog , int n)
{
	void *p;
	AssertArg(rlog && n >= 0);

	while(rlog->buf.len - rlog->buf.cursor < n)
	{
		if(rxact_log_read_internal(rlog) == false)
			rxact_report_log_error(rlog->fd, ERROR);
	}
	p = rlog->buf.data + rlog->buf.cursor;
	rlog->buf.cursor += n;
	return p;
}

static bool rxact_log_read_internal(RXactLog rlog)
{
	int read_res;
	AssertArg(rlog);
	if(rlog->buf.len == rlog->buf.maxlen)
	{
		rlog->buf.maxlen += 1024;
		rlog->buf.data = repalloc(rlog->buf.data, rlog->buf.maxlen);
	}

	read_res = FileRead(rlog->fd, rlog->buf.data + rlog->buf.len
		, rlog->buf.maxlen - rlog->buf.len);
	if(read_res < 0)
	{
		ereport(FATAL,
			(errcode_for_file_access(),
			errmsg("Can not read file \"%s\":%m", FilePathName(rlog->fd))));
	}else if(read_res == 0)
	{
		return false;
	}
	rlog->buf.len += read_res;
	return true;
}

void rxact_log_reset(RXactLog rlog)
{
	AssertArg(rlog);
	if(rlog->buf.cursor)
	{
		memmove(rlog->buf.data, rlog->buf.data + rlog->buf.cursor
			, rlog->buf.len - rlog->buf.cursor);
		rlog->buf.len -= rlog->buf.cursor;
		rlog->buf.cursor = 0;
	}
}

void rxact_log_read_bytes(RXactLog rlog, void *p, int n)
{
	AssertArg(rlog && p && n >= 0);

	while(rlog->buf.len - rlog->buf.cursor < n)
	{
		if(rxact_log_read_internal(rlog) == false)
			rxact_report_log_error(rlog->fd, ERROR);
	}
	memcpy(p, rlog->buf.data + rlog->buf.cursor, n);
	rlog->buf.cursor += n;
}

void rxact_log_seek_bytes(RXactLog rlog, int n)
{
	AssertArg(rlog);
	if(n < 0)
	{
		ExceptionalCondition("RXACT seek bytes", "BadArgument"
			, __FILE__, __LINE__);
	}else if(n == 0)
	{
		return;
	}

	if(rlog->buf.cursor != rlog->buf.len)
	{
		Assert(rlog->buf.cursor < rlog->buf.len);
		if(rlog->buf.len - rlog->buf.cursor >= n)
		{
			rlog->buf.cursor += n;
			return;
		}
		n -= (rlog->buf.len - rlog->buf.cursor);
		rlog->buf.cursor = rlog->buf.len;
	}
	Assert(rlog->buf.cursor == rlog->buf.len);
	if(FileSeek(rlog->fd, n, SEEK_CUR) < 0)
		rxact_report_log_error(rlog->fd, ERROR);
}

RXactLog rxact_begin_write_log(File fd)
{
	return rxact_begin_read_log(fd);
}

void rxact_end_write_log(RXactLog rlog)
{
	if(rlog->buf.len > 0)
		rxact_log_simple_write(rlog->fd, rlog->buf.data, rlog->buf.len);
	pfree(rlog->buf.data);
	pfree(rlog);
}

extern void rxact_write_log(RXactLog rlog)
{
	rxact_log_simple_write(rlog->fd, rlog->buf.data, rlog->buf.len);
	resetStringInfo(&(rlog->buf));
}

void rxact_log_write_byte(RXactLog rlog, char c)
{
	rxact_log_write_bytes(rlog, &c, 1);
}

void rxact_log_write_int(RXactLog rlog, int n)
{
	rxact_log_write_bytes(rlog, &n, sizeof(n));
}

void rxact_log_write_bytes(RXactLog rlog, const void *p, int n)
{
	AssertArg(rlog && p);
	if(rlog->buf.maxlen - rlog->buf.len < n)
	{
		int new_size = rlog->buf.maxlen;
		while(new_size - rlog->buf.len < n)
			new_size += 1024;
		rlog->buf.data = repalloc(rlog->buf.data, new_size);
		rlog->buf.maxlen = new_size;
	}
	Assert(rlog->buf.maxlen - rlog->buf.len >= n);
	memcpy(rlog->buf.data + rlog->buf.len, p, n);
	rlog->buf.len += n;
}

void rxact_log_write_string(RXactLog rlog, const char *str)
{
	int len;
	AssertArg(rlog && str);
	len = strlen(str);
	rxact_log_write_bytes(rlog, str, len+1);
}

void rxact_log_simple_write(File fd, const void *p, int n)
{
	volatile off_t cur;
	int res;
	AssertArg(fd != -1 && p && n > 0);
	cur = FileSeek(fd, 0, SEEK_END);
	PG_TRY();
	{
		res = FileWrite(fd, (char*)p, n);
		if(res != n)
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("could not write rlog to file \"%s\":%m", FilePathName(fd))));
		}
	}PG_CATCH();
	{
		FileTruncate(fd, cur);
		PG_RE_THROW();
	}PG_END_TRY();
}

void rxact_report_log_error(File fd, int elevel)
{
	const char *name = FilePathName(fd);
	ereport(elevel,
		(errmsg("invalid format rxact log file \"%s\"", name)));
}

Datum rxact_get_running(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	RxactTransactionInfo *info;
	HeapTuple tuple;
	ArrayBuildState *astate,*astate2;
	Datum values[6];
	int i;
	static bool nulls[6] = {false,false,false,false,false,false};

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		List *list;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(6, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dbid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "type",
						   CHAROID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "backend",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "nodes",
						   OIDARRAYOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "status",
						   BOOLARRAYOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		list = RxactGetRunningList();
		funcctx->user_fctx = list_head(list);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if(funcctx->user_fctx)
	{
		info = lfirst((ListCell*)funcctx->user_fctx);
		funcctx->user_fctx = lnext((ListCell*)funcctx->user_fctx);

		values[0] = PointerGetDatum(cstring_to_text(info->gid));
		values[1] = ObjectIdGetDatum(info->db_oid);
		switch(info->type)
		{
		case RX_PREPARE:
			values[2] = CharGetDatum('p');
			break;
		case RX_COMMIT:
			values[2] = CharGetDatum('c');
			break;
		case RX_ROLLBACK:
			values[2] = CharGetDatum('r');
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("unknown transaction state '%d'", info->type)));
		}
		values[3] = BoolGetDatum(!info->failed);
		for(i=0,astate=astate2=NULL;i<info->count_nodes;++i)
		{
			astate = accumArrayResult(astate, ObjectIdGetDatum(info->remote_nodes[i]),
				false, OIDOID, CurrentMemoryContext);
			astate2 = accumArrayResult(astate2, BoolGetDatum(info->remote_success[i]),
				false, BOOLOID, CurrentMemoryContext);
		}
		values[4] = PointerGetDatum(makeArrayResult(astate, CurrentMemoryContext));
		values[5] = PointerGetDatum(makeArrayResult(astate2, CurrentMemoryContext));

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

Datum rxact_wait_gid(PG_FUNCTION_ARGS)
{
	text *arg = PG_GETARG_TEXT_P(0);
	char *gid = text_to_cstring(arg);

	RxactWaitGID(gid);
	pfree(gid);

	PG_RETURN_VOID();
}
