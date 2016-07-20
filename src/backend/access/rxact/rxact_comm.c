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


#include "access/rxact_comm.h"
#include "access/rxact_mgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"

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
		if(rlog->buf.cursor == rlog->buf.len)
			rxact_log_read_internal(rlog);
		if(rlog->buf.cursor == rlog->buf.len)
			rxact_report_log_error(rlog->fd, ERROR);
		if(rlog->buf.data[len++] == '\0')
			break;
	}

	str = rlog->buf.data + rlog->buf.cursor;
	rlog->buf.cursor += len;
	return str;
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
	rxact_log_simple_write(rlog->fd, rlog->buf.data, rlog->buf.len);
	pfree(rlog->buf.data);
	pfree(rlog);
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