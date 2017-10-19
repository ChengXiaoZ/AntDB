#include "postgres.h"

#include <unistd.h>

#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/mgr_host.h"
#include "libpq/ip.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

struct ManagerAgent
{
	pgsocket sock;
	StringInfoData out_buf;
	StringInfoData in_buf;
	StringInfoData err_buf;
	struct addrinfo *addrs;
};

/* saved ManagerAgent for release when got error */
static List * list_ma = NIL;
static const char ma_idle_msg_str[5] = {AGT_MSG_IDLE, '\0', '\0', '\0', '\4'};

static bool ma_recv_data(ManagerAgent *ma);
static void left_stringbuf(StringInfo buf);
static ManagerAgent *make_manager_agent_handle(const char *host, unsigned short port);
static void ma_set_error(ManagerAgent *ma, const char *fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

ManagerAgent* ma_connect(const char *host, unsigned short port)
{
	struct addrinfo *addr;
	ManagerAgent *agent;
	int ret;

	AssertArg(host != NULL && port != 0);

	agent = make_manager_agent_handle(host, port);
	Assert(agent);
	/* has error ? */
	if(agent->err_buf.len)
		return agent;

	agent->sock = PGINVALID_SOCKET;
	for(addr = agent->addrs;addr;addr=addr->ai_next)
	{
		/* skip unix socket */
		if(IS_AF_UNIX(addr->ai_family))
			continue;
		agent->sock = socket(addr->ai_family, SOCK_STREAM, 0);
		if(agent->sock == PGINVALID_SOCKET)
		{
			if(addr->ai_next)
				continue;
			ma_set_error(agent, "could not create socket: %m");
			return agent;
		}

re_connect_:
		ret = connect(agent->sock, addr->ai_addr, addr->ai_addrlen);
		if(ret < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if(errno == EINTR)
				goto re_connect_;

			closesocket(agent->sock);
			agent->sock = PGINVALID_SOCKET;
			if(addr->ai_next)
				continue;

			ma_set_error(agent, "could not connect socket for agent \"%s\":%m", host);
			return agent;
		}

		/* wait idle message */
		while(agent->in_buf.len - agent->in_buf.cursor < sizeof(ma_idle_msg_str))
		{
			if(ma_recv_data(agent) == false)
			{
				if(agent->sock != PGINVALID_SOCKET)
				{
					closesocket(agent->sock);
					agent->sock = PGINVALID_SOCKET;
				}
				resetStringInfo(&(agent->in_buf));
				if(addr->ai_next)
				{
					resetStringInfo(&(agent->in_buf));
					continue;
				}
				return agent;
			}
		}
		if(memcmp(agent->in_buf.data + agent->in_buf.cursor
			, ma_idle_msg_str, sizeof(ma_idle_msg_str)) != 0)
		{
			closesocket(agent->sock);
			agent->sock = PGINVALID_SOCKET;
			if(addr->ai_next)
				continue;

			ma_set_error(agent, "remote server \"%s:%u\" is not manager agent", host, port);
			return agent;
		}
		agent->in_buf.cursor += sizeof(ma_idle_msg_str);
		break;
	}

	pg_freeaddrinfo_all(AF_UNSPEC, agent->addrs);
	agent->addrs = NULL;
	if(agent->sock == PGINVALID_SOCKET)
		ma_set_error(agent, "can not connect any address for host \"%s:%u\"", host, port);

	return agent;
}

ManagerAgent* ma_connect_hostoid(Oid hostoid)
{
	ManagerAgent *ma;
	Datum host_addr;
	Form_mgr_host mgr_host;
	HeapTuple tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostoid));
	bool isNull;
	if(!(HeapTupleIsValid(tup)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", hostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	mgr_host = (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	host_addr = SysCacheGetAttr(HOSTHOSTOID, tup, Anum_mgr_host_hostaddr, &isNull);
	if(isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	ma = ma_connect(TextDatumGetCString(host_addr), mgr_host->hostagentport);
	ReleaseSysCache(tup);
	return ma;
}

static ManagerAgent *make_manager_agent_handle(const char *host, unsigned short port)
{
	ManagerAgent volatile *ma;
	MemoryContext old_context;
	struct addrinfo hint;
	int ret;
	char str_port[6];
	AssertArg(host);

	/* initialize ManagerAgent */
	old_context = MemoryContextSwitchTo(TopMemoryContext);
	ma = palloc0(sizeof(*ma));
	ma->sock = PGINVALID_SOCKET;
	PG_TRY();
	{
		list_ma = lappend(list_ma, (void*)ma);
	}PG_CATCH();
	{
		pfree((void*)ma);
		PG_RE_THROW();
	}PG_END_TRY();
	initStringInfo((StringInfo)&(ma->in_buf));
	initStringInfo((StringInfo)&(ma->out_buf));
	initStringInfo((StringInfo)&(ma->err_buf));
	MemoryContextSwitchTo(old_context);
	ma->addrs = NULL;

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;
	hint.ai_flags = AI_PASSIVE;

	/* get address info */
	sprintf(str_port, "%u", port);
	ret = pg_getaddrinfo_all(host, str_port, &hint, (struct addrinfo**)&(ma->addrs));
	if(ret != 0 || ma->addrs == NULL)
	{
		ma_set_error((ManagerAgent*)ma, "could not resolve \"%s\": %s"
			, host, gai_strerror(ret));
	}
	return (ManagerAgent*)ma;
}

bool ma_isconnected(const ManagerAgent *ma)
{
	return ma && ma->sock != PGINVALID_SOCKET;
}

const char* ma_last_error_msg(const ManagerAgent *ma)
{
	if(ma == NULL || ma->err_buf.data == NULL)
		return _("out of memory");
	return ma->err_buf.data;
}

static void ma_set_error(ManagerAgent *ma, const char *fmt, ...)
{
	va_list args;
	bool success;
	AssertArg(ma && fmt);
	resetStringInfo(&(ma->err_buf));
	for(;;)
	{
		va_start(args, fmt);
		success = appendStringInfoVA(&(ma->err_buf), _(fmt), args);
		va_end(args);
		if(success)
			break;
		enlargeStringInfo(&(ma->err_buf), ma->err_buf.maxlen+1024);
	}
}

void ma_close(ManagerAgent *ma)
{
	if(ma == NULL)
		return;
	if(ma->sock != PGINVALID_SOCKET)
		closesocket(ma->sock);
	if(ma->out_buf.data)
		pfree(ma->out_buf.data);
	if(ma->in_buf.data)
		pfree(ma->in_buf.data);
	if(ma->err_buf.data)
		pfree(ma->err_buf.data);
	if(ma->addrs)
		pg_freeaddrinfo_all(AF_UNSPEC, ma->addrs);
	list_ma = list_delete(list_ma, ma);
	pfree(ma);
}

bool ma_flush(ManagerAgent *ma, bool report_error)
{
	StringInfo buf;
	int result;
	AssertArg(ma);

	if(ma->sock == PGINVALID_SOCKET)
	{
		if(report_error)
			ereport(ERROR, (errmsg("invalid socket")));
		ma_set_error(ma, "invalid socket");
		return false;
	}

	buf = &(ma->out_buf);
	while(ma->out_buf.cursor < ma->out_buf.len)
	{
		result = send(ma->sock, buf->data + buf->cursor, buf->len - buf->cursor, 0);
		if(result < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if(errno == EINTR)
				continue;
			if(report_error)
				ereport(ERROR, (errcode_for_socket_access(),
					errmsg("can not send message to agent:%m")));
			ma_set_error(ma, "can not send message to agent:%m");
			return false;
		}
		buf->cursor += result;
	}
	ma->out_buf.cursor = ma->out_buf.len = 0;
	return true;
}

/*
 * return message type and save message to buf, and save message type to buf.cursor
 * return 0 if has error
 */
char ma_get_message(ManagerAgent *ma, StringInfo buf)
{
	StringInfo in_buf;
	uint32 n32;
	char msg_type;
	AssertArg(ma);

	in_buf = &(ma->in_buf);
	/* get message type and length */
	while(in_buf->len - in_buf->cursor < 5)
	{
		if(ma_recv_data(ma) == false)
			return '\0';
	}
	msg_type = in_buf->data[in_buf->cursor];
	memcpy(&n32, in_buf->data + in_buf->cursor + 1, 4);
	n32 = htonl(n32);
	if(n32 < 4)
	{
		ma_set_error(ma, "invalid message length from agent");
		return '\0';
	}
	n32 -= 4;

	if(n32)
	{
		if(n32 > AGT_MSG_MAX_LEN)
		{
			ma_set_error(ma, "too large message length from agent");
			return '\0';
		}

		while(in_buf->len - in_buf->cursor < n32 + 5)
		{
			enlargeStringInfo(in_buf, n32+5-in_buf->len-in_buf->cursor);
			if(ma_recv_data(ma) == false)
				return '\0';
		}
		enlargeStringInfo(buf, n32);
		memcpy(buf->data, in_buf->data + in_buf->cursor + 5, n32);
		buf->len = n32;
		buf->cursor = msg_type;
	}
	in_buf->cursor += (n32 + 5);
	return msg_type;
}

char* ma_get_err_info(const StringInfo buf, int field)
{
	const char *msg;
	register int cursor;
	char diag;
	AssertArg(buf);

	for(cursor = 0;cursor < buf->len;)
	{
		diag = buf->data[cursor++];
		msg = buf->data + cursor;
		cursor += strlen(msg);
		++cursor;	/* '\0' */
		if(cursor >= buf->len)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid string in message")));
		}
		if(diag == field)
			return (char*)msg;
	}
	if(field == PG_DIAG_MESSAGE_PRIMARY)
		return _("missing error text");
	return NULL;
}

void ma_put_msg(ManagerAgent *ma, char msg_type, const char *msg, Size msg_len)
{
	StringInfo buf;
	uint32 len;
	AssertArg(ma && msg_type > 0);

	left_stringbuf(&(ma->out_buf));

	enlargeStringInfo(&(ma->out_buf), msg_len + 5);
	buf = &(ma->out_buf);

	buf->data[buf->len] = msg_type;
	++(buf->cursor);

	len = htonl((uint32)(msg_len + 4));
	memcpy(buf->data + buf->len, &len, 4);
	buf->len += 4;

	if(msg_len)
	{
		AssertArg(msg);
		memcpy(buf->data + buf->len, msg, msg_len);
		buf->len += msg_len;
	}
}

void ma_beginmessage(StringInfo buf, char msgtype)
{
	AssertArg(msgtype != '\0');
	initStringInfo(buf);

	appendStringInfoCharMacro(buf, msgtype);
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

void ma_sendstring(StringInfo buf, const char *str)
{
	int			slen = strlen(str);
	appendBinaryStringInfo(buf, str, slen + 1);
}

void ma_endmessage(StringInfo buf, ManagerAgent *ma)
{
	uint32 n32;
	AssertArg(buf && ma);
	Assert(buf->len >= 5 && buf->data[0] != '\0');
	Assert(memcmp(&(buf->data[1]), "\0\0\0\0", 4) == 0);

	/* write message length */
	n32 = htonl((uint32)(buf->len - 1));
	memcpy(buf->data + 1, &n32, 4);

	/* left buffer */
	left_stringbuf(&(ma->out_buf));

	/* put message */
	enlargeStringInfo(&(ma->out_buf), buf->len);
	memcpy(ma->out_buf.data + ma->out_buf.len, buf->data, buf->len);
	ma->out_buf.len += buf->len;

	pfree(buf->data);
	memset(buf, 0, sizeof(*buf));
}

void ma_clean(void)
{
	ListCell *lc;
	while((lc = list_head(list_ma)) != NULL)
		ma_close(lfirst(lc));
}

const char *ma_getmsgstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	msg->cursor += slen + 1;

	return str;
}

static bool ma_recv_data(ManagerAgent *ma)
{
	StringInfo buf;
	fd_set rfd;
	int rval;
	AssertArg(ma);

	if(ma->sock == PGINVALID_SOCKET)
	{
		ma_set_error(ma, "invalid socket");
		return false;
	}

	buf = &(ma->in_buf);
	left_stringbuf(buf);

re_select_:
	FD_ZERO(&rfd);
	FD_SET(ma->sock, &rfd);
	rval = select(ma->sock + 1, &rfd, NULL, NULL, NULL);
	if(rval < 0)
	{
		CHECK_FOR_INTERRUPTS();
		if(errno == EINTR)
			goto re_select_;
	}

	rval = recv(ma->sock, buf->data + buf->len
		, buf->maxlen - buf->len, 0);
	if(rval < 0)
	{
		if(errno == EINTR)
			goto re_select_;
		ma_set_error(ma, "recv message from agent error:%m");
		return false;
	}else if(rval == 0)
	{
		ma_set_error(ma, "recv socket closed from remote agent");
		return false;
	}
	buf->len += rval;
	return true;
}

static void left_stringbuf(StringInfo buf)
{
	AssertArg(buf);
	if(buf->cursor > 0)
	{
		memmove(buf->data, buf->data + buf->cursor, buf->len - buf->cursor);
		buf->len -= buf->cursor;
		buf->cursor = 0;
	}
}

