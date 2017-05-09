
#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqnode.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/memutils.h"

struct pq_comm_node
{
	StringInfoData out_buf;
	StringInfoData in_buf;
	pgsocket	sock;
	int			last_reported_send_errno;
	int			pq_id;
	bool		busy;
	bool		noblock;
	bool		doing_copy_out;
	bool		write_only;
	bool		in_start;
	bool		sended_ssl;
};

static void pq_node_comm_reset(void);
static void pq_node_comm_reset_sock(pq_comm_node *node);
static void pq_node_set_nonblocking(bool nonblocking);
static void pq_node_set_nonblocking_sock(pq_comm_node *node, bool nonblocking);
static int	pq_node_flush(void);
static int	pq_node_flush_if_writable(void);
static int	pq_node_flush_if_writable_sock(pq_comm_node *node);
static bool pq_node_is_send_pending(void);
static int	pq_node_putmessage(char msgtype, const char *s, size_t len);
static int	pq_node_putmessage_sock(pq_comm_node *node, char msgtype, const char *s, size_t len);
static void pq_node_putmessage_noblock(char msgtype, const char *s, size_t len);
static void pq_node_putmessage_noblock_sock(pq_comm_node *node, char msgtype, const char *s, size_t len);
static void pq_node_startcopyout(void);
static void pq_node_startcopyout_sock(pq_comm_node *node);
static void pq_node_endcopyout(bool errorAbort);
static void pq_node_endcopyout_sock(pq_comm_node *node, bool errorAbort);

static int pq_node_putbytes(pq_comm_node *node, const char *s, size_t len);
static int pq_node_internal_flush(pq_comm_node *node);
static int pq_node_internal_putbytes(pq_comm_node *node, const char *s, size_t len);
static void pq_node_proc_start_msg(pq_comm_node *node);
static bool pq_node_ProcessStartupPacket(pq_comm_node *node);

static PQcommMethods PqCommoNodeMethods = {
	pq_node_comm_reset,
	pq_node_flush,
	pq_node_flush_if_writable,
	pq_node_is_send_pending,
	pq_node_putmessage,
	pq_node_putmessage_noblock,
	pq_node_startcopyout,
	pq_node_endcopyout
};
static pq_comm_node *current_pq_node = NULL;
static List *list_pq_node = NIL;

static void pq_node_comm_reset(void)
{
	Assert(current_pq_node);
	pq_node_comm_reset_sock(current_pq_node);
}

static void pq_node_comm_reset_sock(pq_comm_node *node)
{
	AssertArg(node);
	node->busy = false;
	pq_node_endcopyout_sock(node, true);
}

static void pq_node_set_nonblocking(bool nonblocking)
{
	if (current_pq_node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
				 errmsg("there is no client connection")));
	pq_node_set_nonblocking_sock(current_pq_node, nonblocking);
}

static void pq_node_set_nonblocking_sock(pq_comm_node *node, bool nonblocking)
{
	AssertArg(node);
	/*
	 * Use COMMERROR on failure, because ERROR would try to send the error to
	 * the client, which might require changing the mode again, leading to
	 * infinite recursion.
	 */
	if (nonblocking)
	{
		if (!pg_set_noblock(node->sock))
			ereport(COMMERROR,
					(errmsg("could not set socket to nonblocking mode: %m")));
	}
	else
	{
		if (!pg_set_block(node->sock))
			ereport(COMMERROR,
					(errmsg("could not set socket to blocking mode: %m")));
	}

	node->noblock = nonblocking;
}

static int	pq_node_flush(void)
{
	Assert(current_pq_node);
	return pq_node_flush_sock(current_pq_node);
}

int pq_node_flush_sock(pq_comm_node *node)
{
	int res;
	AssertArg(node);

	/* No-op if reentrant call */
	if (node->busy)
		return 0;
	node->busy = true;
	pq_node_set_nonblocking_sock(node, false);
	res = pq_node_internal_flush(node);
	node->busy = false;
	return res;
}

static int pq_node_putbytes(pq_comm_node *node, const char *s, size_t len)
{
	int			res;
	AssertArg(node);

	/* Should only be called by old-style COPY OUT */
	Assert(node->doing_copy_out);
	/* No-op if reentrant call */
	if (node->busy)
		return 0;
	node->busy = true;
	res = pq_node_internal_putbytes(node, s, len);
	node->busy = false;
	return res;
}

static int pq_node_internal_flush(pq_comm_node *node)
{
	StringInfo buf;
	int result;
	AssertArg(node);

	buf = &(node->out_buf);
	while(buf->len > buf->cursor)
	{
		result = send(node->sock, buf->data + buf->cursor
			, buf->len - buf->cursor, 0);
		if(result <= 0)
		{
			if(errno == EINTR)
				continue;		/* Ok if we were interrupted */

			/*
			 * Ok if no data writable without blocking, and the socket is in
			 * non-blocking mode.
			 */
			if (errno == EAGAIN ||
				errno == EWOULDBLOCK)
			{
				return 0;
			}

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If a client disconnects while we're in the midst of output, we
			 * might write quite a bit of data before we get to a safe query
			 * abort point.  So, suppress duplicate log messages.
			 */
			if (errno != node->last_reported_send_errno)
			{
				node->last_reported_send_errno = errno;
				ereport(COMMERROR,
						(errcode_for_socket_access(),
						 errmsg("could not send data to client: %m")));
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon. We also set a
			 * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
			 * the connection.
			 */
			resetStringInfo(buf);
			/*ClientConnectionLost = 1;*/
			InterruptPending = 1;
			return EOF;
		}
		node->last_reported_send_errno = 0;
		buf->cursor += result;
	}

	buf->cursor = buf->len = 0;
	return 0;
}

static int pq_node_internal_putbytes(pq_comm_node *node, const char *s, size_t len)
{
	size_t		amount;
	StringInfo	buf;

	AssertArg(node);
	buf = &(node->out_buf);
	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if(buf->len >= buf->maxlen)
		{
			pq_node_set_nonblocking_sock(node, false);
			if (pq_node_internal_flush(node))
				return EOF;
		}
		amount = buf->maxlen - buf->len;
		if (amount > len)
			amount = len;
		appendBinaryStringInfo(buf, s, amount);
		s += amount;
		len -= amount;
	}
	return 0;
}

static int	pq_node_flush_if_writable(void)
{
	Assert(current_pq_node);
	return pq_node_flush_if_writable_sock(current_pq_node);
}

static int	pq_node_flush_if_writable_sock(pq_comm_node *node)
{
	int			res;
	AssertArg(node);

	/* Quick exit if nothing to do */
	if(node->out_buf.cursor == node->out_buf.len)
		return 0;

	/* No-op if reentrant call */
	if (node->busy)
		return 0;

	/* Temporarily put the socket into non-blocking mode */
	pq_node_set_nonblocking(true);

	node->busy = true;
	res = pq_node_internal_flush(node);
	node->busy = false;
	return res;
}

static bool pq_node_is_send_pending(void)
{
	return pq_node_send_pending(current_pq_node);
}

bool pq_node_send_pending(pq_comm_node *node)
{
	AssertArg(node);
	return node->out_buf.cursor < node->out_buf.len;
}

static int	pq_node_putmessage(char msgtype, const char *s, size_t len)
{
	Assert(current_pq_node);
	return pq_node_putmessage_sock(current_pq_node, msgtype, s, len);
}

static int	pq_node_putmessage_sock(pq_comm_node *node, char msgtype, const char *s, size_t len)
{
	AssertArg(node);
	if (node->doing_copy_out || node->busy)
		return 0;
	node->busy = true;
	if (msgtype)
		if (pq_node_internal_putbytes(node, &msgtype, 1))
			goto fail;
	/*if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)*/
	{
		uint32		n32;

		n32 = htonl((uint32) (len + 4));
		if (pq_node_internal_putbytes(node, (char *) &n32, 4))
			goto fail;
	}
	if (pq_node_internal_putbytes(node, s, len))
		goto fail;
	node->busy = false;
	return 0;

fail:
	node->busy = false;
	return EOF;
}

static void pq_node_putmessage_noblock(char msgtype, const char *s, size_t len)
{
	Assert(current_pq_node);
	pq_node_putmessage_noblock_sock(current_pq_node, msgtype, s, len);
}

static void pq_node_putmessage_noblock_sock(pq_comm_node *node, char msgtype, const char *s, size_t len)
{
	int res		PG_USED_FOR_ASSERTS_ONLY;
	StringInfo	buf;
	AssertArg(node);

	/*
	 * Ensure we have enough space in the output buffer for the message header
	 * as well as the message itself.
	 */
	buf = &(node->out_buf);
	enlargeStringInfo(buf, buf->len + 1 + 4 + len);
	res = pq_node_putmessage_sock(node, msgtype, s, len);
	Assert(res == 0);			/* should not fail when the message fits in
								 * buffer */
}

static void pq_node_startcopyout(void)
{
	Assert(current_pq_node);
	pq_node_startcopyout_sock(current_pq_node);
}

static void pq_node_startcopyout_sock(pq_comm_node *node)
{
	AssertArg(node);
	node->doing_copy_out = true;
}

static void pq_node_endcopyout(bool errorAbort)
{
	Assert(current_pq_node);
	pq_node_endcopyout_sock(current_pq_node, errorAbort);
}

int	pq_node_get_id_socket(pq_comm_node *node)
{
	AssertArg(node);
	return node->pq_id;
}

static void pq_node_endcopyout_sock(pq_comm_node *node, bool errorAbort)
{
	AssertArg(node);
	if (!node->doing_copy_out)
		return;
	if (errorAbort)
		pq_node_putbytes(node, "\n\n\\.\n", 5);
	/* in non-error case, copy.c will have emitted the terminator line */
	node->doing_copy_out = false;
}

List* get_all_pq_node(void)
{
	return list_pq_node;
}

pgsocket socket_pq_node(pq_comm_node *node)
{
	AssertArg(node);
	return node->sock;
}

bool pq_node_is_write_only(pq_comm_node *node)
{
	AssertArg(node);
	return node->write_only;
}

void pq_node_new(pgsocket sock)
{
	pq_comm_node volatile *node = NULL;
	MemoryContext old_ctx;
	PG_TRY();
	{
		old_ctx = MemoryContextSwitchTo(TopMemoryContext);
		node = palloc0(sizeof(*node));
		initStringInfo((StringInfo)&(node->in_buf));
		initStringInfo((StringInfo)&(node->out_buf));
		list_pq_node = lappend(list_pq_node, (pq_comm_node*)node);
		MemoryContextSwitchTo(old_ctx);
	}PG_CATCH();
	{
		if(node)
		{
			if(node->out_buf.data)
				pfree(node->out_buf.data);
			if(node->in_buf.data)
				pfree(node->in_buf.data);
			/*list_pq_node = list_delete_ptr(list_pq_node, node);*/
		}
		closesocket(sock);
		PG_RE_THROW();
	}PG_END_TRY();
	node->sock = sock;
	node->in_start = true;
}

int	pq_node_recvbuf(pq_comm_node *node)
{
	StringInfo buf;
	int r;
	AssertArg(node);

	if(node->write_only)
		return EOF;

	buf = &(node->in_buf);

	/* move unread data to left */
	if(buf->cursor > 0)
	{
		if(buf->len > buf->cursor)
		{
			buf->len -= buf->cursor;
			memmove(buf->data, buf->data + buf->cursor, buf->len);
			buf->cursor = 0;
		}else
		{
			buf->cursor = buf->len = 0;
		}
	}

	/* enlarge buffer if no free space */
	if(buf->len == buf->maxlen)
		enlargeStringInfo(buf, buf->maxlen+1024);

	/* recv data */
	Assert(buf->maxlen > buf->len);
re_recv_:
	r = recv(node->sock, buf->data + buf->len, buf->maxlen-buf->len, 0);
	if(r == 0)
	{
		/* close by remote */
		return EOF;
	}else if(r < 0)
	{
		if(errno == EINTR)
			goto re_recv_;
		/*
		 * Careful: an ereport() that tries to write to the client would
		 * cause recursion to here, leading to stack overflow and core
		 * dump!  This message must go *only* to the postmaster log.
		 */
		ereport(COMMERROR,
				(errcode_for_socket_access(),
				 errmsg("could not receive data from client: %m")));
		return EOF;
	}
	buf->len += r;
	return 0;
}

int pq_node_get_msg(StringInfo s, pq_comm_node *node)
{
	StringInfo buf;
	int32 msg_len;
	int msg_type;
	AssertArg(s && node);

	if(node->write_only)
		return 0;

	if(node->in_start)
	{
		pq_node_proc_start_msg(node);
		if(node->in_start)
			return 0;
	}

	buf = &(node->in_buf);
	if(buf->len - buf->cursor < 5)
		return 0;

	memcpy(&msg_len, buf->data+buf->cursor+1, 4);
	msg_len = htonl(msg_len);
	if(msg_len < 4)
	{
		pq_node_switch_to(node);
		node->write_only = true;
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			,errmsg("invalid message length %d", msg_len)));
	}
	++msg_len; /* add msg type length */
	if(buf->len - buf->cursor >= msg_len)
	{
		appendBinaryStringInfo(s, buf->data+buf->cursor+5, msg_len - 5);
		msg_type = buf->data[buf->cursor];
		buf->cursor += msg_len;
		if(buf->len > buf->cursor)
		{
			buf->len -= buf->cursor;
			memmove(buf->data, buf->data + buf->cursor, buf->len);
			buf->cursor = 0;
		}else
		{
			buf->cursor = buf->len = 0;
		}
		return msg_type;
	}
	return 0;
}

static void pq_node_proc_start_msg(pq_comm_node *node)
{
	PQcommMethods *save_methods = PqCommMethods;
	AssertArg(node);

	if(node->write_only)
		return;

	pq_node_switch_to(node);
	PG_TRY();
	{
		if(pq_node_ProcessStartupPacket(node))
			node->in_start = false;		/* auth success */
		else
			node->in_buf.cursor = 0;	/* reset readed buf */
	}PG_CATCH();
	{
		node->write_only = true;
		PG_RE_THROW();
	}PG_END_TRY();
	PqCommMethods = save_methods;
}

static bool pq_node_ProcessStartupPacket(pq_comm_node *node)
{
	int32 len;
	ProtocolVersion proto;
	StringInfoData s;
	StringInfo buf;
	AssertArg(node);
	buf = &(node->in_buf);

	if(buf->len < 4)
		return false;
	len = pq_getmsgint(buf, 4);
	len -= 4;
	if(len < (int32) sizeof(ProtocolVersion) ||
		len > MAX_STARTUP_PACKET_LENGTH)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid length of startup packet")));
	}

	if(len > buf->len - buf->cursor)
		return false;
	proto = pq_getmsgint(buf, sizeof(proto));
	if(proto == NEGOTIATE_SSL_CODE)
	{
		/* not support ssl mode */
		if(node->sended_ssl == false)
		{
			appendStringInfoChar(&(node->out_buf), 'N');
			node->sended_ssl = true;
		}
		if(buf->cursor < buf->len)
			return pq_node_ProcessStartupPacket(node);
		return false;
	}else if(proto != PG_PROTOCOL_LATEST)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("invalid protocol")));
	}

	while(buf->cursor < buf->len)
	{
		const char *nameptr;
		const char *valptr;
		nameptr = pq_getmsgstring(buf);
		if(nameptr[0] == '\0')
			break;
		valptr = pq_getmsgstring(buf);
		if(strcmp(nameptr, "database") == 0)
		{
			if(strcmp(MyProcPort->database_name, valptr) != 0)
				ereport(ERROR, (errmsg("database name \"%s\" not match current database name \"%s\""
					, valptr, MyProcPort->database_name)));
		}else if(strcmp(nameptr, "user") == 0)
		{
			if(strcmp(MyProcPort->user_name, valptr) != 0)
				ereport(ERROR, (errmsg("user name \"%s\" not match current user name \"%s\""
					, valptr, MyProcPort->user_name)));
		}else if(strcmp(nameptr, "options") == 0)
		{
			if(strcmp(MyProcPort->cmdline_options, valptr) != 0)
				ereport(ERROR, (errmsg("options \"%s\" not match current options \"%s\""
					, valptr, MyProcPort->cmdline_options)));
		}
	}

	if(buf->cursor != buf->len)
	{
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("invalid startup packet layout: expected terminator as last byte")));
	}

	/* send auth ok */
	pq_beginmessage(&s, 'R');
	pq_sendint(&s, (int32)AUTH_REQ_OK, sizeof(int32));
	pq_endmessage(&s);

	BeginReportingGUCOptions();

	/* Send this backend's cancellation info to the frontend. */
	pq_beginmessage(&s, 'K');
	pq_sendint(&s, (int32) MyProcPid, sizeof(int32));
	pq_sendint(&s, (int32) MyCancelKey, sizeof(int32));
	pq_endmessage(&s);

	/* send read for query */
	pq_beginmessage(&s, 'Z');
	pq_sendbyte(&s, TransactionBlockStatusCode());
	pq_endmessage(&s);
	return true;
}

void pq_node_switch_to(pq_comm_node *node)
{
	AssertArg(node);
	current_pq_node = node;
	PqCommMethods = &PqCommoNodeMethods;
}

void pq_node_close(pq_comm_node *node)
{
	if(node == NULL)
		return;
	if(current_pq_node == node)
		current_pq_node = NULL;
	list_pq_node = list_delete_ptr(list_pq_node, node);
	if(node->sock)
		closesocket(node->sock);
	if(node->in_buf.data)
		pfree(node->in_buf.data);
	if(node->out_buf.data)
		pfree(node->out_buf.data);
	pfree(node);
}
