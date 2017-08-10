#include "agent.h"

#include <unistd.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "agt_msg.h"
#include "mgr/mgr_msg_type.h"
#include "utils/memutils.h"

#define AGT_OUT_BUFFER_SIZE 8192
#define AGT_IN_BUFFER_SIZE 8192

static pgsocket client_sock = PGINVALID_SOCKET;

static char *agt_out_msg_buf = NULL;
static int agt_out_msg_size;	/* sizeof agt_out_msg_buf */
static int agt_out_msg_end;		/* next store for agt_out_msg_buf */
static int agt_out_msg_cur;		/* next send for agt_out_msg_buf */

//static char agt_in_msg_buf[AGT_IN_BUFFER_SIZE];
//static int agt_in_msg_cur;		/* next read index for agt_in_msg_buf */
//static int agt_in_msg_len;		/* end of recv message index */
static bool agt_comm_busy = false;

static void agt_recv_msg(char *buf, int len);
static int agt_internal_flush(void);
static int agt_internal_putbytes(const char *s, size_t len);

void agt_msg_init(pgsocket fd_client)
{
	AssertArg(fd_client != PGINVALID_SOCKET);
	Assert(client_sock == PGINVALID_SOCKET);

	client_sock = fd_client;

	agt_out_msg_buf = MemoryContextAlloc(TopMemoryContext, AGT_OUT_BUFFER_SIZE);
	agt_out_msg_size = AGT_OUT_BUFFER_SIZE;
	agt_out_msg_cur = agt_out_msg_end = 0;

	/*agt_in_msg_cur = agt_in_msg_len = 0;*/
}

int agt_get_msg(StringInfo msg)
{
	char msg_type;
	int32 len;

	pg_set_block(client_sock);
	while(agt_out_msg_end > agt_out_msg_cur)
	{
		if(agt_flush() != 0)
			exit(EXIT_SUCCESS);
	}

	resetStringInfo(msg);
	HOLD_CANCEL_INTERRUPTS();
	/* read message type */
	agt_recv_msg(&msg_type, 1);
	/* read message length */
	agt_recv_msg((char*)&len, 4);
	len = htonl(len);
	if(len < 4 || len > AGT_MSG_MAX_LEN)
		ereport(FATAL, (errmsg("invalid message length")));
	len -= 4;

	if(len > 0)
	{
		enlargeStringInfo(msg, len);
		agt_recv_msg(msg->data, len);
		msg->len = len;
	}

	RESUME_CANCEL_INTERRUPTS();

	return msg_type;
}

static void agt_recv_msg(char *buf, int len)
{
	int rval;
	AssertArg(buf);
	while(len > 0)
	{
		rval = recv(client_sock, buf, len, 0);
		if(rval < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if(errno == EAGAIN)
			{
				pg_set_noblock(client_sock);
				continue;
			}else if(errno == EINTR || errno == EWOULDBLOCK)
			{
				continue;
			}
#ifdef EBADFD
			else if(errno != EBADFD)
			{
				closesocket(client_sock);
			}
#elif defined(EBADF)
			else if(errno != EBADF)
			{
				closesocket(client_sock);
			}
#endif
			client_sock = PGINVALID_SOCKET;
			ereport(FATAL, (errmsg("read message from client faile:%m")));
		}else if(rval == 0)
		{
			closesocket(client_sock);
			client_sock = PGINVALID_SOCKET;
			ereport(FATAL, (errmsg("client stream closed")));
		}else
		{
			buf += rval;
			len -= rval;
		}
	}
}

/* --------------------------------
 *		agt_put_msg	- send a normal message (suppressed in COPY OUT mode)
 *
 *		If msgtype is not '\0', it is a message type code to place before
 *		the message body.  If msgtype is '\0', then the message has no type
 *		code (this is only valid in pre-3.0 protocols).
 *
 *		len is the length of the message body data at *s.  In protocol 3.0
 *		and later, a message length word (equal to len+4 because it counts
 *		itself too) is inserted by this routine.
 *
 *		All normal messages are suppressed while old-style COPY OUT is in
 *		progress.  (In practice only a few notice messages might get emitted
 *		then; dropping them is annoying, but at least they will still appear
 *		in the postmaster log.)
 *
 *		We also suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int agt_put_msg(char msgtype, const char *s, size_t len)
{
	uint32 n32;

	AssertArg(msgtype);
	if(agt_comm_busy)
		return 0;

	agt_comm_busy = true;
	if(agt_internal_putbytes(&msgtype, 1) != 0)
		goto agt_put_msg_fail;
	n32 = htonl((uint32) (len + 4));
	if(agt_internal_putbytes((char*)&n32, 4) != 0
		|| agt_internal_putbytes(s, len) != 0)
		goto agt_put_msg_fail;
	agt_comm_busy = false;
	return 0;

agt_put_msg_fail:
	agt_comm_busy = false;
	return EOF;
}

/* --------------------------------
 *		agt_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int agt_flush(void)
{
	int			res;

	/* No-op if reentrant call */
	if (agt_comm_busy)
		return 0;
	agt_comm_busy = true;
	//pq_set_nonblocking(false);
	pg_set_block(client_sock);
	res = agt_internal_flush();
	agt_comm_busy = false;
	return res;
}
/* --------------------------------
 *		apt_internal_flush - flush pending output
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
static int agt_internal_flush(void)
{
	static int	last_reported_send_errno = 0;

	char *bufptr = agt_out_msg_buf + agt_out_msg_cur;
	char *bufend = agt_out_msg_buf + agt_out_msg_end;
	int r;

	while(bufptr < bufend)
	{
		r = send(client_sock, bufptr, bufend - bufptr, 0);

		if (r <= 0)
		{
			if (errno == EINTR)
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
			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;
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
			agt_out_msg_cur = agt_out_msg_end = 0;
			closesocket(client_sock);
			client_sock = PGINVALID_SOCKET;
			return EOF;
		}
		last_reported_send_errno = 0;
		bufptr += r;
		agt_out_msg_cur += r;
	}
	agt_out_msg_cur = agt_out_msg_end = 0;
	return 0;
}


static int agt_internal_putbytes(const char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if(agt_out_msg_end >= agt_out_msg_size)
		{
			pg_set_block(client_sock);
			if(agt_internal_flush() != 0)
				return EOF;
		}
		amount = agt_out_msg_size - agt_out_msg_end;
		if (amount > len)
			amount = len;
		memcpy(agt_out_msg_buf + agt_out_msg_end, s, amount);
		s = s + amount;
		agt_out_msg_end += amount;
		len -= amount;
	}
	return 0;
}

/* --------------------------------
 *		agt_beginmessage		- initialize for sending a message
 * --------------------------------
 */
void agt_beginmessage(StringInfo buf, char msgtype)
{
	AssertArg(msgtype != '\0');
	initStringInfo(buf);

	appendStringInfoCharMacro(buf, msgtype);
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

/* --------------------------------
 *		agt_sendbyte		- append a raw byte to a StringInfo buffer
 * --------------------------------
 */
void
agt_sendbyte(StringInfo buf, int byt)
{
	appendStringInfoCharMacro(buf, byt);
}

/* --------------------------------
 *		agt_sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void
agt_sendbytes(StringInfo buf, const char *data, int datalen)
{
	appendBinaryStringInfo(buf, data, datalen);
}

/* --------------------------------
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
agt_sendstring(StringInfo buf, const char *str)
{
	int			slen = strlen(str);
	appendBinaryStringInfo(buf, str, slen + 1);
}

/* --------------------------------
 *		agt_sendint		- append a binary integer to a StringInfo buffer
 * --------------------------------
 */
void
agt_sendint(StringInfo buf, int i, int b)
{
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			n8 = (unsigned char) i;
			appendBinaryStringInfo(buf, (char *) &n8, 1);
			break;
		case 2:
			n16 = htons((uint16) i);
			appendBinaryStringInfo(buf, (char *) &n16, 2);
			break;
		case 4:
			n32 = htonl((uint32) i);
			appendBinaryStringInfo(buf, (char *) &n32, 4);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			break;
	}
}

/* --------------------------------
 *		agt_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *
 * It is tempting to merge this with pq_sendint, but we'd have to make the
 * argument int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
void
agt_sendint64(StringInfo buf, int64 i)
{
	uint32		n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = htonl(n32);
	appendBinaryStringInfo(buf, (char *) &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	appendBinaryStringInfo(buf, (char *) &n32, 4);
}

/* --------------------------------
 *		agt_endmessage	- send the completed message to the frontend
 *
 * The data buffer is pfree()d, but if the StringInfo was allocated with
 * makeStringInfo then the caller must still pfree it.
 * --------------------------------
 */
void
agt_endmessage(StringInfo buf)
{
	uint32 n32;
	Assert(buf->len >= 5 && buf->data[0] != '\0');
	/*Assert(memcmp(&(buf->data[1]), "\0\0\0\0", 4) == 0);*/
	if(agt_comm_busy || client_sock == PGINVALID_SOCKET)
		return;

	/* write message length */
	n32 = htonl((uint32)(buf->len - 1));
	memcpy(buf->data + 1, &n32, 4);

	/* put message */
	agt_comm_busy = true;
	pg_set_block(client_sock);
	(void)agt_internal_putbytes(buf->data, buf->len);
	agt_comm_busy = false;
	/* no need to complain about any failure, since pqcomm.c already did */
	pfree(buf->data);
	/*memset(buf, 0, sizeof(*buf));*/
}


/* --------------------------------
 *		agt_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
agt_getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("no data left in message")));
	return (unsigned char) msg->data[msg->cursor++];
}

/* --------------------------------
 *		agt_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
agt_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			agt_copymsgbytes(msg, (char *) &n8, 1);
			result = n8;
			break;
		case 2:
			agt_copymsgbytes(msg, (char *) &n16, 2);
			result = ntohs(n16);
			break;
		case 4:
			agt_copymsgbytes(msg, (char *) &n32, 4);
			result = ntohl(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* --------------------------------
 *		agt_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with pq_getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64
agt_getmsgint64(StringInfo msg)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	agt_copymsgbytes(msg, (char *) &h32, 4);
	agt_copymsgbytes(msg, (char *) &l32, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

/* --------------------------------
 *		agt_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
agt_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* --------------------------------
 *		agt_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
agt_copymsgbytes(StringInfo msg, char *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		agt_getmsgstring - get a null-terminated text string (with conversion)
 *
 *		May return a pointer directly into the message buffer, or a pointer
 *		to a palloc'd conversion result.
 * --------------------------------
 */
const char *
agt_getmsgstring(StringInfo msg)
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

/* --------------------------------
 *		agt_getmsgend	- verify message fully consumed
 * --------------------------------
 */
void
agt_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format")));
}
