/*-------------------------------------------------------------------------
 *
 * poolcomm.c
 *
 *	  Communication functions between the pool manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stddef.h>
#include <netinet/in.h>

#include "pgxc/poolcomm.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "miscadmin.h"

static int	pool_recvbuf(PoolPort *port);
static int	pool_discardbytes(PoolPort *port, size_t len);
static void pool_report_error(pgsocket sock, uint32 msg_len);
static int pool_block_recv(pgsocket sock, void *ptr, uint32 size);

#ifdef HAVE_UNIX_SOCKETS

static const char sock_path[] = {".s.PGPOOL"};

static void StreamDoUnlink(int code, Datum arg);

static int	Lock_AF_UNIX(void);
#endif

/*
 * Open server socket on specified port to accept connection from sessions
 */
int
pool_listen()
{
#ifdef HAVE_UNIX_SOCKETS
	int			fd,
				len;
	struct sockaddr_un unix_addr;

	if (Lock_AF_UNIX() < 0)
		return -1;

	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	/* fill in socket address structure */
	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	/* bind the name to the descriptor */
	if (bind(fd, (struct sockaddr *) & unix_addr, len) < 0)
		return -1;

	/* tell kernel we're a server */
	if (listen(fd, 5) < 0)
		return -1;

	/* Arrange to unlink the socket file at exit */
	on_proc_exit(StreamDoUnlink, 0);

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pool manager only supports UNIX socket")));
	return -1;
#endif
}

/* StreamDoUnlink()
 * Shutdown routine for pooler connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void
StreamDoUnlink(int code, Datum arg)
{
	Assert(sock_path[0]);
	unlink(sock_path);
}
#endif   /* HAVE_UNIX_SOCKETS */

#ifdef HAVE_UNIX_SOCKETS
static int
Lock_AF_UNIX(void)
{
	CreateSocketLockFile(sock_path, true, "");

	unlink(sock_path);

	return 0;
}
#endif

/*
 * Connect to pooler listening on specified port
 */
int
pool_connect(void)
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
	strcpy(unix_addr.sun_path, sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	if (connect(fd, (struct sockaddr *) & unix_addr, len) < 0)
		return -1;

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pool manager only supports UNIX socket")));
	return -1;
#endif
}


/*
 * Get one byte from the buffer, read data from the connection if buffer is empty
 */
int
pool_getbyte(PoolPort *port)
{
	while (port->RecvPointer >= port->RecvLength)
	{
		if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Get one byte from the buffer if it is not empty
 */
int
pool_pollbyte(PoolPort *port)
{
	if (port->RecvPointer >= port->RecvLength)
	{
		return EOF;				/* Empty buffer */
	}
	return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Read pooler protocol message from the buffer.
 */
int
pool_getmessage(PoolPort *port, StringInfo s, int maxlen)
{
	int32		len;

	resetStringInfo(s);

	/* Read message length word */
	if (pool_getbytes(port, (char *) &len, 4) == EOF)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected EOF within message length word")));
		return EOF;
	}

	len = ntohl(len);

	if (len < 4 ||
		(maxlen > 0 && len > maxlen))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));
		return EOF;
	}

	len -= 4;					/* discount length itself */

	if (len > 0)
	{
		/*
		 * Allocate space for message.	If we run out of room (ridiculously
		 * large message), we will elog(ERROR)
		 */
		PG_TRY();
		{
			enlargeStringInfo(s, len);
		}
		PG_CATCH();
		{
			if (pool_discardbytes(port, len) == EOF)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("incomplete message from client")));
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* And grab the message */
		if (pool_getbytes(port, s->data, len) == EOF)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete message from client")));
			return EOF;
		}
		s->len = len;
		/* Place a trailing null per StringInfo convention */
		s->data[len] = '\0';
	}

	return 0;
}


/* --------------------------------
 * pool_getbytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_getbytes(PoolPort *port, char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (port->RecvPointer >= port->RecvLength)
		{
			if (pool_recvbuf(port))		/* If nothing in buffer, then recv
										 * some */
				return EOF;		/* Failed to recv data */
		}
		amount = port->RecvLength - port->RecvPointer;
		if (amount > len)
			amount = len;
		memcpy(s, port->RecvBuffer + port->RecvPointer, amount);
		port->RecvPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 * pool_discardbytes - discard a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_discardbytes(PoolPort *port, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (port->RecvPointer >= port->RecvLength)
		{
			if (pool_recvbuf(port))		/* If nothing in buffer, then recv
										 * some */
				return EOF;		/* Failed to recv data */
		}
		amount = port->RecvLength - port->RecvPointer;
		if (amount > len)
			amount = len;
		port->RecvPointer += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 * pool_recvbuf - load some bytes into the input buffer
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_recvbuf(PoolPort *port)
{
	if (port->RecvPointer > 0)
	{
		if (port->RecvLength > port->RecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(port->RecvBuffer, port->RecvBuffer + port->RecvPointer,
					port->RecvLength - port->RecvPointer);
			port->RecvLength -= port->RecvPointer;
			port->RecvPointer = 0;
		}
		else
			port->RecvLength = port->RecvPointer = 0;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		int			r;

		r = recv(Socket(*port), port->RecvBuffer + port->RecvLength,
				 POOL_BUFFER_SIZE - port->RecvLength, 0);

		if (r < 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if interrupted */

			/*
			 * Report broken connection
			 */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
			return EOF;
		}
		if (r == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return EOF;
		}
		/* r contains number of bytes read, so just incr length */
		port->RecvLength += r;
		return 0;
	}
}


/*
 * Put a known number of bytes into the connection buffer
 */
int
pool_putbytes(PoolPort *port, const char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if (port->SendPointer >= POOL_BUFFER_SIZE)
			if (pool_flush(port))
				return EOF;
		amount = POOL_BUFFER_SIZE - port->SendPointer;
		if (amount > len)
			amount = len;
		memcpy(port->SendBuffer + port->SendPointer, s, amount);
		port->SendPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 *		pool_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_flush(PoolPort *port)
{
	static int	last_reported_send_errno = 0;

	char	   *bufptr = port->SendBuffer;
	char	   *bufend = port->SendBuffer + port->SendPointer;

	while (bufptr < bufend)
	{
		int			r;

		r = send(Socket(*port), bufptr, bufend - bufptr, 0);

		if (r <= 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if we were interrupted */

			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;

				/*
				 * Handle a seg fault that may later occur in proc array
				 * when this fails when we are already shutting down
				 * If shutting down already, do not call.
				 */
				if (!proc_exit_inprogress)
					return 0;
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon.
			 */
			port->SendPointer = 0;
			return EOF;
		}

		last_reported_send_errno = 0;	/* reset after any successful send */
		bufptr += r;
	}

	port->SendPointer = 0;
	return 0;
}


/*
 * Put the pooler protocol message into the connection buffer
 */
int
pool_putmessage(PoolPort *port, char msgtype, const char *s, size_t len)
{
	uint		n32;

	if (pool_putbytes(port, &msgtype, 1))
		return EOF;

	n32 = htonl((uint32) (len + 4));
	if (pool_putbytes(port, (char *) &n32, 4))
		return EOF;

	if (pool_putbytes(port, s, len))
		return EOF;

	return 0;
}

/* message code('f'), size(8), node_count */
#define SEND_MSG_BUFFER_SIZE 9
/* message code('s'), result */
#define SEND_RES_BUFFER_SIZE 5
#define SEND_PID_BUFFER_SIZE (5 + (MaxConnections - 1) * 4)

#ifndef CMSG_LEN
#       define CMSG_LEN(l)      (sizeof(struct cmsghdr) + (l))
#endif /* CMSG_LEN */

/*
 * Build up a message carrying file descriptors or process numbers and send them over specified
 * connection
 */
int
pool_sendfds(PoolPort *port, int *fds, int count)
{
	struct iovec iov[1];
	struct msghdr msg;
	char		buf[SEND_MSG_BUFFER_SIZE];
	uint		n32;
	int			controllen = CMSG_LEN(count * sizeof(int));
	struct cmsghdr *cmptr = NULL;

	buf[0] = 'f';
	n32 = 8;//htonl((uint32) 8);
	memcpy(buf + 1, &n32, 4);
	n32 = count;//htonl((uint32) count);
	memcpy(buf + 5, &n32, 4);

	iov[0].iov_base = buf;
	iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	if (count == 0)
	{
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
	}
	else
	{
		if ((cmptr = malloc(controllen)) == NULL)
			return EOF;
		cmptr->cmsg_level = SOL_SOCKET;
		cmptr->cmsg_type = SCM_RIGHTS;
		cmptr->cmsg_len = controllen;
		msg.msg_control = (caddr_t) cmptr;
		msg.msg_controllen = controllen;
		/* the fd to pass */
		memcpy(CMSG_DATA(cmptr), fds, count * sizeof(int));
	}

	if (sendmsg(Socket(*port), &msg, 0) != SEND_MSG_BUFFER_SIZE)
	{
		if (cmptr)
			free(cmptr);
		return EOF;
	}

	if (cmptr)
		free(cmptr);

	return 0;
}


/*
 * Read a message from the specified connection carrying file descriptors
 */
int
pool_recvfds(PoolPort *port, pgsocket *fds, int count)
{
	struct iovec iov[1];
	struct msghdr msg;
	char buf[SEND_MSG_BUFFER_SIZE];
	static Size controllen = 0;
	static struct cmsghdr *cmptr = NULL;
	Size need_size;
	uint32 msg_size;
	int rval;

	AssertArg(port && fds && count>0);
	need_size = CMSG_LEN(count * sizeof(int));
	if(controllen < need_size)
	{
		cmptr = realloc(cmptr, need_size);
		if(cmptr == NULL)
			ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("out of memory for get connect")));
		controllen = need_size;
	}

	/* first read message head */
	HOLD_CANCEL_INTERRUPTS();
retry_recvmsg_:
	iov[0].iov_base = buf;
	iov[0].iov_len = 5;
	memset(&msg, 0, sizeof(msg));
	msg.msg_iov = iov;
	msg.msg_iovlen = lengthof(iov);
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	msg.msg_control = (caddr_t)cmptr;
	msg.msg_controllen = need_size;

	rval = recvmsg(Socket(*port), &msg, 0);
	if(rval < 0)
	{
		CHECK_FOR_INTERRUPTS();
		if(errno == EINTR)
		{
			goto retry_recvmsg_;
		}
		ereport(FATAL, (errcode_for_socket_access(),
			errmsg("could not receive pool data from client: %m")));
	}else if (rval == 0)
	{
		RESUME_CANCEL_INTERRUPTS();
		return EOF;
	}else if (rval != 5)
	{
		ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("incomplete message pooler process")));
	}

	memmove(&msg_size, &buf[1], 4);

	if(buf[0] == 'E')
	{
		/* poolmgr send us an error message */
		msg_size = htonl(msg_size);
		pool_report_error(Socket(*port), msg_size-4);
		/* if run to here, socket is closed */
		RESUME_CANCEL_INTERRUPTS();
		return EOF;
	}else if(buf[0] == 'f')
	{
		if(msg_size != 8)
		{
			ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
				errmsg("invalid message size from pooler process")));
		}
	}else
	{
		ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("invalid message type from pooler process")));
	}

	/* read other message */
	if(pool_block_recv(Socket(*port), &buf[5], sizeof(buf)-5) != sizeof(buf)-5)
	{
		RESUME_CANCEL_INTERRUPTS();
		return EOF;
	}
	/* recv message end, resume cancle interrupts */
	RESUME_CANCEL_INTERRUPTS();

	memcpy(&rval, buf + 5, 4);
	if(rval == 0)
	{
		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("failed to acquire connections")));
		return EOF;
	}

	if(rval != count)
	{
		ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("unexpected connection count from pooler process")));
	}

	memcpy(fds, CMSG_DATA(cmptr), count * sizeof(pgsocket));
	return 0;
}

/*
 * Send result to specified connection
 */
int
pool_sendres(PoolPort *port, int res)
{
	char		buf[SEND_RES_BUFFER_SIZE];
	uint		n32;

	/* Header */
	buf[0] = 's';
	/* Result */
	n32 = htonl(res);
	memcpy(buf + 1, &n32, 4);

	if (send(Socket(*port), &buf, SEND_RES_BUFFER_SIZE, 0) != SEND_RES_BUFFER_SIZE)
		return EOF;

	return 0;
}

/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error.
 */
int
pool_recvres(PoolPort *port)
{
	int			r;
	uint		n32;
	char		buf[SEND_RES_BUFFER_SIZE];

	r = recv(Socket(*port), &buf, SEND_RES_BUFFER_SIZE, 0);
	if (r < 0)
	{
		/*
		 * Report broken connection
		 */
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not receive data from client: %m")));
		goto failure;
	}
	else if (r == 0)
	{
		goto failure;
	}
	else if (r != SEND_RES_BUFFER_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("incomplete message from client")));
		goto failure;
	}

	memmove(&n32, &buf[1], 4);
	n32 = htonl(n32);

	/* Verify response */
#ifdef ADB
	if (buf[0] == 'E')
	{
		pool_report_error(Socket(*port), n32-4);
		/* run to here socket is closed */
		goto failure;
	}else
#endif /* ADB */
	if (buf[0] != 's')
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code")));
		goto failure;
	}

	return n32;

failure:
	return EOF;
}

/*
 * Read a message from the specified connection carrying pid numbers
 * of transactions interacting with pooler
 */
int
pool_recvpids(PoolPort *port, int **pids)
{
	int			r;
	uint		n32;
	char		buf[5];

	/*
	 * Buffer size is upper bounded by the maximum number of connections,
	 * as in the pooler each connection has one Pooler Agent.
	 */

	if(pool_block_recv(Socket(*port), buf, sizeof(buf)) != sizeof(buf))
		goto failure;
	memmove(&n32, &buf[1], 4);
	n32 = htonl(n32);

	if(buf[0] == 'E')
	{
		pool_report_error(Socket(*port), n32-4);
		goto failure;
	}else if(buf[0] != 'p')
	{
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code")));
	}

	if (n32 == 0)
	{
		elog(WARNING, "No transaction to abort");
		return n32;
	}

	START_CRIT_SECTION();
	*pids = (int *) palloc(sizeof(int) * n32);
	END_CRIT_SECTION();

	r = n32*sizeof(int);
	if(pool_block_recv(Socket(*port), *pids, r) != r)
		goto failure;

	return n32;

failure:
	return 0;
}

/*
 * Send a message containing pid numbers to the specified connection
 */
int
pool_sendpids(PoolPort *port, int *pids, int count)
{
	int res = 0;
	char		buf[SEND_PID_BUFFER_SIZE];
	uint		n32;

	buf[0] = 'p';
	n32 = htonl((uint32) count);
	memcpy(buf + 1, &n32, 4);
	n32 = count*4;
	memcpy(buf + 5, pids, n32);

	n32 += 5;
	if (send(Socket(*port), &buf, n32,0) != n32)
	{
		res = EOF;
	}

	return res;
}

const char* pool_get_sock_path(void)
{
	return sock_path;
}

static void pool_report_error(pgsocket sock, uint32 msg_len)
{
	char *err_msg;
	uint32 recv_len;
	if(msg_len > 0)
	{
		START_CRIT_SECTION();
		err_msg = palloc(msg_len+1);
		END_CRIT_SECTION();

		recv_len = pool_block_recv(sock, err_msg, msg_len);
		if(recv_len != msg_len)
		{
			pfree(err_msg);
			return;
		}
		err_msg[msg_len] = '\0';
	}
	ereport(ERROR, (errmsg("error message from poolmgr:%s", msg_len>0 ? err_msg:"missing error text")));
}

/*
 * EINTR continue
 */
static int pool_block_recv(pgsocket sock, void *ptr, uint32 size)
{
	uint32 recv_len;
	int rval;

	HOLD_CANCEL_INTERRUPTS();
	for(recv_len=0;recv_len<size;)
	{
		rval = recv(sock, ((char*)ptr) + recv_len, size-recv_len, 0);
		if(rval < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if(errno == EINTR)
				continue;
			ereport(FATAL, (errcode_for_socket_access(),
				errmsg("could not receive pool data from client: %m")));
		}else if(rval == 0)
		{
			break;
		}
		recv_len += rval;
	}
	RESUME_CANCEL_INTERRUPTS();
	return recv_len;
}
