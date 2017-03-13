
#include "agtm/agtm_transaction.h"
#include "libpq/pqnone.h"
#include "libpq/pqnode.h"

static pgsocket agtm_listen_socket = PGINVALID_SOCKET;
int agtm_listen_port = 0;
static StringInfoData agtm_pq_buf = {NULL, 0, 0, 0};

static void start_agtm_listen(void);
static void close_agtm_socket(int code, Datum arg);
static int agtm_ReadCommand(StringInfo inBuf);
static int agtm_setup_select_recv_fd(fd_set *rset, fd_set *wset, List *list, int maxfd);
static int agtm_select_fd(fd_set *rset, fd_set *wset, int maxfd);
static int agtm_socket_event(const fd_set *rfd, const fd_set *wfd, List *pqlist, StringInfo s);
static int agtm_try_port_msg(StringInfo s);

static void start_agtm_listen(void)
{
	struct sockaddr_in listen_addr;
	socklen_t slen;
	pgsocket sock;
	Assert(agtm_listen_socket == PGINVALID_SOCKET);

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if(sock == PGINVALID_SOCKET)
	{
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("Can not create %s socket:%m", _("IPv4"))));
	}
	on_proc_exit(close_agtm_socket, 0);

	memset(&listen_addr, 0, sizeof(listen_addr));
	listen_addr.sin_family = AF_INET;
	/*listen_addr.sin_port = 0;*/
	listen_addr.sin_addr.s_addr = INADDR_ANY;
	if(bind(sock, (struct sockaddr *)&listen_addr,sizeof(listen_addr)) < 0
		|| listen(sock, PG_SOMAXCONN) < 0)
	{
		closesocket(sock);
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("could not listen on %s socket: %m", _("IPv4"))));
	}

	/* get listen port on */
	memset(&listen_addr, 0, sizeof(listen_addr));
	slen = sizeof(listen_addr);
	if(getsockname(sock, (struct sockaddr*)&listen_addr, &slen) < 0)
	{
		closesocket(sock);
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("could not get listen port on socket:%m")));
	}
	agtm_listen_socket = sock;
	agtm_listen_port = htons(listen_addr.sin_port);
}

static void close_agtm_socket(int code, Datum arg)
{
	if(agtm_listen_socket != PGINVALID_SOCKET)
	{
		closesocket(agtm_listen_socket);
		agtm_listen_socket = PGINVALID_SOCKET;
	}
}

static int agtm_ReadCommand(StringInfo inBuf)
{
	fd_set rfd,wfd;
	List *list_pq_node;
	ListCell *lc;
	int maxfd;
	int firstChar;
	Assert(agtm_listen_socket != PGINVALID_SOCKET);

	HOLD_CANCEL_INTERRUPTS();

	do
	{
		/* here we have no client to send message */
		pq_switch_to_none();

		/* have unread message ? */
		if(MyProcPort->sock != PGINVALID_SOCKET)
		{
			firstChar = agtm_try_port_msg(inBuf);
			if(firstChar == 'X')
			{
				closesocket(MyProcPort->sock);
				MyProcPort->sock = PGINVALID_SOCKET;
			}else if(firstChar != 0)
			{
				pq_switch_to_socket();
				break;
			}
		}

re_try_node_:
		list_pq_node = get_all_pq_node();
		if(list_pq_node == NIL && MyProcPort->sock == PGINVALID_SOCKET)
			return EOF;	/* have no any client */

		/* test have unread message from node */
		foreach(lc, list_pq_node)
		{
			pq_comm_node *node = lfirst(lc);
			firstChar = pq_node_get_msg(inBuf, node);
			if(firstChar == 'X')
			{
				pq_node_close(node);
				goto re_try_node_;
			}else if(firstChar != 0)
			{
				pq_node_switch_to(node);
				goto end_try_node_;
			}
		}

		/* wait client(s) message data */
		FD_ZERO(&rfd);
		FD_ZERO(&wfd);

		FD_SET(agtm_listen_socket, &rfd);
		maxfd = agtm_setup_select_recv_fd(&rfd, &wfd, list_pq_node, agtm_listen_socket);

		(void)agtm_select_fd(&rfd, &wfd, maxfd);
		list_pq_node = get_all_pq_node();
		firstChar = agtm_socket_event(&rfd, &wfd, list_pq_node, inBuf);
	}while(firstChar == 0);

end_try_node_:
	/* wait client */
	RESUME_CANCEL_INTERRUPTS();
	return firstChar;
}

static int agtm_setup_select_recv_fd(fd_set *rset, fd_set *wset, List *list, int maxfd)
{
	ListCell *lc;
	pq_comm_node *node;
	pgsocket sock;
	AssertArg(rset && wset);

	if(MyProcPort->sock != PGINVALID_SOCKET)
	{
		FD_SET(MyProcPort->sock, rset);
		if(socket_is_send_pending())
			FD_SET(MyProcPort->sock, wset);
		if(maxfd < MyProcPort->sock)
			maxfd = MyProcPort->sock;
	}

	for(lc=list_head(list);lc;)
	{
		node = lfirst(lc);
		Assert(node);
		sock = socket_pq_node(lfirst(lc));
		if(sock == PGINVALID_SOCKET)
		{
			lc = lnext(lc);
			pq_node_close(node);
			continue;
		}
		if(pq_node_is_write_only(node) && !pq_node_send_pending(node))
		{
			/* write only and no data to send, close it */
			lc = lnext(lc);
			pq_node_close(node);
			continue;
		}
		if(!pq_node_is_write_only(node))
		{
			FD_SET(sock, rset);
			if(maxfd < sock)
				maxfd = sock;
		}
		if(pq_node_send_pending(lfirst(lc)))
		{
			FD_SET(sock, wset);
			if(maxfd < sock)
				maxfd = sock;
		}
		lc = lnext(lc);
	}
	return maxfd;
}

static int agtm_select_fd(fd_set *rset, fd_set *wset, int maxfd)
{
	fd_set rfd,wfd;
	int result;
	AssertArg(rset && wset);

	HOLD_CANCEL_INTERRUPTS();
re_select_fd_:
	memcpy(&rfd, rset, sizeof(rfd));
	memcpy(&wfd, wset, sizeof(wfd));
	result = select(maxfd+1, &rfd, &wfd, NULL, NULL);
	if(result < 0)
	{
		if(errno == EINTR)
			goto re_select_fd_;
		ereport(FATAL, (errcode_for_socket_access(),
			errmsg("can not select socket(s):%m")));
	}
	memcpy(rset, &rfd, sizeof(rfd));
	memcpy(wset, &wfd, sizeof(wfd));
	RESUME_CANCEL_INTERRUPTS();
	return result;
}

static int agtm_socket_event(const fd_set *rfd, const fd_set *wfd, List *pqlist, StringInfo s)
{
	ListCell *lc;
	pq_comm_node *node;
	pgsocket sock;
	int msg_type;
	Assert(rfd && wfd);

	if(MyProcPort->sock != PGINVALID_SOCKET)
	{
		sock = MyProcPort->sock;
		if(FD_ISSET(sock, wfd))
		{
			Assert(socket_is_send_pending());
			if(socket_flush() != 0)
			{
				/* flush failed */
				closesocket(MyProcPort->sock);
				MyProcPort->sock = sock = PGINVALID_SOCKET;
			}
		}
		if(sock != PGINVALID_SOCKET && FD_ISSET(sock, rfd))
		{
			if(pq_recvbuf() != 0)
			{
				closesocket(MyProcPort->sock);
				MyProcPort->sock = PGINVALID_SOCKET;
				goto event_node_;
			}

			msg_type = agtm_try_port_msg(s);
			if(msg_type == 'X')
			{
				closesocket(MyProcPort->sock);
				MyProcPort->sock = PGINVALID_SOCKET;
				goto event_node_;
			}
			if(msg_type != 0)
			{
				pq_switch_to_socket();
				return msg_type;
			}
		}
	}

event_node_:
	for(lc=list_head(pqlist);lc;)
	{
		node = lfirst(lc);
		Assert(node != NULL);
		sock = socket_pq_node(node);
		if(sock == PGINVALID_SOCKET)
		{
			lc = lnext(lc);
			pq_node_close(node);
			continue;
		}
		if(FD_ISSET(sock, wfd) && pq_node_flush_sock(node) != 0)
		{
			lc = lnext(lc);
			pq_node_close(node);
			continue;
		}
		if(FD_ISSET(sock, rfd))
		{
			if(pq_node_recvbuf(node) != 0)
			{
				lc = lnext(lc);
				pq_node_close(node);
				continue;
			}
			msg_type = pq_node_get_msg(s, node);
			if(msg_type != 0 && msg_type != 'X')
			{
				pq_node_switch_to(node);
				return msg_type;
			}
		}
		lc = lnext(lc);
	}

	if(agtm_listen_socket != PGINVALID_SOCKET
		&& FD_ISSET(agtm_listen_socket, rfd))
	{
		/* new client */
		sock = accept(agtm_listen_socket, NULL, 0);
		if(sock == PGINVALID_SOCKET)
		{
			ereport(COMMERROR, (errcode_for_socket_access(),
				errmsg("backend can not accept new client:%m")));
		}else
		{
			pq_node_new(sock);
		}
	}
	return 0;
}

static int agtm_try_port_msg(StringInfo s)
{
	int32 msg_len;
	int msg_type;
	AssertArg(s);
	Assert(MyProcPort->sock != PGINVALID_SOCKET);

	/* init stringInfo if not inited */
	if(agtm_pq_buf.data == NULL)
	{
		MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
		initStringInfo(&agtm_pq_buf);
		MemoryContextSwitchTo(old_ctx);
	}

	/* get message type and length */
	if(agtm_pq_buf.len < 5)
		pq_getmessage_noblock(&agtm_pq_buf, 5-agtm_pq_buf.len);
	if(agtm_pq_buf.len >= 5)
	{
		memcpy(&msg_len, agtm_pq_buf.data + 1, sizeof(msg_len));
		msg_len = htonl(msg_len);
		msg_len++;	/* add length of msg type */
		if(msg_len > 5 && agtm_pq_buf.len < msg_len)
			pq_getmessage_noblock(&agtm_pq_buf, msg_len-agtm_pq_buf.len);
		if(agtm_pq_buf.len >= msg_len)
		{
			msg_type = agtm_pq_buf.data[0];
			appendBinaryStringInfo(s, agtm_pq_buf.data+5, msg_len-5);
			if(agtm_pq_buf.len > msg_len)
			{
				agtm_pq_buf.len -= msg_len;
				memmove(agtm_pq_buf.data, agtm_pq_buf.data + msg_len, agtm_pq_buf.len);
			}else
			{
				agtm_pq_buf.len = 0;
			}
			return msg_type;
		}
	}
	return 0;
}
