#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/rxact_comm.h"
#include "access/rxact_mgr.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "catalog/pgxc_node.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#define AGTM_OID			OID_MAX
#define RETRY_TIME			1	/* 1 second */
#define INVALID_INDEX		((Index)-1)
#define MAX_RXACT_BUF_SIZE	4096
#if defined(EAGAIN) && EAGAIN != EINTR
#define IS_ERR_INTR() (errno == EINTR || errno == EAGAIN)
#else
#define IS_ERR_INTR() (errno == EINTR)
#endif

#define REMOTE_CONN_INTERVAL_STEP	2	/* connection failure add interval value */
#define REMOTE_CONN_INTERVAL_MAX	30	/* max connection remote interval (in second)*/
#define REMOTE_IDLE_TIMEOUT			60	/* close remote connect if it idle in second */

#define RXACT_MSG_CONNECT	'C'
#define RXACT_MSG_DO		'D'
#define RXACT_MSG_SUCCESS	'S'
#define RXACT_MSG_FAILED	'F'
#define RXACT_MSG_OK		'K'
#define RXACT_MSG_ERROR		'E'
#define RXACT_MSG_NODE_INFO	'I'

#define RXACT_TYPE_IS_VALID(t) (t == RX_PREPARE || t == RX_COMMIT || t == RX_ROLLBACK)

typedef struct RxactAgent
{
	/* index in of first, start with 0, INVALID_INDEX for not in use */
	Index	index;
	pgsocket sock;
	Oid		dboid;
	bool	in_error;
	char	last_gid[NAMEDATALEN];
	StringInfoData out_buf;
	StringInfoData in_buf;
}RxactAgent;

typedef struct RemoteNode
{
	Oid			nodeOid;
	uint16		nodePort;
	char		nodeHost[NAMEDATALEN];
} RemoteNode;

typedef struct DatabaseNode
{
	Oid		dbOid;
	char	dbname[NAMEDATALEN];
	char	owner[NAMEDATALEN];
}DatabaseNode;

typedef struct DbAndNodeOid
{
	Oid node_oid;
	Oid db_oid;
}DbAndNodeOid;

typedef struct NodeConn
{
	DbAndNodeOid oids;	/* must be first */
	PGconn *conn;
	time_t last_use;	/* when conn != NULL: last use time
						 * when conn == NULL: next connect time */
	uint32 conn_interval; /* when connection failure, next connect interval */
}NodeConn;

typedef struct GlobalTransactionInfo
{
	char gid[NAMEDATALEN];	/* 2pc id */
	Oid *remote_nodes;		/* all remote nodes, include AGTM */
	bool *remote_success;	/* remote execute success ? */
	int count_nodes;		/* count of remote nodes */
	Oid db_oid;				/* transaction database Oid */
	RemoteXactType type;	/* remote 2pc type */
	bool failed;			/* backend do it failed ? */
}GlobalTransactionInfo;

extern char	*AGtmHost;
extern int	AGtmPort;
extern int MaxBackends;

static HTAB *htab_remote_node = NULL;	/* RemoteNode */
static HTAB *htab_db_node = NULL;		/* DatabaseNode */
static HTAB *htab_node_conn = NULL;		/* NodeConn */
static HTAB *htab_rxid = NULL;			/* GlobalTransactionInfo */

static pgsocket rxact_server_fd = PGINVALID_SOCKET;
static RxactAgent *allRxactAgent = NULL;
static Index *indexRxactAgent = NULL;
static volatile unsigned int agentCount = 0;
/*static volatile bool rxact_has_filed_gid = false;*/

static volatile pgsocket rxact_client_fd = PGINVALID_SOCKET;

static void CreateRxactAgent(int agent_fd);
static void RxactMgrQuickdie(SIGNAL_ARGS);
static void RxactLoop(void);
static void RemoteXactBaseInit(void);
static void RemoteXactMgrInit(void);
static void DestroyRemoteConnHashTab(void);
static void RxactLoadLog(void);
static void on_exit_rxact_mgr(int code, Datum arg);

static bool rxact_agent_recv_data(RxactAgent *agent);
static void rxact_agent_input(RxactAgent *agent);
static void agent_error_hook(void *arg);
static void rxact_agent_output(RxactAgent *agent);
static void rxact_agent_destroy(RxactAgent *agent);
static void rxact_agent_end_msg(RxactAgent *agent, StringInfo msg);
static void rxact_agent_simple_msg(RxactAgent *agent, char msg_type);

/* parse message from backend */
static void rxact_agent_connect(RxactAgent *agent, StringInfo msg);
static void rxact_agent_do(RxactAgent *agent, StringInfo msg);
static void rxact_agent_mark(RxactAgent *agent, StringInfo msg, bool success);
static void rxact_agent_node_info(RxactAgent *agent, StringInfo msg);
/* if any oid unknown, get it from backend */
static bool query_remote_oid(RxactAgent *agent, Oid *oid, int count);

/* htab functions */
static uint32 hash_DbAndNodeOid(const void *key, Size keysize);
static int match_DbAndNodeOid(const void *key1, const void *key2,
											Size keysize);
static void rxact_insert_database(Oid db_oid, const char *dbname, const char *owner);
static void
rxact_insert_gid(const char *gid, const Oid *oids, int count, RemoteXactType type);
static void rxact_mark_gid(const char *gid, RemoteXactType type, bool success);
static void rxact_insert_node_info(Oid oid, short port, const char *addr, bool update, bool is_redo);

/* 2pc redo functions */
static void rxact_2pc_do(void);
static NodeConn* rxact_get_node_conn(Oid db_oid, Oid node_oid, time_t cur_time);
static void rxact_build_2pc_cmd(StringInfo cmd, const char *gid, RemoteXactType type);
static void rxact_close_timeout_remote_conn(time_t cur_time);

static void
CreateRxactAgent(pgsocket agent_fd)
{
	RxactAgent *agent;
	unsigned int i;
	AssertArg(agent_fd != PGINVALID_SOCKET);

	if(agentCount >= MaxBackends)
	{
		closesocket(agent_fd);
		ereport(WARNING, (errmsg("too many connect for RXACT")));
	}

	agent = NULL;
	for(i=0;i<agentCount;++i)
	{
		if(allRxactAgent[i].index == INVALID_INDEX)
		{
			agent = &allRxactAgent[i];
			agent->index = i;
			break;
		}
	}
	Assert(agent != NULL && agent->index == i);
#ifdef USE_ASSERT_CHECKING
	for(i=0;i<agentCount;++i)
		Assert(indexRxactAgent[i] != agent->index);
#endif

	agent->sock = agent_fd;
	indexRxactAgent[agentCount++] = agent->index;
	resetStringInfo(&(agent->in_buf));
	resetStringInfo(&(agent->out_buf));
}

static void
RxactMgrQuickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}

static void RxactLoop(void)
{
	sigjmp_buf			local_sigjmp_buf;
	RxactAgent 			*agent;
	struct pollfd		*pollfds, *tmpfd;
	StringInfoData		message;
	time_t				last_time,cur_time;
	unsigned int		i, count;
	Index 				index;
	int					agent_fd;

	Assert(rxact_server_fd != PGINVALID_SOCKET);
	if(pg_set_noblock(rxact_server_fd) == false)
		ereport(FATAL, (errmsg("Can not set RXACT listen socket to noblock:%m")));

	MemoryContextSwitchTo(TopMemoryContext);

	pollfds = palloc(sizeof(pollfds[0]) * (MaxBackends+1));
	pollfds[0].fd = rxact_server_fd;
	pollfds[0].events = POLLIN;
	initStringInfo(&message);

	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Cleanup something */
	}
	PG_exception_stack = &local_sigjmp_buf;
	(void)MemoryContextSwitchTo(MessageContext);

	last_time = cur_time = time(NULL);
	for (;;)
	{
		int		pollres;

		MemoryContextResetAndDeleteChildren(MessageContext);

		if (!PostmasterIsAlive())
			exit(0);

		for (i = 0; i < agentCount; i++)
		{
			index = indexRxactAgent[i];
			Assert(index >= 0 && index < (Index)MaxBackends);
			agent = &allRxactAgent[index];
			Assert(agent->index == index && agent->sock != PGINVALID_SOCKET);

			pollfds[i+1].fd = agent->sock;
			if(agent->out_buf.len > agent->out_buf.cursor)
				pollfds[i].events = POLLOUT;
			else
				pollfds[i].events = POLLIN;
		}

re_poll_:
		/* for we wait 1 second */
		pollres = poll(pollfds, agentCount+1, 1000);
		CHECK_FOR_INTERRUPTS();

		if (pollres < 0)
		{
			if(IS_ERR_INTR())
				goto re_poll_;
			ereport(PANIC, (errcode_for_socket_access(),
				errmsg("pool failed(%d) in pooler process, error %m", pollres)));
		} /*else if (pollres == 0)*/
		{
			/* timeout do nothing */
		}

		count = 0;
		for(i=agentCount;i && count < (unsigned int)pollres;)
		{
			tmpfd = &pollfds[i];
			--i;
			if(tmpfd->revents == 0)
				continue;

			index = indexRxactAgent[i];
			Assert(index >= 0 && index < (Index)MaxBackends);
			agent = &allRxactAgent[index];
			Assert(agent->index == index);
			Assert(agent->sock != PGINVALID_SOCKET && agent->sock == tmpfd->fd);
			++count;
			if(tmpfd->revents & POLLOUT)
			{
				Assert(agent->out_buf.len > agent->out_buf.cursor);
				rxact_agent_output(agent);
			}else if(tmpfd->revents & (POLLIN | POLLERR | POLLHUP))
			{
				rxact_agent_input(agent);
			}
		}

		/* Get a new connection */
		if (pollfds[0].revents & POLLIN)
		{
			for(;;)
			{
				agent_fd = accept(rxact_server_fd, NULL, NULL);
				if(agent_fd == PGINVALID_SOCKET)
				{
					if(errno != EWOULDBLOCK)
						ereport(WARNING, (errcode_for_socket_access()
							,errmsg("RXACT accept new connect failed:%m")));
					break;
				}
				CreateRxactAgent(agent_fd);
			}
		}

		cur_time = time(NULL);
		if(last_time != cur_time)
		{
			rxact_2pc_do();
			/* get current time again
			 * because rxact_2pc_do() maybe run one more second times
			 */
			cur_time = time(NULL);
			rxact_close_timeout_remote_conn(cur_time);
			last_time = cur_time;
		}
	}
}

static void RemoteXactBaseInit(void)
{
	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(pool manager probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif
	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, RxactMgrQuickdie);
	pqsignal(SIGHUP, SIG_IGN);
	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
	sigdelset(&BlockSig, SIGQUIT);
#else
	BlockSig &= ~(sigmask(SIGQUIT));
#endif

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);
}

static void RemoteXactMgrInit(void)
{
	HASHCTL hctl;
	unsigned int i;

	START_CRIT_SECTION();

	/* init listen socket */
	Assert(rxact_server_fd == PGINVALID_SOCKET);
	rxact_server_fd = rxact_listen();
	if(rxact_server_fd == PGINVALID_SOCKET)
	{
		ereport(FATAL,
			(errmsg("Remote xact can not create listen socket on \"%s\":%m", rxact_get_sock_path())));
	}

	/* create HTAB for RemoteNode */
	Assert(htab_remote_node == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(RemoteNode);
	hctl.hash = oid_hash;
	hctl.hcxt = TopMemoryContext;
	htab_remote_node = hash_create("RemoteNode"
		, 64, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/* create HTAB for DatabaseNode */
	Assert(htab_db_node == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(DatabaseNode);
	hctl.hash = oid_hash;
	hctl.hcxt = TopMemoryContext;
	htab_db_node = hash_create("DatabaseNode"
		, 64, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/* insert AGTM info */
	rxact_insert_node_info(AGTM_OID, (short)AGtmPort, AGtmHost, false, true);

	/* create HTAB for NodeConn */
	Assert(htab_node_conn == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(DbAndNodeOid);
	hctl.entrysize = sizeof(NodeConn);
	hctl.hash = hash_DbAndNodeOid;
	hctl.match = match_DbAndNodeOid;
	hctl.hcxt = TopMemoryContext;
	htab_node_conn = hash_create("DatabaseNode"
		, 64
		, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* create HTAB for GlobalTransactionInfo */
	Assert(htab_rxid == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(((GlobalTransactionInfo*)0)->gid);
	hctl.entrysize = sizeof(GlobalTransactionInfo);
	hctl.hcxt = TopMemoryContext;
	htab_rxid = hash_create("DatabaseNode"
		, 512
		, &hctl, HASH_ELEM | HASH_CONTEXT);

	Assert(agentCount == 0);
	allRxactAgent = palloc(sizeof(allRxactAgent[0]) * MaxBackends);
	for(i=0;i<MaxBackends;++i)
	{
		allRxactAgent[i].index = INVALID_INDEX;
		initStringInfo(&(allRxactAgent[i].in_buf));
		initStringInfo(&(allRxactAgent[i].out_buf));
	}
	indexRxactAgent = palloc(sizeof(indexRxactAgent[0]) * MaxBackends);

	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	END_CRIT_SECTION();
	on_proc_exit(on_exit_rxact_mgr, (Datum)0);
}

static void
DestroyRemoteConnHashTab(void)
{
}

void
RemoteXactMgrMain(void)
{
	PG_exception_stack = NULL;

	RemoteXactBaseInit();

	/* Initinalize something */
	RemoteXactMgrInit();

	RxactLoadLog();

	/* Server loop */
	RxactLoop();

	proc_exit(1);
}

static void RxactLoadLog(void)
{
}

static void
on_exit_rxact_mgr(int code, Datum arg)
{
	closesocket(rxact_server_fd);
	rxact_server_fd = PGINVALID_SOCKET;
	DestroyRemoteConnHashTab();
}

/*
 * Destroy RxactAgent
 */
static void
rxact_agent_destroy(RxactAgent *agent)
{
	unsigned int i,n;
	AssertArg(agent && agent->index != INVALID_INDEX);
	for(i=0;i<agentCount;++i)
	{
		if(indexRxactAgent[i] == agent->index)
			break;
	}

	Assert(i<agentCount);
	n = agentCount-1;
	for(;i<n;++i)
		indexRxactAgent[i] = indexRxactAgent[i+1];
	closesocket(agent->sock);
	agent->sock = PGINVALID_SOCKET;
	agent->dboid = InvalidOid;

	if(agent->last_gid[0] != '\0')
	{
		GlobalTransactionInfo *ginfo;
		bool found;
		ginfo = hash_search(htab_rxid, agent->last_gid, HASH_FIND, &found);
		if(!found)
		{
			ereport(WARNING,
				(errmsg("Can not found gid '%s' at destroy agent", agent->last_gid)));
		}else
		{
			Assert(ginfo && ginfo->failed == false);
			rxact_mark_gid(ginfo->gid, ginfo->type, false);
		}
		agent->last_gid[0] = '\0';
	}
}

static void rxact_agent_end_msg(RxactAgent *agent, StringInfo msg)
{
	AssertArg(agent && msg);
	if(agent->sock != PGINVALID_SOCKET)
	{
		rxact_put_finsh(msg);
		appendBinaryStringInfo(&(agent->out_buf), msg->data, msg->len);
	}
	pfree(msg->data);
}

static void rxact_agent_simple_msg(RxactAgent *agent, char msg_type)
{
	union
	{
		int len;
		char str[5];
	}msg;
	AssertArg(agent);

	enlargeStringInfo(&(agent->out_buf), 5);
	msg.len = 5;
	msg.str[4] = msg_type;
	appendBinaryStringInfo(&(agent->out_buf), msg.str, 5);
}

/* true for recv some data, false for closed by remote */
static bool
rxact_agent_recv_data(RxactAgent *agent)
{
	StringInfo buf;
	ssize_t recv_res;

	AssertArg(agent && agent->sock != PGINVALID_SOCKET);
	buf = &(agent->in_buf);

	if(buf->cursor > 0)
	{
		if(buf->len > buf->cursor)
		{
			memmove(buf->data, buf->data + buf->cursor, buf->len - buf->cursor);
			buf->len -= buf->cursor;
			buf->cursor = 0;
		}else
		{
			buf->cursor = buf->len = 0;
		}
	}

	if(buf->len >= buf->maxlen)
	{
		if(buf->len >= MAX_RXACT_BUF_SIZE)
		{
			ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("too many data from backend for remote xact manager")));
			return false;
		}
		PG_TRY_HOLD();
		{
			enlargeStringInfo(buf, buf->maxlen + 1024);
		}PG_CATCH_HOLD();
		{
		}PG_END_TRY_HOLD();
		if(buf->len >= buf->maxlen)
			return false;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		recv_res = recv(agent->sock
			, buf->data + buf->len
			, buf->maxlen - buf->len, 0);
		CHECK_FOR_INTERRUPTS();

		if (recv_res < 0)
		{
			if (IS_ERR_INTR())
				continue;		/* Ok if interrupted */

			/*
			 * Report broken connection
			 */
			ereport(WARNING,
					(errcode_for_socket_access(),
					 errmsg("RXACT could not receive data from client: %m")));
			return false;
		}else if (recv_res == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return false;
		}
		/* rval contains number of bytes read, so just incr length */
		buf->len += recv_res;
		Assert(buf->len <= buf->maxlen);
		break;
	}
	return true;
}

/* get message if has completion message */
static bool
agent_has_completion_msg(RxactAgent *agent, StringInfo msg, int *msg_type)
{
	StringInfo buf;
	Size unread_len;
	int len;
	AssertArg(agent && msg);

	buf = &(agent->in_buf);

	unread_len = buf->len - buf->cursor;
	/* 5 is message type (char) and message length(int) */
	if(unread_len < 5)
		return false;

	/* get message length */
	memcpy(&len, buf->data + buf->cursor, 4);
	if(len > unread_len)
		return false;

	*msg_type = buf->data[buf->cursor + 4];

	/* okay, copy message */
	resetStringInfo(msg);
	enlargeStringInfo(msg, len);
	memcpy(msg->data, buf->data + buf->cursor, len);
	msg->len = len;
	msg->cursor = 5; /* skip message type and length */
	buf->cursor += len;
	return true;
}

static void
rxact_agent_input(RxactAgent *agent)
{
	ErrorContextCallback	err_calback;
	StringInfoData			s;
	int						qtype;

	/* try recv data */
	if(rxact_agent_recv_data(agent) == false)
	{
		/* closed by remote */
		rxact_agent_destroy(agent);
		return;
	}

	/* setup error callback */
	err_calback.arg = agent;
	err_calback.callback = agent_error_hook;
	err_calback.previous = error_context_stack;
	error_context_stack = &err_calback;

	initStringInfo(&s);
	while(agent_has_completion_msg(agent, &s, &qtype))
	{
		switch(qtype)
		{
		case RXACT_MSG_CONNECT:
			rxact_agent_connect(agent, &s);
			break;
		case RXACT_MSG_DO:
			rxact_agent_do(agent, &s);
			break;
		case RXACT_MSG_SUCCESS:
			rxact_agent_mark(agent, &s, true);
			break;
		case RXACT_MSG_FAILED:
			rxact_agent_mark(agent, &s, false);
			break;
		case RXACT_MSG_NODE_INFO:
			rxact_agent_node_info(agent, &s);
			break;
		default:
			ereport(ERROR, (errmsg("unknown message type %d", qtype)));
		}
		rxact_get_msg_end(&s);
	}
	error_context_stack = err_calback.previous;
}

static void agent_error_hook(void *arg)
{
	const ErrorData *err;
	RxactAgent *agent = arg;
	AssertArg(agent);

	if(agent->in_error == false
		&& agent->sock != PGINVALID_SOCKET
		&& (err = err_current_data()) != NULL
		&& err->elevel >= ERROR)
	{
		StringInfoData msg;
		rxact_begin_msg(&msg, RXACT_MSG_ERROR);
		rxact_put_string(&msg, err->context ? err->context : "miss error message");
		rxact_agent_end_msg(arg, &msg);
	}
}

static void rxact_agent_output(RxactAgent *agent)
{
	ssize_t send_res;
	AssertArg(agent && agent->sock != PGINVALID_SOCKET);
	AssertArg(agent->out_buf.len > agent->out_buf.cursor);

re_send_:
	send_res = send(agent->sock
		, agent->out_buf.data + agent->out_buf.cursor
		, agent->out_buf.len - agent->out_buf.cursor, 0);
	CHECK_FOR_INTERRUPTS();

	if(send_res == 0)
	{
		rxact_agent_destroy(agent);
	}else if(send_res > 0)
	{
		agent->out_buf.cursor += send_res;
		if(agent->out_buf.cursor >= agent->out_buf.len)
			resetStringInfo(&(agent->out_buf));
	}else
	{
		if(IS_ERR_INTR())
			goto re_send_;
		ereport(WARNING, (errcode_for_socket_access()
			, errmsg("Can not send message to RXACT client:%m")));
		rxact_agent_destroy(agent);
	}
}

static void rxact_agent_connect(RxactAgent *agent, StringInfo msg)
{
	char *owner;
	char *dbname;
	AssertArg(agent && msg);

	agent->dboid = (Oid)rxact_get_int(msg);
	dbname = rxact_get_string(msg);
	owner = rxact_get_string(msg);
	rxact_insert_database(agent->dboid, dbname, owner);
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_do(RxactAgent *agent, StringInfo msg)
{
	Oid *oids;
	char *gid;
	RemoteXactType type;
	int count;
	AssertArg(agent && msg);

	if(!OidIsValid(agent->dboid))
	{
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			,errmsg("not got database oid")));
	}

	type = (RemoteXactType)rxact_get_int(msg);
	if(!RXACT_TYPE_IS_VALID(type))
		ereport(ERROR, (errmsg("Unknown remote xact type %d", type)));
	count = rxact_get_int(msg);
	oids = rxact_get_bytes(msg, sizeof(Oid)*count);
	gid = rxact_get_string(msg);

	rxact_insert_gid(gid, oids, count, type);
	strncpy(agent->last_gid, gid, sizeof(agent->last_gid)-1);

	/*
	 * check is all remote oid known
	 * when has unknown Oid, it send query message, we get at next message
	 */
	if(query_remote_oid(agent, oids, count) == false)
		rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_mark(RxactAgent *agent, StringInfo msg, bool success)
{
	const char *gid;
	RemoteXactType type;
	AssertArg(agent && msg);

	type = (RemoteXactType)rxact_get_int(msg);
	gid = rxact_get_string(msg);
	rxact_mark_gid(gid, type, success);
	agent->last_gid[0] = '\0';
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_node_info(RxactAgent *agent, StringInfo msg)
{
	const char *address;
	int i,count;
	Oid oid;
	short port;
	AssertArg(agent && msg);

	count = rxact_get_int(msg);
	for(i=0;i<count;++i)
	{
		oid = (Oid)rxact_get_int(msg);
		port = rxact_get_short(msg);
		address = rxact_get_string(msg);
		rxact_insert_node_info(oid, port, address, false, false);
	}
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

/* if any oid unknown, get it from backend */
static bool query_remote_oid(RxactAgent *agent, Oid *oid, int count)
{
	StringInfoData buf;
	int i;
	int unknown_offset;
	int unknown_count;

	AssertArg(agent && oid && count > 0);

	for(i=0;i<count;++i)
	{
		if(hash_search(htab_remote_node, &oid[i], HASH_FIND, NULL) == NULL)
			break;
	}
	if(i<count)
		return false; /* all known */

	rxact_begin_msg(&buf, RXACT_MSG_NODE_INFO);
	unknown_offset = buf.len;
	rxact_put_int(&buf, 0);
	for(unknown_count=i=0;i<count;++i)
	{
		if(hash_search(htab_remote_node, &oid[i], HASH_FIND, NULL) == NULL)
		{
			rxact_put_int(&buf, (int)(oid[i]));
			unknown_count++;
		}
	}
	memcpy(buf.data + unknown_offset, &unknown_count, 4);
	rxact_agent_end_msg(agent, &buf);
	return true;
}

/* HTAB functions */
static uint32 hash_DbAndNodeOid(const void *key, Size keysize)
{
	Datum datum;
	Assert(keysize == sizeof(DbAndNodeOid));

	datum = hash_any((const unsigned char *) key
		, sizeof(DbAndNodeOid));
	return DatumGetUInt32(datum);
}

static int match_DbAndNodeOid(const void *key1, const void *key2,
											Size keysize)
{
	const DbAndNodeOid *l;
	const DbAndNodeOid *r;
	Assert(keysize == sizeof(DbAndNodeOid));
	AssertArg(key1 && key2);

	l = key1;
	r = key2;
	if(l->node_oid > r->node_oid)
		return 1;
	else if(l->node_oid < r->node_oid)
		return -1;
	else if(l->db_oid > r->db_oid)
		return 1;
	else if(l->db_oid < r->db_oid)
		return -1;
	return 0;
}

static void rxact_insert_database(Oid db_oid, const char *dbname, const char *owner)
{
	DatabaseNode *db;
	bool found;
	db = hash_search(htab_db_node, &db_oid, HASH_ENTER, &found);
	if(found)
	{
		Assert(db->dbOid == db_oid);
		return;
	}

	db->dbOid = db_oid;
	strcpy(db->dbname, dbname);
	strcpy(db->owner, owner);
	/* TODO insert into log file */
}

static void
rxact_insert_gid(const char *gid, const Oid *oids, int count, RemoteXactType type)
{
	GlobalTransactionInfo *ginfo;
	bool found;
	AssertArg(gid && gid[0] && oids && count > 0);
	if(!RXACT_TYPE_IS_VALID(type))
		ereport(ERROR, (errmsg("Unknown remote xact type %d", type)));

	ginfo = hash_search(htab_rxid, gid, HASH_ENTER, &found);
	if(found)
		ereport(ERROR, (errmsg("gid '%s' exists", gid)));

	PG_TRY();
	{
		/* TODO insert into log file */

		ginfo->remote_nodes
			= MemoryContextAllocZero(TopMemoryContext
				, (sizeof(Oid)+sizeof(bool))*(count+1));
		memcpy(ginfo->remote_nodes, oids, sizeof(Oid)*(count));
		ginfo->remote_nodes[count] = AGTM_OID;

		ginfo->remote_success = (bool*)(&ginfo->remote_nodes[count+1]);
		ginfo->count_nodes = count+1;

		ginfo->type = type;
		strncpy(ginfo->gid, gid, lengthof(ginfo->gid)-1);
		ginfo->failed = false;
	}PG_CATCH();
	{
		hash_search(htab_rxid, gid, HASH_REMOVE, NULL);
		PG_RE_THROW();
	}PG_END_TRY();

}

static void rxact_mark_gid(const char *gid, RemoteXactType type, bool success)
{
	GlobalTransactionInfo *ginfo;
	bool found;
	AssertArg(gid && gid[0]);

	/* TODO save to log file */

	ginfo = hash_search(htab_rxid, gid, HASH_FIND, &found);
	if(success)
	{
		if(!found)
		{
			/* ignore error */
			ereport(WARNING, (errmsg("gid '%s' not exists", gid)));
		}else
		{
			Assert(ginfo->type == type);
			pfree(ginfo->remote_nodes);
			hash_search(htab_rxid, gid, HASH_REMOVE, NULL);
		}
	}else
	{
		Assert(ginfo->type == type);
		ginfo->failed = true;
		/*rxact_has_filed_gid = true;*/
	}
}

static void rxact_insert_node_info(Oid oid, short port, const char *addr, bool update, bool is_redo)
{
	AssertArg(addr && addr[0]);

	RemoteNode *rnode;
	bool found;
	rnode = hash_search(htab_db_node, &oid, HASH_ENTER, &found);
	if(!found)
	{
		if(!is_redo)
		{
			/* TODO insert log file */
		}
		rnode->nodeOid = oid;
		rnode->nodePort = (uint16)port;
		strncpy(rnode->nodeHost, addr, sizeof(rnode->nodeHost)-1);
	}else if(update)
	{
		/* TODO update log file */
		Assert(rnode && rnode->nodeOid == oid);
		rnode->nodePort = (uint16)port;
		strncpy(rnode->nodeHost, addr, sizeof(rnode->nodeHost)-1);
	}
}

static void rxact_2pc_do(void)
{
	GlobalTransactionInfo *ginfo;
	NodeConn *node_conn;
	PGresult *pg_res;
	HASH_SEQ_STATUS hstatus;
	time_t start_time,cur_time;
	StringInfoData buf;
	int i;
	/*bool all_finish;*/
	bool gid_finish;
	bool cmd_is_ok;

	hash_seq_init(&hstatus, htab_rxid);
	buf.data = NULL;
	start_time = cur_time = time(NULL);
	/*all_finish = true;*/
	while((ginfo = hash_seq_search(&hstatus)) != NULL)
	{
		Assert(ginfo->count_nodes > 0);
		if(ginfo->failed == false)
			continue;

		cmd_is_ok = false;
		gid_finish = true;
		for(i=0;i<ginfo->count_nodes;++i)
		{
			/* skip successed node */
			if(ginfo->remote_success[i])
				continue;

			/* get node connection, skip if not connectiond */
			node_conn = rxact_get_node_conn(ginfo->db_oid, ginfo->remote_nodes[i], time(NULL));
			if(node_conn == NULL || node_conn->conn == NULL)
			{
				gid_finish = false;
				continue;
			}

			/* when SQL not maked, make it */
			if(cmd_is_ok == false)
				rxact_build_2pc_cmd(&buf, ginfo->gid, ginfo->type);

			pg_res = PQexec(node_conn->conn, buf.data);
			if(PQresultStatus(pg_res) != PGRES_COMMAND_OK)
				gid_finish = false;
			else
				ginfo->remote_success[i] = true;
			PQclear(pg_res);

			/* do not run one more second */
			node_conn->last_use = cur_time = time(NULL);
			if(cur_time != start_time)
				break;
		}

		if(gid_finish)
		{
#ifdef USE_ASSERT_CHECKING
			for(i=0;i<ginfo->count_nodes;++i)
				Assert(ginfo->remote_success[i] == true);
#endif /* USE_ASSERT_CHECKING */
			rxact_mark_gid(ginfo->gid, ginfo->type, true);
		}/*else
		{
			all_finish = false;
		}*/

		if(cur_time != start_time)
		{
			hash_seq_term(&hstatus);
			break;
		}
	}

	/*if(all_finish == true)
		rxact_has_filed_gid = false;*/
	if(buf.data)
		pfree(buf.data);
}

static NodeConn* rxact_get_node_conn(Oid db_oid, Oid node_oid, time_t cur_time)
{
	NodeConn *conn;
	DbAndNodeOid key;
	bool found;

	key.db_oid = db_oid;
	key.node_oid = node_oid;
	conn = hash_search(htab_node_conn, &key, HASH_ENTER, &found);
	if(!found)
	{
		conn->oids = key;
		conn->conn = NULL;
		conn->last_use = cur_time;
		conn->conn_interval = 0;
	}

	Assert(conn && conn->oids.db_oid == db_oid && conn->oids.node_oid == node_oid);
	if(conn->conn != NULL && PQstatus(conn->conn) != CONNECTION_OK)
	{
		PQfinish(conn->conn);
		conn->conn = NULL;
	}

	if(conn->conn == NULL && conn->last_use <= cur_time)
	{
		/* connection to remote node */
		RemoteNode *rnode;
		DatabaseNode *dnode;
		char port[15];
		if(conn->conn)
		{
			PQfinish(conn->conn);
			conn->conn = NULL;
		}
		rnode = hash_search(htab_remote_node, &key.node_oid, HASH_FIND, NULL);
		dnode = hash_search(htab_db_node, &key.db_oid, HASH_FIND, NULL);
		if(rnode && dnode)
		{
			sprintf(port, "%u", rnode->nodePort);
			conn->conn = PQsetdbLogin(rnode->nodeHost, port
				, NULL, NULL
				, dnode->dbname, dnode->owner, NULL);
			if(PQstatus(conn->conn) != CONNECTION_OK)
			{
				PQfinish(conn->conn);
				conn->conn = NULL;
			}
		}

		/* test connection is successed */
		if(conn->conn == NULL)
		{
			/* connection failed, update next connection time */
			conn->conn_interval += REMOTE_CONN_INTERVAL_STEP;
			if(conn->conn_interval > REMOTE_CONN_INTERVAL_MAX)
				conn->conn_interval = REMOTE_CONN_INTERVAL_MAX;
			/* do not use "cur_time" value here */
			conn->last_use = time(NULL) + conn->conn_interval;
		}else
		{
			Assert(PQstatus(conn->conn) == CONNECTION_OK);
		}
	}

	return conn;
}

static void rxact_build_2pc_cmd(StringInfo cmd, const char *gid, RemoteXactType type)
{
	AssertArg(cmd);
	if(cmd->data == NULL)
		initStringInfo(cmd);
	else
		resetStringInfo(cmd);

	switch(type)
	{
	case RX_PREPARE:
	case RX_ROLLBACK:
		appendStringInfoString(cmd, "rollback");
		break;
	case RX_COMMIT:
		appendStringInfoString(cmd, "commit");
		break;
	/* no default, keep compiler warning when not case all value*/
	}
	appendStringInfoString(cmd, " prepared if exists '");
	appendStringInfoString(cmd, gid);
	appendStringInfoChar(cmd, '\'');
}

static void rxact_close_timeout_remote_conn(time_t cur_time)
{
	NodeConn *node_conn;
	HASH_SEQ_STATUS seq_status;

	hash_seq_init(&seq_status, htab_node_conn);
	while((node_conn = hash_seq_search(&seq_status)) != NULL)
	{
		if(node_conn->conn == NULL)
			continue;
		if(cur_time - node_conn->last_use >= REMOTE_IDLE_TIMEOUT)
		{
			PQfinish(node_conn->conn);
			node_conn->conn = NULL;
		}
	}
}

/* ---------------------- interface for xact client --------------------------*/
static void record_rxact_status(const char *gid, RemoteXactType type, bool success);
static void send_msg_to_rxact(StringInfo buf);
static void recv_msg_from_rxact(StringInfo buf);
static void wait_socket(pgsocket sock, bool wait_send);
static void recv_socket(pgsocket sock, StringInfo buf, int max_recv);
static void connect_rxact(void);
static void disconnect_rxact(void);

void RecordRemoteXact(const char *gid, Oid *nodes, int count, RemoteXactType type)
{
	StringInfoData buf;
	AssertArg(gid && gid[0] && nodes && count > 0);
	AssertArg(RXACT_TYPE_IS_VALID(type));
	if(rxact_client_fd == PGINVALID_SOCKET)
		connect_rxact();

	rxact_begin_msg(&buf, RXACT_MSG_DO);
	rxact_put_int(&buf, (int)type);
	rxact_put_int(&buf, count);
	rxact_put_bytes(&buf, nodes, sizeof(nodes[0]) * count);
	rxact_put_string(&buf, gid);
	send_msg_to_rxact(&buf);

	recv_msg_from_rxact(&buf);
	pfree(buf.data);
}

void RecordRemoteXactSuccess(const char *gid, RemoteXactType type)
{
	record_rxact_status(gid, type, true);
}

void RecordRemoteXactFailed(const char *gid, RemoteXactType type)
{
	record_rxact_status(gid, type, false);
}

static void record_rxact_status(const char *gid, RemoteXactType type, bool success)
{
	StringInfoData buf;
	AssertArg(gid && gid[0] && RXACT_TYPE_IS_VALID(type));
	if(rxact_client_fd == PGINVALID_SOCKET)
		connect_rxact();

	rxact_begin_msg(&buf, success ? RXACT_MSG_SUCCESS : RXACT_MSG_FAILED);
	rxact_put_int(&buf, (int)type);
	rxact_put_string(&buf, gid);
	send_msg_to_rxact(&buf);

	recv_msg_from_rxact(&buf);
	pfree(buf.data);
}

static void send_msg_to_rxact(StringInfo buf)
{
	ssize_t send_res;
	AssertArg(buf);

	Assert(rxact_client_fd != PGINVALID_SOCKET);

	rxact_put_finsh(buf);
	Assert(buf->len >= 5);

	HOLD_CANCEL_INTERRUPTS();
	PG_TRY();
	{
re_send_:
		Assert(buf->len > buf->cursor);
		send_res = send(rxact_client_fd
			, buf->data + buf->cursor
			, buf->len - buf->cursor, 0);
		CHECK_FOR_INTERRUPTS();
		if(send_res == 0)
		{
			ereport(ERROR, (errcode_for_socket_access()
				, errmsg("Send message to RXACT manager close by remote")));
		}else if(send_res < 0)
		{
			if(IS_ERR_INTR())
				goto re_send_;
			ereport(ERROR, (errcode_for_socket_access()
				, errmsg("Can not send message to RXACT manager:%m")));
		}
		buf->cursor += send_res;
		if(buf->len > buf->cursor)
		{
			wait_socket((pgsocket)rxact_client_fd, true);
			goto re_send_;
		}
	}PG_CATCH();
	{
		closesocket(rxact_client_fd);
		rxact_client_fd = PGINVALID_SOCKET;
		PG_RE_THROW();
	}PG_END_TRY();
	RESUME_CANCEL_INTERRUPTS();
	Assert(buf->cursor == buf->len);
}

static void recv_msg_from_rxact(StringInfo buf)
{
	int len;
	char msg_type;

re_recv_msg_:
	AssertArg(buf);
	Assert(rxact_client_fd != PGINVALID_SOCKET);
	resetStringInfo(buf);

	HOLD_CANCEL_INTERRUPTS();
	PG_TRY();
	{
		while(buf->len < 5)
		{
			wait_socket(rxact_client_fd, false);
			recv_socket(rxact_client_fd, buf, 5-buf->len);
		}
		len = *(int*)(buf->data);
		while(buf->len < len)
		{
			wait_socket(rxact_client_fd, false);
			recv_socket(rxact_client_fd, buf, len-buf->len);
		}

	}PG_CATCH();
	{
		closesocket(rxact_client_fd);
		rxact_client_fd = PGINVALID_SOCKET;
		PG_RE_THROW();
	}PG_END_TRY();
	RESUME_CANCEL_INTERRUPTS();

	/* parse message */
	buf->cursor += 5;	/* 5 is sizeof(length) and message type */
	msg_type = buf->data[4];
	if(msg_type == RXACT_MSG_OK)
	{
		rxact_get_msg_end(buf);
		return;
	}else if(msg_type == RXACT_MSG_NODE_INFO)
	{
		/* RXACT manager need known node(s) info */
		Oid *oids;
		Form_pgxc_node xc_node;
		HeapTuple tuple;
		int i,n;

		PG_TRY();
		{
			n = rxact_get_int(buf);
			oids = palloc(n*sizeof(Oid));
			rxact_copy_bytes(buf, oids, n*sizeof(Oid));
			rxact_get_msg_end(buf);

			rxact_reset_msg(buf, RXACT_MSG_NODE_INFO);
			rxact_put_int(buf, n);

			for(i=0;i<n;++i)
			{
				tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(oids[i]));
				if(!HeapTupleIsValid(tuple))
					ereport(ERROR, (errmsg("Node %u not exists", oids[i])));
				xc_node = (Form_pgxc_node)GETSTRUCT(tuple);
				rxact_put_int(buf, oids[i]);
				rxact_put_short(buf, (short)(xc_node->node_port));
				rxact_put_string(buf, NameStr(xc_node->node_host));
				ReleaseSysCache(tuple);
			}
		}PG_CATCH();
		{
			disconnect_rxact();
			PG_RE_THROW();
		}PG_END_TRY();
		pfree(oids);

		send_msg_to_rxact(buf);
		goto re_recv_msg_;
	}else if(msg_type == RXACT_MSG_ERROR)
	{
		ereport(ERROR, (errmsg("error message from RXACT manager:%s", rxact_get_string(buf))));
	}else
	{
		ereport(ERROR, (errmsg("Unknown message type %d from RXACT manager", msg_type)));
	}
}

static void recv_socket(pgsocket sock, StringInfo buf, int max_recv)
{
	ssize_t recv_res;
	AssertArg(sock != PGINVALID_SOCKET && buf && max_recv > 0);

	enlargeStringInfo(buf, max_recv);

re_recv_:
	recv_res = recv(sock, buf->data + buf->len, max_recv, 0);
	if(recv_res == 0)
	{
		ereport(ERROR, (errcode_for_socket_access()
			, errmsg("Recv message from RXACT manager close by remote")));
	}else if(recv_res < 0)
	{
		if(IS_ERR_INTR())
			goto re_recv_;
		ereport(ERROR, (errcode_for_socket_access()
			, errmsg("Can recv message from RXACT manager:%m")));
	}
	buf->len += recv_res;
}

static void wait_socket(pgsocket sock, bool wait_send)
{
	int ret;
#ifdef HAVE_POLL
	struct pollfd poll_fd;
	poll_fd.fd = sock;
	poll_fd.events = (wait_send ? POLLOUT : POLLIN) | POLLERR;
	poll_fd.revents = 0;
re_poll_:
	ret = poll(&poll_fd, 1, -1);
#else
	fd_set mask;
re_poll_:
	FD_ZERO(&mask);
	FD_SET(sock, &mask);
	if(wait_send)
		ret = select(sock+1, NULL, &mask, NULL, NULL);
	else
		ret = select(sock+1, &mask, NULL, NULL, NULL);
#endif

	if(ret < 0)
	{
		if(IS_ERR_INTR())
			goto re_poll_;
		ereport(WARNING, (errcode_for_socket_access()
			, errmsg("wait socket failed:%m")));
	}
}

static void connect_rxact(void)
{
	HeapTuple tuple;
	Form_pg_database form_db;
	Form_pg_authid form_authid;
	StringInfoData buf;
	Oid owner;

	if(rxact_client_fd != PGINVALID_SOCKET)
		return;

	rxact_begin_msg(&buf, RXACT_MSG_CONNECT);

	/* put Database OID and name */
	rxact_put_int(&buf, MyDatabaseId);
	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	Assert(HeapTupleIsValid(tuple));
	form_db = (Form_pg_database)GETSTRUCT(tuple);
	rxact_put_string(&buf, NameStr(form_db->datname));
	owner = form_db->datdba;
	ReleaseSysCache(tuple);

	/* put Database owner name */
	tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner));
	Assert(HeapTupleIsValid(tuple));
	form_authid = (Form_pg_authid)GETSTRUCT(tuple);
	rxact_put_string(&buf, NameStr(form_authid->rolname));
	ReleaseSysCache(tuple);

	/* connect and send message */
	rxact_client_fd = rxact_connect();
	if(rxact_client_fd == PGINVALID_SOCKET)
	{
		ereport(ERROR, (errcode_for_socket_access()
			, errmsg("Can not connect to RXACT manager:%m")));
	}
	send_msg_to_rxact(&buf);
	recv_msg_from_rxact(&buf);
	pfree(buf.data);
}

static void disconnect_rxact(void)
{
	if(rxact_client_fd != PGINVALID_SOCKET)
	{
		closesocket(rxact_client_fd);
		rxact_client_fd = PGINVALID_SOCKET;
	}
}
