#include "postgres.h"

#include <poll.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include "access/hash.h"
#include "access/rxact_mgr.h"
#include "access/xact.h"
#include "agtm/agtm_client.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "postmaster/postmaster.h"		/* For Unix_socket_directories */
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"

typedef struct RemoteNode
{
	Oid			nodeId;
	int			nodePort;
	char		nodeHost[NAMEDATALEN];
} RemoteNode;

typedef struct RemoteConnKey
{
	RemoteNode	rnode;
	char		dbname[NAMEDATALEN];
	char		user[NAMEDATALEN];
} RemoteConnKey;

typedef struct RemoteConnEnt
{
	RemoteConnKey	 key;
	PGconn			*conn;
} RemoteConnEnt;

#define DefPollTimeout		1000		/* milliseconds */

extern char	*AGtmHost;
extern int 	 AGtmPort;

static HTAB *RemoteConnHashTab = NULL;
static pgsocket server_fd = PGINVALID_SOCKET;
static RxactAgent **rxactAgents = NULL;
static volatile int agentCount = 0;

static void CreateRxactAgent(int agent_fd);
static void RxactMgrQuickdie(SIGNAL_ARGS);
static void ServerLoop(MemoryContext topCtx);
static void RemoteXactMgrInit(void);
static void DestroyRemoteConnHashTab(void);
static void on_exit_rxact_mgr(int code, Datum arg);

static void agent_destroy(RxactAgent *agent);
static bool agent_recv_data(RxactAgent *agent);
static bool agent_has_completion_msg(RxactAgent *agent, StringInfo msg, int *msg_type);
static void agent_handle_input(RxactAgent * agent);
static void agent_error_hook(void *arg);

static void
CreateRxactAgent(int agent_fd)
{
	RxactAgent *new_agent;

	Assert(agent_fd != PGINVALID_SOCKET);
	new_agent = (RxactAgent *)palloc0(sizeof(RxactAgent));
	new_agent->fdsock = agent_fd;

	rxactAgents[agentCount++] = new_agent;
}

static void
RxactMgrQuickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}

static void
ServerLoop(MemoryContext topCtx)
{
	sigjmp_buf			local_sigjmp_buf;
	MemoryContext		loopContext;
	RxactAgent 			*agent;
	struct pollfd		*pollfds, *tmpfd;
	nfds_t				npollfds;
	int					timeout;
	int					i, count;
	int					agent_fd;

	server_fd = rxact_listen();
	if (server_fd == PGINVALID_SOCKET)
	{
		ereport(PANIC, (errcode_for_socket_access(),
			errmsg("Can not listen unix socket on %s", rxact_get_sock_path())));
	}

	npollfds = (nfds_t)MaxConnections + 10;
	pollfds = palloc0(npollfds * sizeof(struct pollfd));
	pollfds[0].fd = server_fd;
	pollfds[0].events = POLLIN;
	for (i = 1; i < npollfds; i++)
	{
		pollfds[i].fd = PGINVALID_SOCKET;
		pollfds[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
	}
	on_proc_exit(on_exit_rxact_mgr, (Datum) 0);

	loopContext = AllocSetContextCreate(CurrentMemoryContext,
										"Remote Xact Server",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Cleanup something */
	}
	PG_exception_stack = &local_sigjmp_buf;
	(void)MemoryContextSwitchTo(loopContext);

	for (; ;)
	{
		int		pollres;

		MemoryContextResetAndDeleteChildren(loopContext);

		if (!PostmasterIsAlive())
			exit(0);

		for (i = 0; i < agentCount; i++)
			pollfds[i+1].fd = SOCKET(rxactAgents[i]);

		timeout = DefPollTimeout;
		pollres = poll(pollfds, npollfds, timeout);

		if (pollres < 0)
		{
			if(errno == EINTR
#if defined(EAGAIN) && EAGAIN != EINTR
				|| errno == EAGAIN
#endif
			)
			{
				continue;
			}
			ereport(PANIC, (errcode_for_socket_access(),
				errmsg("pool failed(%d) in pooler process, error %m", pollres)));
		} else if (pollres == 0)
		{
			/* timeout */
			continue;
		}

		count = 0;
		/* Get a new connection */
		if (pollfds[0].revents & POLLIN)
		{
			agent_fd = rxact_connect();
			if (agent_fd == PGINVALID_SOCKET)
			{
				ereport(WARNING, (errcode_for_socket_access(),
					errmsg("pool manager accept failed:%m")));
			} else
			{
				CreateRxactAgent(agent_fd);
			}
			count++;
		}

		/* Agents can be read */
		for (i = agentCount; i > 0 && count < pollres;)
		{
			tmpfd = &(pollfds[i]);
			i--;
			agent = rxactAgents[i];
			if (tmpfd->fd == SOCKET(agent) &&
				tmpfd->revents != 0)
			{
				count++;
				agent_handle_input(agent);
			}
		}
	}
}

static void
RemoteXactMgrInit(void)
{
	long nelem;

	nelem = (long)(MaxConnections * (NumCoords + NumDataNodes));

	/* initialize hash table of remote connections */
	if (RemoteConnHashTab == NULL)
	{
		HASHCTL hctl;

		MemSet(&hctl, 0, sizeof(hctl));
		hctl.keysize = sizeof(RemoteConnKey);
		hctl.entrysize = sizeof(RemoteConnEnt);
		hctl.hash = tag_hash;
		hctl.hcxt = CurrentMemoryContext;

		RemoteConnHashTab = hash_create("RemoteConnHashTab",
										nelem,
										&hctl,
										HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	rxactAgents = (RxactAgent **)palloc0(MaxConnections * sizeof(RxactAgent *));
	agentCount = 0;
}

static void
DestroyRemoteConnHashTab(void)
{
	if (RemoteConnHashTab)
	{
		HASH_SEQ_STATUS	 status;
		RemoteConnEnt	*item = NULL;

		hash_seq_init(&status, RemoteConnHashTab);
		while ((item = (RemoteConnEnt *) hash_seq_search(&status)) != NULL)
		{
			PQfinish(item->conn);
		}

		hash_destroy(RemoteConnHashTab);
	}

	RemoteConnHashTab = NULL;
}

int
RemoteXactMgrMain(void)
{
	MemoryContext	rxact_mgr_context;

	rxact_mgr_context = AllocSetContextCreate(TopMemoryContext,
											  "Remote Xact Manager",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

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

	(void)MemoryContextSwitchTo(rxact_mgr_context);

	/* Initinalize something */
	RemoteXactMgrInit();

	/* Server loop */
	ServerLoop(rxact_mgr_context);

	proc_exit(1);
}

static void
on_exit_rxact_mgr(int code, Datum arg)
{
	closesocket(server_fd);
	DestroyRemoteConnHashTab();
}

/* --------------- function for server to deal with agent ----------------- */
#define RM_MSG_CONNECT				'C'
#define RM_MSG_DISCONNECT			'D'
#define RM_MSG_ERROR				'E'
#define RM_MSG_RXACT				'R'

/*
 * Destroy RxactAgent
 */
static void
agent_destroy(RxactAgent *agent)
{
	Size	i;

	AssertArg(agent);

	if(SOCKET(agent) != PGINVALID_SOCKET)
		closesocket(SOCKET(agent));

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (rxactAgents[i] == agent)
		{
			memcpy(&(rxactAgents[i]), &(rxactAgents[i+1]), (MaxConnections - i -1)*sizeof(agent));
			break;
		}
	}
	Assert(i<agentCount);
	--agentCount;
}

/* true for recv some data, false for closed by remote */
static bool
agent_recv_data(RxactAgent *agent)
{
	int rval;

	AssertArg(agent);

	if (agent->RecvPointer > 0)
	{
		if (agent->RecvLength > agent->RecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(agent->RecvBuffer, agent->RecvBuffer + agent->RecvPointer,
					agent->RecvLength - agent->RecvPointer);
			agent->RecvLength -= agent->RecvPointer;
			agent->RecvPointer = 0;
		}
		else
			agent->RecvLength = agent->RecvPointer = 0;
	}

	if(agent->RecvLength >= RXACT_BUFFER_SIZE)
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("too many data from backend for remote xact manager")));
		return false;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		rval = recv(SOCKET(agent), agent->RecvBuffer + agent->RecvLength,
				 RXACT_BUFFER_SIZE - agent->RecvLength, 0);
		if (rval < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if (errno == EINTR
#if defined(EAGAIN) && EINTR != EAGAIN
				|| errno == EAGAIN
#endif
			)
				continue;		/* Ok if interrupted */

			/*
			 * Report broken connection
			 */
			ereport(WARNING,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
			return false;
		}else if (rval == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return false;
		}
		/* rval contains number of bytes read, so just incr length */
		agent->RecvLength += rval;
		break;
	}
	return true;
}

/* get message if has completion message */
static bool 
agent_has_completion_msg(RxactAgent *agent, StringInfo msg, int *msg_type)
{
	Size unread_len;
	int len;
	AssertArg(agent && msg);

	Assert(agent->RecvLength >= 0 && agent->RecvPointer >= 0
		&& agent->RecvLength >= agent->RecvPointer);

	unread_len = agent->RecvLength - agent->RecvPointer;
	/* 5 is message type (char) and message length(int) */
	if(unread_len < 5)
		return false;

	/* get message length */
	memcpy(&len, agent->RecvBuffer + agent->RecvPointer + 1, 4);
	len = htonl(len);
	if((len+1) > unread_len)
		return false;

	*msg_type = agent->RecvBuffer[agent->RecvPointer];

	/* okay, copy message */
	len++;	/* add char length */
	resetStringInfo(msg);
	enlargeStringInfo(msg, len);
	Assert(msg->data);
	memcpy(msg->data, agent->RecvBuffer + agent->RecvPointer, len);
	agent->RecvPointer += len;
	msg->len = len;
	msg->cursor = 5; /* skip message type and length */
	return true;
}

static void 
agent_handle_input(RxactAgent *agent)
{
	ErrorContextCallback	err_calback;
	StringInfoData			s;
	int						qtype;

	/* try recv data */
	if(agent_recv_data(agent) == false)
	{
		/* closed by remote */
		agent_destroy(agent);
		return;
	}

	/* setup error callback */
	err_calback.arg = NULL;
	err_calback.callback = agent_error_hook;
	err_calback.previous = error_context_stack;
	error_context_stack = &err_calback;

	initStringInfo(&s);
	while(agent_has_completion_msg(agent, &s, &qtype))
	{
		/* set need report error if have */
		err_calback.arg = agent;

		switch(qtype)
		{
			/* TODO */
		}
		pq_getmsgend(&s);
	}
end_agent_input_:
	error_context_stack = err_calback.previous;
}

static void agent_error_hook(void *arg)
{
	const ErrorData *err;
	if(arg && (err = err_current_data()) != NULL
		&& err->elevel >= ERROR)
	{
		rxact_putmessage(arg, RM_MSG_ERROR, NULL, 0);
		rxact_flush(arg);
	}
}

/* --------- function for caller communicate with remote xact manager ------- */
static RxactHandle *rxactHandle = NULL;

RxactHandle *
GetRxactManagerHandle(void)
{
	RxactHandle *handle;
	int			fdsock;

	/* Connect to the pooler */
	fdsock = rxact_connect();
	if (fdsock < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to connect to remote xact manager: %m")));
	}

	/* Allocate handle */
	PG_TRY();
	{
		handle = MemoryContextAllocZero(TopMemoryContext, sizeof(*handle));

		handle->fdsock = fdsock;
	}PG_CATCH();
	{
		closesocket(fdsock);
		PG_RE_THROW();
	}PG_END_TRY();

	return handle;
}

/*
 * Close handle
 */
void
RxactManagerCloseHandle(RxactHandle *handle)
{
	closesocket(SOCKET(handle));
	pfree(handle);
}

/* ----------- interface for remote xact message -------- */
int
RecordRemoteXact(TransactionId xid,		/* xid for a key */
				 const char *gid,		/* gid for a key */
				 uint8 info,			/* info for rxact kind */
				 StringInfo rbinary)	/* binary string of rxact */
{
	/* TODO: */
	return 0;
}

int
RecordRemoteXactSuccess(TransactionId xid,	/* xid for a key */
						const char *gid,	/* gid for a key */
						uint8 info)			/* info for rxact kind */
{
	/* TODO: */
	return 0;
}

int
RecordRemoteXactCancel(TransactionId xid,	/* xid for a key */
					   const char *gid,		/* gid for a key */
					   uint8 info)			/* info for rxact kind */
{
	/* TODO: */
	return 0;
}

