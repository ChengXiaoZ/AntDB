#include "postgres.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/rxact_comm.h"
#include "access/rxact_mgr.h"
#include "access/rxact_msg.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "catalog/pgxc_node.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include <unistd.h>
#include <sys/stat.h>
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

#define REMOTE_IDLE_TIMEOUT			60	/* close remote connect if it idle in second */

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
	PostgresPollingStatusType
			status;
	char doing_gid[NAMEDATALEN];
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
extern bool enableFsync;

static HTAB *htab_remote_node = NULL;	/* RemoteNode */
static HTAB *htab_db_node = NULL;		/* DatabaseNode */
static HTAB *htab_node_conn = NULL;		/* NodeConn */
static HTAB *htab_rxid = NULL;			/* GlobalTransactionInfo */

/* remote xact log files */
static File rxlf_remote_node = -1;
static File rxlf_db_node = -1;
static const char rxlf_remote_node_filename[] = {"remote_node"};
static const char rxlf_db_node_filename[] = {"db_node"};
static const char rxlf_xact_filename[] = {"rxact"};
static const char rxlf_directory[] = {"pg_rxlog"};
static StringInfoData rxlf_xlog_buf = {NULL, 0, 0, 0};
#define MAX_RLOG_FILE_NAME 24

static pgsocket rxact_server_fd = PGINVALID_SOCKET;
static RxactAgent *allRxactAgent = NULL;
static Index *indexRxactAgent = NULL;
static int MaxRxactAgent;
static volatile unsigned int agentCount = 0;
/*static volatile bool rxact_has_filed_gid = false;*/

static volatile pgsocket rxact_client_fd = PGINVALID_SOCKET;

/*
 * Flag to mark SIGHUP. Whenever the main loop comes around it
 * will reread the configuration file. (Better than doing the
 * reading in the signal handler, ey?)
 */
static volatile sig_atomic_t got_SIGHUP = false;
static void RxactHupHandler(SIGNAL_ARGS);

static void CreateRxactAgent(int agent_fd);
static void RxactMgrQuickdie(SIGNAL_ARGS);
static void RxactLoop(void);
static void RemoteXactBaseInit(void);
static void RemoteXactMgrInit(void);
static void RemoteXactHtabInit(void);
static void DestroyRemoteConnHashTab(void);
static void RxactLoadLog(void);
static void RxactSaveLog(bool flush);
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
static void rxact_agent_change(RxactAgent *agent, StringInfo msg);
static void rxact_agent_checkpoint(RxactAgent *agent, StringInfo msg);
static void rxact_agent_node_info(RxactAgent *agent, StringInfo msg, bool is_update);
/* if any oid unknown, get it from backend */
static bool query_remote_oid(RxactAgent *agent, Oid *oid, int count);

/* htab functions */
static uint32 hash_DbAndNodeOid(const void *key, Size keysize);
static int match_DbAndNodeOid(const void *key1, const void *key2,
											Size keysize);
static int match_oid(const void *key1, const void *key2, Size keysize);
static void rxact_insert_database(Oid db_oid, const char *dbname, const char *owner, bool is_redo);
static void
rxact_insert_gid(const char *gid, const Oid *oids, int count, RemoteXactType type, Oid db_oid, bool is_redo);
static void rxact_mark_gid(const char *gid, RemoteXactType type, bool success, bool is_redo);
static void rxact_change_gid(const char *gid, RemoteXactType type, bool is_redo);
static void rxact_insert_node_info(Oid oid, short port, const char *addr, bool is_redo);

/* 2pc redo functions */
static void rxact_2pc_do(void);
static void rxact_2pc_result(NodeConn *conn);
static NodeConn* rxact_get_node_conn(Oid db_oid, Oid node_oid, time_t cur_time);
static bool rxact_check_node_conn(NodeConn *conn);
static void rxact_finish_node_conn(NodeConn *conn);
static void rxact_build_2pc_cmd(StringInfo cmd, const char *gid, RemoteXactType type);
static void rxact_close_timeout_remote_conn(time_t cur_time);
static File rxact_log_open_file(const char *log_name, int fileFlags, int fileMode);
static void rxact_xlog_insert(char *data, uint32 len, uint8 info, bool flush);

/* interface for client */
static void record_rxact_status(const char *gid, RemoteXactType type, bool success);
static void send_msg_to_rxact(StringInfo buf);
static void recv_msg_from_rxact(StringInfo buf);
static bool wait_socket(pgsocket sock, bool wait_send, bool block);
static void recv_socket(pgsocket sock, StringInfo buf, int max_recv);
static void connect_rxact(void);

static void
CreateRxactAgent(pgsocket agent_fd)
{
	RxactAgent *agent;
	unsigned int i;
	AssertArg(agent_fd != PGINVALID_SOCKET);

	if(agentCount >= MaxRxactAgent)
	{
		closesocket(agent_fd);
		ereport(WARNING, (errmsg("too many connect for RXACT")));
	}

	agent = NULL;
	for(i=0;i<MaxRxactAgent;++i)
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
	pg_set_noblock(agent_fd);
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
	NodeConn			*pconn;
	HASH_SEQ_STATUS		seq_status;
	unsigned int		i, count;
	Index 				index;
	pgsocket			agent_fd;
	int					poll_count;
	int					max_pool;

	Assert(rxact_server_fd != PGINVALID_SOCKET);
	if(pg_set_noblock(rxact_server_fd) == false)
		ereport(FATAL, (errmsg("Can not set RXACT listen socket to noblock:%m")));

	MemoryContextSwitchTo(TopMemoryContext);

	max_pool = MaxRxactAgent+1;
	pollfds = palloc(sizeof(pollfds[0]) * max_pool);
	pollfds[0].fd = rxact_server_fd;
	pollfds[0].events = POLLIN;
	initStringInfo(&message);

	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Cleanup something */
		EmitErrorReport();
		FlushErrorState();
		error_context_stack = NULL;
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

		for (i = agentCount; i--;)
		{
			index = indexRxactAgent[i];
			Assert(index >= 0 && index < (Index)MaxRxactAgent);
			agent = &allRxactAgent[index];
			Assert(agent->index == index && agent->sock != PGINVALID_SOCKET);

			pollfds[i+1].fd = agent->sock;
			if(agent->out_buf.len > agent->out_buf.cursor)
				pollfds[i+1].events = POLLOUT;
			else
				pollfds[i+1].events = POLLIN;
		}

		/* append node sockets */
		hash_seq_init(&seq_status, htab_node_conn);
		poll_count = agentCount+1;
		while((pconn = hash_seq_search(&seq_status)) != NULL)
		{
			bool wait_write;
			if(pconn->conn == NULL)
				continue;
			if(PQstatus(pconn->conn) == CONNECTION_BAD)
			{
				rxact_finish_node_conn(pconn);
				continue;
			}

			switch(pconn->status)
			{
			case PGRES_POLLING_ACTIVE:
			case PGRES_POLLING_FAILED:
				rxact_finish_node_conn(pconn);
				continue;
			case PGRES_POLLING_OK:
				if(pconn->doing_gid[0] == '\0')
					continue;
				wait_write = false;
				break;
			case PGRES_POLLING_WRITING:
				wait_write = true;
				break;
			case PGRES_POLLING_READING:
				wait_write = false;
				break;
			default:
				Assert(0);
			}

			if(poll_count >= max_pool)
			{
				START_CRIT_SECTION();
				max_pool += 16;
				pollfds = repalloc(pollfds, max_pool*sizeof(pollfds[0]));
				END_CRIT_SECTION();
			}
			pollfds[poll_count].fd = PQsocket(pconn->conn);
			Assert(pollfds[poll_count].fd != PGINVALID_SOCKET);
			pollfds[poll_count].events = wait_write ? POLLOUT:POLLIN;
			pollfds[poll_count].revents = 0;
			++poll_count;
		}

re_poll_:
		if(got_SIGHUP)
		{
			DbAndNodeOid key;

			key.db_oid = InvalidOid;
			key.node_oid = AGTM_OID;
			pconn = hash_search(htab_node_conn, &key, HASH_FIND, NULL);
			Assert(pconn != NULL);
			ProcessConfigFile(PGC_SIGHUP);
			got_SIGHUP = false;
			if(pconn->conn != NULL
				&& (strcmp(PQhost(pconn->conn), AGtmHost) != 0
					|| atoi(PQport(pconn->conn)) != AGtmPort))
			{
				rxact_finish_node_conn(pconn);
				continue;
			}
		}
		/* for we wait 1 second */
		pollres = poll(pollfds, poll_count, 1000);
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
		for(i=agentCount+1;i < poll_count && count < (unsigned int)pollres;++i)
		{
			tmpfd = &pollfds[i];
			if(tmpfd->revents == 0)
				continue;

			hash_seq_init(&seq_status, htab_node_conn);
			while((pconn = hash_seq_search(&seq_status)) != NULL)
			{
				if(tmpfd->fd != PQsocket(pconn->conn))
					continue;

				++count;
				hash_seq_term(&seq_status);
				break;
			}
			Assert(pconn != NULL);

			if(pconn->status != PGRES_POLLING_OK)
			{
				pconn->status = PQconnectPoll(pconn->conn);
				if(pconn->status == PGRES_POLLING_FAILED)
					rxact_finish_node_conn(pconn);
			}else
			{
				Assert(pconn->doing_gid[0] != '\0');
				rxact_2pc_result(pconn);
			}
		}

		for(i=agentCount;i && count < (unsigned int)pollres;)
		{
			tmpfd = &pollfds[i];
			--i;
			if(tmpfd->revents == 0)
				continue;

			index = indexRxactAgent[i];
			Assert(index >= 0 && index < (Index)MaxRxactAgent);
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

		rxact_2pc_do();

		cur_time = time(NULL);
		if(last_time != cur_time)
		{
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
	pqsignal(SIGHUP, RxactHupHandler);
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
	unsigned int i;

	MaxRxactAgent = MaxBackends * 2;
	START_CRIT_SECTION();

	/* init listen socket */
	Assert(rxact_server_fd == PGINVALID_SOCKET);
	rxact_server_fd = rxact_listen();
	if(rxact_server_fd == PGINVALID_SOCKET)
	{
		ereport(FATAL,
			(errmsg("Remote xact can not create listen socket on \"%s\":%m", rxact_get_sock_path())));
	}

	Assert(agentCount == 0);
	allRxactAgent = palloc(sizeof(allRxactAgent[0]) * MaxRxactAgent);
	for(i=0;i<MaxRxactAgent;++i)
	{
		allRxactAgent[i].index = INVALID_INDEX;
		initStringInfo(&(allRxactAgent[i].in_buf));
		initStringInfo(&(allRxactAgent[i].out_buf));
	}
	indexRxactAgent = palloc(sizeof(indexRxactAgent[0]) * MaxRxactAgent);

	initStringInfo(&rxlf_xlog_buf);

	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	END_CRIT_SECTION();
	on_proc_exit(on_exit_rxact_mgr, (Datum)0);
}

static void RemoteXactHtabInit(void)
{
	HASHCTL hctl;
	DbAndNodeOid key;
	NodeConn *pconn;
	START_CRIT_SECTION();
	/* create HTAB for RemoteNode */
	Assert(htab_remote_node == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(RemoteNode);
	hctl.hash = oid_hash;
	hctl.match = match_oid;
	hctl.hcxt = TopMemoryContext;
	htab_remote_node = hash_create("RemoteNode"
		, 64, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* create HTAB for DatabaseNode */
	Assert(htab_db_node == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(DatabaseNode);
	hctl.hash = oid_hash;
	hctl.hcxt = TopMemoryContext;
	htab_db_node = hash_create("DatabaseNode"
		, 64, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

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
	/* insert AGTM node */
	key.db_oid = InvalidOid;
	key.node_oid = AGTM_OID;
	pconn =  hash_search(htab_node_conn, &key, HASH_ENTER, NULL);
	Assert(pconn != NULL);
	pconn->conn = NULL;
	pconn->status = PGRES_POLLING_FAILED;
	pconn->doing_gid[0] = '\0';

	/* create HTAB for GlobalTransactionInfo */
	Assert(htab_rxid == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(((GlobalTransactionInfo*)0)->gid);
	hctl.entrysize = sizeof(GlobalTransactionInfo);
	hctl.hcxt = TopMemoryContext;
	htab_rxid = hash_create("DatabaseNode"
		, 512
		, &hctl, HASH_ELEM | HASH_CONTEXT);
	END_CRIT_SECTION();
}

static void
DestroyRemoteConnHashTab(void)
{
	GlobalTransactionInfo *ginfo;
	HASH_SEQ_STATUS hash_status;

	hash_seq_init(&hash_status, htab_rxid);
	while((ginfo=hash_seq_search(&hash_status))!=NULL)
	{
		if(ginfo->remote_nodes)
			pfree(ginfo->remote_nodes);
	}
	hash_destroy(htab_rxid);
	htab_rxid = NULL;
	hash_destroy(htab_db_node);
	htab_db_node = NULL;
	hash_destroy(htab_remote_node);
	htab_remote_node = NULL;
}

void
RemoteXactMgrMain(void)
{
	PG_exception_stack = NULL;

	RemoteXactBaseInit();

	/* Initinalize something */
	RemoteXactHtabInit();
	RemoteXactMgrInit();
	(void)MemoryContextSwitchTo(MessageContext);

	RxactLoadLog();

	enableFsync = true; /* force enable it */

	/* Server loop */
	RxactLoop();

	proc_exit(1);
}

static void RxactLoadLog(void)
{
	RXactLog rlog;
	File rfile;
	int res;

	/* mkdir rxlog directory if not exists */
	res = mkdir(rxlf_directory, S_IRWXU);
	if(res < 0)
	{
		if(errno != EEXIST)
		{
			ereport(FATAL,
				(errcode_for_file_access(),
				errmsg("could not create directory \"%s\":%m", rxlf_directory)));
		}
	}

	/* open database node file */
	Assert(rxlf_db_node== -1);
	rxlf_db_node = rxact_log_open_file(rxlf_db_node_filename, O_RDWR | O_CREAT | PG_BINARY, 0600);
	{
		/* read saved database node */
		DatabaseNode db_node;
		rlog = rxact_begin_read_log(rxlf_db_node);
		for(;;)
		{
			rxact_log_reset(rlog);
			if(rxact_log_is_eof(rlog))
				break;

			rxact_log_read_bytes(rlog, &db_node, sizeof(db_node));

			if(db_node.dbOid == InvalidOid)
			{
				/* deleted */
				continue;
			}
			rxact_insert_database(db_node.dbOid,
				db_node.dbname, db_node.owner, true);
		}
		rxact_end_read_log(rlog);
	}

	/* open remote node */
	Assert(rxlf_remote_node == -1);
	rxlf_remote_node = rxact_log_open_file(rxlf_remote_node_filename, O_RDWR | O_CREAT | PG_BINARY, 0600);
	{
		/* read remote node */
		RemoteNode rnode;
		rlog = rxact_begin_read_log(rxlf_remote_node);
		for(;;)
		{
			rxact_log_reset(rlog);
			if(rxact_log_is_eof(rlog))
				break;

			rxact_log_read_bytes(rlog, &rnode, sizeof(rnode));

			if(rnode.nodeOid == InvalidOid)
			{
				/* deleted */
				continue;
			}
			rxact_insert_node_info(rnode.nodeOid, rnode.nodePort
				, rnode.nodeHost, true);
		}
		rxact_end_read_log(rlog);
	}

	/* load xact */
	rfile = rxact_log_open_file(rxlf_xact_filename, O_RDONLY|O_CREAT|PG_BINARY, 0600);
	rlog = rxact_begin_read_log(rfile);
	for(;;)
	{
		const char *gid;
		Oid *oids;
		Oid db_oid;
		int count;
		char c;
		rxact_log_reset(rlog);
		if(rxact_log_is_eof(rlog))
			break;

		gid = rxact_log_get_string(rlog);
		rxact_log_read_bytes(rlog, (char*)&db_oid, sizeof(db_oid));
		rxact_log_read_bytes(rlog, (char*)&count, sizeof(count));
		if(count > 0)
			oids = rxact_log_get_bytes(rlog, count*sizeof(oids[0]));
		else
			oids = NULL;
		rxact_log_read_bytes(rlog, &c, 1);
		rxact_insert_gid(gid, oids, count, (RemoteXactType)c, db_oid, true);
	}
	rxact_end_read_log(rlog);
	FileClose(rfile);
}

static void RxactSaveLog(bool flush)
{
	GlobalTransactionInfo *ginfo;
	void *p;
	RXactLog rlog;
	HASH_SEQ_STATUS hash_status;
	File rfile;
	off_t cursor;

	/* save remote node */
	Assert(rxlf_remote_node != -1);
	hash_seq_init(&hash_status, htab_remote_node);
	cursor = FileSeek(rxlf_remote_node, 0, SEEK_SET);
	if(cursor != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("Can not seek file \"%s\" to start", FilePathName(rxlf_remote_node))));
	}
	while((p=hash_seq_search(&hash_status))!=NULL)
		rxact_log_simple_write(rxlf_remote_node, p, sizeof(RemoteNode));
	cursor = FileSeek(rxlf_remote_node, 0, SEEK_CUR);
	FileTruncate(rxlf_remote_node, cursor);

	/* save database node file*/
	Assert(rxlf_db_node != -1);
	hash_seq_init(&hash_status, htab_db_node);
	cursor = FileSeek(rxlf_db_node, 0, SEEK_SET);
	if(cursor != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("Can not seek file \"%s\" to start", FilePathName(rxlf_db_node))));
	}
	while((p=hash_seq_search(&hash_status))!=NULL)
		rxact_log_simple_write(rxlf_db_node, p, sizeof(DatabaseNode));
	cursor = FileSeek(rxlf_db_node, 0, SEEK_CUR);
	FileTruncate(rxlf_db_node, cursor);

	/* save xact */
	hash_seq_init(&hash_status, htab_rxid);
	rfile = rxact_log_open_file(rxlf_xact_filename, O_WRONLY | O_TRUNC | PG_BINARY, 0);
	rlog = rxact_begin_write_log(rfile);
	while((ginfo = hash_seq_search(&hash_status)) != NULL)
	{
		rxact_log_write_string(rlog, ginfo->gid);
		rxact_log_write_bytes(rlog, &(ginfo->db_oid), sizeof(ginfo->db_oid));
		/* don't need save AGTM OID */
		rxact_log_write_int(rlog, ginfo->count_nodes-1);
		Assert(ginfo->remote_nodes[ginfo->count_nodes-1] == AGTM_OID);
		rxact_log_write_bytes(rlog, ginfo->remote_nodes
			, sizeof(ginfo->remote_nodes[0]) * (ginfo->count_nodes-1));
		rxact_log_write_byte(rlog, (char)(ginfo->type));
		rxact_write_log(rlog);
	}
	rxact_end_write_log(rlog);
	if(flush)
	{
		bool save_fsync = enableFsync;
		enableFsync = true;
		FileSync(rxlf_remote_node);
		FileSync(rxlf_db_node);
		FileSync(rfile);
		enableFsync = save_fsync;
	}
	FileClose(rfile);
}

static void
on_exit_rxact_mgr(int code, Datum arg)
{
	closesocket(rxact_server_fd);
	rxact_server_fd = PGINVALID_SOCKET;
	RxactSaveLog(true);
	DestroyRemoteConnHashTab();
	if(rxlf_db_node != -1)
	{
		FileClose(rxlf_db_node);
		rxlf_db_node = -1;
	}
	if(rxlf_remote_node != -1)
	{
		FileClose(rxlf_remote_node);
		rxlf_remote_node = -1;
	}
}

/*
 * Destroy RxactAgent
 */
static void
rxact_agent_destroy(RxactAgent *agent)
{
	unsigned int i;
	AssertArg(agent && agent->index != INVALID_INDEX);
	for(i=0;i<agentCount;++i)
	{
		if(indexRxactAgent[i] == agent->index)
			break;
	}

	Assert(i<agentCount);
	--agentCount;
	for(;i<agentCount;++i)
		indexRxactAgent[i] = indexRxactAgent[i+1];
	closesocket(agent->sock);
	agent->sock = PGINVALID_SOCKET;
	agent->dboid = InvalidOid;
	agent->index = INVALID_INDEX;

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
			rxact_mark_gid(ginfo->gid, ginfo->type, false, false);
		}
		agent->last_gid[0] = '\0';
	}
}

static void rxact_agent_end_msg(RxactAgent *agent, StringInfo msg)
{
	AssertArg(agent && msg);
	if(agent->sock != PGINVALID_SOCKET)
	{
		bool need_try;
		if(agent->out_buf.len > agent->out_buf.cursor)
		{
			need_try = false;
		}else
		{
			need_try = true;
			resetStringInfo(&(agent->out_buf));
		}
		rxact_put_finsh(msg);
		appendBinaryStringInfo(&(agent->out_buf), msg->data, msg->len);
		if(need_try)
			rxact_agent_output(agent);
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
	bool need_try;
	AssertArg(agent);

	if(agent->sock == PGINVALID_SOCKET)
		return;
	if(agent->out_buf.len > agent->out_buf.cursor)
	{
		need_try = false;
	}else
	{
		need_try = true;
		resetStringInfo(&(agent->out_buf));
	}
	enlargeStringInfo(&(agent->out_buf), 5);
	msg.len = 5;
	msg.str[4] = msg_type;
	appendBinaryStringInfo(&(agent->out_buf), msg.str, 5);
	if(need_try)
		rxact_agent_output(agent);
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
agent_has_completion_msg(RxactAgent *agent, StringInfo msg, uint8 *msg_type)
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
	uint8					qtype;

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
		agent->in_error = false;
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
		case RXACT_MSG_CHANGE:
			rxact_agent_change(agent, &s);
			break;
		case RXACT_MSG_CHECKPOINT:
			rxact_agent_checkpoint(agent, &s);
			break;
		case RXACT_MSG_NODE_INFO:
			rxact_agent_node_info(agent, &s, false);
			break;
		case RXACT_MSG_UPDATE_NODE:
			rxact_agent_node_info(agent, &s, true);
			break;
		default:
			PG_TRY();
			{
				ereport(ERROR, (errmsg("unknown message type %d", qtype)));
			}PG_CATCH();
			{
				rxact_agent_destroy(agent);
				PG_RE_THROW();
			}PG_END_TRY();
		}
		PG_TRY();
		{
			rxact_get_msg_end(&s);
		}PG_CATCH();
		{
			rxact_agent_destroy(agent);
			PG_RE_THROW();
		}PG_END_TRY();
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
		agent->in_error = true;
		rxact_begin_msg(&msg, RXACT_MSG_ERROR);
		rxact_put_string(&msg, err->message ? err->message : "miss error message");
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
		, agent->out_buf.len - agent->out_buf.cursor
#ifdef MSG_DONTWAIT
		, MSG_DONTWAIT);
#else
		, 0);
#endif
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
		if(errno != EAGAIN
#if defined(EWOULDBLOCK) && EWOULDBLOCK != EAGAIN
			&& errno != EWOULDBLOCK
#endif
			)
		{
			ereport(WARNING, (errcode_for_socket_access()
				, errmsg("Can not send message to RXACT client:%m")));
			rxact_agent_destroy(agent);
		}
	}
}

static void rxact_agent_connect(RxactAgent *agent, StringInfo msg)
{
	char *owner;
	char *dbname;
	AssertArg(agent && msg);

	agent->dboid = (Oid)rxact_get_int(msg);
	if(OidIsValid(agent->dboid))
	{
		dbname = rxact_get_string(msg);
		owner = rxact_get_string(msg);
		rxact_insert_database(agent->dboid, dbname, owner, false);
	}
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
	if(count < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("insufficient data left in message")));
	}
	if(count > 0)
		oids = rxact_get_bytes(msg, sizeof(Oid)*count);
	else
		oids = NULL;
	gid = rxact_get_string(msg);

	rxact_insert_gid(gid, oids, count, type, agent->dboid, false);
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
	rxact_mark_gid(gid, type, success, false);
	agent->last_gid[0] = '\0';
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_change(RxactAgent *agent, StringInfo msg)
{
	const char *gid;
	RemoteXactType type;
	AssertArg(agent && msg);

	type = (RemoteXactType)rxact_get_int(msg);
	if(type != RX_COMMIT && type != RX_ROLLBACK)
	{
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("invalid rxact type '%d'", (int)type)));
	}
	gid = rxact_get_string(msg);
	rxact_change_gid(gid, type, false);
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_checkpoint(RxactAgent *agent, StringInfo msg)
{
	int flags = rxact_get_int(msg);
	RxactSaveLog(flags & CHECKPOINT_IMMEDIATE ? false:true);
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_node_info(RxactAgent *agent, StringInfo msg, bool is_update)
{
	const char *address;
	List *oid_list;
	int i,count;
	Oid oid;
	short port;
	AssertArg(agent && msg);

	if(is_update)
	{
		RemoteNode *rnode;
		HASH_SEQ_STATUS seq_status;
		/* delete old info */
		hash_seq_init(&seq_status, htab_remote_node);
		while((rnode = hash_seq_search(&seq_status)) != NULL)
		{
			hash_search(htab_remote_node, &(rnode->nodeOid), HASH_REMOVE, NULL);
		}

		Assert(rxlf_remote_node != -1);
		FileTruncate(rxlf_remote_node, 0);
		oid_list = NIL;
	}

	count = rxact_get_int(msg);
	for(i=0;i<count;++i)
	{
		oid = (Oid)rxact_get_int(msg);
		port = rxact_get_short(msg);
		address = rxact_get_string(msg);
		rxact_insert_node_info(oid, port, address, false);
		if(is_update)
			oid_list = lappend_oid(oid_list, oid);
	}
	FileSync(rxlf_remote_node);
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);

	if(is_update)
	{
		/* disconnect remote */
		NodeConn *pconn;
		ListCell *lc;
		HASH_SEQ_STATUS seq_status;
		hash_seq_init(&seq_status, htab_node_conn);
		while((pconn = hash_seq_search(&seq_status)) != NULL)
		{
			foreach(lc, oid_list)
			{
				if(pconn->oids.node_oid == lfirst_oid(lc))
				{
					rxact_finish_node_conn(pconn);
					break;
				}
			}
		}
		list_free(oid_list);
	}
}

/* if any oid unknown, get it from backend */
static bool query_remote_oid(RxactAgent *agent, Oid *oid, int count)
{
	StringInfoData buf;
	int i;
	int unknown_offset;
	int unknown_count;
	if(count == 0)
		return false;

	AssertArg(agent && oid && count > 0);

	for(i=0;i<count;++i)
	{
		if(hash_search(htab_remote_node, &oid[i], HASH_FIND, NULL) == NULL)
			break;
	}
	if(i>=count)
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

static int match_oid(const void *key1, const void *key2, Size keysize)
{
	Oid l,r;
	AssertArg(keysize == sizeof(Oid));

	l = *(Oid*)key1;
	r = *(Oid*)key2;
	if(l<r)
		return -1;
	else if(l > r)
		return 1;
	return 0;
}

static void rxact_insert_database(Oid db_oid, const char *dbname, const char *owner, bool is_redo)
{
	DatabaseNode *db;
	bool found;
	db = hash_search(htab_db_node, &db_oid, HASH_ENTER, &found);
	if(found)
	{
		Assert(db->dbOid == db_oid);
		return;
	}else
	{
		MemSet(db->dbname, 0, sizeof(db->dbname));
		MemSet(db->owner, 0, sizeof(db->owner));
		strcpy(db->dbname, dbname);
		strcpy(db->owner, owner);

		/* insert into log file */
		if(!is_redo)
		{
			Assert(rxlf_db_node != -1);
			rxact_log_simple_write(rxlf_db_node, db, sizeof(*db));
			FileSync(rxlf_db_node);
		}
	}
}

static void
rxact_insert_gid(const char *gid, const Oid *oids, int count, RemoteXactType type, Oid db_oid, bool is_redo)
{
	GlobalTransactionInfo *ginfo;
	bool found;
	AssertArg(gid && gid[0] && count >= 0);
	if(!RXACT_TYPE_IS_VALID(type))
		ereport(ERROR, (errmsg("Unknown remote xact type %d", type)));

	ginfo = hash_search(htab_rxid, gid, HASH_ENTER, &found);
	if(found)
	{
		if(is_redo)
			return;
		ereport(ERROR, (errmsg("gid '%s' exists", gid)));
	}

	PG_TRY();
	{
		/* insert into log file */
		if(!is_redo)
		{
			resetStringInfo(&rxlf_xlog_buf);
			appendBinaryStringInfo(&rxlf_xlog_buf, (char*)&db_oid, sizeof(db_oid));
			appendStringInfoChar(&rxlf_xlog_buf, (char)type);
			appendBinaryStringInfo(&rxlf_xlog_buf, (char*)&count, sizeof(count));
			if(count > 0)
			{
				AssertArg(oids);
				appendBinaryStringInfo(&rxlf_xlog_buf, (char*)oids, count*(sizeof(oids[0])));
			}
			appendStringInfoString(&rxlf_xlog_buf, gid);
			/* include gid's '\0' */
			rxact_xlog_insert(rxlf_xlog_buf.data, rxlf_xlog_buf.len+1, RXACT_MSG_DO, true);
		}

		ginfo->remote_nodes
			= MemoryContextAllocZero(TopMemoryContext
				, (sizeof(Oid)+sizeof(bool))*(count+1));
		if(count > 0)
			memcpy(ginfo->remote_nodes, oids, sizeof(Oid)*(count));
		ginfo->remote_nodes[count] = AGTM_OID;

		ginfo->remote_success = (bool*)(&ginfo->remote_nodes[count+1]);
		ginfo->count_nodes = count+1;

		ginfo->type = type;
		ginfo->failed = is_redo;
		ginfo->db_oid = db_oid;
	}PG_CATCH();
	{
		hash_search(htab_rxid, gid, HASH_REMOVE, NULL);
		PG_RE_THROW();
	}PG_END_TRY();

}

static void rxact_mark_gid(const char *gid, RemoteXactType type, bool success, bool is_redo)
{
	GlobalTransactionInfo *ginfo;
	bool found;
	AssertArg(gid && gid[0]);

	/* save to log file */
	if(!is_redo && success)
	{
		/* we don't need save faile log */
		resetStringInfo(&rxlf_xlog_buf);
		appendStringInfoChar(&rxlf_xlog_buf, (char)type);
		appendStringInfoString(&rxlf_xlog_buf, gid);
		rxact_xlog_insert(rxlf_xlog_buf.data, rxlf_xlog_buf.len+1, RXACT_MSG_SUCCESS, false);
	}

	ginfo = hash_search(htab_rxid, gid, HASH_FIND, &found);
	if(found)
	{
		Assert(ginfo->type == type);
		if(success)
		{
			pfree(ginfo->remote_nodes);
			hash_search(htab_rxid, gid, HASH_REMOVE, NULL);
		}else
		{
			ginfo->failed = true;
			/*rxact_has_filed_gid = true;*/
		}
	}else
	{
		if(!is_redo)
			ereport(ERROR, (errmsg("gid '%s' not exists", gid)));
	}
}

static void rxact_change_gid(const char *gid, RemoteXactType type, bool is_redo)
{
	GlobalTransactionInfo *ginfo;
	bool found;
	if(gid == NULL || gid[0] == '\0')
		ereport(ERROR, (errmsg("invalid gid")));
	if(type != RX_COMMIT && type != RX_ROLLBACK)
		ereport(ERROR, (errmsg("invalid rxact type '%d'", (int)type)));

	ginfo = hash_search(htab_rxid, gid, HASH_FIND, &found);
	if(!found)
	{
		if(!is_redo)
			ereport(ERROR, (errmsg("gid '%s' not exists", gid)));
		return; /* for redo */
	}
	if(ginfo->type == type)
	{
		if(!is_redo)
			ereport(WARNING
				, (errmsg("change rxact \"%s\" same type '%d'", gid, type)));
	}else
	{
		if(!is_redo)
		{
			/* save to log file */
			resetStringInfo(&rxlf_xlog_buf);
			appendStringInfoChar(&rxlf_xlog_buf, (char)type);
			appendStringInfoString(&rxlf_xlog_buf, gid);
			rxact_xlog_insert(rxlf_xlog_buf.data, rxlf_xlog_buf.len+1, RXACT_MSG_CHANGE, true);
		}
		ginfo->type = type;
	}
}

static void rxact_insert_node_info(Oid oid, short port, const char *addr, bool is_redo)
{
	RemoteNode *rnode;
	bool found;

	AssertArg(addr && addr[0]);
	rnode = hash_search(htab_remote_node, &oid, HASH_ENTER, &found);
	if(!found)
	{
		rnode->nodePort = (uint16)port;
		MemSet(rnode->nodeHost, 0, sizeof(rnode->nodeHost));
		strncpy(rnode->nodeHost, addr, sizeof(rnode->nodeHost)-1);
		if(!is_redo)
		{
			/* insert log file */
			Assert(rxlf_remote_node != -1);
			rxact_log_simple_write(rxlf_remote_node, rnode, sizeof(*rnode));
		}
	}else
	{
		if(rnode->nodePort != (uint16)port
			|| strcmp(rnode->nodeHost, addr) != 0)
		{
			ereport(WARNING, (errmsg("remote node %d info conflict, use old info", oid)
				, errhint("old:%s:%u, new:%s:%u", rnode->nodeHost, (unsigned)rnode->nodePort, addr, (unsigned)port)));
		}
	}
}

static void rxact_2pc_do(void)
{
	GlobalTransactionInfo *ginfo;
	NodeConn *node_conn;
	HASH_SEQ_STATUS hstatus;
	StringInfoData buf;
	int i;
	bool cmd_is_ok;
	bool node_is_ok;	/* except AGTM nodes is ok? */

	hash_seq_init(&hstatus, htab_rxid);
	buf.data = NULL;
	/*all_finish = true;*/
	while((ginfo = hash_seq_search(&hstatus)) != NULL)
	{
		Assert(ginfo->count_nodes > 0);
		if(ginfo->failed == false)
			continue;

		node_is_ok = true;
		for(i=0;i<ginfo->count_nodes;++i)
		{
			if(ginfo->remote_success[i] == false
				&& ginfo->remote_nodes[i] != AGTM_OID)
			{
				node_is_ok = false;
				break;
			}
		}

		cmd_is_ok = false;
		for(i=0;i<ginfo->count_nodes;++i)
		{
			/* skip successed node */
			if(ginfo->remote_success[i])
				continue;

			/* we first finish except AGTM nodes */
			if(ginfo->remote_nodes[i] == AGTM_OID && node_is_ok == false)
				continue;

			/* get node connection, skip if not connectiond */
			node_conn = rxact_get_node_conn(ginfo->db_oid, ginfo->remote_nodes[i], time(NULL));
			if(node_conn == NULL || node_conn->conn == NULL || node_conn->doing_gid[0] != '\0')
				continue;

			/* when SQL not maked, make it */
			if(cmd_is_ok == false)
			{
				rxact_build_2pc_cmd(&buf, ginfo->gid, ginfo->type);
				cmd_is_ok = true;
			}

			if(PQsendQuery(node_conn->conn, buf.data))
			{
				strcpy(node_conn->doing_gid, ginfo->gid);
				node_conn->last_use = time(NULL);
			}else
			{
				rxact_finish_node_conn(node_conn);
			}
		}
	}

	/*if(all_finish == true)
		rxact_has_filed_gid = false;*/
	if(buf.data)
		pfree(buf.data);
}

static void rxact_2pc_result(NodeConn *conn)
{
	GlobalTransactionInfo *ginfo;
	PGresult *res;
	ExecStatusType status;
	int i;
	bool finish;
	Assert(conn->doing_gid[0] != '\0');

	res = PQgetResult(conn->conn);
	status = PQresultStatus(res);
	PQclear(res);
	if(status != PGRES_COMMAND_OK)
		return;
	ginfo = hash_search(htab_rxid, conn->doing_gid, HASH_FIND, NULL);
	Assert(ginfo != NULL && ginfo->failed == true);
	Assert(conn->oids.node_oid == AGTM_OID || conn->oids.db_oid == ginfo->db_oid);
	conn->last_use = time(NULL);
	conn->doing_gid[0] = '\0';

	finish = true;
	for(i=0;i<ginfo->count_nodes;++i)
	{
		if(ginfo->remote_nodes[i] == conn->oids.node_oid)
		{
			Assert(ginfo->remote_success[i] == false);
			ginfo->remote_success[i] = true;
			break;
		}
		if(ginfo->remote_success[i] == false)
			finish = false;
	}
	Assert(i < ginfo->count_nodes);

	if(finish)
	{
		for(++i;i<ginfo->count_nodes;++i)
		{
			if(ginfo->remote_success[i] == false)
			{
				finish = false;
				break;
			}
		}
		if(finish)
			rxact_mark_gid(ginfo->gid, ginfo->type, true, false);
	}
}

static NodeConn* rxact_get_node_conn(Oid db_oid, Oid node_oid, time_t cur_time)
{
	NodeConn *conn;
	DbAndNodeOid key;
	bool found;

	key.node_oid = node_oid;
	if(node_oid == AGTM_OID)
		key.db_oid = InvalidOid;
	else
		key.db_oid = db_oid;

	conn = hash_search(htab_node_conn, &key, HASH_ENTER, &found);
	if(!found)
	{
		Assert(node_oid != AGTM_OID);
		conn->conn = NULL;
		conn->last_use = 0;
		conn->status = PGRES_POLLING_FAILED;
		conn->doing_gid[0] = '\0';
	}
	Assert(conn && conn->oids.node_oid == node_oid);
	Assert(conn->oids.db_oid == db_oid || (conn->oids.db_oid == InvalidOid && node_oid == AGTM_OID));

	if(conn->conn != NULL && PQstatus(conn->conn) == CONNECTION_BAD)
		rxact_finish_node_conn(conn);

	if(conn->conn == NULL && conn->last_use < cur_time)
	{
		StringInfoData buf;
		buf.data = NULL;
		/* connection to remote node */
		if(node_oid == AGTM_OID)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf, "host='%s' port=%u", AGtmHost, AGtmPort);
			appendStringInfoString(&buf, " user='" AGTM_USER "'"
									" dbname='" AGTM_DBNAME "'");
		}else
		{
			RemoteNode *rnode;
			DatabaseNode *dnode;

			rnode = hash_search(htab_remote_node, &node_oid, HASH_FIND, NULL);
			dnode = hash_search(htab_db_node, &db_oid, HASH_FIND, NULL);
			if(rnode && dnode)
			{
				initStringInfo(&buf);
				appendStringInfo(&buf, "host='%s' port=%u user='%s' dbname='%s'"
					,rnode->nodeHost, rnode->nodePort, dnode->owner, dnode->dbname);
				appendStringInfoString(&buf, " options='-c remotetype=rxactmgr'");
			}
		}
		if(buf.data)
		{
			conn->conn = PQconnectStart(buf.data);
			conn->status = PGRES_POLLING_WRITING;
			pfree(buf.data);
		}
	}

	return rxact_check_node_conn(conn) ? conn:NULL;
}

static bool rxact_check_node_conn(NodeConn *conn)
{
	AssertArg(conn);
	if(conn->conn == NULL)
		return false;
	if(PQstatus(conn->conn) == CONNECTION_BAD)
	{
		rxact_finish_node_conn(conn);
		return false;
	}

re_poll_conn_:
	switch(conn->status)
	{
	case PGRES_POLLING_READING:
		if(wait_socket(PQsocket(conn->conn), false, false) == false)
			return false;
		break;
	case PGRES_POLLING_WRITING:
		if(wait_socket(PQsocket(conn->conn), true, false) == false)
			return false;
		break;
	case PGRES_POLLING_OK:
		conn->last_use = time(NULL);
		return true;
	case PGRES_POLLING_FAILED:
	case PGRES_POLLING_ACTIVE:	/* should be not happen */
		rxact_finish_node_conn(conn);
		return false;
	}
	conn->status = PQconnectPoll(conn->conn);
	goto re_poll_conn_;

	return false;
}

static void rxact_finish_node_conn(NodeConn *conn)
{
	AssertArg(conn);
	if(conn->conn != NULL)
	{
		PQfinish(conn->conn);
		conn->conn = NULL;
		conn->last_use = time(NULL);
	}
	conn->status = PGRES_POLLING_FAILED;
	conn->doing_gid[0] = '\0';
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

		if(PQstatus(node_conn->conn) == CONNECTION_OK
			&& node_conn->doing_gid[0] == '\0'
			&& cur_time - node_conn->last_use >= REMOTE_IDLE_TIMEOUT)
		{
			rxact_finish_node_conn(node_conn);
		}
	}
}

static File rxact_log_open_file(const char *log_name, int fileFlags, int fileMode)
{
	File rfile;
	char file_name[MAX_RLOG_FILE_NAME];
	AssertArg(log_name);

	snprintf(file_name, sizeof(file_name), "%s/%s", rxlf_directory, log_name);
	rfile = PathNameOpenFile(file_name, fileFlags, fileMode);
	if(rfile == -1)
	{
		const char *tmp = (fileFlags & O_CREAT) == O_CREAT ? " or create" : "";
		ereport(FATAL,
			(errcode_for_file_access(),
			errmsg("could not open%s file \"%s\":%m", tmp, file_name)));
	}
	return rfile;
}

static void rxact_xlog_insert(char *data, uint32 len, uint8 info, bool flush)
{
	XLogRecPtr xptr;
	XLogRecData xlog;

	AssertArg(data && len>0);
	xlog.next = NULL;
	xlog.buffer = InvalidBuffer;
	xlog.buffer_std = false;
	xlog.data = data;
	xlog.len = len;
	xptr = XLogInsert(RM_RXACT_MGR_ID, info, &xlog);
	if(flush)
		XLogFlush(xptr);
}

static const char* RemoteXactType2String(RemoteXactType type)
{
	switch(type)
	{
	case RX_PREPARE:
		return "prepare";
	case RX_COMMIT:
		return "commit";
	case RX_ROLLBACK:
		return "rollback";
	}
	return "unknown";
}

/* ---------------------- interface for xlog ---------------------------------*/

void rxact_redo(XLogRecPtr lsn, XLogRecord *record)
{
	const char *gid;
	RemoteXactType type;
	StringInfoData buf;
	int count;
	Oid db_oid;
	Oid *oids;

	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	buf.data = XLogRecGetData(record);
	buf.len = record->xl_len;
	buf.cursor = 0;
	switch(info)
	{
	case RXACT_MSG_DO:
		pq_copymsgbytes(&buf, (char*)&db_oid, sizeof(db_oid));
		type = (RemoteXactType)pq_getmsgbyte(&buf);
		pq_copymsgbytes(&buf, (char*)&count, sizeof(count));
		oids = (Oid*)pq_getmsgbytes(&buf, count*sizeof(oids[0]));
		gid = pq_getmsgstring(&buf);
		rxact_insert_gid(gid, oids, count, type, db_oid, true);
		break;
	case RXACT_MSG_SUCCESS:
		type = (RemoteXactType)pq_getmsgbyte(&buf);
		gid = pq_getmsgstring(&buf);
		rxact_mark_gid(gid, type, true, true);
		break;
	case RXACT_MSG_CHANGE:
		type = (RemoteXactType)pq_getmsgbyte(&buf);
		gid = pq_getmsgstring(&buf);
		rxact_change_gid(gid, type, true);
		break;
	default:
		ereport(PANIC,
			(errmsg("rxact_redo: unknown op code %u", info)));
	}
}

void rxact_xlog_startup(void)
{
	RemoteXactHtabInit();
	RxactLoadLog();
}

void rxact_xlog_cleanup(void)
{
	RxactSaveLog(false);
	DestroyRemoteConnHashTab();
}

void CheckPointRxact(int flags)
{
	StringInfoData buf;
	if(!IS_PGXC_COORDINATOR || !IsUnderPostmaster || (flags & CHECKPOINT_END_OF_RECOVERY))
		return;

	if(rxact_client_fd == PGINVALID_SOCKET)
		connect_rxact();

	rxact_begin_msg(&buf, RXACT_MSG_CHECKPOINT);
	rxact_put_int(&buf, flags);
	send_msg_to_rxact(&buf);

	recv_msg_from_rxact(&buf);
	pfree(buf.data);
}

/* ---------------------- interface for xact client --------------------------*/

void RecordRemoteXact(const char *gid, Oid *nodes, int count, RemoteXactType type)
{
	StringInfoData buf;
	AssertArg(gid && gid[0] && count >= 0);
	AssertArg(RXACT_TYPE_IS_VALID(type));

	ereport(DEBUG1, (errmsg("[ADB]Record %s rxact %s", RemoteXactType2String(type), gid)));

	if(rxact_client_fd == PGINVALID_SOCKET)
		connect_rxact();

	rxact_begin_msg(&buf, RXACT_MSG_DO);
	rxact_put_int(&buf, (int)type);
	rxact_put_int(&buf, count);
	if(count > 0)
	{
		AssertArg(nodes);
		rxact_put_bytes(&buf, nodes, sizeof(nodes[0]) * count);
	}
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

void RecordRemoteXactChange(const char *gid, RemoteXactType type)
{
	StringInfoData buf;
	AssertArg(gid);
	if(gid[0] == '\0')
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, errmsg("invalid gid")));
	}
	if(type != RX_COMMIT && type != RX_ROLLBACK)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, errmsg("invalid remote xact type '%d'", (int)type)));
	}

	elog(DEBUG1, "[ADB]Record change rxact %s to %s", gid, RemoteXactType2String(type));

	if(rxact_client_fd == PGINVALID_SOCKET)
		rxact_connect();

	rxact_begin_msg(&buf, RXACT_MSG_CHANGE);
	rxact_put_int(&buf, (int)type);
	rxact_put_string(&buf, gid);
	send_msg_to_rxact(&buf);

	recv_msg_from_rxact(&buf);
	pfree(buf.data);
}

static void record_rxact_status(const char *gid, RemoteXactType type, bool success)
{
	StringInfoData buf;
	AssertArg(gid && gid[0] && RXACT_TYPE_IS_VALID(type));

	elog(DEBUG1, "[ADB]Record %s rxact %s %s",
		RemoteXactType2String(type), gid, success ? "SUCCESS" : "FAILED");

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
			wait_socket((pgsocket)rxact_client_fd, true, true);
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
	uint8 msg_type;

re_recv_msg_:
	AssertArg(buf);
	Assert(rxact_client_fd != PGINVALID_SOCKET);
	resetStringInfo(buf);

	HOLD_CANCEL_INTERRUPTS();
	PG_TRY();
	{
		while(buf->len < 5)
		{
			wait_socket(rxact_client_fd, false, true);
			recv_socket(rxact_client_fd, buf, 5-buf->len);
		}
		len = *(int*)(buf->data);
		while(buf->len < len)
		{
			wait_socket(rxact_client_fd, false, true);
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
			DisconnectRemoteXact();
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

static bool wait_socket(pgsocket sock, bool wait_send, bool block)
{
	int ret;
#ifdef HAVE_POLL
	struct pollfd poll_fd;
	poll_fd.fd = sock;
	poll_fd.events = (wait_send ? POLLOUT : POLLIN) | POLLERR;
	poll_fd.revents = 0;
re_poll_:
	ret = poll(&poll_fd, 1, block ? -1:0);
#else
	fd_set mask;
	struct timeval tv;
re_poll_:
	tv.tv_sec 0;
	tv.tv_usec = 0;
	FD_ZERO(&mask);
	FD_SET(sock, &mask);
	if(wait_send)
		ret = select(sock+1, NULL, &mask, NULL, block ? NULL:&tv);
	else
		ret = select(sock+1, &mask, NULL, NULL, block ? NULL:&tv);
#endif

	if(ret < 0)
	{
		if(IS_ERR_INTR())
			goto re_poll_;
		ereport(WARNING, (errcode_for_socket_access()
			, errmsg("wait socket failed:%m")));
	}
	return ret == 0 ? false:true;
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
	if(OidIsValid(MyDatabaseId))
	{
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
	}

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

void RemoteXactReloadNode(void)
{
	Form_pgxc_node xc_node;
	HeapTuple tuple;
	HeapScanDesc scan;
	Relation rel;
	StringInfoData buf;
	int count,offset;

	if(rxact_client_fd == PGINVALID_SOCKET)
		connect_rxact();

	rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	scan = heap_beginscan(rel, SnapshotSelf, 0, NULL);

	rxact_begin_msg(&buf, RXACT_MSG_UPDATE_NODE);
	offset = buf.len;
	rxact_put_int(&buf, 0);
	count = 0;
	while((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		xc_node = (Form_pgxc_node)GETSTRUCT(tuple);
		rxact_put_int(&buf, (int)HeapTupleGetOid(tuple));
		rxact_put_short(&buf, (short)(xc_node->node_port));
		rxact_put_string(&buf, NameStr(xc_node->node_host));
		++count;
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	memcpy(buf.data + offset, &count, 4);
	send_msg_to_rxact(&buf);

	recv_msg_from_rxact(&buf);
	pfree(buf.data);
}

void DisconnectRemoteXact(void)
{
	if(rxact_client_fd != PGINVALID_SOCKET)
	{
		closesocket(rxact_client_fd);
		rxact_client_fd = PGINVALID_SOCKET;
	}
}

static void RxactHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}
