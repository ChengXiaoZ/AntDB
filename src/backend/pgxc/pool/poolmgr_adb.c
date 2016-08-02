
#include "postgres.h"

#include <poll.h>
#include <signal.h>
#include <time.h>
#include "access/hash.h"
#include "access/xact.h"
#include "agtm/agtm_client.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "postmaster/postmaster.h"		/* For Unix_socket_directories */
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"

#define MAX_IDLE_TIME 60
/* debug macros */
#define ADB_DEBUG_POOL 1
#define ADB_DEBUG_CHECK_SLOT

#define PM_MSG_ABORT_TRANSACTIONS	'a'
#define PM_MSG_SEND_LOCAL_COMMAND	'b'
#define PM_MSG_CONNECT				'c'
#define PM_MSG_DISCONNECT			'd'
#define PM_MSG_CLEAN_CONNECT		'f'
#define PM_MSG_GET_CONNECT			'g'
#define PM_MSG_CANCEL_QUERY			'h'
#define PM_MSG_LOCK					'o'
#define PM_MSG_RELOAD_CONNECT		'p'
#define PM_MSG_CHECK_CONNECT		'q'
#define PM_MSG_RELEASE_CONNECT		'r'
#define PM_MSG_SET_COMMAND			's'
#define PM_MSG_CLOSE_CONNECT		'C'
#define PM_MSG_ERROR				'E'

#define SLOT_STATE_UNINIT			0
#define SLOT_STATE_IDLE				1
#define SLOT_STATE_LOCKED			2
#define SLOT_STATE_RELEASED			3

#define PFREE_SAVE(p)				\
	do{								\
		void *p_ = (p);				\
		if(p_)	pfree(p_);			\
	}while(0)

/* Connection pool entry */
typedef struct PGXCNodePoolSlot
{
	NODE_CONNECTION		*conn;
	PGcancel			*xc_cancelConn;		/* null if not used */
	struct PGXCNodePool	*parent;
	time_t				released_time;
	unsigned int		state;				/* SLOT_STATE_* */
	int					last_user_pid;
	int					last_agtm_port;		/* last send agtm port */
} PGXCNodePoolSlot;

/* Pool of connections to specified pgxc node */
typedef struct PGXCNodePool
{
	Oid			nodeoid;	/* Node Oid related to this pool */
	PGXCNodePoolSlot *slot;
	char	   *connstr;
	Size		last_idle;
	struct DatabasePool *parent;
} PGXCNodePool;

typedef struct DatabaseInfo
{
	char	   *database;
	char	   *user_name;
	char	   *pgoptions;		/* Connection options */
}DatabaseInfo;

/* All pools for specified database */
typedef struct DatabasePool
{
	DatabaseInfo	db_info;
	HTAB		   *htab_nodes; 		/* Hashtable of PGXCNodePool, one entry for each
										 * Coordinator or DataNode */
#ifdef ADB_DEBUG_POOL
	List		   *list_nodes;
#endif /* ADB_DEBUG_POOL */
} DatabasePool;

/*
 * Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 * and managing session parameters
 */
typedef struct PoolAgent
{
	/* communication channel */
	PoolPort		port;
	DatabasePool   *db_pool;
	Size			num_dn_connections;
	Size			num_coord_connections;
	PGXCNodePoolSlot **dn_connections; /* one for each Datanode */
	PGXCNodePoolSlot **coord_connections; /* one for each Coordinator */
	Oid			   *datanode_oids;
	Oid			   *coord_oids;
	char		   *session_params;
	char		   *local_params;
	MemoryContext	mctx;
	/* Process ID of postmaster child process associated to pool agent */
	int				pid;
	int				agtm_port;
	bool			is_temp; /* Temporary objects used for this pool session? */
} PoolAgent;

struct PoolHandle
{
	/* communication channel */
	PoolPort	port;
};

/* Configuration options */
int			MinPoolSize = 1;
int			MaxPoolSize = 100;
int			PoolRemoteCmdTimeout = 0;

bool			PersistentConnections = false;

/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

/* The root memory context */
static MemoryContext PoolerMemoryContext;

/* PoolAgents */
static volatile Size	agentCount;
static PoolAgent **poolAgents;

static PoolHandle *poolHandle = NULL;

static int	is_pool_locked = false;
static pgsocket server_fd = PGINVALID_SOCKET;

/* Signal handlers */
static void pooler_quickdie(SIGNAL_ARGS);
static void PoolerLoop(void) __attribute__((noreturn));

static void agent_handle_input(PoolAgent * agent, StringInfo s);
static void agent_error_hook(void *arg);
static bool agent_recv_data(PoolAgent *agent);
static bool agent_has_completion_msg(PoolAgent *agent, StringInfo msg, int *msg_type);
static char * build_node_conn_str(Oid node, DatabasePool *dbPool);
static int *abort_pids(int *count, int pid, const char *database, const char *user_name);
static int clean_connection(List *node_discard, const char *database, const char *user_name);
#ifdef ADB_DEBUG_CHECK_SLOT
static bool check_slot_status(PGXCNodePoolSlot *slot, bool re_connect);
#define ADB_CHECK_SLOT(slot_,re_conn_) check_slot_status(slot_,re_conn_)
#else
#define ADB_CHECK_SLOT(slot_,re_conn_) (true)
#endif /* ADB_DEBUG_CHECK_SLOT */

static void agent_create(volatile pgsocket new_fd);
static void agent_release_connections(PoolAgent *agent, bool force_destroy);
static void agent_idle_connections(PoolAgent *agent, bool force_destroy);
static int *agent_acquire_connections(PoolAgent *agent, const List *datanodelist, const List *coordlist);
static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, Oid node);
static bool agent_acquire_conn_list(PGXCNodePoolSlot **slots, const Oid *oids, const List *node_list, PoolAgent *agent);
static void agent_lock_connect_list(PGXCNodePoolSlot **slots, const List *node_list, int *fds, const PoolAgent *agent);
static void cancel_query_on_connections(PoolAgent *agent, Size count, PGXCNodePoolSlot **slots, const List *nodelist);
static void reload_database_pools(PoolAgent *agent);
static int node_info_check(PoolAgent *agent);
static int agent_session_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type);
static int send_local_commands(PoolAgent *agent, List *datanodelist, List *coordlist);

static void destroy_slot(PGXCNodePoolSlot *slot, bool send_cancel);
static void release_slot(PGXCNodePoolSlot *slot, bool force_close);
static void idle_slot(PGXCNodePoolSlot *slot);
static void destroy_node_pool(PGXCNodePool *node_pool, bool bfree);
static bool node_pool_in_using(PGXCNodePool *node_pool);
static time_t close_timeout_idle_slots(time_t timeout);
static bool send_agtm_listen_port(NODE_CONNECTION *conn, int port);
static bool pool_exec_set_query(NODE_CONNECTION *conn, const char *query);
static int pool_wait_pq(PGconn *conn);

/* for hash DatabasePool */
static HTAB *htab_database;
#ifdef ADB_DEBUG_POOL
static List* list_database;
#endif /* ADB_DEBUG_POOL */
static uint32 hash_database_info(const void *key, Size keysize);
static int match_database_info(const void *key1, const void *key2, Size keysize);
static void create_htab_database(void);
static void destroy_htab_database(void);
static DatabasePool *get_database_pool(const char *database, const char *user_name, const char *pgoptions);
static void destroy_database_pool(DatabasePool *db_pool, bool bfree);
static bool slot_in_node_pool(const PGXCNodePoolSlot *slot, const PGXCNodePool *node_pool);

static void pool_end_flush_msg(PoolPort *port, StringInfo buf);
static void pool_sendstring(StringInfo buf, const char *str);
static const char *pool_getstring(StringInfo buf);
static void pool_sendint(StringInfo buf, int ival);
static int pool_getint(StringInfo buf);
static void pool_sendint_array(StringInfo buf, int count, const int *arr);
static void pool_send_nodeid_list(StringInfo buf, const List *list);
static List* pool_get_nodeid_list(StringInfo buf);
static void on_exit_pooler(int code, Datum arg);

void PGXCPoolerProcessIam(void)
{
	am_pgxc_pooler = true;
}

bool IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	/* set it to NULL well exit wen has an error */
	PG_exception_stack = NULL;

	elog(DEBUG1, "Pooler process is started: %d", getpid());

	/*
	 * Set up memory contexts for the pooler objects
	 */
	PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												"PoolerMemoryContext",
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
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, pooler_quickdie);
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

	/* Allocate pooler structures in the Pooler context */
	MemoryContextSwitchTo(PoolerMemoryContext);

	poolAgents = (PoolAgent **) palloc(MaxConnections * sizeof(PoolAgent *));
	agentCount = 0;

	create_htab_database();

	PoolerLoop();	/* should never return */
	proc_exit(1);
}

static void PoolerLoop(void)
{
	MemoryContext context;
	Size maxfd,i,count;
	struct pollfd *poll_fd,*pollfd_tmp;
	PoolAgent *agent;
	sigjmp_buf	local_sigjmp_buf;
	time_t next_close_idle_time, cur_time;
	StringInfoData input_msg;
	int rval;
	pgsocket new_socket;

	server_fd = pool_listen();
	if(server_fd == PGINVALID_SOCKET)
	{
		ereport(PANIC, (errcode_for_socket_access(),
			errmsg("Can not listen unix socket on %s", pool_get_sock_path())));
	}

	maxfd = (Size)MaxConnections + 10; /* add additional 10 */
	poll_fd = palloc(maxfd * sizeof(struct pollfd));
	initStringInfo(&input_msg);
	context = AllocSetContextCreate(CurrentMemoryContext,
										"PoolerMemoryContext",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	poll_fd[0].fd = server_fd;
	poll_fd[0].events = POLLIN;
	for(i=1;i<maxfd;++i)
	{
		poll_fd[i].fd = PGINVALID_SOCKET;
		poll_fd[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
	}
	on_proc_exit(on_exit_pooler, (Datum)0);
	cur_time = time(NULL);
	next_close_idle_time = cur_time + MAX_IDLE_TIME;

	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Cleanup something */
		EmitErrorReport();
		FlushErrorState();
		error_context_stack = NULL;
	}
	PG_exception_stack = &local_sigjmp_buf;
	(void)MemoryContextSwitchTo(context);

	for(;;)
	{
		MemoryContextResetAndDeleteChildren(context);

		if(!PostmasterIsAlive())
			exit(1);

		for(i=0;i<agentCount;++i)
			poll_fd[i+1].fd = Socket(poolAgents[i]->port);

		rval = poll(poll_fd, agentCount + 1, 1000);
		CHECK_FOR_INTERRUPTS();
		if(rval < 0)
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
				errmsg("pool failed(%d) in pooler process, error %m", rval)));
		}else if(rval == 0)
		{
			/* nothing to do */
		}

		count = 0;
		for(i=agentCount; i > 0 && count < (Size)rval;)
		{
			pollfd_tmp = &(poll_fd[i]);
			--i;
			agent = poolAgents[i];
			if(pollfd_tmp->fd == Socket(agent->port)
				&& pollfd_tmp->revents != 0)
			{
				++count;
				agent_handle_input(agent, &input_msg);
			}
		}

		if(poll_fd[0].revents & POLLIN)
		{
			new_socket = accept(server_fd, NULL, NULL);
			if(new_socket == PGINVALID_SOCKET)
			{
				ereport(WARNING, (errcode_for_socket_access(),
					errmsg("pool manager accept failed:%m")));
			}else
			{
				agent_create(new_socket);
			}
			++count;
		}
		cur_time = time(NULL);
		/* close timeout idle slot(s) */
		if(cur_time >= next_close_idle_time)
		{
			next_close_idle_time = close_timeout_idle_slots(cur_time - MAX_IDLE_TIME)
				+ MAX_IDLE_TIME;
		}
	}
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
	if (PoolerMemoryContext)
	{
		MemoryContextDelete(PoolerMemoryContext);
		PoolerMemoryContext = NULL;
	}

	return 0;
}


/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 */
PoolHandle *
GetPoolManagerHandle(void)
{
	PoolHandle *handle;
	int			fdsock;

	/* Connect to the pooler */
	fdsock = pool_connect();
	if (fdsock < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to connect to pool manager: %m")));
	}

	/* Allocate handle */
	PG_TRY();
	{
		handle = MemoryContextAlloc(TopMemoryContext, sizeof(*handle));

		handle->port.fdsock = fdsock;
		handle->port.RecvLength = 0;
		handle->port.RecvPointer = 0;
		handle->port.SendPointer = 0;
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
PoolManagerCloseHandle(PoolHandle *handle)
{
	closesocket(Socket(handle->port));
	pfree(handle);
}


/*
 * Create agent
 */
static void agent_create(volatile pgsocket new_fd)
{
	PoolAgent  * agent;
	MemoryContext volatile context = NULL;

	AssertArg(new_fd != PGINVALID_SOCKET);
	Assert(agentCount < MaxConnections);

	PG_TRY();
	{
		/* Allocate MemoryContext */
		context = AllocSetContextCreate(PoolerMemoryContext,
								"PoolAgent",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
		agent = MemoryContextAllocZero(context, sizeof(*agent));
		agent->port.fdsock = new_fd;
		agent->mctx = context;
	}PG_CATCH();
	{
		closesocket(new_fd);
		if(context)
			MemoryContextDelete(context);
		PG_RE_THROW();
	}PG_END_TRY();

	/* Append new agent to the list */
	poolAgents[agentCount++] = agent;
}

/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char *session_options(void)
{
	int				 i;
	const char		*pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle"};
	StringInfoData	 options;
	List			*value_list;
	char			*value;
	const char		*tmp;
	ListCell		*lc;

	initStringInfo(&options);

	/* first add "lc_monetary" */
	appendStringInfoString(&options, " -c lc_monetary=");
	appendStringInfoString(&options, GetConfigOptionResetString("lc_monetary"));

	/* add other options */
	for (i = 0; i < lengthof(pgoptions); i++)
	{
		appendStringInfo(&options, " -c %s=", pgoptions[i]);

		value = pstrdup(GetConfigOptionResetString(pgoptions[i]));

		SplitIdentifierString(value, ',', &value_list);
		tmp = "";
		foreach(lc, value_list)
		{
			appendStringInfoString(&options, tmp);
			appendStringInfoString(&options, lfirst(lc));
			tmp = ",";
		}
		list_free(value_list);
		pfree(value);
	}

	return options.data;
}

/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void
PoolManagerConnect(PoolHandle *handle,
	               const char *database, const char *user_name,
	               const char *pgoptions)
{
	StringInfoData buf;
	AssertArg(handle && database && user_name);

	/* save the handle */
	poolHandle = handle;

	pq_beginmessage(&buf, PM_MSG_CONNECT);

	/* PID number */
	pq_sendbytes(&buf, (char*)&MyProcPid, sizeof(MyProcPid));
	pool_sendstring(&buf, database);
	pool_sendstring(&buf, user_name);
	pool_sendstring(&buf, pgoptions);

	pool_end_flush_msg(&(handle->port), &buf);
}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
	PoolHandle *handle;
	char *options = session_options();

	if (poolHandle)
	{
		PoolManagerDisconnect();
	}

	handle = GetPoolManagerHandle();
	PoolManagerConnect(handle,
					   get_database_name(MyDatabaseId),
					   GetUserNameFromId(GetUserId()),
					   options);
	pfree(options);
}

int
PoolManagerSetCommand(PoolCommandType command_type, const char *set_command)
{
	StringInfoData buf;
	int res = 0;

	if (poolHandle)
	{
		pq_beginmessage(&buf, PM_MSG_SET_COMMAND);

		/*
		 * If SET LOCAL is in use, flag current transaction as using
		 * transaction-block related parameters with pooler agent.
		 */
		if (command_type == POOL_CMD_LOCAL_SET)
			SetCurrentLocalParamStatus(true);

		/* LOCAL or SESSION parameter ? */
		pool_sendint(&buf, command_type);

		pool_sendstring(&buf, set_command);

		pool_end_flush_msg(&(poolHandle->port), &buf);

		/* Get result */
		res = pool_recvres(&poolHandle->port);
	}
	return res;
}

/*
 * Send commands to alter the behavior of current transaction and update begin sent status
 */
int
PoolManagerSendLocalCommand(int dn_count, int* dn_list, int co_count, int* co_list)
{
	StringInfoData buf;

	if (poolHandle == NULL)
		return EOF;

	if (dn_count == 0 && co_count == 0)
		return EOF;

	if (dn_count != 0 && dn_list == NULL)
		return EOF;

	if (co_count != 0 && co_list == NULL)
		return EOF;

	pq_beginmessage(&buf, PM_MSG_SEND_LOCAL_COMMAND);

	pq_sendbytes(&buf, (char*)&dn_count, sizeof(dn_count));
	pq_sendbytes(&buf, (char*)dn_list, sizeof(int)*dn_count);
	pq_sendbytes(&buf, (char*)&co_count, sizeof(co_count));
	pq_sendbytes(&buf, (char*)co_list, sizeof(int)*co_count);

	pool_end_flush_msg(&(poolHandle->port), &buf);

	/* Get result */
	return pool_recvres(&poolHandle->port);
}

/*
 * Lock/unlock pool manager
 * During locking, the only operations not permitted are abort, connection and
 * connection obtention.
 */
void
PoolManagerLock(bool is_lock)
{
	/* add by jiangmj for execute direct on (coord2) select pgxc_pool_reload()*/
	if(IS_PGXC_COORDINATOR && IsConnFromCoord())
	{
		if (poolHandle == NULL)
		{
			MemoryContext old_context;
			/* Now session information is reset in correct memory context */
			old_context = MemoryContextSwitchTo(TopMemoryContext);

			/* And reconnect to pool manager */
			PoolManagerReconnect();

			MemoryContextSwitchTo(old_context);
		}
	}
	/* end */
	Assert(poolHandle);

	pool_putmessage(&(poolHandle->port), PM_MSG_LOCK, &is_lock, 1);
	pool_flush(&poolHandle->port);
}

/*
 * Init PoolAgent
 */
static bool
agent_init(PoolAgent *agent, const char *database, const char *user_name,
           const char *pgoptions)
{
	MemoryContext oldcontext;
	int num_coord,num_datanode;
	volatile bool has_error;

	AssertArg(agent);
	if(database == NULL || user_name == NULL)
		return false;
	if(pgoptions == NULL)
		pgoptions = "";

	/* disconnect if we are still connected */
	if (agent->db_pool)
		agent_release_connections(agent, false);

	oldcontext = MemoryContextSwitchTo(agent->mctx);

	has_error = false;
	PG_TRY();
	{
		/* Get needed info and allocate memory */
		PgxcNodeGetOids(&(agent->coord_oids), &(agent->datanode_oids)
				, &num_coord, &num_datanode, false);

		agent->coord_connections = (PGXCNodePoolSlot **)
				palloc0(num_coord * sizeof(PGXCNodePoolSlot *));
		agent->dn_connections = (PGXCNodePoolSlot **)
				palloc0(num_datanode * sizeof(PGXCNodePoolSlot *));
		/* get database */
		agent->db_pool = get_database_pool(database, user_name, pgoptions);

		agent->num_coord_connections = num_coord;
		agent->num_dn_connections = num_datanode;
	}PG_CATCH();
	{
		has_error = true;
	}PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	return has_error == false ? true:false;
}

/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	Size	i;

	AssertArg(agent);

	if(Socket(agent->port) != PGINVALID_SOCKET)
		closesocket(Socket(agent->port));

	/*
	 * idle them all.
	 * Force disconnection if there are temporary objects on agent.
	 */
	agent_idle_connections(agent, agent->is_temp);

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (poolAgents[i] == agent)
		{
			Size end = --agentCount;
			if(end > i)
				memmove(&(poolAgents[i]), &(poolAgents[i+1]), (end-i)*sizeof(agent));
			poolAgents[end] = NULL;
			break;
		}
	}
	Assert(i<=agentCount);
	MemoryContextDelete(agent->mctx);
}

static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, Oid node)
{
	PGXCNodePool *node_pool;
	PGXCNodePoolSlot *slot
					,*released_slot
					,*uninit_slot;
	Size i;
	bool found;

	AssertArg(dbPool && dbPool->htab_nodes);

	node_pool = hash_search(dbPool->htab_nodes, &node, HASH_ENTER, &found);
	if(!found)
	{
		/* initialize a node pool */
		PGXCNodePool tmp_node_pool;
		PG_TRY();
		{
			MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
			memset(&tmp_node_pool, 0, sizeof(tmp_node_pool));
			tmp_node_pool.connstr = build_node_conn_str(node, dbPool);
			tmp_node_pool.slot = palloc0(sizeof(*slot)*MaxConnections);
			StaticAssertExpr(SLOT_STATE_UNINIT == 0, "todo set slot's state");
			for(i=0;i<(Size)MaxConnections;++i)
			{
				tmp_node_pool.slot[i].parent = node_pool;
#if SLOT_STATE_UNINIT != 0
				tmp_node_pool.slot[i].state = SLOT_STATE_UNINIT;
#endif /* SLOT_STATE_UNINIT != 0 */
			}
			tmp_node_pool.nodeoid = node;
#ifdef ADB_DEBUG_POOL
			dbPool->list_nodes = lappend(dbPool->list_nodes, node_pool);
#endif /* ADB_DEBUG_POOL */
			(void)MemoryContextSwitchTo(old_context);
		}PG_CATCH();
		{
			/* got an error, we release resources */
			if(tmp_node_pool.slot)
				pfree(tmp_node_pool.slot);
			if(tmp_node_pool.connstr)
				pfree(tmp_node_pool.connstr);
			hash_search(dbPool->htab_nodes, &node, HASH_REMOVE, NULL);
			PG_RE_THROW();
		}PG_END_TRY();
		memcpy(node_pool, &tmp_node_pool, sizeof(*node_pool));
		node_pool->parent = dbPool;
	}

	/* find an idle slot */
	Assert(node_pool && node_pool->slot && node_pool->parent == dbPool);
	released_slot = uninit_slot = slot = NULL;
	for(i=0;i<MaxConnections;++i)
	{
		/* first find an idle slot */
		if(node_pool->slot[i].state == SLOT_STATE_IDLE)
		{
			slot = &(node_pool->slot[i]);
			break;
		}else if(released_slot == NULL
			&& node_pool->slot[i].state == SLOT_STATE_RELEASED)
		{
			released_slot = &(node_pool->slot[i]);
		}else if(uninit_slot == NULL
			&& node_pool->slot[i].state == SLOT_STATE_UNINIT)
		{
			uninit_slot = &(node_pool->slot[i]);
		}
	}
	if(slot != NULL)
	{
		/* we got an idel slot */
		return slot;
	}

	/* now we try connection a new slot */
	if(uninit_slot)
	{
		Assert(uninit_slot->conn == NULL);
		uninit_slot->conn = PGXCNodeConnect(node_pool->connstr);
		if(PGXCNodeConnected(uninit_slot->conn))
		{
			uninit_slot->state = SLOT_STATE_IDLE;
			return uninit_slot;
		}
		/* connection failed */
		ereport(WARNING, (errmsg("can not connection node %u", node)
			,errhint("%s", PQerrorMessage((PGconn*)(uninit_slot->conn)))));
		PGXCNodeClose(uninit_slot->conn);
		uninit_slot->conn = NULL;
	}

	/* at lost, we need use a released slot */
	if(released_slot)
	{
		Assert(PGXCNodeConnected(released_slot->conn));
		return released_slot;
	}

	/* at end, we have no way to get a slot */
	return NULL;
}

/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
	if (poolHandle)
	{

		pool_putmessage(&poolHandle->port, PM_MSG_RELEASE_CONNECT, NULL, 0);
		pool_flush(&poolHandle->port);

		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
	}
}


/*
 * Get pooled connections
 */
int *
PoolManagerGetConnections(List *datanodelist, List *coordlist)
{
	StringInfoData buf;
	pgsocket *fds;
	int val;

	Assert(poolHandle != NULL);
	if(datanodelist == NIL && coordlist == NIL)
		return NULL;

	pq_beginmessage(&buf, PM_MSG_GET_CONNECT);

	/* send agtm listen port */
	val = agtm_GetListenPort();
	if(val < 1 || val > 65535)
		ereport(ERROR, (errmsg("Invalid agtm listen port %d", val)));
	pool_sendint(&buf, val);

	/* datanode count and oid(s) */
	pool_send_nodeid_list(&buf, datanodelist);

	/* coord count and oid(s) */
	pool_send_nodeid_list(&buf, coordlist);

	/* send message */
	pool_putmessage(&poolHandle->port, (char)(buf.cursor), buf.data, buf.len);
	pool_flush(&poolHandle->port);

	/* Receive response */
	val = list_length(datanodelist) + list_length(coordlist);
	/* we reuse buf.data, here palloc maybe failed */
	Assert(buf.maxlen >= sizeof(pgsocket)*val);
	fds = (int*)(buf.data);
	if(pool_recvfds(&(poolHandle->port), fds, val) != 0)
	{
		pfree(fds);
		return NULL;
	}

	return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
	StringInfoData buf;
	AssertArg(proc_pids);
	Assert(poolHandle);

	pq_beginmessage(&buf, PM_MSG_ABORT_TRANSACTIONS);

	/* send database name */
	pool_sendstring(&buf, dbname);

	/* send user name */
	pool_sendstring(&buf, username);

	pool_end_flush_msg(&(poolHandle->port), &buf);

	return pool_recvpids(&(poolHandle->port), proc_pids);
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username)
{
	StringInfoData buf;
	ListCell *lc;
	int ival;

	pq_beginmessage(&buf, PM_MSG_CLEAN_CONNECT);

	/* list datanode(s) */
	ival = list_length(datanodelist);
	pq_sendbytes(&buf, (char*)&ival, sizeof(ival));
	foreach(lc, datanodelist)
		pq_sendbytes(&buf, (char*)&(lfirst_int(lc)), sizeof(lfirst_int(lc)));

	/* list coord(s) */
	ival = list_length(coordlist);
	pq_sendbytes(&buf, (char*)&ival, sizeof(ival));
	foreach(lc, coordlist)
		pq_sendbytes(&buf, (char*)&(lfirst_int(lc)), sizeof(lfirst_int(lc)));

	/* send database string */
	pool_sendstring(&buf, dbname);

	/* send user name */
	pool_sendstring(&buf, username);

	pool_end_flush_msg(&(poolHandle->port), &buf);

	/* Receive result message */
	if (pool_recvres(&poolHandle->port) != CLEAN_CONNECTION_COMPLETED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Clean connections not completed")));
}


/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool
PoolManagerCheckConnectionInfo(void)
{
	int res;

	Assert(poolHandle);
	PgxcNodeListAndCount();
	pool_putmessage(&poolHandle->port, PM_MSG_CHECK_CONNECT, NULL, 0);
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port);

	if (res == POOL_CHECK_SUCCESS)
		return true;

	return false;
}


/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void
PoolManagerReloadConnectionInfo(void)
{
	Assert(poolHandle);
	PgxcNodeListAndCount();
	pool_putmessage(&poolHandle->port, PM_MSG_RELOAD_CONNECT, NULL, 0);
	pool_flush(&poolHandle->port);
}

void PoolManagerReleaseConnections(bool force_close)
{
	Assert(poolHandle);
	pool_putmessage(&(poolHandle->port)
		, force_close ? PM_MSG_CLOSE_CONNECT:PM_MSG_RELEASE_CONNECT
		, NULL, 0);
	pool_flush(&(poolHandle->port));
}

void PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list)
{
	StringInfoData buf;
	if (poolHandle == NULL)
		return;

	if (dn_count == 0 && co_count == 0)
		return;

	if (dn_count != 0 && dn_list == NULL)
		return;

	if (co_count != 0 && co_list == NULL)
		return;

	pq_beginmessage(&buf, PM_MSG_CANCEL_QUERY);
	pool_sendint_array(&buf, dn_count, dn_list);
	pool_sendint_array(&buf, co_count, co_list);
	pool_end_flush_msg(&(poolHandle->port), &buf);
}

/*
 *
 */
static void pooler_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}

bool IsPoolHandle(void)
{
	return poolHandle != NULL;
}


/*
 * Given node identifier, dbname and user name build connection string.
 * Get node connection details from the shared memory node table
 */
static char * build_node_conn_str(Oid node, DatabasePool *dbPool)
{
	NodeDefinition *nodeDef;
	char 		   *connstr;

	nodeDef = PgxcNodeGetDefinition(node);
	if (nodeDef == NULL)
	{
		/* No such definition, node is dropped? */
		return NULL;
	}

	connstr = PGXCNodeConnStr(NameStr(nodeDef->nodehost),
							  nodeDef->nodeport,
							  dbPool->db_info.database,
							  dbPool->db_info.user_name,
							  dbPool->db_info.pgoptions,
							  IS_PGXC_COORDINATOR ? "coordinator" : "datanode");
	pfree(nodeDef);

	return connstr;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{
	int *pids = NULL;
	DatabaseInfo *db_info;
	int i;
	int count;

	Assert(!is_pool_locked);
	Assert(agentCount > 0);

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	is_pool_locked = true;

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = i = 0; i < agentCount; i++)
	{
		Assert(poolAgents[i] && poolAgents[i]->db_pool);
		if (poolAgents[i]->pid == pid)
			continue;

		db_info = &(poolAgents[i]->db_pool->db_info);
		if (database && strcmp(db_info->database, database) != 0)
			continue;

		if (user_name && strcmp(db_info->user_name, user_name) != 0)
			continue;

		if (kill(poolAgents[i]->pid, SIGTERM) < 0)
			elog(ERROR, "kill(%ld,%d) failed: %m",
						(long) poolAgents[i]->pid, SIGTERM);

		pids[count] = poolAgents[i]->pid;
		++count;
	}

	*len = count;

	return pids;
}

static void agent_handle_input(PoolAgent * agent, StringInfo s)
{
	ErrorContextCallback err_calback;
	const char *database;
	const char *user_name;
	const char *pgoptions;
	List		*nodelist;
	List		*coordlist;
	List		*datanodelist;
	int			*fds;
	int			*pids;
	int			i,len,res;
	int qtype;

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

	while(agent_has_completion_msg(agent, s, &qtype))
	{
		/* set need report error if have */
		err_calback.arg = agent;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_locked
			&& (qtype == PM_MSG_ABORT_TRANSACTIONS ||
				qtype == PM_MSG_GET_CONNECT ||
				qtype == PM_MSG_CONNECT))
			elog(WARNING,"Pool operation cannot run during pool lock");

		switch(qtype)
		{
		case PM_MSG_ABORT_TRANSACTIONS:
			database = pool_getstring(s);
			user_name = pool_getstring(s);

			pids = abort_pids(&len, agent->pid, database, user_name);
			pool_sendpids(&agent->port, pids, len);
			if(pids)
				pfree(pids);
			break;
		case PM_MSG_SEND_LOCAL_COMMAND:
			datanodelist = pool_get_nodeid_list(s);
			coordlist = pool_get_nodeid_list(s);
			res = send_local_commands(agent, datanodelist, coordlist);
			pool_sendres(&agent->port, res);
			list_free(datanodelist);
			list_free(coordlist);
			break;
		case PM_MSG_CONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			pq_copymsgbytes(s, (char*)&(agent->pid), sizeof(agent->pid));
			database = pool_getstring(s);
			user_name = pool_getstring(s);
			pgoptions = pool_getstring(s);
			if(agent_init(agent, database, user_name, pgoptions) == false)
			{
				agent_destroy(agent);
				goto end_agent_input_;
			}
			break;
		case PM_MSG_DISCONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			agent_destroy(agent);
			pq_getmsgend(s);
			goto end_agent_input_;
		case PM_MSG_CLEAN_CONNECT:
			{
				PGXCNodePoolSlot *slot;
				int idx;
				pq_copymsgbytes(s, (char*)&len, sizeof(len));
				nodelist = NIL;
				for(i=0;i<len;++i)
				{
					pq_copymsgbytes(s, (char*)&idx, sizeof(idx));
					if((Size)idx > agent->num_dn_connections)
						ereport(ERROR, (errmsg("invalid index for clean connection from backend")));
					if(agent->dn_connections == NULL || agent->dn_connections[idx] == NULL)
						continue;
					slot = agent->dn_connections[idx];
					Assert(slot->parent);
					nodelist = lappend_oid(nodelist, slot->parent->nodeoid);
				}

				pq_copymsgbytes(s, (char*)&len, sizeof(len));
				for(i=0;i<len;++i)
				{
					pq_copymsgbytes(s, (char*)&idx, sizeof(idx));
					if((Size)idx > agent->num_coord_connections)
						ereport(ERROR, (errmsg("invalid index for clean connection from backend")));
					if(agent->coord_connections == NULL || agent->coord_connections[idx] == NULL)
						continue;
					slot = agent->coord_connections[idx];
					Assert(slot->parent);
					nodelist = lappend_oid(nodelist, slot->parent->nodeoid);
				}
				database = pool_getstring(s);
				user_name = pool_getstring(s);
				res = clean_connection(nodelist, database, user_name);
				list_free(nodelist);
				pool_sendres(&agent->port, res);
			}
			break;
		case PM_MSG_GET_CONNECT:
			agent->agtm_port = pool_getint(s);
			datanodelist = pool_get_nodeid_list(s);
			coordlist = pool_get_nodeid_list(s);
			fds = agent_acquire_connections(agent, datanodelist, coordlist);
			len = fds ? list_length(datanodelist) + list_length(coordlist):0;
			res = pool_sendfds(&agent->port, fds, len);
			if(res != 0)
				ereport(ERROR, (errmsg("can not send fds to backend")));
			if(fds)
				pfree(fds);
			list_free(coordlist);
			list_free(datanodelist);
			break;
		case PM_MSG_CANCEL_QUERY:
			err_calback.arg = NULL; /* do not send error if have */
			datanodelist = pool_get_nodeid_list(s);
			coordlist = pool_get_nodeid_list(s);

			cancel_query_on_connections(agent, agent->num_dn_connections, agent->dn_connections, datanodelist);
			cancel_query_on_connections(agent, agent->num_coord_connections, agent->coord_connections, coordlist);
			list_free(datanodelist);
			list_free(coordlist);
			break;
		case PM_MSG_LOCK:		/* Lock/unlock pooler */
			err_calback.arg = NULL; /* do not send error if have */
			is_pool_locked = pq_getmsgbyte(s);
			break;
		case PM_MSG_RELOAD_CONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			reload_database_pools(agent);
			break;
		case PM_MSG_CHECK_CONNECT:
			res = node_info_check(agent);
			pool_sendres(&agent->port, res);
			break;
		case PM_MSG_RELEASE_CONNECT:
		case PM_MSG_CLOSE_CONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			pq_getmsgend(s);
			agent_release_connections(agent, qtype == PM_MSG_CLOSE_CONNECT);
			break;
		case PM_MSG_SET_COMMAND:
			{
				PoolCommandType cmd_type;
				const char *set_cmd;
				cmd_type = (PoolCommandType)pool_getint(s);
				set_cmd = pool_getstring(s);
				res = agent_session_command(agent, set_cmd, cmd_type);
				pool_sendres(&agent->port, res);
			}
			break;
		default:
			agent_destroy(agent);
			ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("invalid backend message type %d", qtype)));
			goto end_agent_input_;
		}
		pq_getmsgend(s);
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
		pool_putmessage(arg, PM_MSG_ERROR, NULL, 0);
		pool_flush(arg);
	}
}

/* true for recv some data, false for closed by remote */
static bool agent_recv_data(PoolAgent *agent)
{
	PoolPort *port;
	int rval;
	AssertArg(agent);
	port = &(agent->port);

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

	if(port->RecvLength >= POOL_BUFFER_SIZE)
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("too many data from backend for pooler")));
		return false;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		rval = recv(Socket(*port), port->RecvBuffer + port->RecvLength,
				 POOL_BUFFER_SIZE - port->RecvLength, 0);

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
		port->RecvLength += rval;
		break;
	}
	return true;
}

/* get message if has completion message */
static bool agent_has_completion_msg(PoolAgent *agent, StringInfo msg, int *msg_type)
{
	PoolPort *port;
	Size unread_len;
	int len;
	AssertArg(agent && msg);

	port = &agent->port;
	Assert(port->RecvLength >= 0 && port->RecvPointer >= 0
		&& port->RecvLength >= port->RecvPointer);

	unread_len = port->RecvLength - port->RecvPointer;
	/* 5 is message type (char) and message length(int) */
	if(unread_len < 5)
		return false;

	/* get message length */
	memcpy(&len, port->RecvBuffer + port->RecvPointer + 1, 4);
	len = htonl(len);
	if((len+1) > unread_len)
		return false;

	*msg_type = port->RecvBuffer[port->RecvPointer];

	/* okay, copy message */
	len++;	/* add char length */
	resetStringInfo(msg);
	enlargeStringInfo(msg, len);
	Assert(msg->data);
	memcpy(msg->data, port->RecvBuffer + port->RecvPointer, len);
	port->RecvPointer += len;
	msg->len = len;
	msg->cursor = 5; /* skip message type and length */
	return true;
}

static int clean_connection(List *node_discard, const char *database, const char *user_name)
{
	DatabasePool *db_pool;
	PGXCNodePool *nodes_pool;
	ListCell *lc;
	HASH_SEQ_STATUS hash_db_status;
	int res;

	AssertArg(database);
	if(htab_database == NULL || node_discard == NIL)
		return CLEAN_CONNECTION_COMPLETED;

	res = CLEAN_CONNECTION_COMPLETED;

retry_clean_connection_:
	hash_seq_init(&hash_db_status, htab_database);
	while((db_pool = hash_seq_search(&hash_db_status)) != NULL)
	{
		if(strcmp(db_pool->db_info.database, database) != 0)
			continue;
		if(user_name && strcmp(db_pool->db_info.user_name, user_name) != 0)
			continue;

		foreach(lc, node_discard)
		{
			nodes_pool = hash_search(db_pool->htab_nodes, &(lfirst_oid(lc)), HASH_FIND, NULL);
			if(nodes_pool == NULL)
				continue;

			/* check slots is using in agents */
			if(node_pool_in_using(nodes_pool) == false)
			{
				destroy_node_pool(nodes_pool, true);
			}else
			{
				res = CLEAN_CONNECTION_NOT_COMPLETED;
			}
		}

		/* clean db pool if it's empty */
		if(hash_get_num_entries(db_pool->htab_nodes) == 0)
		{
			hash_seq_term(&hash_db_status);
			destroy_database_pool(db_pool, true);
			goto retry_clean_connection_;
		}
	}

	is_pool_locked = false;
	return res;
}

#ifdef ADB_DEBUG_CHECK_SLOT
static bool check_slot_status(PGXCNodePoolSlot *slot, bool re_connect)
{
	struct pollfd poll_fd;
	int rval;
	bool status_error;

	if(slot == NULL)
		return true;
	if(PQsocket((PGconn*)(slot->conn)) == PGINVALID_SOCKET)
	{
		status_error = true;
		goto end_check_slot_status_;
	}

	poll_fd.fd = PQsocket((PGconn*)(slot->conn));
	poll_fd.events = POLLIN|POLLPRI;

recheck_slot_status_:
	rval = poll(&poll_fd, 1, 0);
	CHECK_FOR_INTERRUPTS();

	status_error = false;
	if(rval == 0)
	{
		/* timeout */
		return true;
	}else if(rval < 0)
	{
		if(errno == EINTR
#if defined(EAGAIN) && (EAGAIN!=EINTR)
			|| errno == EAGAIN
#endif
			)
		{
			goto recheck_slot_status_;
		}
		ereport(WARNING, (errcode_for_socket_access(),
			errmsg("check_slot_status poll error:%m")));
		status_error = true;
	}else /* rval > 0 */
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("check_slot_status connect has unread data. last backend is %d", slot->last_user_pid)));
		status_error = true;
	}

end_check_slot_status_:
	if(status_error)
	{
		destroy_slot(slot, false);
		if(re_connect)
		{
			slot->conn = PGXCNodeConnect(slot->parent->connstr);
			if(PGXCNodeConnected(slot->conn))
			{
				slot->state = SLOT_STATE_IDLE;
				return true;
			}else
			{
				PGXCNodeClose(slot->conn);
				slot->conn = NULL;
				return false;
			}
		}
		return false;
	}
	return true;
}
#endif /* ADB_DEBUG_CHECK_SLOT */

/*
 * send transaction local commands if any, set the begin sent status in any case
 */
static int
send_local_commands(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	int			tmp;
	int			res;
	ListCell		*nodelist_item;
	PGXCNodePoolSlot	*slot;

	Assert(agent);

	res = 0;

	if (datanodelist != NULL)
	{
		res = list_length(datanodelist);
		if (res > 0 && agent->dn_connections == NULL)
			return 0;

		foreach(nodelist_item, datanodelist)
		{
			int	node = lfirst_int(nodelist_item);

			if(node < 0 || node >= agent->num_dn_connections)
				continue;

			slot = agent->dn_connections[node];

			if (slot == NULL)
				continue;

			if (agent->local_params != NULL)
			{
				tmp = PGXCNodeSendSetQuery(slot->conn, agent->local_params);
				res = res + tmp;
			}
		}
	}

	if (coordlist != NULL)
	{
		res = list_length(coordlist);
		if (res > 0 && agent->coord_connections == NULL)
			return 0;

		foreach(nodelist_item, coordlist)
		{
			int	node = lfirst_int(nodelist_item);

			if(node < 0 || node >= agent->num_coord_connections)
				continue;

			slot = agent->coord_connections[node];

			if (slot == NULL)
				continue;

			if (agent->local_params != NULL)
			{
				tmp = PGXCNodeSendSetQuery(slot->conn, agent->local_params);
				res = res + tmp;
			}
		}
	}

	if (res < 0)
		return -res;
	return 0;
}

/*------------------------------------------------*/

static void destroy_slot(PGXCNodePoolSlot *slot, bool send_cancel)
{
	AssertArg(slot);

	if(send_cancel)
	{
		if(slot->xc_cancelConn == NULL)
			slot->xc_cancelConn = PQgetCancel((PGconn*)slot->conn);
		if(slot->xc_cancelConn)
		{
			char err_msg[256];
			PQcancel(slot->xc_cancelConn, err_msg, sizeof(err_msg));
			/* ignore result */
		}
	}
	if(slot->xc_cancelConn)
	{
		PQfreeCancel(slot->xc_cancelConn);
		slot->xc_cancelConn = NULL;
	}
	if(slot->conn)
	{
		PGXCNodeClose(slot->conn);
		slot->conn = NULL;
	}
	slot->last_user_pid = 0;
	slot->last_agtm_port = 0;
	slot->state = SLOT_STATE_UNINIT;
}

static void release_slot(PGXCNodePoolSlot *slot, bool force_close)
{
	AssertArg(slot);
	if(force_close)
	{
		destroy_slot(slot, true);
	}else if(ADB_CHECK_SLOT(slot, false) == false)
	{
		if(slot->state != SLOT_STATE_UNINIT)
		{
			slot->state = SLOT_STATE_RELEASED;
			slot->released_time = time(NULL);
		}
	}else
	{
		slot->state = SLOT_STATE_RELEASED;
		slot->released_time = time(NULL);
	}
}

static void idle_slot(PGXCNodePoolSlot *slot)
{
	AssertArg(slot);
	if(pool_exec_set_query(slot->conn, "RESET ALL;") == false
		|| send_agtm_listen_port(slot->conn, 0) == false)
	{
		destroy_slot(slot, true);
	}else
	{
		slot->state = SLOT_STATE_IDLE;
		slot->last_user_pid = 0;
		slot->released_time = time(NULL);
		slot->last_agtm_port = 0;
	}
}

static void destroy_node_pool(PGXCNodePool *node_pool, bool bfree)
{
	Size i;
	AssertArg(node_pool);
	if(node_pool->slot)
	{
		for(i=0;i<(Size)MaxConnections;++i)
			destroy_slot(&(node_pool->slot[i]), false);
	}
	if(bfree)
	{
		Assert(node_pool->parent);
		hash_search(node_pool->parent->htab_nodes, &node_pool->nodeoid, HASH_REMOVE, NULL);
	}
}

static bool node_pool_in_using(PGXCNodePool *node_pool)
{
	Size i,j;
	PoolAgent *agent;
	AssertArg(node_pool);

	for(i=0;i<agentCount;++i)
	{
		agent = poolAgents[i];
		Assert(agent);
		for(j=0;j<agent->num_dn_connections;++j)
		{
			if(slot_in_node_pool(agent->dn_connections[j], node_pool))
				return true;
		}
		for(j=0;j<agent->num_coord_connections;++j)
		{
			if(slot_in_node_pool(agent->coord_connections[j], node_pool))
				return true;
		}
	}
	return false;
}

/*
 * close idle slots when slot->released_time <= timeout
 * return earliest idle slot
 */
static time_t close_timeout_idle_slots(time_t timeout)
{
	HASH_SEQ_STATUS hash_database_stats;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	PGXCNodePool *node_pool;
	PGXCNodePoolSlot *slot;
	Size i;
	time_t earliest_time = time(NULL);

	hash_seq_init(&hash_database_stats, htab_database);
	while((db_pool = hash_seq_search(&hash_database_stats)) != NULL)
	{
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			for(i=0;i<MaxConnections;++i)
			{
				slot = &(node_pool->slot[i]);
				if(slot->state != SLOT_STATE_IDLE)
					continue;
				if(slot->released_time <= timeout)
					destroy_slot(slot, false);
				else if(earliest_time > slot->released_time)
					earliest_time = slot->released_time;
			}
		}
	}
	return earliest_time;
}

/* find pool, if not exist create a new */
static DatabasePool *get_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
	DatabaseInfo info;
	DatabasePool * dbpool;
	bool found;

	AssertArg(database && user_name);
	Assert(htab_database);
	if(pgoptions == NULL)
		pgoptions = "";

	info.database = (char*)database;
	info.user_name = (char*)user_name;
	info.pgoptions = (char*)pgoptions;
	dbpool = hash_search(htab_database, &info, HASH_ENTER, &found);
	if(!found)
	{
		MemoryContext old_context;
		HASHCTL hctl;
		volatile DatabasePool tmp_pool;
		memset((void*)&tmp_pool, 0, sizeof(tmp_pool));
		PG_TRY();
		{
			old_context = MemoryContextSwitchTo(TopMemoryContext);
			tmp_pool.db_info.database = pstrdup(database);
			tmp_pool.db_info.user_name = pstrdup(user_name);
			tmp_pool.db_info.pgoptions = pstrdup(pgoptions);

			memset(&hctl, 0, sizeof(hctl));
			hctl.keysize = sizeof(Oid);
			hctl.entrysize = sizeof(PGXCNodePool);
			hctl.hash = oid_hash;
			hctl.hcxt = TopMemoryContext;
			tmp_pool.htab_nodes = hash_create("hash PGXCNodePool", 97, &hctl
				, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
#ifdef ADB_DEBUG_POOL
			tmp_pool.list_nodes = NIL;
			list_database = lappend(list_database, dbpool);
#endif /* ADB_DEBUG_POOL */
			(void)MemoryContextSwitchTo(old_context);
		}PG_CATCH();
		{
			destroy_database_pool((DatabasePool*)&tmp_pool, false);
			hash_search(htab_database, &info, HASH_REMOVE, NULL);
			PG_RE_THROW();
		}PG_END_TRY();
		memcpy(dbpool, (void*)&tmp_pool, sizeof(*dbpool));
	}
	return dbpool;
}

static void destroy_database_pool(DatabasePool *db_pool, bool bfree)
{
	DatabaseInfo info;
	if(db_pool == NULL)
		return;
	if(db_pool->htab_nodes)
	{
		PGXCNodePool *node_pool;
		HASH_SEQ_STATUS status;
		hash_seq_init(&status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&status)) != NULL)
			destroy_node_pool(node_pool, false);
		hash_destroy(db_pool->htab_nodes);
	}
#ifdef ADB_DEBUG_POOL
	list_free(db_pool->list_nodes);
#endif /* ADB_DEBUG_POOL */

	memcpy(&info, &(db_pool->db_info), sizeof(info));
	if(bfree)
	{
		hash_search(htab_database, &info, HASH_REMOVE, NULL);
#ifdef ADB_DEBUG_POOL
		list_database = list_delete_ptr(list_database, db_pool);
#endif /* ADB_DEBUG_POOL */
	}
	if(info.pgoptions)
		pfree(info.pgoptions);
	if(info.user_name)
		pfree(info.user_name);
	if(db_pool->db_info.database)
		pfree(info.database);
}

static bool slot_in_node_pool(const PGXCNodePoolSlot *slot, const PGXCNodePool *node_pool)
{
	return slot >= node_pool->slot && slot < &(node_pool->slot[MaxConnections]);
}

static void agent_release_connections(PoolAgent *agent, bool force_destroy)
{
	PGXCNodePoolSlot *slot;
	Size i;
	AssertArg(agent);
	for(i=0;i<agent->num_dn_connections;++i)
	{
		Assert(agent->dn_connections);
		slot = agent->dn_connections[i];
		if(slot)
		{
			if(slot->state == SLOT_STATE_LOCKED
				&& slot->last_user_pid == agent->pid)
			{
				release_slot(slot, force_destroy);
			}
			agent->dn_connections[i] = NULL;
		}
	}
	for(i=0;i<agent->num_coord_connections;++i)
	{
		Assert(agent->coord_connections);
		slot = agent->coord_connections[i];
		if(slot)
		{
			if(slot->state == SLOT_STATE_LOCKED
				&& slot->last_user_pid == agent->pid)
			{
				release_slot(slot, force_destroy);
			}
			agent->coord_connections[i] = NULL;
		}
	}
}

/* set agent's all slots to idle, include reset */
static void agent_idle_connections(PoolAgent *agent, bool force_destroy)
{
	PGXCNodePoolSlot *slot;
	Size i;
	AssertArg(agent);
	for(i=0;i<agent->num_dn_connections;++i)
	{
		Assert(agent->dn_connections);
		slot = agent->dn_connections[i];
		if(slot)
		{
			if(slot->state == SLOT_STATE_LOCKED
				&& slot->last_user_pid == agent->pid)
			{
				idle_slot(slot);
			}
			agent->dn_connections[i] = NULL;
		}
	}
	for(i=0;i<agent->num_coord_connections;++i)
	{
		Assert(agent->coord_connections);
		slot = agent->coord_connections[i];
		if(slot)
		{
			if(slot->state == SLOT_STATE_LOCKED
				&& slot->last_user_pid == agent->pid)
			{
				idle_slot(agent->coord_connections[i]);
			}
			agent->coord_connections[i] = NULL;
		}
	}
}
static bool agent_acquire_conn_list(PGXCNodePoolSlot **slots, const Oid *oids, const List *node_list, PoolAgent *agent)
{
	ListCell *lc;
	PGXCNodePoolSlot *slot;
	Size retry_count;

	if(node_list == NIL)
		return true;
	AssertArg(slots && oids && agent);

	retry_count = 0;
	foreach(lc, node_list)
	{
		slot = slots[lfirst_int(lc)];
		if(ADB_CHECK_SLOT(slot, false) == false)
			slot = slots[lfirst_int(lc)] = NULL;
		if(slot == NULL || (slot->state == SLOT_STATE_LOCKED)
			|| (slot->state == SLOT_STATE_IDLE) )
		{
#ifdef USE_ASSERT_CHECKING
			if(slot && slot->state == SLOT_STATE_LOCKED)
				Assert(slot->last_user_pid != agent->pid);
#endif
retry_get_connection_:
			if(retry_count)
				pg_usleep(500*1000);
			if((++retry_count) > 10)
				return false;

			if(slot == NULL || slot->state != SLOT_STATE_IDLE)
			{
				slot = acquire_connection(agent->db_pool, oids[lfirst_int(lc)]);
				if(slot == NULL)
					return false;
			}
			if(slot->state == SLOT_STATE_RELEASED 
				|| slot->state == SLOT_STATE_IDLE)
			{
				slot->last_user_pid = 0;
				/* we need reset it and send agtm listen port */

				if((agent->agtm_port != slot->last_agtm_port && send_agtm_listen_port(slot->conn, agent->agtm_port) == false)
					|| pool_exec_set_query(slot->conn, "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;") == false
					|| (agent->session_params && pool_exec_set_query(slot->conn, agent->session_params) == false))
				{
					destroy_slot(slot, true);
					goto retry_get_connection_;
				}
				slot->last_agtm_port = agent->agtm_port;
				slot->state = SLOT_STATE_RELEASED;
			}else
			{
				Assert(0);
			}
			slots[lfirst_int(lc)] = slot;
		}
		slot = slots[lfirst_int(lc)];
		Assert(slot && slot->state == SLOT_STATE_RELEASED && slot->conn);
		if(slot->last_user_pid != agent->pid)
		{
			if(agent->local_params 
				&& pool_exec_set_query(slot->conn, agent->local_params) == false)
			{
				destroy_slot(slot, true);
				slots[lfirst_int(lc)] = NULL;
				goto retry_get_connection_;
			}
			slot->last_user_pid = agent->pid;
		}
		if(agent->agtm_port != slot->last_agtm_port)
		{
			if(send_agtm_listen_port(slot->conn, agent->agtm_port) == false)
			{
				destroy_slot(slot, true);
				goto retry_get_connection_;
			}
			slot->last_agtm_port = agent->agtm_port;
		}
	}

	return true;
}

static void agent_lock_connect_list(PGXCNodePoolSlot **slots, const List *node_list, int *fds, const PoolAgent *agent)
{
	const ListCell *lc;
	PGXCNodePoolSlot *slot;
	Size i;

	AssertArg(agent && fds);
	if(node_list == NIL)
		return;
	AssertArg(slots);

	i = 0;
	foreach(lc,node_list)
	{
		slot = slots[lfirst_int(lc)];
		Assert(slot && slot->conn 
			&& slot->state == SLOT_STATE_RELEASED
			&& slot->last_user_pid == agent->pid);

		fds[i] = PQsocket((PGconn *)(slot->conn));
		slot->state = SLOT_STATE_LOCKED;
		++i;
	}
}

static int *agent_acquire_connections(PoolAgent *agent, const List *datanodelist, const List *coordlist)
{
	int *result;
	AssertArg(agent);

	/* Check if pooler can accept those requests */
	if (list_length(datanodelist) > agent->num_dn_connections ||
			list_length(coordlist) > agent->num_coord_connections)
	{
		ereport(ERROR, (errmsg("invalid connection id form backend %d", agent->pid)));
	}

	result = palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));

	if(agent_acquire_conn_list(agent->dn_connections, agent->datanode_oids, datanodelist, agent) == false
		|| agent_acquire_conn_list(agent->coord_connections, agent->coord_oids, coordlist, agent) == false)
	{
		pfree(result);
		return NULL;
	}

	agent_lock_connect_list(agent->dn_connections, datanodelist, result, agent);
	agent_lock_connect_list(agent->coord_connections, coordlist, &(result[list_length(datanodelist)]), agent);
	return result;
}

static void cancel_query_on_connections(PoolAgent *agent, Size count, PGXCNodePoolSlot **slots, const List *nodelist)
{
	const ListCell *lc;
	PGXCNodePoolSlot *slot;
	Size node_idx;
	char errbuf[256];

	if(agent == NULL || slots == NULL || nodelist == NIL)
		return;

	foreach(lc, nodelist)
	{
		node_idx = (Size)lfirst_int(lc);

		if(node_idx > count)
			continue;
		slot = agent->dn_connections[node_idx];
		if(slot == NULL)
			continue;
		/* need an error ? */
		if(slot->last_user_pid != agent->pid || slot->state != SLOT_STATE_LOCKED)
			continue;

		if(slot->xc_cancelConn == NULL)
			slot->xc_cancelConn = PQgetCancel((PGconn*)slot->conn);
		if(PQcancel(slot->xc_cancelConn, errbuf, sizeof(errbuf)) == false)
		{
			ereport(WARNING, (errmsg("cancel query remote query failed:%s", errbuf)));
		}
	}
}

/*
 * Rebuild information of database pools
 */
static void reload_database_pools(PoolAgent *agent)
{
	HASH_SEQ_STATUS hash_database_status;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	PGXCNodePool *node_pool;
	char *connstr;

	/*
	 * Release node connections if any held. It is not guaranteed client session
	 * does the same so don't ever try to return them to pool and reuse
	 */
	agent_release_connections(agent, false);

	/* realloc */
	PFREE_SAVE(agent->datanode_oids);
	PFREE_SAVE(agent->dn_connections);
	PFREE_SAVE(agent->coord_oids);
	PFREE_SAVE(agent->coord_connections);
	{
		int num_datanode,num_coord;
		MemoryContext old_context = MemoryContextSwitchTo(agent->mctx);
		PgxcNodeGetOids(&agent->coord_oids, &agent->datanode_oids,
			&num_coord, &num_datanode, false);
		agent->num_coord_connections = num_coord;
		agent->num_dn_connections = num_datanode;
		agent->coord_connections = (PGXCNodePoolSlot**)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot*));
		agent->dn_connections = (PGXCNodePoolSlot**)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot*));
		(void)MemoryContextSwitchTo(old_context);
	}

	/*
	 * Scan the list and destroy any altered pool. They will be recreated
	 * upon subsequent connection acquisition.
	 */
	hash_seq_init(&hash_database_status, htab_database);
	while((db_pool = hash_seq_search(&hash_database_status)) != NULL)
	{
recheck_node_pool_:
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			connstr = build_node_conn_str(node_pool->nodeoid, db_pool);
			/* Node has been removed or altered */
			if((connstr == NULL || strcmp(connstr, node_pool->connstr) != 0)
				/* and node pool not in using */
				&& node_pool_in_using(node_pool) == false)
			{
				PFREE_SAVE(connstr);
				destroy_node_pool(node_pool, true);
				hash_seq_term(&hash_nodepool_status);
				goto recheck_node_pool_;
			}
			PFREE_SAVE(connstr);
		}
	}
}

/*
 * Check connection info consistency with system catalogs
 */
static int node_info_check(PoolAgent *agent)
{
	HASH_SEQ_STATUS hash_database_status;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	PGXCNodePool *node_pool;
	List		 *checked_oids;
	Oid			 *coOids;
	Oid			 *dnOids;
	char		 *connstr;
	int			numCo;
	int			numDn;
	int res;

	/*
	 * First check if agent's node information matches to current content of the
	 * shared memory table.
	 */
	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	res = POOL_CHECK_SUCCESS;
	if (agent->num_coord_connections != (Size)numCo ||
			agent->num_dn_connections != (Size)numDn ||
			memcmp(agent->coord_oids, coOids, numCo * sizeof(Oid)) ||
			memcmp(agent->datanode_oids, dnOids, numDn * sizeof(Oid)))
	{
		res = POOL_CHECK_FAILED;
	}
	pfree(coOids);
	pfree(dnOids);
	if(res != POOL_CHECK_SUCCESS)
		return res;

	checked_oids = NIL;
	hash_seq_init(&hash_database_status, htab_database);
	while((db_pool = hash_seq_search(&hash_database_status)) != NULL)
	{
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			if(list_member_oid(checked_oids, node_pool->nodeoid))
				continue;

			connstr = build_node_conn_str(node_pool->nodeoid, db_pool);
			if(connstr == NULL || strcmp(connstr, node_pool->connstr) != 0)
			{
				if(connstr)
					pfree(connstr);
				hash_seq_term(&hash_nodepool_status);
				hash_seq_term(&hash_database_status);
				res = POOL_CHECK_FAILED;
				goto node_info_check_end_;
			}
			PFREE_SAVE(connstr);
			checked_oids = lappend_oid(checked_oids, node_pool->nodeoid);
		}
	}
node_info_check_end_:
	list_free(checked_oids);
	return res;
}

static int agent_session_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type)
{
	char **ppstr;
	Size i;
	int res;
	AssertArg(agent);
	if(command_type == POOL_CMD_TEMP)
	{
		agent->is_temp = true;
		return 0;
	}else if(set_command == NULL)
	{
		return 0;
	}else if(command_type != POOL_CMD_LOCAL_SET
		&& command_type != POOL_CMD_GLOBAL_SET)
	{
		return -1;
	}
	Assert(set_command && (command_type == POOL_CMD_LOCAL_SET || command_type == POOL_CMD_GLOBAL_SET));

	/* params = "params;set_command" */
	if(command_type == POOL_CMD_LOCAL_SET)
	{
		ppstr = &(agent->local_params);
	}else
	{
		ppstr = &(agent->session_params);
	}
	if(*ppstr == NULL)
	{
		*ppstr = MemoryContextStrdup(agent->mctx, set_command);
	}else
	{
		*ppstr = repalloc(*ppstr, strlen(*ppstr) + strlen(set_command) + 2);
		strcat(*ppstr, ";");
		strcat(*ppstr, set_command);
	}

	/*
	 * Launch the new command to all the connections already hold by the agent
	 * It does not matter if command is local or global as this has explicitely been sent
	 * by client. PostgreSQL backend also cannot send to its pooler agent SET LOCAL if current
	 * transaction is not in a transaction block. This has also no effect on local Coordinator
	 * session.
	 */
	res = 0;
	for(i=0;i<agent->num_dn_connections;++i)
	{
		if(agent->dn_connections[i] && agent->dn_connections[i]->conn)
			res |= PGXCNodeSendSetQuery(agent->dn_connections[i]->conn, set_command);
	}
	for (i = 0; i < agent->num_coord_connections; i++)
	{
		if (agent->coord_connections[i] && agent->coord_connections[i]->conn)
			res |= PGXCNodeSendSetQuery(agent->coord_connections[i]->conn, set_command);
	}
	return res;
}

static uint32 hash_database_info(const void *key, Size keysize)
{
	const DatabaseInfo *info = key;
	Datum datums[3];
	AssertArg(info != NULL && keysize == sizeof(*info));
	datums[0] = hash_any((const unsigned char*)info->database, strlen(info->database));
	datums[1] = hash_any((const unsigned char*)info->user_name, strlen(info->user_name));
	datums[2] = hash_any((const unsigned char*)info->pgoptions, strlen(info->pgoptions));
	return DatumGetUInt32(hash_any((const unsigned char*)datums, sizeof(datums)));
}

static int match_database_info(const void *key1, const void *key2, Size keysize)
{
	const DatabaseInfo *l,*r;
	int rval;
	Assert(keysize == sizeof(DatabaseInfo));

	l = key1;
	r = key2;

	rval = strcmp(l->database, r->database);
	if(rval != 0)
		return rval;

	rval = strcmp(l->user_name, r->user_name);
	if(rval != 0)
		return rval;

	rval = strcmp(l->pgoptions, r->pgoptions);
	if(rval != 0)
		return rval;
	return 0;
}

static void create_htab_database(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(DatabaseInfo);
	hctl.entrysize = sizeof(DatabasePool);
	hctl.hash = hash_database_info;
	hctl.match = match_database_info;
	hctl.hcxt = TopMemoryContext;
	htab_database = hash_create("hash DatabasePool", 97, &hctl
			, HASH_ELEM|HASH_FUNCTION|HASH_COMPARE|HASH_CONTEXT);
#ifdef ADB_DEBUG_POOL
	list_database = NIL;
#endif /* ADB_DEBUG_POOL */
}

static void destroy_htab_database(void)
{
	DatabasePool *pool;
	HASH_SEQ_STATUS status;

	if(htab_database == NULL)
		return;

	for(;;)
	{
		hash_seq_init(&status, htab_database);
		pool = hash_seq_search(&status);
		if(pool == NULL)
			break;
		hash_seq_term(&status);
		destroy_database_pool(pool, true);
	}
	hash_destroy(htab_database);
	htab_database = NULL;

#ifdef ADB_DEBUG_POOL
	list_free(list_database);
	list_database = NIL;
#endif /* ADB_DEBUG_POOL */
}

/*---------------------------------------------------------------------------*/

static void pool_end_flush_msg(PoolPort *port, StringInfo buf)
{
	AssertArg(port && buf);
	pool_putmessage(port, (char)(buf->cursor), buf->data, buf->len);
	pfree(buf->data);
	pool_flush(port);
}

static void pool_sendstring(StringInfo buf, const char *str)
{
	if(str)
	{
		pq_sendbyte(buf, false);
		pq_sendbytes(buf, str, strlen(str)+1);
	}else
	{
		pq_sendbyte(buf, true);
	}
}

static const char *pool_getstring(StringInfo buf)
{
	char *str;
	int slen;
	AssertArg(buf);

	if(pq_getmsgbyte(buf))
		return NULL;
	str = &buf->data[buf->cursor];
	slen = strlen(str);
	if (buf->cursor + slen >= buf->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	buf->cursor += slen + 1;
	return str;
}

static void pool_sendint(StringInfo buf, int ival)
{
	pq_sendbytes(buf, (char*)&ival, 4);
}

static int pool_getint(StringInfo buf)
{
	int ival;
	pq_copymsgbytes(buf, (char*)&ival, 4);
	return ival;
}

static void pool_sendint_array(StringInfo buf, int count, const int *arr)
{
	int i;
	AssertArg(count >= 0);
	pq_sendbytes(buf, (char*)&count, 4);
	for(i=0;i<count;++i)
		pq_sendbytes(buf, (char*)&(arr[i]), 4);
}

static void pool_send_nodeid_list(StringInfo buf, const List *list)
{
	const ListCell *lc;
	pool_sendint(buf, list_length(list));
	foreach(lc, list)
		pool_sendint(buf, lfirst_int(lc));
}

static List* pool_get_nodeid_list(StringInfo buf)
{
	List *list = NIL;
	int int_val,count;
	pq_copymsgbytes(buf, (char*)&count, sizeof(count));
	for(;count>0;--count)
	{
		pq_copymsgbytes(buf, (char*)&int_val, sizeof(int_val));
		list = lappend_int(list, int_val);
	}
	return list;
}

static void on_exit_pooler(int code, Datum arg)
{
	closesocket(server_fd);
	/* destroy agents and MemoryContext*/
	destroy_htab_database();
}

static bool send_agtm_listen_port(NODE_CONNECTION *conn, int port)
{
	PGresult *res;
	ExecStatusType state;
	bool success;
	if(pqSendAgtmListenPort((PGconn*)conn, port) < 0
		|| pool_wait_pq((PGconn*)conn) < 0)
	{
		return false;
	}

	res = PQexecFinish((PGconn*)conn);
	state = PQresultStatus(res);
	success = true;
	if(state != PGRES_COMMAND_OK
		|| atoi(PQcmdStatus(res)) != port)
	{
		success = false;
	}
	PQclear(res);
	return success;
}

static bool pool_exec_set_query(NODE_CONNECTION *conn, const char *query)
{
	PGresult *result;
	bool res;

	AssertArg(query);
	if(!PQsendQuery((PGconn*)conn, query))
		return false;

	res = true;
	for(;;)
	{
		if(pool_wait_pq((PGconn*)conn) < 0)
		{
			res = false;
			break;
		}
		result = PQgetResult((PGconn*)conn);
		if(result == NULL)
			break;
		if(PQresultStatus(result) == PGRES_FATAL_ERROR)
			res = false;
		PQclear(result);
	}
	return res;
}

/* return
 * < 0: EOF or timeout
 */
static int pool_wait_pq(PGconn *conn)
{
	time_t finish_time;
	AssertArg(conn);

	if(conn->inEnd > conn->inStart)
		return 1;

	if(PoolRemoteCmdTimeout == 0)
	{
		finish_time = (time_t)-1;
	}else
	{
		Assert(PoolRemoteCmdTimeout > 0);
		finish_time = time(NULL);
		finish_time += PoolRemoteCmdTimeout;
	}
	return pqWaitTimed(1, 0, conn, finish_time);
}
