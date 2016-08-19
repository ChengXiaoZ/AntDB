
#include "postgres.h"

#include "pgxc/adbnode.h"
#include "pgxc/poolmgr.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#elif defined(HAVE_SYS_POLL_H)
#include <sys/poll.h>
#endif

static AdbNodeConnInfo *datanode_conns = NULL;
static AdbNodeConnInfo *coord_conns = NULL;
static AdbNodeConnInfo *all_node_conns = NULL;
List *intlist_datanodes = NULL;
List *intlist_coord = NULL;

volatile int NumDataNodes;
volatile int NumCoords;

extern int	MyProcPid;

static void adb_node_free_all(void);
static void adb_attach_conns(List *list, const int *fds);

/* custom parse PQ functions */
static int noneGetRowDesc(PGconn *conn, int msgLength);
static int noneGetAnotherTuple(PGconn *conn, int msgLength);
static int noneGetCopyData(PGconn *conn);
static const PGcustumFuns pq_none_funs =
{
	noneGetRowDesc,
	noneGetAnotherTuple
};

/*
 * Allocate and initialize memory to store Datanode and Coordinator handles.
 */
void
InitMultinodeExecutor(bool is_force)
{
	int				i,count;
	Oid				*coOids = NULL;
	Oid				*dnOids = NULL;
	char			*nodeName;
	MemoryContext	old_context;

	/* Free all the existing information first */
	if (is_force)
		adb_node_free_all();

	if (datanode_conns != NULL && coord_conns != NULL)
		return;

	/* Update node table in the shared memory */
	PgxcNodeListAndCount();

	/* Get classified list of node Oids */
	PgxcNodeGetOids(&coOids, &dnOids, (int*)&NumCoords, (int*)&NumDataNodes, true);
	Assert(NumCoords >= 0 && NumDataNodes >= 0);

	count = NumCoords + NumDataNodes;

	PG_TRY();
	{
		old_context  = MemoryContextSwitchTo(TopMemoryContext);
		all_node_conns = palloc0(sizeof(all_node_conns[0]) * count);

		PGXCNodeId = -1;
		datanode_conns = NumDataNodes > 0 ? all_node_conns:NULL;
		for(i=0;i<NumDataNodes;++i)
		{
			datanode_conns[i].node_type = ADB_NODE_TYPE_DATA;
			datanode_conns[i].node_oid = dnOids[i];
			nodeName = get_pgxc_nodename(dnOids[i]);
			namestrcpy(&(datanode_conns[i].node_name), nodeName);
			intlist_datanodes = lappend_int(intlist_datanodes, i);
		}
		coord_conns = NumCoords > 0 ? &(all_node_conns[NumDataNodes]) : NULL;
		for(i=0;i<NumCoords;++i)
		{
			coord_conns[i].node_type = ADB_NODE_TYPE_COORD;
			coord_conns[i].node_oid = coOids[i];
			nodeName = get_pgxc_nodename(coOids[i]);
			namestrcpy(&(coord_conns[i].node_name), nodeName);
			if(PGXCNodeId == 0 && pg_strcasecmp(PGXCNodeName, nodeName) == 0)
			{
				PGXCNodeId = i+1;
				PGXCNodeOid = coOids[i];
			}
			intlist_coord = lappend_int(intlist_coord, i);
		}

		/*
		 * No node-self?
		 * PGXCTODO: Change error code
		 */
		if (PGXCNodeId == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("Coordinator cannot identify itself")));
		(void)MemoryContextSwitchTo(old_context);
	}PG_CATCH();
	{
		adb_node_free_all();
		PG_RE_THROW();
	}PG_END_TRY();

	if (coOids)
		pfree(coOids);
	if (dnOids)
		pfree(dnOids);

	datanode_count = 0;
	coord_count = 0;
}

/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Clean up prepared transactions before releasing connections */
	DropAllPreparedStatements();

	/* Release Datanode connections */
	release_handles();

	/* Disconnect from Pooler */
	PoolManagerDisconnect();

	/* Close connection with AGTM */
	agtm_Close();
}

ADBNodeAllConn *adb_get_connections(List *datanodelist, List *coordlist, int primary_node)
{
	ADBNodeAllConn	*all_conn;
	ListCell		*lc;
	List			*dn_allocate = NIL;
	List			*co_allocate = NIL;
	int 			i,n,count;

	count = list_length(datanodelist) + list_length(coordlist);
	if(primary_node >= 0 && list_member_int(datanodelist, primary_node) == false)
		++count;

	all_conn = palloc(offsetof(ADBNodeAllConn, all_conns) + sizeof(all_conn->all_conns[0]) * count);
	all_conn->primary_conn = NULL;

	all_conn->co_count = list_length(coordlist);
	all_conn->co_conns = all_conn->co_count > 0 ? all_conn->all_conns : NULL;

	all_conn->dn_count = list_length(datanodelist);
	all_conn->dn_conns = all_conn->dn_count > 0 ? &(all_conn->all_conns[all_conn->co_count]) : NULL;

	for(lc=list_head(datanodelist),i=0; lc != NULL; lc=lnext(lc),++i)
	{
		n = lfirst_int(lc);
		if(n < 0 || n >= NumDataNodes)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Invalid datanode number \"%d\"", n)));
		}
		all_conn->dn_conns[i] = &datanode_conns[n];
		if(datanode_conns[n].node_conn == NULL)
			dn_allocate = lappend_int(dn_allocate, n);
	}
	if(primary_node >= 0)
	{
		if(primary_node >= NumDataNodes)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Invalid primary datanode number \"%d\"", n)));
		}
		all_conn->primary_conn = &datanode_conns[primary_node];
		if(all_conn->primary_conn->node_conn == NULL
			&& list_member_int(primary_node) == false)
		{
			dn_allocate = lappend_int(dn_allocate, primary_node);
		}
	}

	for(lc=list_head(coordlist),i=0;lc!=NULL;lc=lnext(lc),++i)
	{
		n = lfirst_int(lc);
		if(n < 0 || n >= NumCoords)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Invalid coordinator number \"%d\"", n)));
		}
		all_conn->co_conns[i] = &coord_conns[n];
		if(coord_conns[i].node_conn == NULL)
			co_allocate = lappend_int(co_allocate, n);
	}

	if(dn_allocate || co_allocate)
	{
		int *fds;
		List *list_new;
		AdbNodeConnInfo *node;

		fds = PoolManagerGetConnections(dn_allocate, co_allocate);
		Assert(fds);

		list_new = NIL;
		foreach(lc, dn_allocate)
		{
			n = lfirst_int(lc);
			Assert(n >= 0 && n < NumDataNodes);
			Assert(datanode_conns[n].node_conn == NULL);
			list_new = lappend(list_new, &(datanode_conns[n]));
		}
		foreach(lc, co_allocate)
		{
			n = lfirst_int(lc);
			Assert(n >= 0 && n < NumCoords);
			Assert(coord_conns[n].node_conn == NULL);
			list_new = lappend(list_new, &(coord_conns[n]));
		}

		adb_attach_conns(list_new, fds);
		pfree(fds);
		list_free(list_new);
	}
	return all_conn;
}

void pfree_adb_all_connections(ADBNodeAllConn *handles)
{
	if(handles == NULL)
		return;
	pfree(handles);
}

void adb_release_connections(void)
{
	AdbNodeConnInfo *conn;
	int count;
	bool have_connected = false;

	for(count = NumCoords + NumDataNodes;count--;)
	{
		conn = &all_node_conns[count];
		if(conn->node_conn == NULL)
		{
			Assert(conn->node_cancel == NULL);
			continue;
		}
		have_connected = true;
		if(conn->node_cancel)
		{
			PQfreeCancel(conn->node_cancel);
			conn->node_cancel = NULL;
		}
		PQdetach(conn->node_conn);
		conn->node_conn = NULL;
	}
	if(have_connected)
		PoolManagerReleaseConnections(false);
}

void adb_cancel_query(void)
{
	AdbNodeConnInfo *conn;
	char err_msg[128];
	for(count = NumCoords + NumDataNodes;count--;)
	{
		conn = &all_node_conns[count];
		if(conn->node_conn == NULL)
			continue;
		if(!PQrequestCancel(conn->node_conn))
			ereport(COMMERROR, (errmsg("%s", PQerrorMessage(conn->node_conn))));
	}
}

void adb_clear_connections(void)
{
	AdbNodeConnInfo *conn;
	List *list;
	PGresult *res;
	int count;

	for(list=NIL,count = NumCoords+NumDataNodes;count--;)
	{
		conn = &all_node_conns[count];
		if(conn->node_conn == NULL
			|| !PQflush(conn->node_conn)
			|| conn->node_conn->asyncStatus == PGASYNC_IDLE)
			continue;
		switch(conn->node_conn->asyncStatus)
		{
		case PGASYNC_IDLE:
			continue;
		case PGASYNC_COPY_IN:
		case PGASYNC_COPY_BOTH:
			PQputCopyEnd(conn->node_conn, NULL);
			break;
		case PGASYNC_COPY_OUT:
			PQendcopy(conn->node_conn);
			break;
		default:
			break;
		}
		conn->node_conn->funs = pq_none_funs; /* just dorp recv data */
		PQsetSingleRowMode(conn->node_conn); /* ignore result */
		list = lappend(list, conn);
	}
	if(list == NIL)
		return;

	HOLD_CANCEL_INTERRUPTS();
	while((res = adb_get_connections(list, &conn)) != NULL)
	{
		if(PQresultStatus(res) == PGRES_COPY_OUT)
			noneGetCopyData(conn->node_conn);
		PQclear(res);
	}
	list_free(list);
	RESUME_CANCEL_INTERRUPTS();
}

PGresult* adb_get_result(const List *list, AdbNodeConnInfo **ppconn_from)
{
	AdbNodeConnInfo *conn;
	ListCell *lc;
	List *query_list;
	pollfd *pfds;
	int i,count;
	#error
}

/*----------------------------------------static functions--------------------*/

static void adb_node_free_all(void)
{
	PGconn *conn;
	int i,count;

	coord_conns = datanode_conns = NULL;
	count = NumCoords + NumDataNodes;
	NumCoords = NumDataNodes = 0;
	datanode_count = coord_count = 0;

	if(all_node_conns)
	{
		for(i=0;i<count;++i)
		{
			conn = all_node_conns[i].node_conn;
			if(conn != NULL)
			{
				if(conn->custom)
					pfree(conn->custom);
				PQdetach(conn);
				all_node_conns[i].node_conn = NULL;
			}
		}
		pfree(all_node_conns);
		all_node_conns = NULL;
	}
	if(intlist_datanodes)
	{
		list_free(intlist_datanodes);
		intlist_datanodes = NIL;
	}
	if(intlist_coord)
	{
		list_free(intlist_coord);
		intlist_coord = NIL;
	}
}

static void adb_attach_conns(List *list, const int *fds)
{
	AdbNodeConnInfo *conn;
	pollfd *poll_fds;
	PostgresPollingStatusType *poll_status;
	ListCell *lc,*lc_next;
	List *connecting_list;
	StringInfoData err_msg;
	int i,count,res;

	Assert(list != NIL && fds != NULL);

	/* start attach */
	err_msg.data = NULL;
	for(lc=list_head(list),i=0,connecting_list=NIL;lc!=NULL;lc=lnext(lc),i++)
	{
		conn = lfirst(lc);
		Assert(conn->node_conn == NULL);
		Assert(fds[i] != PGINVALID_SOCKET);
		conn->node_conn = PQbeginAttach(fds[i], NULL, true);
		if(PQstatus(conn->node_conn) == CONNECTION_BAD)
		{
			if(err_msg.data == NULL)
				initStringInfo(&err_msg);
			if(conn->node_conn == NULL)
				appendStringInfoString(&errMsg, "out of memory.\n");
			else
				appendStringInfoString(&errmsg, PQerrorMessage(conn->node_conn));
			break;
		}
		connecting_list = lappend(connecting_list, conn);
	}

	/* begin wait attach done */
	count = list_length(connecting_list);
	if(count == 0)
	{
		Assert(err_msg.data != NULL);
		ereport(ERROR, (errmsg("%s", err_msg.data)));
	}
	poll_fds = palloc(sizeof(poll_fds[0])*count);
	poll_status = palloc(sizeof(poll_status[0])*count);
	for(i=0;i<count;++i)
		poll_status[i] = PGRES_POLLING_READING;

	/* wait attach done */
	HOLD_CANCEL_INTERRUPTS();
	while(connecting_list != NIL)
	{
		for(i=0,lc=list_head(connecting_list);lc!=NULL;lc=lnext(lc),++i)
		{
			conn = lfirst(lc);
			poll_fds[i].fd = PQsocket(conn->node_conn);
			if(poll_status[i] == PGRES_POLLING_READING)
				poll_fds[i].events = POLLIN;
			else if(poll_status[i] == PGRES_POLLING_WRITING)
				poll_fds[i].events = POLLOUT;
			else
				Assert(0);
			poll_fds[i].revents = 0;
		}
		count = list_length(connecting_list);

re_poll_:
		res = poll(poll_fds, count, -1);
		if(res < 0)
		{
			if(errno == EINTR)
				goto re_poll_;
			ereport(ERROR,
				(errcode_for_socket_access(),
				errmsg("can not poll socket")));
		}

		for(i=0,lc=list_head(connecting_list);lc!=NULL;lc=lnext(lc),++i)
		{
re_check_:
			conn = lfirst(lc);
			Assert(poll_fds[i].fd == PQsocket(conn->node_conn));
			if(poll_fds[i].revents == 0)
				continue;

			poll_status[i] = PQconnectPoll(conn->node_conn);
			if(poll_status[i] == PGRES_POLLING_READING
				|| poll_status[i] == PGRES_POLLING_WRITING)
				continue;

			Assert(poll_status[i] == PGRES_POLLING_OK
				|| poll_status[i] == PGRES_POLLING_FAILED);
			if(poll_status[i] == PGRES_POLLING_FAILED)
			{
				if(err_msg.data == NULL)
					initStringInfo(&err_msg);
				appendStringInfoString(&err_msg, "[%s]%s"
					, NameStr(conn->node_name)
					, PQerrorMessage(conn->node_conn));
				if(err_msg.data[err_msg.len-1] != '\n')
					appendStringInfoChar(&err_msg, '\n');
			}
			lc_next = lnext(lc);
			if(lc_next == NULL)
				break;

			connecting_list = list_delete_cell(connecting_list, lc_next, lc);
			count = list_length(connecting_list) - i;
			--count;
			memmove(&poll_fds[i], &poll_fds[i+1], sizeof(poll_fds[0])*count);
			memmove(&poll_status[i], &poll_status[i+1], sizeof(poll_status[0])*count);
			lc = lnext(lc);
			goto re_check_;
		}
	}
	if(err_msg.data)
		ereport(ERROR, (errmsg("%s", err_msg.data)));
	RESUME_CANCEL_INTERRUPTS();
	pfree(poll_fds);
	pfree(poll_status);
}

static int noneGetRowDesc(PGconn *conn, int msgLength)
{
	PGresult   *result;
	const char *errmsg;
	PQExpBufferData pq_buf;
	int			i;
	int			nfields;

	/*
	 * When doing Describe for a prepared statement, there'll already be a
	 * PGresult created by getParamDescriptions, and we should fill data into
	 * that.  Otherwise, create a new, empty PGresult.
	 */
	if (conn->queryclass == PGQUERY_DESCRIBE)
	{
		if (conn->result)
			result = conn->result;
		else
			result = PQmakeEmptyPGresult(conn, PGRES_COMMAND_OK);
	}
	else
		result = PQmakeEmptyPGresult(conn, PGRES_TUPLES_OK);
	pq_buf.data = NULL;
	if (!result)
	{
		errmsg = NULL;			/* means "out of memory", see below */
		goto advance_and_error;
	}

	/* parseInput already read the 'T' label and message length. */
	/* the next two bytes are the number of fields */
	if (pqGetInt(&(result->numAttributes), 2, conn))
	{
		/* We should not run out of data here, so complain */
		errmsg = libpq_gettext("insufficient data in \"T\" message");
		goto advance_and_error;
	}
	nfields = result->numAttributes;

	/* allocate space for the attribute descriptors */
	if (nfields > 0)
	{
		result->attDescs = (PGresAttDesc *)
			pqResultAlloc(result, nfields * sizeof(PGresAttDesc), TRUE);
		if (!result->attDescs)
		{
			errmsg = NULL;		/* means "out of memory", see below */
			goto advance_and_error;
		}
		MemSet(result->attDescs, 0, nfields * sizeof(PGresAttDesc));
		initPQExpBuffer(&pq_buf);
	}

	/* result->binary is true only if ALL columns are binary */
	result->binary = (nfields > 0) ? 1 : 0;

	/* get type info */
	for (i = 0; i < nfields; i++)
	{
		int			tableid;
		int			columnid;
		int			typid;
		int			typlen;
		int			atttypmod;
		int			format;

		resetPQExpBuffer(&pq_buf);
		if (pqGets(&conn->workBuffer, conn) ||
			pqGets(&pq_buf, conn) ||
			pqGetInt(&tableid, 4, conn) ||
			pqGetInt(&columnid, 2, conn) ||
			pqGetInt(&typid, 4, conn) ||
			pqGetInt(&typlen, 2, conn) ||
			pqGetInt(&atttypmod, 4, conn) ||
			pqGetInt(&format, 2, conn))
		{
			/* We should not run out of data here, so complain */
			errmsg = libpq_gettext("insufficient data in \"T\" message");
			goto advance_and_error;
		}

		/*
		 * Since pqGetInt treats 2-byte integers as unsigned, we need to
		 * coerce these results to signed form.
		 */
		columnid = (int) ((int16) columnid);
		typlen = (int) ((int16) typlen);
		format = (int) ((int16) format);

		result->attDescs[i].name = pqResultStrdup(result,
												  conn->workBuffer.data);
		if (!result->attDescs[i].name)
		{
			errmsg = NULL;		/* means "out of memory", see below */
			goto advance_and_error;
		}
		result->attDescs[i].tableid = tableid;
		result->attDescs[i].columnid = columnid;
		result->attDescs[i].format = format;
		result->attDescs[i].typid = typid;
		result->attDescs[i].typlen = typlen;
		result->attDescs[i].atttypmod = atttypmod;

		if (format != 1)
			result->binary = 0;
	}

	/* Sanity check that we absorbed all the data */
	if (conn->inCursor != conn->inStart + 5 + msgLength)
	{
		errmsg = libpq_gettext("extraneous data in \"T\" message");
		goto advance_and_error;
	}

	/* Success! */
	conn->result = result;

	/* Advance inStart to show that the "T" message has been processed. */
	conn->inStart = conn->inCursor;

	/*
	 * If we're doing a Describe, we're done, and ready to pass the result
	 * back to the client.
	 */
	if (conn->queryclass == PGQUERY_DESCRIBE)
	{
		conn->asyncStatus = PGASYNC_READY;
		return 0;
	}

	if(pq_buf.data)
		termPQExpBuffer(&pq_buf);

	/*
	 * We could perform additional setup for the new result set here, but for
	 * now there's nothing else to do.
	 */

	/* And we're done. */
	return 0;

advance_and_error:
	/* Discard unsaved result, if any */
	if (result && result != conn->result)
		PQclear(result);

	/* Discard the failed message by pretending we read it */
	conn->inStart += 5 + msgLength;

	/*
	 * Replace partially constructed result with an error result. First
	 * discard the old result to try to win back some memory.
	 */
	pqClearAsyncResult(conn);

	/*
	 * If preceding code didn't provide an error message, assume "out of
	 * memory" was meant.  The advantage of having this special case is that
	 * freeing the old result first greatly improves the odds that gettext()
	 * will succeed in providing a translation.
	 */
	if (!errmsg)
		errmsg = libpq_gettext("out of memory for query result");

	printfPQExpBuffer(&conn->errorMessage, "%s\n", errmsg);
	pqSaveErrorResult(conn);

	if(pq_buf.data)
		termPQExpBuffer(&pq_buf);

	/*
	 * Return zero to allow input parsing to continue.  Subsequent "D"
	 * messages will be ignored until we get to end of data, since an error
	 * result is already set up.
	 */
	return 0;
}

static int noneGetAnotherTuple(PGconn *conn, int msgLength)
{
	const char *errmsg;

	/* just skeep it */
	if(pqSkipnchar(conn, msgLength))
	{
		errmsg = libpq_gettext("insufficient data in \"D\" message");
		goto advance_and_error;
	}

	/* Sanity check that we absorbed all the data */
	if (conn->inCursor != conn->inStart + 5 + msgLength)
	{
		errmsg = libpq_gettext("extraneous data in \"D\" message");
		goto advance_and_error;
	}

	/* Advance inStart to show that the "D" message has been processed. */
	conn->inStart = conn->inCursor;

	return 0;				/* normal, successful exit */

advance_and_error:
	/* Discard the failed message by pretending we read it */
	conn->inStart += 5 + msgLength;

	/*
	 * Replace partially constructed result with an error result. First
	 * discard the old result to try to win back some memory.
	 */
	pqClearAsyncResult(conn);

	/*
	 * If preceding code didn't provide an error message, assume "out of
	 * memory" was meant.  The advantage of having this special case is that
	 * freeing the old result first greatly improves the odds that gettext()
	 * will succeed in providing a translation.
	 */
	if (!errmsg)
		errmsg = libpq_gettext("out of memory for query result");

	printfPQExpBuffer(&conn->errorMessage, "%s\n", errmsg);
	pqSaveErrorResult(conn);

	/*
	 * Return zero to allow input parsing to continue.  Subsequent "D"
	 * messages will be ignored until we get to end of data, since an error
	 * result is already set up.
	 */
	return 0;
}

static int noneGetCopyData(PGconn *conn)
{
	#error
	return 0;
}
