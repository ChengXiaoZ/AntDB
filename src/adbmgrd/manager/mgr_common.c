
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mgr/mgr_cmds.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "mgr/mgr_msg_type.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_cndnnode.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "utils/fmgroids.h"

#define MAXLINE (8192-1)
#define MAXPATH (512-1)

static TupleDesc common_command_tuple_desc = NULL;
static TupleDesc showparam_command_tuple_desc = NULL;
static void myUsleep(long microsec);

TupleDesc get_common_command_tuple_desc(void)
{
	if(common_command_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(3, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "name",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "success",
							   BOOLOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "message",
							   TEXTOID, -1, 0);
			common_command_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_command_tuple_desc);
	return common_command_tuple_desc;
}

HeapTuple build_common_command_tuple(const Name name, bool success, const char *message)
{
	Datum datums[3];
	bool nulls[3];
	TupleDesc desc;
	AssertArg(name && message);
	desc = get_common_command_tuple_desc();

	AssertArg(desc && desc->natts == 3
		&& desc->attrs[0]->atttypid == NAMEOID
		&& desc->attrs[1]->atttypid == BOOLOID
		&& desc->attrs[2]->atttypid == TEXTOID);

	datums[0] = NameGetDatum(name);
	datums[1] = BoolGetDatum(success);
	datums[2] = CStringGetTextDatum(message);
	nulls[0] = nulls[1] = nulls[2] = false;
	return heap_form_tuple(desc, datums, nulls);
}

/*get the the address of host in table host*/
char *get_hostaddress_from_hostoid(Oid hostOid)
{
	Relation rel;
	HeapTuple tuple;
	Datum host_addr;
	char *hostaddress;
	bool isNull = false;
	
	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	host_addr = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(rel), &isNull);
	if(isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	hostaddress = pstrdup(TextDatumGetCString(host_addr));
	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return hostaddress;
}

/*get the the hostname in table host*/
char *get_hostname_from_hostoid(Oid hostOid)
{
	Relation rel;
	HeapTuple tuple;
	Datum host_name;
	char *hostname;
	bool isNull = false;
	
	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	host_name = heap_getattr(tuple, Anum_mgr_host_hostname, RelationGetDescr(rel), &isNull);
	if(isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostname is null")));
	hostname = pstrdup(NameStr(*DatumGetName(host_name)));
	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return hostname;
}

/*get the the user in table host*/
char *get_hostuser_from_hostoid(Oid hostOid)
{
	Relation rel;
	HeapTuple tuple;
	Datum host_user;
	char *hostuser;
	bool isNull = false;
	
	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	host_user = heap_getattr(tuple, Anum_mgr_host_hostuser, RelationGetDescr(rel), &isNull);
	if(isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostuser is null")));
	hostuser = pstrdup(NameStr(*DatumGetName(host_user)));
	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return hostuser;
}

/*
* get msg from agent
*/
bool mgr_recv_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst)
{
	char			msg_type;
	StringInfoData recvbuf;
	bool initdone = false;
	initStringInfo(&recvbuf);
	for(;;)
	{
		resetStringInfo(&recvbuf);
		msg_type = ma_get_message(ma, &recvbuf);
		if(msg_type == AGT_MSG_IDLE)
		{
			/* message end */
			break;
		}else if(msg_type == '\0')
		{
			/* has an error */
			break;
		}else if(msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_get_err_info(&recvbuf, AGT_MSG_RESULT));
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
			break;
		}else if(msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(LOG, (errmsg("receive msg: %s", recvbuf.data)));
		}
		else if(msg_type == AGT_MSG_RESULT)
		{
			getAgentCmdRst->ret = true;
			appendStringInfoString(&(getAgentCmdRst->description), run_success);
			ereport(DEBUG1, (errmsg("receive msg: %s", recvbuf.data)));
			initdone = true;
			break;
		}
	}
	pfree(recvbuf.data);
	return initdone;
}

/*
* get host info from agent for [ADB monitor]
*/
bool mgr_recv_msg_for_monitor(ManagerAgent *ma, bool *ret, StringInfo agentRstStr)
{
	char msg_type;
	bool initdone = false;
	
	for (;;)
	{
		msg_type = ma_get_message(ma, agentRstStr);
		if (msg_type == AGT_MSG_IDLE)
		{
			/* message end */
			break;
		}else if (msg_type == '\0')
		{
			/* has an error */
			break;
		}else if (msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			*ret = false;
			appendStringInfoString(agentRstStr, ma_get_err_info(agentRstStr, AGT_MSG_RESULT));
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(agentRstStr, AGT_MSG_RESULT))));
			break;
		}else if (msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(LOG, (errmsg("receive msg: %s", agentRstStr->data)));
		}
		else if (msg_type == AGT_MSG_RESULT)
		{
			*ret = true;
			ereport(LOG, (errmsg("receive msg: %s", agentRstStr->data)));
			initdone = true;
			break;
		}
	}
	return initdone;
}

/* ping someone node for monitor */
int pingNode(char *host, char *port)
{
        PGPing status;
        char conninfo[MAXLINE+1];
        char editBuf[MAXPATH+1];
#define RETRY 3
#define sleepMicro 100*1000     /* 100 millisec */
        int retry;

        conninfo[0] = 0;
        if (host)
        {
                snprintf(editBuf, MAXPATH, "host = '%s' ", host);
        strncat(conninfo, editBuf, MAXLINE);
    }   
    if (port)
    {   
        snprintf(editBuf, MAXPATH, "port = %d ", atoi(port));
        strncat(conninfo, editBuf, MAXLINE);
    }   
    if (conninfo[0])
    {   
        elog(DEBUG1, "Ping node string: %s.\n",conninfo);
        for (retry = RETRY; retry; retry--){
            status = PQping(conninfo);
            if (status == PQPING_OK)
                return 0;       
            else        
            {           
                myUsleep(sleepMicro);
                continue;       
            }           
        }       
        return 1;
    }   
    else
        return -1;
#undef RETRY
#undef sleepMicro
}

static void
myUsleep(long microsec)
{
    struct timeval delay;

    if (microsec <= 0)
        return; 

    delay.tv_sec = microsec / 1000000L;
    delay.tv_usec = microsec % 1000000L;
    (void) select(0, NULL, NULL, NULL, &delay);
}

/*check the host in use or not*/
bool	mgr_check_host_in_use(Oid hostoid)
{
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Form_mgr_node mgr_node;
	Relation rel;
	
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		/*check this tuple initd or not, if it has inited, cannot be dropped*/
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodehost == hostoid)
		{
			heap_endscan(rel_scan);
			heap_close(rel, RowExclusiveLock);
			return true;
		}
	}
	heap_endscan(rel_scan);
	heap_close(rel, RowExclusiveLock);
	return false;
}

/*
* get database namelist
*/
List *monitor_get_dbname_list(char *user, char *address, int port)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	char *oneCoordValueStr;
	List *nodenamelist =NIL;
	int iN = 0;
	char *sqlstr = "select datname from pg_database  where datname != \'template0\' and datname != \'template1\' order by 1;";
		
	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/postgres", user, address, port);
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK) 
	{
		ereport(LOG, 
			(errmsg("Connection to database failed: %s\n", PQerrorMessage(conn))));
		PQfinish(conn);
		pfree(constr.data);
		return NULL;
	}
	res = PQexec(conn, sqlstr);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(LOG, 
			(errmsg("Select failed: %s\n" , PQresultErrorMessage(res))));
		PQclear(res);
		PQfinish(conn);
		pfree(constr.data);
		return NULL;
	}
	/*check column number*/
	Assert(1 == PQnfields(res));
	for (iN=0; iN < PQntuples(res); iN++)
	{
		oneCoordValueStr = PQgetvalue(res, iN, 0 );
		nodenamelist = lappend(nodenamelist, pstrdup(oneCoordValueStr));
	}
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
	
	return nodenamelist;
}

/*
* get user, hostaddress from coordinator
*/
void monitor_get_one_node_user_address_port(Relation rel_node, char **user, char **address, int *coordport, char nodetype)
{
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		*coordport = mgr_node->nodeport;
		*address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		*user = get_hostuser_from_hostoid(mgr_node->nodehost);
		break;
	}
	heap_endscan(rel_scan);
}

/*
* get len values to iarray, the values get from the given sqlstr's result
*/
bool monitor_get_sqlvalues_one_node(char *sqlstr, char *user, char *address, int port, char * dbname, int iarray[], int len)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	char *pvalue;
	int iloop = 0;
	
	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, dbname);
	appendStringInfoCharMacro(&constr, '\0');
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK) 
	{
		ereport(LOG,
		(errmsg("Connection to database failed: %s\n", PQerrorMessage(conn))));
		PQfinish(conn);
		pfree(constr.data);
		return false;
	}
	res = PQexec(conn, sqlstr);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(LOG,
		(errmsg("Select failed: %s\n" , PQresultErrorMessage(res))));
		PQclear(res);
		PQfinish(conn);
		pfree(constr.data);
		return false;
	}
	/*check row number*/
	Assert(len == PQntuples(res));
	/*check column number*/
	Assert(1 == PQnfields(res));
	for (iloop=0; iloop<len; iloop++)
	{
		pvalue = PQgetvalue(res, iloop, 0);
		iarray[iloop] = atoi(pvalue);
	}
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
	return true;
}

/*
* to make the columns name for the result of command: show nodename parameter
*/
TupleDesc get_showparam_command_tuple_desc(void)
{
	if(showparam_command_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(3, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "type",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "success",
							   BOOLOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "message",
							   TEXTOID, -1, 0);
			common_command_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_command_tuple_desc);
	return common_command_tuple_desc;
}