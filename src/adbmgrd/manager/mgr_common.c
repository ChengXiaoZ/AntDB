
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
#include "utils/array.h"
#include "utils/tqual.h"
#include "executor/spi.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "utils/fmgroids.h"
#include "executor/spi.h"

#define MAXLINE (8192-1)
#define MAXPATH (512-1)
#define RETRY 3
#define SLEEP_MICRO 100*1000     /* 100 millisec */

static TupleDesc common_command_tuple_desc = NULL;
static TupleDesc common_list_acl_tuple_desc = NULL;
static TupleDesc showparam_command_tuple_desc = NULL;
static TupleDesc ha_replication_tuple_desc = NULL;

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

HeapTuple build_list_acl_command_tuple(const Name name, const char *message)
{
	Datum datums[2];
	bool nulls[2];
	TupleDesc desc;
	AssertArg(name && message);
	desc = get_list_acl_command_tuple_desc();

	AssertArg(desc && desc->natts == 2
		&& desc->attrs[0]->atttypid == NAMEOID
		&& desc->attrs[1]->atttypid == TEXTOID);

	datums[0] = NameGetDatum(name);
	datums[1] = CStringGetTextDatum(message);
	nulls[0] = nulls[1] = false;
	return heap_form_tuple(desc, datums, nulls);
}

TupleDesc get_list_acl_command_tuple_desc(void)
{
	if(common_list_acl_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(2, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "name",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "message",
							   TEXTOID, -1, 0);
			common_list_acl_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_list_acl_tuple_desc);
	return common_list_acl_tuple_desc;
}
/*hba replication tuple desc*/
HeapTuple build_ha_replication_tuple(const Name type, const Name nodename, const Name app, const Name client_addr, const Name state, const Name sent_location, const Name replay_location, const Name sync_state, const Name master_location, const Name sent_delay, const Name replay_delay)
{
	Datum datums[11];
	bool nulls[11];
	TupleDesc desc;
	int i = 0;
	AssertArg(type && nodename && app && client_addr && state && sent_location && replay_location && sync_state && master_location && sent_delay && replay_delay);
	desc = get_ha_replication_tuple_desc();

	AssertArg(desc && desc->natts == 11);
	while(i<11)
	{
		AssertArg(desc->attrs[i]->atttypid == NAMEOID);
		nulls[i] = false;
		i++;
	}

	datums[0] = NameGetDatum(type);
	datums[1] = NameGetDatum(nodename);
	datums[2] = NameGetDatum(app);
	datums[3] = NameGetDatum(client_addr);
	datums[4] = NameGetDatum(state);
	datums[5] = NameGetDatum(sent_location);
	datums[6] = NameGetDatum(replay_location);
	datums[7] = NameGetDatum(sync_state);
	datums[8] = NameGetDatum(master_location);
	datums[9] = NameGetDatum(sent_delay);
	datums[10] = NameGetDatum(replay_delay);
	return heap_form_tuple(desc, datums, nulls);
}

TupleDesc get_ha_replication_tuple_desc(void)
{
	if(ha_replication_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(11, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "type", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "nodename", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "application_name", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 4, "client_addr", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 5, "state", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 6, "sent_location", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 7, "replay_location", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 8, "sync_state", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 9, "master_location", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 10, "sent_delay", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 11, "replay_delay", NAMEOID, -1, 0);
			ha_replication_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(ha_replication_tuple_desc);
	return ha_replication_tuple_desc;
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
			ereport(DEBUG1, (errmsg("receive msg: %s", agentRstStr->data)));
			initdone = true;
			break;
		}
	}
	return initdone;
}

bool is_valid_ip(char *ip)
{
	FILE *pPipe;
	char psBuffer[1024];
	char ping_str[1024];
	struct hostent *hptr;

	if ((hptr = gethostbyname(ip)) == NULL)
	{
		return false;
	}

	snprintf(ping_str, sizeof(ping_str), "ping -c 1 %s", ip);

	if((pPipe = popen(ping_str, "r" )) == NULL )
	{
		return false;
	}
	while(fgets(psBuffer, 1024, pPipe))
	{
		if (strstr(psBuffer, "0 received") != NULL ||
			strstr(psBuffer, "Unreachable") != NULL)
		{
			pclose(pPipe);
			return false;
		}
	}
	pclose(pPipe);
	return true;
}

/* ping someone node for monitor */
int pingNode_user(char *host_addr, char *node_port, char *node_user)
{
	int ping_status;
	bool execok = false;
	ManagerAgent *ma;
	StringInfoData sendstrmsg;
	StringInfoData buf;
	char pid_file_path[MAXPATH] = {0};
	Datum nodepath;
	int32 agent_port;
	Relation rel;
	HeapScanDesc rel_scan;
	ScanKeyData key[2];
	HeapTuple tuple;
	Oid host_tuple_oid;
	Form_mgr_host mgr_host;
	GetAgentCmdRst getAgentCmdRst;
	bool isnull;
	Assert(host_addr && node_port && node_user);

	/*get the host port base on the port of node and host*/
	ScanKeyInit(&key[0]
				,Anum_mgr_host_hostaddr
				,BTEqualStrategyNumber
				,F_TEXTEQ
				,CStringGetTextDatum(host_addr));
	rel = heap_open(HostRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 1, key);
	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	host_tuple_oid = HeapTupleGetOid(tuple);
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errmsg("host\"%s\" does not exist in the host table", host_addr)));
	}
	mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	agent_port = mgr_host->hostagentport;
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);

	/*get postmaster.pid file path */
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeport
				,BTEqualStrategyNumber
				,F_INT4EQ
				,Int32GetDatum(atol(node_port)));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodehost
				,BTEqualStrategyNumber
				,F_OIDEQ
				,ObjectIdGetDatum(host_tuple_oid));
	rel = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);
	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errmsg("port \"%s\" does not exist in the node table", node_port)));
	}
	nodepath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isnull);
	snprintf(pid_file_path, MAXPATH, "%s/postmaster.pid", TextDatumGetCString(nodepath));
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	
	/*send the node message to agent*/
	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&sendstrmsg, "%s", host_addr);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", node_port);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", node_user);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", pid_file_path);
	
	ma = ma_connect(host_addr, agent_port);;
	if (!ma_isconnected(ma))
	{
		/*report error message*/
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(LOG, (errmsg("could not connect socket for agent \"%s\".",
						host_addr)));
		pfree(sendstrmsg.data);
		pfree(getAgentCmdRst.description.data);
		return AGENT_DOWN;
	}
	getAgentCmdRst.ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PING_NODE);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return -1;
	}
	/*check the receive msg*/
	mgr_recv_msg_for_monitor(ma, &execok, &getAgentCmdRst.description);
	ma_close(ma);
	if (!execok)
	{
		ereport(WARNING, (errmsg("monitor (host=%s port=%s) fail \"%s\"",
			host_addr, node_port, getAgentCmdRst.description.data)));
	}
	if (getAgentCmdRst.description.len == 1)
		ping_status = getAgentCmdRst.description.data[0];
	else
		ereport(ERROR, (errmsg("receive msg from agent \"%s\" error.", host_addr)));
	pfree(getAgentCmdRst.description.data);
	switch(ping_status)
	{
		case PQPING_OK:
		case PQPING_REJECT:
		case PQPING_NO_ATTEMPT:
		case PQPING_NO_RESPONSE:
			return ping_status;
		default:
			return PQPING_NO_RESPONSE;
	}
	pfree(buf.data);
}

/*check the host in use or not*/
bool mgr_check_host_in_use(Oid hostoid, bool check_inited)
{
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Form_mgr_node mgr_node;
	Relation rel;
	ScanKeyData key[1];
	bool is_using = false;
	rel = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	if (!check_inited)
		rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	else
		rel_scan = heap_beginscan(rel, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		/* check this tuple incluster or not, if it has incluster, cannot be dropped/alter. */
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodehost == hostoid)
		{
			break;
		}
	}
	if (HeapTupleIsValid(tuple))
		is_using =  true;
	else
		is_using =  false;
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	return is_using;
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
void monitor_get_one_node_user_address_port(Relation rel_node, int *agentport, char **user, char **address, int *coordport, char nodetype)
{
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;

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
		tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
		if(!(HeapTupleIsValid(tup)))
		{
			ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errcode(ERRCODE_INTERNAL_ERROR)));
		}
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		*agentport = mgr_host->hostagentport;
		ReleaseSysCache(tup);
		break;
	}
	heap_endscan(rel_scan);
}

/*
* get len values to iarray, the values get from the given sqlstr's result
*/
bool monitor_get_sqlvalues_one_node(int agentport, char *sqlstr, char *user, char *address, int port, char * dbname, int iarray[], int len)
{
	int iloop = 0;
	char *pstr;
	char strtmp[64];
	StringInfoData resultstrdata;

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, sqlstr, user, address, port, dbname, &resultstrdata);
	if (resultstrdata.len == 0)
	{
		return false;
	}
	pstr = resultstrdata.data;
	while(pstr != NULL && iloop < len)
	{
		strcpy(strtmp, pstr);
		iarray[iloop] = atoi(strtmp);
		pstr = pstr + strlen(strtmp) + 1;
		iloop++;
	}
	pfree(resultstrdata.data);
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

List * DecodeTextArrayToValueList(Datum textarray)
{
	ArrayType  *array = NULL;
	Datum *elemdatums;
	int num_elems;
	List *value_list = NIL;
	int i;

	Assert( PointerIsValid(DatumGetPointer(textarray)));

	array = DatumGetArrayTypeP(textarray);
	Assert(ARR_ELEMTYPE(array) == TEXTOID);

	deconstruct_array(array, TEXTOID, -1, false, 'i', &elemdatums, NULL, &num_elems);
	for (i = 0; i < num_elems; ++i)
	{
		value_list = lappend(value_list, makeString(TextDatumGetCString(elemdatums[i])));
	}

	return value_list;
}

void check_nodename_isvalid(char *nodename)
{
	HeapTuple tuple;
	ScanKeyData key[2];
	Relation rel;
	HeapScanDesc rel_scan;

	rel = heap_open(NodeRelationId, AccessShareLock);

	ScanKeyInit(&key[0]
			,Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(nodename));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
	rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);

	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	if (tuple == NULL)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("node name \"%s\" does not exist", nodename)));

	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
}

bool mgr_has_function_privilege_name(char *funcname, char *priv_type)
{
	Datum aclresult;
	aclresult = DirectFunctionCall2(has_function_privilege_name,
							CStringGetTextDatum(funcname),
							CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

bool mgr_has_table_privilege_name(char *tablename, char *priv_type)
{
	Datum aclresult;
	aclresult = DirectFunctionCall2(has_table_privilege_name,
							CStringGetTextDatum(tablename),
							CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

/*
* get msg from agent
*/
void mgr_recv_sql_stringvalues_msg(ManagerAgent	*ma, StringInfo resultstrdata)
{
	char msg_type;
	StringInfoData recvbuf;
	initStringInfo(&recvbuf);
	for(;;)
	{
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
			ereport(WARNING, (errmsg("%s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
			break;
		}else if(msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(NOTICE, (errmsg("%s", recvbuf.data)));
		}
		else if(msg_type == AGT_MSG_RESULT)
		{
			appendBinaryStringInfo(resultstrdata, recvbuf.data, recvbuf.len);
			ereport(DEBUG1, (errmsg("%s", recvbuf.data)));
			break;
		}
	}
	pfree(recvbuf.data);
}

/*
* get active coordinator node name
*/
bool mgr_get_active_node(Name nodename, char nodetype)
{
	int ret;
	bool bresult = false;
	StringInfoData sqlstr;

	/*check all node stop*/
	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));
	initStringInfo(&sqlstr);
	appendStringInfo(&sqlstr, "select nodename from mgr_monitor_nodetype_all(%d) where status = true;", nodetype);
	ret = SPI_execute(sqlstr.data, false, 0);
	pfree(sqlstr.data);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("ADB Manager SPI_execute failed: error code %d", ret)));
	if (SPI_processed > 0 && SPI_tuptable != NULL)
	{
		namestrcpy(nodename, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
		bresult = true;
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	
	return bresult;
}

/*
* given the input parameter n_days as interval time, the data in the table before n_days will be droped
*
*/
void monitor_delete_data(MonitorDeleteData *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_alter())
	{
		DirectFunctionCall1(monitor_delete_data_interval_days, Int32GetDatum(node->days));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}

}

/*
* given the input parameter n_days as interval time, the data in the table before n_days will be droped
*
*/

Datum monitor_delete_data_interval_days(PG_FUNCTION_ARGS)
{
	int interval_days = PG_GETARG_INT32(0);
	int ret;
	int iloop = 0;
	StringInfoData sqlstrdata;
	struct del_tablename
	{
		char *tbname;
		char *coltimename;
	}del_tablename[]={
		{"monitor_cpu", "mc_timestamptz"},
		{"monitor_disk", "md_timestamptz"},
		{"monitor_host", "mh_current_time"},
		{"monitor_mem", "mm_timestamptz"},
		{"monitor_net", "mn_timestamptz"},
		{"monitor_resolve", "mr_resolve_timetz"},
		{"monitor_alarm", "ma_alarm_timetz"},
		{"monitor_databaseitem", "monitor_databaseitem_time"},
		{"monitor_databasetps", "monitor_databasetps_time"},
		{"monitor_slowlog", "slowlogtime"},
		{NULL, NULL}
		};

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Monitor SPI_connect failed: error code %d", ret)));
	
	initStringInfo(&sqlstrdata);
	
	for(iloop=0; del_tablename[iloop].tbname != NULL; iloop++)
	{
		appendStringInfo(&sqlstrdata, "delete from %s where %s < timestamp'now()' - interval'%d day';"
			,del_tablename[iloop].tbname, del_tablename[iloop].coltimename, interval_days);
		ret = SPI_execute(sqlstrdata.data, false, 0);
		if (ret != SPI_OK_DELETE)
			ereport(ERROR, (errmsg("ADB Monitor SPI_execute \"%s\"failed: error code %d", sqlstrdata.data, ret)));
		ereport(LOG, (errmsg("ADB Monitor clean data: table \"%s\", data of \"%d\" days ago"
			,del_tablename[iloop].tbname, interval_days)));
		resetStringInfo(&sqlstrdata);
		SPI_freetuptable(SPI_tuptable);
	}
	pfree(sqlstrdata.data);
	SPI_finish();
	
	PG_RETURN_BOOL(true);
}

/*
* set cluster init in mgr_node table,initialized=true, incluster=true
*/
void mgr_set_init(MGRSetClusterInit *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall1(mgr_set_init_cluster, (Datum)0);
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*
* update mgr_node table, set initialized=true, incluster=true
*/

Datum mgr_set_init_cluster(PG_FUNCTION_ARGS)
{
	char *sqlstr = "update mgr_node set nodeinited=true,nodeincluster=true;";
	int ret;

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));

	ret = SPI_execute(sqlstr, false, 0);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR, (errmsg("ADB Manager SPI_execute \"%s\"failed: error code %d", sqlstr, ret)));
	ereport(LOG, (errmsg("update mgr_node table, set initialized=true, incluster=true")));
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	
	PG_RETURN_BOOL(true);
	
}


/*
* promote the gtm or datanode node
*
*/

bool mgr_promote_node(char cmdtype, Oid hostOid, char *path, StringInfo strinfo)
{
	bool res = false;
	StringInfoData infosendmsg;

	/*check the cmdtype*/
	if (AGT_CMD_GTM_SLAVE_FAILOVER != cmdtype || AGT_CMD_DN_FAILOVER != cmdtype)
	{
		appendStringInfo(strinfo, "the cmdtype is \"%d\", not for gtm promote or datanode promote", cmdtype);
		return false;
	}
	/*check the path*/
	if (!path || path[0] != '/')
	{
		appendStringInfoString(strinfo, "the path is not absolute path");
		return false;
	}

	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " promote -w -D %s", path);
	res = mgr_ma_send_cmd(cmdtype, infosendmsg.data, hostOid, strinfo);
	pfree(infosendmsg.data);

	return res;
}

/*
* wait the new master accept connect
*
*/
bool mgr_check_node_connect(char nodetype, Oid hostOid, int nodeport)
{
	char *hostaddr;
	char nodeport_buf[10];
	char *username = NULL;

	hostaddr = get_hostaddress_from_hostoid(hostOid);
	/*check recovery finish*/
	fputs(_("waiting for the new master can accept connections..."), stdout);
	fflush(stdout);
	while(1)
	{
		if (mgr_check_node_recovery_finish(nodetype, hostOid, nodeport, hostaddr))
			break;
		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}
	sprintf(nodeport_buf, "%d", nodeport);
	if (nodetype != GTM_TYPE_GTM_MASTER && nodetype != GTM_TYPE_GTM_SLAVE 
			&& nodetype != GTM_TYPE_GTM_EXTRA)
			username = get_hostname_from_hostoid(hostOid);

	while(1)
	{
		if (pingNode_user(hostaddr, nodeport_buf, username == NULL ? AGTM_USER : username) != 0)
		{
			fputs(_("."), stdout);
			fflush(stdout);
			pg_usleep(1 * 1000000L);
		}
		else
			break;
	}
	fputs(_(" done\n"), stdout);
	fflush(stdout);

	pfree(hostaddr);
	if (username)
		pfree(username);
	
	return true;
}

/*
* rewind the node
*
*/

bool mgr_rewind_node(char nodetype, char *nodename, StringInfo strinfo)
{
	char cmdtype = AGT_CMD_NODE_REWIND;
	char mastertype;
	char *nodetypestr;
	bool res = false;
	bool slave_is_exist = false;
	bool slave_is_running = false;
	bool master_is_exist = false;
	bool master_is_running = false;
	AppendNodeInfo slave_nodeinfo;
	AppendNodeInfo master_nodeinfo;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	Relation rel_node;
	HeapTuple tuple;

	/*check node type*/
	if (nodetype != GTM_TYPE_GTM_SLAVE && nodetype != GTM_TYPE_GTM_EXTRA
		&& nodetype != CNDN_TYPE_DATANODE_SLAVE && nodetype != CNDN_TYPE_DATANODE_EXTRA)
	{
		appendStringInfo(strinfo, "the nodetype is \"%d\", not for gtm rewind or datanode rewind", nodetype);
		return false;
	}

	Assert(nodename);

	nodetypestr = mgr_nodetype_str(nodetype);
	/* check exists */
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(rel_node, nodename, nodetype);
	if(!(HeapTupleIsValid(tuple)))
	{
		heap_close(rel_node, AccessShareLock);
		appendStringInfo(strinfo, "%s \"%s\" does not exist", nodetypestr, nodename);
		return false;
	}
	heap_freetuple(tuple);
	heap_close(rel_node, AccessShareLock);

	/*get the slave info*/
	get_nodeinfo_byname(nodename, nodetype, &slave_is_exist, &slave_is_running, &slave_nodeinfo);
	/*get its master info*/
	mastertype = mgr_get_master_type(nodetype);
	get_nodeinfo_byname(nodename, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
	if (master_is_exist && (!master_is_running))
	{
			pfree_AppendNodeInfo(master_nodeinfo);
			pfree_AppendNodeInfo(slave_nodeinfo);
			nodetypestr = mgr_nodetype_str(mastertype);
			appendStringInfo(strinfo, "%s \"%s\" does not running normal", nodetypestr, nodename);
			return false;
	}

	if (slave_is_running)
	{
		pfree_AppendNodeInfo(slave_nodeinfo);
		appendStringInfo(strinfo, "%s \"%s\" does running, should stopped before node rewind", nodetypestr, nodename);
		return false;
	}
	pfree(nodetypestr);

	/* update master's pg_hba.conf */
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_add_parameters_hbaconf(master_nodeinfo.tupleoid, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
	mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", master_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							master_nodeinfo.nodepath,
							&infosendmsg,
							master_nodeinfo.nodehost,
							&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
		appendStringInfo(strinfo, "%s", getAgentCmdRst.description.data);
		pfree(getAgentCmdRst.description.data);
		return false;
	}

	pfree(getAgentCmdRst.description.data);

	/*node rewind*/
	resetStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " --target-pgdata %s --source-server='host=%s port=%d user=%s dbname=postgres' -N %s", slave_nodeinfo.nodepath, master_nodeinfo.nodeaddr, master_nodeinfo.nodeport, slave_nodeinfo.nodeusername, nodename);

	res = mgr_ma_send_cmd(cmdtype, infosendmsg.data, slave_nodeinfo.nodehost, strinfo);
	pfree(infosendmsg.data);
	pfree_AppendNodeInfo(master_nodeinfo);
	pfree_AppendNodeInfo(slave_nodeinfo);
	return res;
}

/*
* send adbmgr command string to agent; if fail, the error information in strinfo
*
*/
bool mgr_ma_send_cmd(char cmdtype, char *cmdstr, Oid hostOid, StringInfo strinfo)
{
	char *hostaddr;
	char cmdheadstr[64];
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData buf;
	bool res = false;

	hostaddr = get_hostaddress_from_hostoid(hostOid);
	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		appendStringInfo(strinfo, "%s", ma_last_error_msg(ma));
		ma_close(ma);
		pfree(hostaddr);
		return false;
	}

	mgr_get_cmd_head_word(cmdtype, cmdheadstr);
	ereport(LOG, (errmsg("%s, %s %s", hostaddr, cmdheadstr, cmdstr)));
	pfree(hostaddr);
	
	/*send cmd*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,cmdstr);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		appendStringInfoString(strinfo, ma_last_error_msg(ma));
		ma_close(ma);
		return false;
	}
	/*check the receive msg*/
	initStringInfo(&(getAgentCmdRst.description));
	res = mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	appendStringInfoString(strinfo, getAgentCmdRst.description.data);
	pfree(getAgentCmdRst.description.data);
	
	return res;
}