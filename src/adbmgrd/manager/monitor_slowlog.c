/*
 * commands of slowlog
 */
 
#include "../../interfaces/libpq/libpq-fe.h"
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/monitor_slowlog.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "access/xact.h"
#include "utils/date.h"

/*given one sqlstr, return the result*/
char *monitor_get_onestrvalue_one_node(char *sqlstr, char *user, char *address, int port, char * dbname)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	char *oneCoordValueStr = NULL;
	
	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, dbname);
	appendStringInfoCharMacro(&constr, '\0');
	ereport(LOG,
		(errmsg("connect info: %s, sql: %s",constr.data, sqlstr)));
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
	/*check row number*/
	Assert(1 == PQntuples(res));
	/*check column number*/
	Assert(1 == PQnfields(res));
	oneCoordValueStr = pstrdup(PQgetvalue(res, 0, 0 ));
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
	return oneCoordValueStr;
}



/*
* get 5 rows from pg_stat_statements on everyone coordinator using given sql. using follow method to judge which need 
* insert into monitor_slowlog table: 1. judge the query exist in yesterday records or not. if not in yesterday records 
*	or the calls does not equal yesterday's calls on same query, just insert into monitor_slowlog table; if the calls 
*	equals yesterday's calls on same query, ignore the query.
*	2.for today, if not in today's records, insert it into slowlog table; if the calls which we get deos equal the *   
* today's calls on the same query, ignore; if the query does exist in today's records and the calls does not equal, let
* the calls plus 1.
*
*/

int monitor_get_onedb_slowdata_insert(Relation rel, char *user, char *address, int port, char *dbname)
{
	StringInfoData constr;
	StringInfoData sqlslowlogStrData;
	PGconn* conn;
	PGresult *res;
	char *connectdbname = "postgres";
	char *dbuser = NULL;
	char *querystr = NULL;
	char *queryplanstr = NULL;
	char *queryplanres = NULL;
	char *getplanerror = "check the query, cannot get the queryplan.";
	int oneCoordTpsInt = -1;
	int nrow = 0;
	int ncol = 0;
	int rowloop = 0;
	int calls = 0;
	int queryplanlen;
	TimestampTz time;
	float totaltime = 0;
	HeapTuple tup_result;

	
	initStringInfo(&constr);
	initStringInfo(&sqlslowlogStrData);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, connectdbname);
	appendStringInfoCharMacro(&constr, '\0');
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK) 
	{
		ereport(LOG,
		(errmsg("Connection to database failed: %s\n", PQerrorMessage(conn))));
		PQfinish(conn);
		pfree(constr.data);
		pfree(sqlslowlogStrData.data);
		return -1;
	}
	appendStringInfo(&sqlslowlogStrData, "select usename, calls, total_time/1000 as totaltime, query  from pg_stat_statements, pg_user, pg_database where ( total_time/calls/1000) > 2 and userid=usesysid and pg_database.oid = dbid and datname=\'%s\';", dbname);
	res = PQexec(conn, sqlslowlogStrData.data);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(LOG,
			(errmsg("Select failed: %s\n" , PQresultErrorMessage(res))));
		PQclear(res);
		PQfinish(conn);
		pfree(constr.data);
		pfree(sqlslowlogStrData.data);
		return -1;
	}
	/*get row number*/
	nrow = PQntuples(res);
	/*get column number*/
	ncol = PQnfields(res);
	time = GetCurrentTimestamp();

		
	for(rowloop=0; rowloop<nrow; rowloop++)
	{
		/*get username*/
		dbuser = PQgetvalue(res, rowloop, 0 );
		/*get run time*/
		calls = atoi(PQgetvalue(res, rowloop, 1 ));
		/*get total used time*/
		totaltime = atof(PQgetvalue(res, rowloop, 2 ));
		/*get query string*/
		querystr = PQgetvalue(res, rowloop, 3 );
		/*get queryplan*/
		
		queryplanlen = strlen(querystr) + strlen("explain ")+1;
		queryplanstr = (char *)palloc(queryplanlen*sizeof(char));
		strcpy(queryplanstr, "explain ");
		strcpy(queryplanstr + strlen("explain "), querystr);
		queryplanstr[queryplanlen-1] = '\0';
		queryplanres = monitor_get_onestrvalue_one_node(queryplanstr, user, address, port, dbname);
		if (NULL == queryplanres)
		{
			tup_result = monitor_build_slowlog_tuple(rel, time, dbname, dbuser, totaltime/calls, calls, querystr, getplanerror);
		}
		else
		{	
			tup_result = monitor_build_slowlog_tuple(rel, time, dbname, dbuser, totaltime/calls, calls, querystr, queryplanres);
			pfree(queryplanres);
		}
		simple_heap_insert(rel, tup_result);
		CatalogUpdateIndexes(rel, tup_result);
		heap_freetuple(tup_result);
		pfree(queryplanstr);
	}

	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
	return oneCoordTpsInt;
}

/*
* get all database slowlog,then insert them to MslowlogRelationId
*/
Datum monitor_slowlog_insert_data(PG_FUNCTION_ARGS)
{
	int coordport;
	int dbnum = 0;
	char *address = NULL;
	char *user = NULL;
	char *dbname = NULL;
	Relation rel_node;
	Relation rel_slowlog;
	List *dbnamelist = NIL;
	ListCell *cell;
	
	rel_slowlog = heap_open(MslowlogRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*get user, address, coordport from node systbl*/
	monitor_get_one_node_user_address_port(rel_node, &user, &address, &coordport, CNDN_TYPE_COORDINATOR_MASTER);
	Assert(address != NULL);
	Assert(user != NULL);
	/*get database name list*/
	dbnamelist = monitor_get_dbname_list(user, address, coordport);
	if(dbnamelist == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get database namelist error")));
	}
	dbnum = list_length(dbnamelist);
	Assert(dbnum > 0);
	foreach(cell, dbnamelist)
	{
		dbname = (char *)(lfirst(cell));
		monitor_get_onedb_slowdata_insert(rel_slowlog, user, address, coordport, dbname);
	}

	list_free(dbnamelist);
	pfree(user);
	pfree(address);
	heap_close(rel_slowlog, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
	PG_RETURN_TEXT_P(cstring_to_text("insert_data"));
}


/*
* build tuple for table: monitor_slowlog, see: monitor_slowlog.h
*/
HeapTuple monitor_build_slowlog_tuple(Relation rel, TimestampTz time, char *dbname, char *username, float singletime, int totalnum, char *query, char *queryplan)
{
	Datum datums[7];
	bool nulls[7];
	TupleDesc desc;
	NameData dbnamedata;
	NameData usernamedata;
	
	desc = RelationGetDescr(rel);
	namestrcpy(&dbnamedata, dbname);
	namestrcpy(&usernamedata, username);
	AssertArg(desc && desc->natts == 7
		&& desc->attrs[0]->atttypid == TIMESTAMPTZOID
		&& desc->attrs[1]->atttypid == NAMEOID
		&& desc->attrs[2]->atttypid == NAMEOID
		&& desc->attrs[3]->atttypid == FLOAT4OID
		&& desc->attrs[4]->atttypid == INT4OID
		&& desc->attrs[5]->atttypid == TEXTOID
		&& desc->attrs[6]->atttypid == TEXTOID
		);
	memset(datums, 0, sizeof(datums));
	memset(nulls, 0, sizeof(nulls));
	datums[0] = TimestampTzGetDatum(time);
	datums[1] = NameGetDatum(&dbnamedata);
	datums[2] = NameGetDatum(&usernamedata);
	datums[3] = Float4GetDatum(singletime);
	datums[4] = Int32GetDatum(totalnum);
	datums[5] = CStringGetTextDatum(query);
	datums[6] = CStringGetTextDatum(queryplan);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = nulls[5] = nulls[6] =false;
	
	return heap_form_tuple(desc, datums, nulls);
}

	
	

