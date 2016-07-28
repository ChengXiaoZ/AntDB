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

#define GETTODAYSTARTTIME(time) ((time+8*3600)/(3600*24)*(3600*24)-8*3600)
#define GETLASTDAYSTARTTIME(time) ((time+8*3600)/(3600*24)*(3600*24)-8*3600-24*3600)
#define GETTOMARROWSTARTTIME(time) ((time+8*3600)/(3600*24)*(3600*24)-8*3600+24*3600)

/*see the content of adbmgr_init.sql: "insert into pg_catalog.monitor_host_threshold"
* the values are the same in adbmgr_init.sql for given items
*/
typedef enum ThresholdItem
{
	SLOWQUERY_MINTIME = 33,
	SLOWLOG_GETNUMONCE = 34
}ThresholdItem;


/*given one sqlstr, return the result*/
char *monitor_get_onestrvalue_one_node(char *sqlstr, char *user, char *address, int port, char * dbname)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	char *oneCoordValueStr = NULL;
	int nrow = 0;
	int iloop = 0;
	
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
	/*check column number*/
	Assert(1 == PQnfields(res));
	/*get row num*/
	nrow = PQntuples(res);
	oneCoordValueStr = (char *)palloc(nrow*1024);
	for (iloop=0; iloop<nrow; iloop++)
	{
		strcat(oneCoordValueStr, PQgetvalue(res, iloop, 0 ));
		if(iloop != nrow-1)
			strcat(oneCoordValueStr, "\n");
		else
			strcat(oneCoordValueStr, "\0");
	}
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
	return oneCoordValueStr;
}



/*
* get GETMAXROWNUM rows from pg_stat_statements on everyone coordinator using given sql. using follow method to judge which need 
* insert into monitor_slowlog table: 1. judge the query exist in yesterday records or not. if not in yesterday records 
*	or the calls does not equal yesterday's calls on same query, just insert into monitor_slowlog table; if the calls 
*	equals yesterday's calls on same query, ignore the query.
*	2.for today, if not in today's records, insert it into slowlog table; if the calls which we get deos equal the *   
* today's calls on the same query, ignore; if the query does exist in today's records and the calls does not equal, let
* the calls plus 1.
*
*/

void monitor_get_onedb_slowdata_insert(Relation rel, char *user, char *address, int port, char *dbname)
{
	StringInfoData constr;
	StringInfoData sqlslowlogStrData;
	PGconn* conn;
	PGresult *res;
	char *connectdbname = "postgres";
	char *dbuser = NULL;
	char *querystr = NULL;
	int nrow = 0;
	int rowloop = 0;
	int calls = 0;
	TimestampTz time;
	float totaltime = 0;
	float singletime = 0;
	HeapTuple tupleret = NULL;
	int callstoday = 0;
	int callsyestd = 0;
	int callstmp = 0;
	int slowlogmintime = 2;
	int slowlognumoncetime = 5;
	bool gettoday = false;
	bool getyesdt = false;	
	Form_monitor_slowlog monitor_slowlog;	
	pg_time_t ptimenow;
	Monitor_Threshold monitor_threshold;
	
	initStringInfo(&constr);
	initStringInfo(&sqlslowlogStrData);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, connectdbname);
	appendStringInfoCharMacro(&constr, '\0');
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK) 
	{
		PQfinish(conn);
		pfree(constr.data);
		pfree(sqlslowlogStrData.data);
		ereport(ERROR,
		(errmsg("Connection to database failed: %s\n", PQerrorMessage(conn))));
	}
	/*get slowlog min time threshold in MonitorHostThresholdRelationId*/
	get_threshold(SLOWQUERY_MINTIME, &monitor_threshold);
	if (monitor_threshold.threshold_warning != 0)
		slowlogmintime = monitor_threshold.threshold_warning;
	get_threshold(SLOWLOG_GETNUMONCE, &monitor_threshold);
	if (monitor_threshold.threshold_warning != 0)
		slowlognumoncetime = monitor_threshold.threshold_warning;
	appendStringInfo(&sqlslowlogStrData, "select usename, calls, total_time/1000 as totaltime, query  from pg_stat_statements, pg_user, pg_database where ( total_time/calls/1000) > %d and userid=usesysid and pg_database.oid = dbid and datname=\'%s\' limit %d;", slowlogmintime, dbname, slowlognumoncetime);
	res = PQexec(conn, sqlslowlogStrData.data);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		PQfinish(conn);
		pfree(constr.data);
		pfree(sqlslowlogStrData.data);
		ereport(ERROR,
			(errmsg("Select failed: %s\n" , PQresultErrorMessage(res))));
	}
	/*get row number*/
	nrow = PQntuples(res);
	time = GetCurrentTimestamp();
	ptimenow = timestamptz_to_time_t(time);

	for(rowloop=0; rowloop<nrow; rowloop++)
	{
		/*get username*/
		dbuser = PQgetvalue(res, rowloop, 0 );
		/*get run time*/
		calls = atoi(PQgetvalue(res, rowloop, 1 ));
		/*get total used time*/
		totaltime = atof(PQgetvalue(res, rowloop, 2 ));
		singletime = totaltime/calls;
		/*get query string*/
		querystr = PQgetvalue(res, rowloop, 3 );
		/*get queryplan*/
		tupleret = check_record_yestoday_today(rel, &callstoday, &callsyestd, &gettoday, &getyesdt, querystr, dbuser, dbname, ptimenow);
		if (false  == gettoday && false == getyesdt)
		{
			/*insert record*/
			monitor_insert_record(rel, time, dbname, dbuser, singletime, calls, querystr, user, address, port);
		}
		else if (true  == gettoday && false == getyesdt)
		{
			monitor_slowlog = (Form_monitor_slowlog)GETSTRUCT(tupleret);
			Assert(monitor_slowlog);
			if (calls > callstoday)
			{
				monitor_slowlog->slowlogtotalnum = calls;
				monitor_slowlog->slowlogsingletime = singletime;
				heap_inplace_update(rel, tupleret);
				pfree(tupleret);
				continue;
			}
			else if (calls < callstoday)
			{
				monitor_slowlog->slowlogtotalnum = calls + callstoday;
				monitor_slowlog->slowlogsingletime = singletime;
				heap_inplace_update(rel, tupleret);
				pfree(tupleret);
				continue;
			}
			else
			{
				/*do nothing*/
			}
		}
		else if (false  == gettoday && true == getyesdt)
		{
			callstmp = calls < callsyestd ? calls:(calls-callsyestd);
			if (calls != callsyestd)
			{
				/*insert the record*/
				monitor_insert_record(rel, time, dbname, dbuser, singletime, callstmp, querystr, user, address, port);
			}
		}
		else /*true  == gettoday && true == getyesdt*/
		{
			monitor_slowlog = (Form_monitor_slowlog)GETSTRUCT(tupleret);
			Assert(monitor_slowlog);
			if (calls < callstoday)
			{
				monitor_slowlog->slowlogtotalnum = calls + callstoday;
				monitor_slowlog->slowlogsingletime = singletime;
				heap_inplace_update(rel, tupleret);
				pfree(tupleret);
			}
			else if (calls > callstoday)
			{
				if(calls <= callsyestd)
				{
					monitor_slowlog->slowlogtotalnum = calls;
				}
				else
				{
					monitor_slowlog->slowlogtotalnum = calls - callsyestd;
				}
				monitor_slowlog->slowlogsingletime = singletime;
				heap_inplace_update(rel, tupleret);
				pfree(tupleret);
				
			}
			else
			{
				/*do nothing*/
			}
			
		}

	}

	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
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
	bool haveget = false;
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	
	rel_slowlog = heap_open(MslowlogRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (! mgr_node->nodeincluster)
			continue;
		/*get user, address, coordport*/
		coordport = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if(!haveget)
		{
			user = get_hostuser_from_hostoid(mgr_node->nodehost);
			/*get database name list*/
			dbnamelist = monitor_get_dbname_list(user, address, coordport);
			if(dbnamelist == NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("get database namelist error")));
			}
			dbnum = list_length(dbnamelist);
			Assert(dbnum > 0);
		}
		Assert(address != NULL);
		Assert(user != NULL);
		haveget = true;
		foreach(cell, dbnamelist)
		{
			dbname = (char *)(lfirst(cell));
			monitor_get_onedb_slowdata_insert(rel_slowlog, user, address, coordport, dbname);
		}
		pfree(address);
	}
	heap_endscan(rel_scan);
	if (dbnamelist)
		list_free(dbnamelist);
	if(user)
		pfree(user);
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
		&& desc->attrs[0]->atttypid == NAMEOID
		&& desc->attrs[1]->atttypid == NAMEOID
		&& desc->attrs[2]->atttypid == FLOAT4OID
		&& desc->attrs[3]->atttypid == INT4OID
		&& desc->attrs[4]->atttypid == TIMESTAMPTZOID
		&& desc->attrs[5]->atttypid == TEXTOID
		&& desc->attrs[6]->atttypid == TEXTOID
		);
	memset(datums, 0, sizeof(datums));
	memset(nulls, 0, sizeof(nulls));
	datums[0] = NameGetDatum(&dbnamedata);
	datums[1] = NameGetDatum(&usernamedata);
	datums[2] = Float4GetDatum(singletime);
	datums[3] = Int32GetDatum(totalnum);
	datums[4] = TimestampTzGetDatum(time);
	datums[5] = CStringGetTextDatum(query);
	datums[6] = CStringGetTextDatum(queryplan);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = nulls[5] = nulls[6] =false;
	
	return heap_form_tuple(desc, datums, nulls);
}

HeapTuple check_record_yestoday_today(Relation rel, int *callstoday, int *callsyestd, bool *gettoday, bool *getyesdt, char *query, char *user, char *dbname, pg_time_t ptimenow)
{
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	HeapTuple tupleret = NULL;
	Form_monitor_slowlog monitor_slowlog;
	Datum datumquery;
	Datum datumtime;
	bool isNull = false;
	char *querystr = NULL;
	pg_time_t tupleptime;
	pg_time_t pgtimetoday;
	pg_time_t pgtimeyestoday;
	pg_time_t pgtimetomarrow;
	TimestampTz tupletime;

	pgtimetoday = GETTODAYSTARTTIME(ptimenow);
	pgtimeyestoday = GETLASTDAYSTARTTIME(ptimenow);
	pgtimetomarrow = GETTOMARROWSTARTTIME(ptimenow);	
	rel_scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		/*check the time*/
		datumtime = heap_getattr(tuple, Anum_monitor_slowlog_time, RelationGetDescr(rel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_slowlog")
				, errmsg("column slowlogtime is null")));
		}
		tupletime = DatumGetTimestampTz(datumtime);
		tupleptime = timestamptz_to_time_t(tupletime);
		/*for yestoday, today record*/
		if(tupleptime >= pgtimeyestoday && tupleptime < pgtimetomarrow)
		{
			/*check query, user, dbname*/
			monitor_slowlog = (Form_monitor_slowlog)GETSTRUCT(tuple);
			Assert(monitor_slowlog);
			if (strcmp(NameStr(monitor_slowlog->slowloguser), user) == 0 && strcmp(NameStr(monitor_slowlog->slowlogdbname), dbname) == 0)
			{
				/*check query*/
				datumquery = heap_getattr(tuple, Anum_monitor_slowlog_query, RelationGetDescr(rel), &isNull);
				if(isNull)
				{
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_slowlog")
						, errmsg("column slowlogquery is null")));
				}
				querystr = TextDatumGetCString(datumquery);
				if(strcmp(querystr, query)==0)
				{
					if(tupleptime < pgtimetoday)
					{
						*getyesdt = true;
						*callsyestd = monitor_slowlog->slowlogtotalnum;
					}
					else
					{
						*gettoday = true;
						*callstoday = monitor_slowlog->slowlogtotalnum;
						tupleret = heap_copytuple(tuple);
					}
				}
			}
			
		}
	}
	heap_endscan(rel_scan);
	return tupleret;
}

void monitor_insert_record(Relation rel, TimestampTz time, char *dbname, char *dbuser, float singletime, int calls, char *querystr, char *user, char *address, int port)
{
	int queryplanlen = 0;
	char *getplanerror = "check the query, cannot get the queryplan.";
	HeapTuple tup_result;
	char *queryplanstr;
	char *queryplanres = NULL;
	
	queryplanlen = strlen(querystr) + strlen("explain ")+1;
	queryplanstr = (char *)palloc(queryplanlen*sizeof(char));
	strcpy(queryplanstr, "explain ");
	strcpy(queryplanstr + strlen("explain "), querystr);
	queryplanstr[queryplanlen-1] = '\0';
	queryplanres = monitor_get_onestrvalue_one_node(queryplanstr, user, address, port, dbname);
	if (NULL == queryplanres)
	{
		tup_result = monitor_build_slowlog_tuple(rel, time, dbname, dbuser, singletime, calls, querystr, getplanerror);
	}
	else
	{
		tup_result = monitor_build_slowlog_tuple(rel, time, dbname, dbuser, singletime, calls, querystr, queryplanres);
		pfree(queryplanres);
	}
	simple_heap_insert(rel, tup_result);
	CatalogUpdateIndexes(rel, tup_result);
	heap_freetuple(tup_result);
	pfree(queryplanstr);
	
}