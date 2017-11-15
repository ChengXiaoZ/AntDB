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
#include "catalog/mgr_host.h"
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
char *mgr_zone;

/*see the content of adbmgr_init.sql: "insert into pg_catalog.monitor_host_threshold"
* the values are the same in adbmgr_init.sql for given items
*/
typedef enum ThresholdItem
{
	SLOWQUERY_MINTIME = 33,
	SLOWLOG_GETNUMONCE = 34
}ThresholdItem;


/*given one explain sqlstr , return the result*/
char *monitor_get_onestrvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int port, char * dbname)
{
	StringInfoData resultstrdata;

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_EXPLAIN_STRINGVALUES, agentport, sqlstr, user, address, port, dbname, &resultstrdata);	

	return resultstrdata.data;
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

void monitor_get_onedb_slowdata_insert(Relation rel, int agentport, char *user, char *address, int port, char *dbname)
{
	StringInfoData sqlslowlogStrData;
	StringInfoData resultstrdata;
	StringInfoData querystr;
	char strtmp[64];
	char *connectdbname = "postgres";
	char dbuser[64];
	char *pstr = NULL;
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
	int iloop = 0;
	bool gettoday = false;
	bool getyesdt = false;

	Form_monitor_slowlog monitor_slowlog;	
	pg_time_t ptimenow;
	Monitor_Threshold monitor_threshold;
	
	initStringInfo(&sqlslowlogStrData);
	/*get slowlog min time threshold in MonitorHostThresholdRelationId*/
	get_threshold(SLOWQUERY_MINTIME, &monitor_threshold);
	if (monitor_threshold.threshold_warning != 0)
		slowlogmintime = monitor_threshold.threshold_warning;
	get_threshold(SLOWLOG_GETNUMONCE, &monitor_threshold);
	if (monitor_threshold.threshold_warning != 0)
		slowlognumoncetime = monitor_threshold.threshold_warning;
	appendStringInfo(&sqlslowlogStrData, "select usename, calls, total_time/1000 as totaltime, query  from pg_stat_statements, pg_user, pg_database where ( total_time/calls/1000) > %d and userid=usesysid and pg_database.oid = dbid and datname=\'%s\' limit %d;", slowlogmintime, dbname, slowlognumoncetime);
	time = GetCurrentTimestamp();
	ptimenow = timestamptz_to_time_t(time);

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, sqlslowlogStrData.data, user, address, port, connectdbname, &resultstrdata);
	if (resultstrdata.len == 0)
	{
		pfree(resultstrdata.data);
		return;
	}
	initStringInfo(&querystr);
	pstr = resultstrdata.data;
	strtmp[63] = '\0';
	while(*pstr != '\0' && iloop < slowlognumoncetime)
	{
		callstoday = 0;
		callsyestd = 0;
		gettoday = false;
		getyesdt = false;
		iloop++;
		/*get username*/
		strncpy(dbuser, pstr, 63);
		dbuser[strlen(dbuser)] = 0;
		pstr = pstr + strlen(dbuser) + 1;
		/*get run time*/
		if (!pstr)
			ereport(ERROR, (errmsg("get calls from slow log fail")));
		strncpy(strtmp, pstr, 63);
		calls = atoi(strtmp);
		pstr = pstr + strlen(strtmp) + 1;
		/*get total used time*/
		if (!pstr)
			ereport(ERROR, (errmsg("get totaltime from slow log fail")));
		strncpy(strtmp, pstr, 63);
		totaltime = atof(strtmp);
		singletime = totaltime/calls;
		pstr = pstr + strlen(strtmp) + 1;
		/*get query string*/
		if (!pstr)
			ereport(ERROR, (errmsg("get querystr from slow log fail")));
		resetStringInfo(&querystr);
		appendStringInfo(&querystr, "%s", pstr);
		querystr.data[querystr.len] = 0;
		pstr = pstr + querystr.len + 1;
		/*
		* just make all reoords which get from coordinato pg_stat_statements as today's data
		*/
		tupleret = check_record_yestoday_today(rel, &callstoday, &callsyestd, &gettoday, &getyesdt, querystr.data, dbuser, dbname, ptimenow);
	
		if (false  == gettoday && false == getyesdt)
		{
			/*insert record*/
			monitor_insert_record(rel, agentport, time, dbname, dbuser, singletime, calls, querystr.data, user, address, port);
		}
		else if (true  == gettoday && false == getyesdt)
		{
			if (NULL == tupleret)
				continue;
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
				monitor_slowlog->slowlogtotalnum = callstoday;
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
				monitor_insert_record(rel, agentport, time, dbname, dbuser, singletime, callstmp, querystr.data, user, address, port);
			}
		}
		else /*true  == gettoday && true == getyesdt*/
		{
			monitor_slowlog = (Form_monitor_slowlog)GETSTRUCT(tupleret);
			Assert(monitor_slowlog);
			if (calls < callstoday)
			{
				monitor_slowlog->slowlogtotalnum = callstoday;
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

	pfree(resultstrdata.data);
	pfree(querystr.data);
}

/*
* get all database slowlog,then insert them to MslowlogRelationId
*/
Datum monitor_slowlog_insert_data(PG_FUNCTION_ARGS)
{
	int coordport;
	int agentport = 0;
	char *address = NULL;
	char *user = NULL;
	char *dbname = NULL;
	Relation rel_node;
	Relation rel_slowlog;
	List *dbnamelist = NIL;
	ListCell *cell;
	bool haveget = false;
	HeapScanDesc rel_scan;
	ScanKeyData key[2];
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	HeapTuple tuple;
	HeapTuple tup;
	
	rel_slowlog = heap_open(MslowlogRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	rel_scan = heap_beginscan(rel_node, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (! mgr_node->nodeincluster)
			continue;
		/*get user, address, coordport*/
		coordport = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		/*get agent port*/
		tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
		if(!(HeapTupleIsValid(tup)))
		{
			ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errcode(ERRCODE_INTERNAL_ERROR)));
		}
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		agentport = mgr_host->hostagentport;
		ReleaseSysCache(tup);
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
		}
		Assert(address != NULL);
		Assert(user != NULL);
		haveget = true;
		foreach(cell, dbnamelist)
		{
			dbname = (char *)(lfirst(cell));
			monitor_get_onedb_slowdata_insert(rel_slowlog, agentport, user, address, coordport, dbname);
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

/*
* just compare today's data in monitor_slowlog relation table
*/
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
	pg_time_t pgtimetomarrow;
	TimestampTz tupletime;

	*getyesdt = false;
	*callsyestd = 0;

	pgtimetoday = GETTODAYSTARTTIME(ptimenow);
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
		if(tupleptime >= pgtimetoday && tupleptime < pgtimetomarrow)
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
						*gettoday = true;
						*callstoday = monitor_slowlog->slowlogtotalnum;
						tupleret = heap_copytuple(tuple);
				}
			}
			
		}
	}
	heap_endscan(rel_scan);
	return tupleret;
}

void monitor_insert_record(Relation rel, int agentport, TimestampTz time, char *dbname, char *dbuser, float singletime, int calls, char *querystr, char *user, char *address, int port)
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
	queryplanres = monitor_get_onestrvalue_one_node(agentport, queryplanstr, user, address, port, dbname);
	if ('\0' == *queryplanres)
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
