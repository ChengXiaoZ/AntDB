/*
 * commands of node
 */
#include <stdint.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "../../interfaces/libpq/libpq-fe.h"
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/monitor_databaseitem.h"
#include "catalog/monitor_databasetps.h"
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
#include "utils/lsyscache.h"
#include "access/xact.h"
#include "utils/date.h"

static void monitor_get_sum_all_onetypenode_onedb(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int iarray[], int len);

#define DEFAULT_DB "postgres"

typedef enum ResultChoice
{
	GET_MIN = 0,
	GET_MAX,
	GET_SUM
}ResultChoice;

/*see the content of adbmgr_init.sql: "insert into pg_catalog.monitor_host_threshold"
* the values are the same in adbmgr_init.sql for given items
*/
typedef enum ThresholdItem
{
	TPS_TIMEINTERVAL = 31,
	LONGTRANS_MINTIME = 32
}ThresholdItem;
/*
* get one value from the given sql
*/
int monitor_get_onesqlvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname)
{
	int result = -1;
	StringInfoData resultstrdata;

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, sqlstr, user, address, nodeport, dbname, &resultstrdata);
	if (resultstrdata.len != 0)
	{
		result = atoi(resultstrdata.data);
	}
	pfree(resultstrdata.data);

	return result;
}

/*
* get sql'result just need execute on one coordinator
*/
int monitor_get_result_one_node(Relation rel_node, char *sqlstr, char *dbname, char nodetype)
{
	int coordport;
	int agentport;
	int ret;
	char *hostaddress = NULL;
	char *user = NULL;
	
	monitor_get_one_node_user_address_port(rel_node, &agentport, &user, &hostaddress, &coordport, nodetype);
	Assert(hostaddress != NULL);
	Assert(user != NULL);
	ret = monitor_get_onesqlvalue_one_node(agentport, sqlstr, user, hostaddress, coordport, dbname);
	pfree(user);
	pfree(hostaddress);
	
	return ret;	
}

int monitor_get_sqlres_all_typenode_usedbname(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int gettype)
{
	/*get datanode master user, port*/
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	char *user;
	char *address;
	int port;
	int result = 0;
	int resulttmp = 0;
	int agentport;
	bool bfirst = true;
	
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
		port = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
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
		resulttmp = monitor_get_onesqlvalue_one_node(agentport, sqlstr, user, address, port, dbname);
		if(bfirst && gettype==GET_MIN) result = resulttmp;
		bfirst = false;
		switch(gettype)
		{
			case GET_MIN:
				if(resulttmp < result) result = resulttmp;
				break;
			case GET_MAX:
				if(resulttmp>result) result = resulttmp;
				break;
			case GET_SUM:
				result = result + resulttmp;
				break;
			default:
				result = 0;
				break;
		};
		pfree(user);
		pfree(address);
	}
	heap_endscan(rel_scan);
	
	return result;
}

Datum monitor_databaseitem_insert_data(PG_FUNCTION_ARGS)
{
	char *user = NULL;
	char *hostaddress = NULL;
	char *dbname;
	int coordport = 0;
	int dbsize = 0;
	int heaphit = 0;
	int heapread = 0;
	int commit = 0;
	int rollback = 0;
	int preparenum = 0;
	int unusedindexnum = 0;
	int locksnum = 0;
	int longquerynum = 0;
	int idlequerynum = 0;
	int connectnum = 0;
	bool bautovacuum = false;
	bool barchive = false;
	bool bfrist = true;
	int dbage = 0;
	int standbydelay = 0;
	int indexsize = 0;
	int longtransmintime = 100;
	int iloop = 0;
	int iarray_heaphit_read_indexsize[3] = {0,0,0};
	int iarray_commit_connect_longidle_prepare[6] = {0, 0, 0, 0, 0, 0};
	int agentport;
	float heaphitrate = 0;
	float commitrate = 0;
	List *dbnamelist = NIL;
	ListCell *cell;
	HeapTuple tuple;
	TimestampTz time;
	Relation rel;
	Relation rel_node;
	StringInfoData sqldbsizeStrData;
	StringInfoData sqllocksStrData;
	StringInfoData sqlstr_heaphit_read_indexsize;
	StringInfoData sqlstr_commit_connect_longidle_prepare;
	char *sqlunusedindex = "select count(*) from  pg_stat_user_indexes where idx_scan = 0";
	char *sqlstrstandbydelay = "select CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location() THEN 0  ELSE round(EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp())) end;";
	Monitor_Threshold monitor_threshold;
	
	rel = heap_open(MdatabaseitemRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*get database list*/
	monitor_get_one_node_user_address_port(rel_node, &agentport, &user, &hostaddress, &coordport, CNDN_TYPE_COORDINATOR_MASTER);
	Assert(user != NULL);
	Assert(hostaddress != NULL);
	dbnamelist = monitor_get_dbname_list(user, hostaddress, coordport);
	if(dbnamelist == NULL)
	{
		pfree(user);
		pfree(hostaddress);
		heap_close(rel, RowExclusiveLock);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get database namelist error")));
	}
	time = GetCurrentTimestamp();
	/*get long transaction min time from table: MonitorHostThresholdRelationId*/
	get_threshold(LONGTRANS_MINTIME, &monitor_threshold);
	if (monitor_threshold.threshold_warning !=0)
	{
		longtransmintime = monitor_threshold.threshold_warning;
	}
	initStringInfo(&sqldbsizeStrData);
	initStringInfo(&sqllocksStrData);
	initStringInfo(&sqlstr_heaphit_read_indexsize);
	initStringInfo(&sqlstr_commit_connect_longidle_prepare);
	foreach(cell, dbnamelist)
	{
		dbname = (char *)(lfirst(cell));
		/* get database size on coordinator*/
		appendStringInfo(&sqldbsizeStrData, "select round(pg_database_size(datname)::numeric(18,4)/1024/1024) from pg_database where datname=\'%s\';", dbname);
		/*dbsize, unit MB*/
		dbsize = monitor_get_result_one_node(rel_node, sqldbsizeStrData.data, DEFAULT_DB, CNDN_TYPE_COORDINATOR_MASTER);
		for (iloop=0; iloop<3; iloop++)
		{
			iarray_heaphit_read_indexsize[iloop] = 0;
		}
		/*heaphit, heapread, indexsize*/
		appendStringInfoString(&sqlstr_heaphit_read_indexsize, "select sum(heap_blks_hit) from pg_statio_user_tables union all select sum(heap_blks_read) from pg_statio_user_tables union all select round(sum(pg_catalog.pg_table_size(c.oid))::numeric(18,4)/1024/1024) from pg_catalog.pg_class c  WHERE c.relkind = 'i';");
		monitor_get_sum_all_onetypenode_onedb(rel_node, sqlstr_heaphit_read_indexsize.data, dbname, CNDN_TYPE_DATANODE_MASTER, iarray_heaphit_read_indexsize, 3);
		
		heaphit = iarray_heaphit_read_indexsize[0];
		heapread = iarray_heaphit_read_indexsize[1];
		if((heaphit + heapread) == 0)
			heaphitrate = 100;
		else
			heaphitrate = heaphit*100.0/(heaphit + heapread);
		/*the database index size, unit: MB */
		indexsize = iarray_heaphit_read_indexsize[2];
		
		/*get all coordinators' result then sum them*/		
		/*
		* xact_commit, xact_rollback, numbackends, longquerynum, idlequerynum, preparednum
		*/
		for (iloop=0; iloop<3; iloop++)
		{
			iarray_commit_connect_longidle_prepare[iloop] = 0;
		}
		appendStringInfo(&sqlstr_commit_connect_longidle_prepare,"select xact_commit from pg_stat_database where datname = \'%s\' union all select xact_rollback from pg_stat_database where datname = \'%s\' union all select numbackends from pg_stat_database where datname = \'%s\' union all select count(*) from  pg_stat_activity where extract(epoch from (query_start-now())) > %d and datname=\'%s\' union all select count(*) from pg_stat_activity where state='idle' and datname = \'%s\' union all select count(*) from pg_prepared_xacts where database= \'%s\';", dbname, dbname, dbname, longtransmintime, dbname, dbname, dbname);
		monitor_get_sum_all_onetypenode_onedb(rel_node, sqlstr_commit_connect_longidle_prepare.data, dbname, CNDN_TYPE_COORDINATOR_MASTER, iarray_commit_connect_longidle_prepare, 6);
		
		/*xact_commit_rate on coordinator*/
		commit = iarray_commit_connect_longidle_prepare[0];
		rollback = iarray_commit_connect_longidle_prepare[1];
		if((commit + rollback) == 0)
			commitrate = 100;
		else
			commitrate = commit*100.0/(commit + rollback);
		/*connect num*/
		connectnum = iarray_commit_connect_longidle_prepare[2];
		/*get long query num on coordinator*/
		longquerynum = iarray_commit_connect_longidle_prepare[3];
		idlequerynum = iarray_commit_connect_longidle_prepare[4];
		/*prepare query num on coordinator*/
		preparenum = iarray_commit_connect_longidle_prepare[5];
		
		
		/*unused index on datanode master, get min
		* " select count(*) from  pg_stat_user_indexes where idx_scan = 0"  on one database, get min on every dn master
		*/
		unusedindexnum = monitor_get_sqlres_all_typenode_usedbname(rel_node, sqlunusedindex, dbname, CNDN_TYPE_DATANODE_MASTER, GET_MIN);
		
		/*get locks on coordinator, get max*/
		appendStringInfo(&sqllocksStrData, "select count(*) from pg_locks ,pg_database where pg_database.Oid = pg_locks.database and pg_database.datname=\'%s\';", dbname);
		locksnum = monitor_get_sqlres_all_typenode_usedbname(rel_node, sqllocksStrData.data, dbname, CNDN_TYPE_COORDINATOR_MASTER, GET_MAX);
		
		
		/*autovacuum*/
		if(bfrist)
		{
			/*these vars just need get one time, from coordinator*/
			char *sqlstr_vacuum_archive_dbage = "select case when setting = \'on\' then 1 else 0 end from pg_settings where name=\'autovacuum\' union all select case when setting = \'on\' then 1 else 0 end from pg_settings where name=\'archive_mode\' union all select max(age(datfrozenxid)) from pg_database";
			int iarray_vacuum_archive_dbage[3] = {0,0,0};
			monitor_get_sqlvalues_one_node(agentport, sqlstr_vacuum_archive_dbage, user, hostaddress, coordport,DEFAULT_DB, iarray_vacuum_archive_dbage, 3);
			
			bautovacuum = (iarray_vacuum_archive_dbage[0] == 0 ? false:true);
			barchive = (iarray_vacuum_archive_dbage[1] == 0 ? false:true);
			/*get database age*/
			dbage = iarray_vacuum_archive_dbage[2];
		
			/*standby delay*/
			standbydelay = monitor_get_sqlres_all_typenode_usedbname(rel_node, sqlstrstandbydelay, DEFAULT_DB, CNDN_TYPE_DATANODE_SLAVE, GET_MAX);
		}

		/*build tuple*/
		tuple = monitor_build_database_item_tuple(rel, time, dbname, dbsize, barchive, bautovacuum, heaphitrate, commitrate, dbage, connectnum, standbydelay, locksnum, longquerynum, idlequerynum, preparenum, unusedindexnum, indexsize);
		simple_heap_insert(rel, tuple);
		CatalogUpdateIndexes(rel, tuple);
		heap_freetuple(tuple);
		resetStringInfo(&sqldbsizeStrData);
		resetStringInfo(&sqllocksStrData);
		resetStringInfo(&sqlstr_heaphit_read_indexsize);
		resetStringInfo(&sqlstr_commit_connect_longidle_prepare);
		bfrist = false;
	}
	pfree(user);
	pfree(hostaddress);
	pfree(sqldbsizeStrData.data);
	pfree(sqllocksStrData.data);
	pfree(sqlstr_heaphit_read_indexsize.data);
	pfree(sqlstr_commit_connect_longidle_prepare.data);
	list_free(dbnamelist);
	heap_close(rel, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
	PG_RETURN_TEXT_P(cstring_to_text("insert_data"));
}

/*
* build tuple for table: monitor_databasetps, see: monitor_databasetps.h
*/
HeapTuple monitor_build_database_item_tuple(Relation rel, const TimestampTz time, char *dbname
			, int dbsize, bool archive, bool autovacuum, float heaphitrate,  float commitrate, int dbage, int connectnum, int standbydelay, int locksnum, int longquerynum, int idlequerynum, int preparenum, int unusedindexnum, int indexsize)
{
	Datum datums[16];
	bool nulls[16];
	TupleDesc desc;
	NameData name;
	int idex = 0;
	
	desc = RelationGetDescr(rel);
	namestrcpy(&name, dbname);
	AssertArg(desc && desc->natts == 16
		&& desc->attrs[0]->atttypid == TIMESTAMPTZOID
		&& desc->attrs[1]->atttypid == NAMEOID
		&& desc->attrs[2]->atttypid == INT4OID
		&& desc->attrs[3]->atttypid == BOOLOID
		&& desc->attrs[4]->atttypid == BOOLOID
		&& desc->attrs[5]->atttypid == FLOAT4OID
		&& desc->attrs[6]->atttypid == FLOAT4OID
		&& desc->attrs[7]->atttypid == INT4OID
		&& desc->attrs[8]->atttypid == INT4OID
		&& desc->attrs[9]->atttypid == INT4OID
		&& desc->attrs[10]->atttypid == INT4OID
		&& desc->attrs[11]->atttypid == INT4OID
		&& desc->attrs[12]->atttypid == INT4OID
		&& desc->attrs[13]->atttypid == INT4OID
		&& desc->attrs[14]->atttypid == INT4OID
		&& desc->attrs[15]->atttypid == INT4OID
		);
	memset(datums, 0, sizeof(datums));
	memset(nulls, 0, sizeof(nulls));
	datums[0] = TimestampTzGetDatum(time);
	datums[1] = NameGetDatum(&name);
	datums[2] = Int32GetDatum(dbsize);
	datums[3] = BoolGetDatum(archive);
	datums[4] = BoolGetDatum(autovacuum);
	datums[5] = Float4GetDatum(heaphitrate);
	datums[6] = Float4GetDatum(commitrate);
	datums[7] = Int32GetDatum(dbage);
	datums[8] = Int32GetDatum(connectnum);
	datums[9] = Int32GetDatum(standbydelay);
	datums[10] = Int32GetDatum(locksnum);
	datums[11] = Int32GetDatum(longquerynum);
	datums[12] = Int32GetDatum(idlequerynum);
	datums[13] = Int32GetDatum(preparenum);
	datums[14] = Int32GetDatum(unusedindexnum);
	datums[15] = Int32GetDatum(indexsize);
	
	for (idex=0; idex<sizeof(nulls); idex++)
		nulls[idex] = false;
	
	return heap_form_tuple(desc, datums, nulls);
}


/*for table: monitor_databasetps, see monitor_databasetps.h*/


/*
* get database tps, qps in cluster then insert them to table
*/
Datum monitor_databasetps_insert_data(PG_FUNCTION_ARGS)
{
	Relation rel;
	Relation rel_node;
	TimestampTz time;
	int pgdbruntime;
	HeapTuple tup_result;
	List *dbnamelist = NIL;
	ListCell *cell;
	int **dbtps = NULL;
	int **dbqps = NULL;
	int tps = 0;
	int qps = 0;
	int dbnum = 0;
	int coordport = 0;
	int iloop = 0;
	int idex = 0;
	int sleepTime = 3;
	int agentport = 0;
	const int ncol = 2;
	char *user = NULL;
	char *hostaddress = NULL;
	char *dbname = NULL;
	StringInfoData sqltpsStrData;
	StringInfoData sqlqpsStrData;
	StringInfoData sqldbruntimeStrData;
	Monitor_Threshold monitor_threshold;
	
	rel = heap_open(MdatabasetpsRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*get user, address, port of coordinator*/
	monitor_get_one_node_user_address_port(rel_node, &agentport, &user, &hostaddress, &coordport, CNDN_TYPE_COORDINATOR_MASTER);
	Assert(user != NULL);
	Assert(hostaddress != NULL);
	/*get database namelist*/
	dbnamelist = monitor_get_dbname_list(user, hostaddress, coordport);
	pfree(user);
	pfree(hostaddress);
	if(dbnamelist == NULL)
	{
		heap_close(rel, RowExclusiveLock);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get database namelist error")));
	}
	dbnum = list_length(dbnamelist);
	Assert(dbnum > 0);
	dbtps = (int **)palloc(sizeof(int *)*dbnum);
	dbqps = (int **)palloc(sizeof(int *)*dbnum);
	iloop = 0;
	while(iloop < dbnum)
	{
		dbtps[iloop] = (int *)palloc(sizeof(int)*ncol);
		dbqps[iloop] = (int *)palloc(sizeof(int)*ncol);
		iloop++;
	}

	initStringInfo(&sqltpsStrData);
	initStringInfo(&sqlqpsStrData);
	initStringInfo(&sqldbruntimeStrData);
	time = GetCurrentTimestamp();

	iloop = 0;
	/*get tps timeinterval from table:MonitorHostThresholdRelationId*/
	get_threshold(TPS_TIMEINTERVAL, &monitor_threshold);
	if(monitor_threshold.threshold_warning != 0)
		sleepTime = monitor_threshold.threshold_warning;
	while(iloop<ncol)
	{
		idex = 0;
		foreach(cell, dbnamelist)
		{
			dbname = (char *)(lfirst(cell));
			appendStringInfo(&sqltpsStrData, "select xact_commit+xact_rollback from pg_stat_database where datname = \'%s\';",  dbname);
			appendStringInfo(&sqlqpsStrData, "select sum(calls)from pg_stat_statements, pg_database where dbid = pg_database.oid and pg_database.datname=\'%s\';",  dbname);
			/*get given database tps first*/
			dbtps[idex][iloop] = monitor_get_sqlres_all_typenode_usedbname(rel_node, sqltpsStrData.data, DEFAULT_DB, CNDN_TYPE_COORDINATOR_MASTER, GET_SUM);
			/*get given database qps first*/
			dbqps[idex][iloop] = monitor_get_sqlres_all_typenode_usedbname(rel_node, sqlqpsStrData.data, DEFAULT_DB, CNDN_TYPE_COORDINATOR_MASTER, GET_SUM);
			resetStringInfo(&sqltpsStrData);
			resetStringInfo(&sqlqpsStrData);
			idex++;
		}
		iloop++;
		if(iloop < ncol)
			sleep(sleepTime);
	}
	/*insert data*/
	idex = 0;
	foreach(cell, dbnamelist)
	{
		dbname = (char *)(lfirst(cell));
		tps = abs(dbtps[idex][1] - dbtps[idex][0])/sleepTime;
		qps = abs(dbqps[idex][1] - dbqps[idex][0])/sleepTime;
		appendStringInfo(&sqldbruntimeStrData, "select case when  stats_reset IS NULL then  0 else  round(abs(extract(epoch from now())- extract(epoch from  stats_reset))) end from pg_stat_database where datname = \'%s\';", dbname);
		pgdbruntime = monitor_get_result_one_node(rel_node, sqldbruntimeStrData.data, DEFAULT_DB, CNDN_TYPE_COORDINATOR_MASTER);
		tup_result = monitor_build_databasetps_qps_tuple(rel, time, dbname, tps, qps, pgdbruntime);
		simple_heap_insert(rel, tup_result);
		CatalogUpdateIndexes(rel, tup_result);
		heap_freetuple(tup_result);
		resetStringInfo(&sqldbruntimeStrData);
		idex++;
	}
	/*pfree dbtps, dbqps*/
	iloop = 0;
	while(iloop < dbnum)
	{
		pfree((int *)dbtps[iloop]);
		pfree((int *)dbqps[iloop]);
		iloop++;
	}
	pfree(dbtps);
	pfree(dbqps);	
	pfree(sqltpsStrData.data);
	pfree(sqlqpsStrData.data);
	pfree(sqldbruntimeStrData.data);
	list_free(dbnamelist);
	heap_close(rel, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
	PG_RETURN_TEXT_P(cstring_to_text("insert_data"));
}
/*
* build tuple for table: monitor_databasetps, see: monitor_databasetps.h
*/
HeapTuple monitor_build_databasetps_qps_tuple(Relation rel, const TimestampTz time, const char *dbname, const int tps, const int qps, int pgdbruntime)
{
	Datum datums[5];
	bool nulls[5];
	TupleDesc desc;
	NameData name;
	
	desc = RelationGetDescr(rel);
	namestrcpy(&name, dbname);
	AssertArg(desc && desc->natts == 5
		&& desc->attrs[0]->atttypid == TIMESTAMPTZOID
		&& desc->attrs[1]->atttypid == NAMEOID
		&& desc->attrs[2]->atttypid == INT4OID
		&& desc->attrs[3]->atttypid == INT4OID
		&& desc->attrs[4]->atttypid == INT4OID
		);
	memset(datums, 0, sizeof(datums));
	memset(nulls, 0, sizeof(nulls));
	datums[0] = TimestampTzGetDatum(time);
	datums[1] = NameGetDatum(&name);
	datums[2] = Int32GetDatum(tps);
	datums[3] = Int32GetDatum(qps);
	datums[4] = Int32GetDatum(pgdbruntime);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = false;
	
	return heap_form_tuple(desc, datums, nulls);
}
/*get the sqlstr values from given sql on given typenode, then sum them
* for example: we want to get caculate cmmitrate, which on coordinators, we need get sum
* commit on all coordinators and sum rollback on all coordinators. the input parameters: len
* is the num we want, iarray is the values restore.
*/
static void monitor_get_sum_all_onetypenode_onedb(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int iarray[], int len)
{
	/*get node user, port*/
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	char *user = NULL;
	char *address = NULL;
	int port;
	int agentport = 0;
	int *iarraytmp;
	int iloop = 0;
	bool bfirst = true;
	
	iarraytmp = (int *)palloc(sizeof(int)*len);
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
		port = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if(bfirst)
		{
			user = get_hostuser_from_hostoid(mgr_node->nodehost);
		}
		bfirst = false;
		memset(iarraytmp, 0, len*sizeof(int));
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
		monitor_get_sqlvalues_one_node(agentport, sqlstr, user, address, port,dbname, iarraytmp, len);
		for(iloop=0; iloop<len; iloop++)
		{
			iarray[iloop] += iarraytmp[iloop];
		}
		pfree(address);
	}
	if(user)
	{
		pfree(user);
	}
	heap_endscan(rel_scan);
	pfree(iarraytmp);
}

void monitor_get_stringvalues(char cmdtype, int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname, StringInfo resultstrdata)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg;
	StringInfoData buf;
	char *nodeportstr;
	
	resetStringInfo(resultstrdata);
	initStringInfo(&sendstrmsg);
	nodeportstr = (char *) palloc(7);
	memset(nodeportstr, 0, 7*sizeof(char));
	pg_itoa(nodeport, nodeportstr);
	/*sequence:user port dbname sqlstr, delimiter by '\0'*/
	/*user*/
	appendStringInfoString(&sendstrmsg, user);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	/*port*/
	appendStringInfoString(&sendstrmsg, nodeportstr);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	pfree(nodeportstr);
	/*dbname*/
	appendStringInfoString(&sendstrmsg, dbname);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	/*sqlstring*/
	appendStringInfoString(&sendstrmsg, sqlstr);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	ma = ma_connect(address, (unsigned short)agentport);
	if(!ma_isconnected(ma))
	{
		/*report error message */
		ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION)
			,errmsg("%s", ma_last_error_msg(ma))));
		ma_close(ma);
		return;
	}
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION)
			,errmsg("%s", ma_last_error_msg(ma))));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	mgr_recv_sql_stringvalues_msg(ma, resultstrdata);
	ma_close(ma);
	if (resultstrdata->data == NULL)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get sqlstr:%s \n\tresult fail", sqlstr)));
		return;
	}
}