/*
 * commands of dbthreshold
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

#define DEFAULT_DB "postgres"
#define MONITOR_CLUSTERSTR "cluster"

/*see the content : insert into mgr.dbthreshold in adbmgr_init.sql*/
typedef enum DbthresholdObject
{
	OBJECT_NODE_HEAPHIT = 11,
	OBJECT_NODE_COMMITRATE,
	OBJECT_NODE_STANDBYDELAY,
	OBJECT_NODE_LOCKS,
	OBJECT_NODE_CONNECT,
	OBJECT_NODE_LONGTRANS,
	OBJECT_NODE_UNUSEDINDEX,
	OBJECT_CLUSTER_HEAPHIT = 21,
	OBJECT_CLUSTER_COMMITRATE,
	OBJECT_CLUSTER_STANDBYDELAY,
	OBJECT_CLUSTER_LOCKS,
	OBJECT_CLUSTER_CONNECT,
	OBJECT_CLUSTER_LONGTRANS,
	OBJECT_CLUSTER_UNUSEDINDEX
}DbthresholdObject;

typedef enum AlarmLevel
{
	ALARM_WARNING = 1,
	ALARM_CRITICAL,
	ALARM_EMERGENCY
}AlarmLevel;

static void mthreshold_levelvalue_positiveseq(DbthresholdObject objectype, char *address, char *time, int value, char *descp);
static void mthreshold_levelvalue_impositiveseq(DbthresholdObject objectype, char *address, char * time, int value, char *descp);
static void  mthreshold_standbydelay();


Datum get_dbthreshold(PG_FUNCTION_ARGS)
{
	mthreshold_sqlvaluesfrom_dnmaster();
	mthreshold_sqlvaluesfrom_coord();
	mthreshold_standbydelay();
	PG_RETURN_TEXT_P(cstring_to_text("insert_data"));
}

/*get timestamptz from given node */
char *monitor_get_timestamptz_onenode(char *user, char *address, int port)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	char *oneNodeValueStr;
	char *sqlstr = "select now();";
	
	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, DEFAULT_DB);
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
	oneNodeValueStr = pstrdup(PQgetvalue(res, 0, 0 ));
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
	return oneNodeValueStr;
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

/*monitor items: heaphitrate, unusedindex
* the monitor values of heaphitrate and unusedindexs from datanode masters, if the monitor values 
* larger then item threshold, record the data to monitor_alarm table
*/
void  mthreshold_sqlvaluesfrom_dnmaster()
{
	Relation hostrel;
	Relation noderel;
	HeapScanDesc hostrel_scan;
	HeapScanDesc noderel_scan;
	Form_mgr_host mgr_host;
	Form_mgr_node mgr_node;
	HeapTuple hosttuple;
	HeapTuple nodetuple;
	bool isNull = false;
	Datum datumaddress;
	Oid hostoid;
	ScanKeyData key[1];
	int phynodeheaphit = 0;
	int clusterheaphit = 0;
	int phynodeheapread = 0;
	int clusterheapread = 0;
	int phyheaphitrate = 100;
	int clusterheaphitrate = 100;
	int phynodeunusedindex = 0;
	int clusterunusedindex = 0;
	int port = 0;
	/*len include: heap_blks_hit, heap_blks_read, unusedindex*/
	int len = 3;
	int iarray[3]={0,0,0};
	NameData ndatauser;
	List *dbnamelist = NIL;
	ListCell *cell;
	char *address;
	char *dbname = NULL;
	char *sqlstr = "select  case sum(heap_blks_hit) is null when true then 0 else  sum(heap_blks_hit) end from pg_statio_user_tables union all select case sum(heap_blks_read) is null when true then 0 else  sum(heap_blks_hit) end from pg_statio_user_tables union all select count(*) from  pg_stat_user_indexes where idx_scan = 0;";
	bool getnode = false;
	char *nodetime;
	char *clustertime;
	
	hostrel = heap_open(HostRelationId, RowExclusiveLock);
	hostrel_scan = heap_beginscan(hostrel, SnapshotNow, 0, NULL);
	noderel = heap_open(NodeRelationId, RowExclusiveLock);
	while((hosttuple = heap_getnext(hostrel_scan, ForwardScanDirection)) != NULL)
	{
		getnode = false;
		mgr_host = (Form_mgr_host)GETSTRUCT(hosttuple);
		Assert(mgr_host);
		datumaddress = heap_getattr(hosttuple, Anum_mgr_host_hostaddr, RelationGetDescr(hostrel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddress is null")));
		}
		address = TextDatumGetCString(datumaddress);
		namestrcpy(&ndatauser, NameStr(mgr_host->hostuser));
		hostoid = HeapTupleGetOid(hosttuple);
		/*find datanode master in node systbl, which hosttuple's nodehost is hostoid*/
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodehost
			,BTEqualStrategyNumber, F_OIDEQ
			,ObjectIdGetDatum(hostoid));
		noderel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
		phynodeheaphit = 0;
		phynodeheapread = 0;
		phynodeunusedindex = 0;
		while((nodetuple = heap_getnext(noderel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(nodetuple);
			Assert(mgr_node);
			/*check the nodetype*/
			if (mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER)
				continue;
			/*get port*/
			port = mgr_node->nodeport;
			dbnamelist = monitor_get_dbname_list(ndatauser.data, address, port);
			foreach(cell, dbnamelist)
			{
				dbname = (char *)(lfirst(cell));
				iarray[0]=iarray[1]=iarray[2]=0;
				monitor_get_sqlvalues_one_node(sqlstr, ndatauser.data, address, port, dbname, iarray, len);
				phynodeheaphit = phynodeheaphit + iarray[0];
				phynodeheapread = phynodeheapread + iarray[1];
				phynodeunusedindex = phynodeunusedindex + iarray[2];
				
			}
			list_free(dbnamelist);
			getnode = true;
			clusterheaphit = clusterheaphit + phynodeheaphit;
			clusterheapread = clusterheapread + phynodeheapread;
			clusterunusedindex = clusterunusedindex + phynodeunusedindex;
		}
		/*check the phynode heaphitrate*/
		if (phynodeheaphit+phynodeheapread != 0)
			phyheaphitrate = phynodeheaphit*100/(phynodeheaphit+phynodeheapread);
	
		if (getnode)
		{
			nodetime = monitor_get_timestamptz_onenode(ndatauser.data, address, port);
			mthreshold_levelvalue_impositiveseq(OBJECT_NODE_HEAPHIT, address, nodetime, phyheaphitrate, "heaphit rate");
			pfree(nodetime);
		}
		
		heap_endscan(noderel_scan);
		
	}
	heap_endscan(hostrel_scan);
	heap_close(hostrel, RowExclusiveLock);
	heap_close(noderel, RowExclusiveLock);
	/*check the cluster heaphitrate*/
	if (clusterheaphit+clusterheapread != 0)
		clusterheaphitrate = clusterheaphit*100/(clusterheaphit+clusterheapread);
	clustertime = timestamptz_to_str(GetCurrentTimestamp());
	mthreshold_levelvalue_impositiveseq(OBJECT_CLUSTER_HEAPHIT, MONITOR_CLUSTERSTR, clustertime, clusterheaphitrate, "heaphit rate");
	mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_UNUSEDINDEX, MONITOR_CLUSTERSTR, clustertime, clusterunusedindex, "unused index");

}

/*
* monitor items : commit/rollback rate, locks num, long transactions, idle transactions, connect num
* the monitor values of them from coordinators, if the monitor values larger then item threshold,
* record the data to monitor_alarm table
*/

void  mthreshold_sqlvaluesfrom_coord()
{
	Relation hostrel;
	Relation noderel;
	HeapScanDesc hostrel_scan;
	HeapScanDesc noderel_scan;
	Form_mgr_host mgr_host;
	Form_mgr_node mgr_node;
	HeapTuple hosttuple;
	HeapTuple nodetuple;
	bool isNull = false;
	Datum datumaddress;
	Oid hostoid;
	ScanKeyData key[1];
	int phynodecommit = 0;
	int clustercommit = 0;
	int phynoderollback = 0;
	int clusterrollback = 0;
	int phynodecommitrate = 100;
	int clustercommitrate = 100;
	int phynodelocks = 0;
	int clusterlocks = 0;
	int phynodelongtrans = 0;
	int phynodeidletrans = 0;
	int clusterlongtrans = 0;
	int clusteridletrans = 0;
	int phynodeconnect = 0;
	int clusterconnect = 0;
	int port = 0;
	int len = 6;
	int iarray[6] = {0,0,0,0,0,0};
	NameData ndatauser;
	char *address;
	char *sqlstr = "select sum(xact_commit)  from pg_stat_database union all select sum(xact_rollback) from pg_stat_database union all select count(1) from pg_locks where database is not null union all select count(*) from  pg_stat_activity where extract(epoch from (query_start-now())) > 200 union all select count(*) from pg_stat_activity where state='idle' union all select sum(numbackends) from pg_stat_database;";
	bool getnode = false;	
	char *nodetime;
	char *clustertime;
	
	hostrel = heap_open(HostRelationId, RowExclusiveLock);
	hostrel_scan = heap_beginscan(hostrel, SnapshotNow, 0, NULL);
	noderel = heap_open(NodeRelationId, RowExclusiveLock);
	while((hosttuple = heap_getnext(hostrel_scan, ForwardScanDirection)) != NULL)
	{
		getnode = false;
		mgr_host = (Form_mgr_host)GETSTRUCT(hosttuple);
		Assert(mgr_host);
		datumaddress = heap_getattr(hosttuple, Anum_mgr_host_hostaddr, RelationGetDescr(hostrel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddress is null")));
		}
		address = TextDatumGetCString(datumaddress);
		namestrcpy(&ndatauser, NameStr(mgr_host->hostuser));
		hostoid = HeapTupleGetOid(hosttuple);
		/*find datanode master in node systbl, which hosttuple's nodehost is hostoid*/
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodehost
			,BTEqualStrategyNumber, F_OIDEQ
			,ObjectIdGetDatum(hostoid));
		noderel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
		phynodecommit = 0;
		phynoderollback = 0;
		phynodelocks = 0;
		phynodelongtrans = 0;
		phynodeidletrans = 0;
		while((nodetuple = heap_getnext(noderel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(nodetuple);
			Assert(mgr_node);
			/*check the nodetype*/
			if (mgr_node->nodetype != CNDN_TYPE_COORDINATOR_MASTER)
				continue;
			getnode = true;
			/*get port*/
			port = mgr_node->nodeport;
			iarray[0]=iarray[1]=iarray[2]=iarray[3]=iarray[4]=iarray[5]=0;
			monitor_get_sqlvalues_one_node(sqlstr, ndatauser.data, address, port, DEFAULT_DB, iarray, len);
			phynodecommit = phynodecommit + iarray[0];
			phynoderollback = phynoderollback + iarray[1];
			phynodelocks = phynodelocks + iarray[2];
			phynodelongtrans = phynodelongtrans + iarray[3];
			phynodeidletrans = phynodeidletrans + iarray[4];
			phynodeconnect = phynodeconnect + iarray[5];
		}
		heap_endscan(noderel_scan);
		clustercommit = clustercommit + phynodecommit;
		clusterrollback = clusterrollback + phynoderollback;
		clusterlocks = clusterlocks + phynodelocks;
		clusterlongtrans = clusterlongtrans + phynodelongtrans;
		clusteridletrans = clusteridletrans + phynodeidletrans;
		clusterconnect = clusterconnect + phynodeconnect;
		/*check the phynode commitrate*/
		if (phynodecommit+phynoderollback != 0)
			phynodecommitrate = phynodecommit*100/(phynodecommit+phynoderollback);
		if (getnode)
		{
			nodetime = monitor_get_timestamptz_onenode(ndatauser.data, address, port);
			mthreshold_levelvalue_impositiveseq(OBJECT_NODE_COMMITRATE, address, nodetime, phynodecommitrate,  "commit rate");
			mthreshold_levelvalue_positiveseq(OBJECT_NODE_LOCKS, address, nodetime, phynodelocks, "locks");
			mthreshold_levelvalue_positiveseq(OBJECT_NODE_LONGTRANS, address, nodetime, phynodelongtrans, "long transactions");
			mthreshold_levelvalue_positiveseq(OBJECT_NODE_LONGTRANS, address, nodetime, phynodeidletrans, "idle transactions");
			mthreshold_levelvalue_positiveseq(OBJECT_NODE_CONNECT, address, nodetime, phynodeconnect, "connect");
			pfree(nodetime);
		}		
	}
	heap_endscan(hostrel_scan);
	heap_close(hostrel, RowExclusiveLock);
	heap_close(noderel, RowExclusiveLock);
	/*check the cluster commitrate*/
	if(clustercommit+clusterrollback != 0)
		clustercommitrate = clustercommit*100/(clustercommit+clusterrollback);
	clustertime = timestamptz_to_str(GetCurrentTimestamp());
	mthreshold_levelvalue_impositiveseq(OBJECT_CLUSTER_COMMITRATE, MONITOR_CLUSTERSTR, clustertime, clustercommitrate, "commit rate");
	mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LOCKS, MONITOR_CLUSTERSTR, clustertime, clusterlocks, "locks");
	mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LONGTRANS, MONITOR_CLUSTERSTR, clustertime, clusterlongtrans, "long transactions");
	mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LONGTRANS, MONITOR_CLUSTERSTR, clustertime, clusteridletrans, "idle transactions");
	mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_CONNECT, MONITOR_CLUSTERSTR, clustertime, clusterconnect, "connect");
}

/*
* monitor item: standby delay
* the monitor values of standby delay from datanode slave, if the monitor values larger then item 
* threshold, record the data to monitor_alarm table
*/

static void  mthreshold_standbydelay()
{
	Relation hostrel;
	Relation noderel;
	HeapScanDesc hostrel_scan;
	HeapScanDesc noderel_scan;
	Form_mgr_host mgr_host;
	Form_mgr_node mgr_node;
	HeapTuple hosttuple;
	HeapTuple nodetuple;
	bool isNull = false;
	Datum datumaddress;
	Oid hostoid;
	ScanKeyData key[1];
	int phynodestandbydelay = 0;
	int clusterstandbydelay = 0;
	int port = 0;
	NameData ndatauser;
	char *address;
	char *sqlstandbydelay = "select CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location() THEN 0  ELSE abs(round(EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()))) end;";
	bool getnode = false;
	char *nodetime;
	char *clustertime;
	
	hostrel = heap_open(HostRelationId, RowExclusiveLock);
	hostrel_scan = heap_beginscan(hostrel, SnapshotNow, 0, NULL);
	noderel = heap_open(NodeRelationId, RowExclusiveLock);
	while((hosttuple = heap_getnext(hostrel_scan, ForwardScanDirection)) != NULL)
	{
		getnode = false;
		mgr_host = (Form_mgr_host)GETSTRUCT(hosttuple);
		Assert(mgr_host);
		datumaddress = heap_getattr(hosttuple, Anum_mgr_host_hostaddr, RelationGetDescr(hostrel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddress is null")));
		}
		address = TextDatumGetCString(datumaddress);
		namestrcpy(&ndatauser, NameStr(mgr_host->hostuser));
		hostoid = HeapTupleGetOid(hosttuple);
		/*find datanode master in node systbl, which hosttuple's nodehost is hostoid*/
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodehost
			,BTEqualStrategyNumber, F_OIDEQ
			,ObjectIdGetDatum(hostoid));
		noderel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
		phynodestandbydelay = 0;
		while((nodetuple = heap_getnext(noderel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(nodetuple);
			Assert(mgr_node);
			/*check the nodetype*/
			if (mgr_node->nodetype != CNDN_TYPE_DATANODE_SLAVE)
				continue;
			getnode = true;
			/*get port*/
			port = mgr_node->nodeport;
			phynodestandbydelay = phynodestandbydelay + monitor_get_onesqlvalue_one_node(sqlstandbydelay, ndatauser.data, address, port, DEFAULT_DB);
			clusterstandbydelay = clusterstandbydelay + phynodestandbydelay;
		}
		heap_endscan(noderel_scan);
		/*check phyical node */
		if (getnode)
		{
			nodetime = monitor_get_timestamptz_onenode(ndatauser.data, address, port);
			mthreshold_levelvalue_positiveseq(OBJECT_NODE_STANDBYDELAY, address, nodetime, phynodestandbydelay, "standby delay");
			pfree(nodetime);
		}
		
	}
	heap_endscan(hostrel_scan);
	heap_close(hostrel, RowExclusiveLock);
	heap_close(noderel, RowExclusiveLock);
  /*check cluster*/
	clustertime = timestamptz_to_str(GetCurrentTimestamp());
	mthreshold_levelvalue_positiveseq(OBJECT_NODE_STANDBYDELAY, MONITOR_CLUSTERSTR, clustertime, clusterstandbydelay, "standby delay");

}

/*
* check the monitor value, the threshold include: warning, critical, emergency, the threshold value is large to small
* for example: heaphitrate, commitrate
*/
static void mthreshold_levelvalue_impositiveseq(DbthresholdObject objectype, char *address, char * time, int value, char *descp)
{
	Monitor_Alarm Monitor_Alarm;
	Monitor_Threshold dbthreshold;
	
	get_threshold(objectype, &dbthreshold);
	if (value > dbthreshold.threshold_warning)
	{
		/*do nothing*/
		return;
	}
	initStringInfo(&(Monitor_Alarm.alarm_text));
	initStringInfo(&(Monitor_Alarm.alarm_source));
	initStringInfo(&(Monitor_Alarm.alarm_timetz));

	if (value <= dbthreshold.threshold_warning && value > dbthreshold.threshold_critical)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d%%, under %d%%", descp,
			value, dbthreshold.threshold_warning);
		Monitor_Alarm.alarm_level = ALARM_WARNING;
	}
	else if (value <= dbthreshold.threshold_critical && value > dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d%%, under %d%%", descp,
			value, dbthreshold.threshold_critical);
		Monitor_Alarm.alarm_level = ALARM_CRITICAL;
	}
	else if (value <= dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d%%, under %d%%", descp,
			value, dbthreshold.threshold_emergency);
		Monitor_Alarm.alarm_level = ALARM_EMERGENCY;
	}
		
	appendStringInfo(&(Monitor_Alarm.alarm_source), "%s",address);
	appendStringInfo(&(Monitor_Alarm.alarm_timetz), "%s", time);
	Monitor_Alarm.alarm_type = 2;
	Monitor_Alarm.alarm_status = 1;
	/*insert data*/
	insert_into_monitor_alarm(&Monitor_Alarm);
	pfree(Monitor_Alarm.alarm_text.data);
	pfree(Monitor_Alarm.alarm_source.data);
	pfree(Monitor_Alarm.alarm_timetz.data);	
}

/*
* check the monitor value, the threshold include: warning, critical, emergency, the threshold value is small to large
* for example: locks, unused_index, connectnums, longtrans, idletrans, unused_index
*/
static void mthreshold_levelvalue_positiveseq(DbthresholdObject objectype, char *address, char *time, int value, char *descp)
{
	Monitor_Alarm Monitor_Alarm;
	Monitor_Threshold dbthreshold;
	
	get_threshold(objectype, &dbthreshold);
	if (value < dbthreshold.threshold_warning)
	{
		/*do nothing*/
		return;
	}
	
	initStringInfo(&(Monitor_Alarm.alarm_text));
	initStringInfo(&(Monitor_Alarm.alarm_source));
	initStringInfo(&(Monitor_Alarm.alarm_timetz));

	if (value>=dbthreshold.threshold_warning && value < dbthreshold.threshold_critical)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d, over %d", descp,
			value, dbthreshold.threshold_warning);
		Monitor_Alarm.alarm_level = ALARM_WARNING;
	}
	else if (value>=dbthreshold.threshold_critical && value < dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d, over %d", descp,
			value, dbthreshold.threshold_critical);
		Monitor_Alarm.alarm_level = ALARM_CRITICAL;
	}
	else if (value>=dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d, over %d", descp,
			value, dbthreshold.threshold_emergency);
		Monitor_Alarm.alarm_level = ALARM_EMERGENCY;
	}

	appendStringInfo(&(Monitor_Alarm.alarm_source), "%s", address);
	appendStringInfo(&(Monitor_Alarm.alarm_timetz), "%s", time);
	Monitor_Alarm.alarm_type = 2;
	Monitor_Alarm.alarm_status = 1;
	/*insert data*/
	insert_into_monitor_alarm(&Monitor_Alarm);
	pfree(Monitor_Alarm.alarm_text.data);
	pfree(Monitor_Alarm.alarm_source.data);
	pfree(Monitor_Alarm.alarm_timetz.data);	
}