/*
 * commands of parm
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_cndnnode.h"
#include "catalog/mgr_parm.h"
#include "catalog/mgr_updateparm.h"
#include "commands/defrem.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "utils/tqual.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "parser/scansup.h"

#if (Natts_mgr_updateparm != 5)
#error "need change code"
#endif

/*if the parmeter in gtm or coordinator or datanode pg_settins, the nodetype in mgr_parm is '*'
 , if the parmeter in coordinator or datanode pg_settings, the nodetype in mgr_parm is '#'
*/
#define PARM_IN_GTM_CN_DN '*'
#define PARM_IN_CN_DN '#'

typedef enum CheckInsertParmStatus
{
	PARM_NEED_INSERT=1,
	PARM_NEED_UPDATE,
	PARM_NEED_NONE
}CheckInsertParmStatus;

/*
 * Displayable names for context types (enum GucContext)
 *
 * Note: these strings are deliberately not localized.
 */
const char *const GucContext_Parmnames[] =
{
	 /* PGC_INTERNAL */ "internal",
	 /* PGC_POSTMASTER */ "postmaster",
	 /* PGC_SIGHUP */ "sighup",
	 /* PGC_BACKEND */ "backend",
	 /* PGC_SUSET */ "superuser",
	 /* PGC_USERSET */ "user"
};

/*check the enum type parm's value is right*/
const int enumparnnum = 22;
struct enumstruct
{
  char name[50];
  int valuenum;
  char value[20][20];
}enumstruct[22] = {
	{"backslash_quote", 3, {"safe_encoding", "on", "off"}},
	{"bytea_output", 2, {"escape","hex"}},
	{"client_min_messages", 9, {"debug5", "debug4", "debug3", "debug2", "debug1", "log", "notice", "warning", "error"}},
	{"constraint_exclusion", 3, {"partition", "on", "off"}},
	{"default_transaction_isolation", 4, {"serializable", "repeatable read", "read committed", "read uncommitted"}},
	{"grammar", 2, {"postgres", "oracle"}},
	{"IntervalStyle", 4, {"postgres", "postgres_verbose", "sql_standard", "iso_8601"}},
	{"log_error_verbosity", 3, {"terse", "default", "verbose"}},
	{"log_min_error_statement", 11, {"debug5", "debug4", "debug3", "debug2", "debug1", "info", "notice", "warning", "error", "log", "fatal", "panic"}},
	{"log_min_messages", 12, {"debug5", "debug4", "debug3", "debug2", "debug1", "info", "notice", "warning", "error", "log", "fatal", "panic"}},
	{"log_statement", 4, {"none", "ddl", "mod", "all"}},
	{"remotetype", 4, {"application", "coordinator", "datanode", "rxactmgr"}},
	{"session_replication_role", 3, {"origin", "replica", "local"}},
	{"snapshot_level", 6, {"mvcc", "now", "self", "any", "toast", "dirty"}},
	{"synchronous_commit", 4, {"local", "remote_write", "on", "off"}},
	{"syslog_facility", 8, {"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7"}},
	{"trace_recovery_messages", 9, {"debug5", "debug4", "debug3", "debug2", "debug1", "log", "notice", "warning", "error"}},
	{"track_functions", 3, {"none", "pl", "all"}},
	{"wal_level", 3, {"minimal", "archive", "hot_standby"}},
	{"wal_sync_method", 4, {"fsync", "fdatasync", "open_sync", "open_datasync"}},
	{"xmlbinary", 2, {"base64", "hex"}},
	{"xmloption", 2, {"content", "document"}}
};

static void mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, Name value, int *vartype, Name parmunit, Name parmmin, Name parmmax, int *effectparmstatus);
static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value);
static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, char *key, char *value, int effectparmstatus);
static void mgr_updateparm_send_parm(StringInfo infosendmsg, GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, char *parmkey, char *parmvalue, int effectparmstatus);
static int mgr_delete_tuple_not_all(Relation noderel, char nodetype, Name key);
static int mgr_check_parm_value(char *name, char *value, int vartype, char *parmunit, char *parmmin, char *parmmax);
static int mgr_get_parm_unit_type(char *nodename, char *parmunit);
static bool mgr_parm_enum_lookup_by_name(char *name, char *value, StringInfo valuelist);

/* 
* for command: set {datanode|coordinaotr} xx {master|slave|extra} {key1=value1,key2=value2...} , to record the parameter in mgr_updateparm
*/
void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel_updateparm;
	Relation rel_parm;
	Relation rel_node;
	HeapTuple newtuple;
	NameData nodename;
	Datum datum[Natts_mgr_updateparm];
	ListCell *lc;
	DefElem *def;
	NameData key;
	NameData value;
	NameData defaultvalue;
	NameData parmunit;
	NameData parmmin;
	NameData parmmax;
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	char parmtype;			/*coordinator or datanode or gtm */
	char nodetype;			/*master/slave/extra*/
	int insertparmstatus;
	int effectparmstatus;
	int vartype;  /*the parm value type: bool, string, enum, int*/
	Assert(node && node->nodename && node->nodetype && node->parmtype);
	nodetype = node->nodetype;
	parmtype =  node->parmtype;
	/*nodename*/
	namestrcpy(&nodename, node->nodename);
	
	/*open systbl: mgr_parm*/
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		namestrcpy(&key, def->defname);	
		namestrcpy(&value, defGetString(def));
		if (strcmp(key.data, "port") == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				, errmsg("permission denied: \"port\" shoule be modified in \"node\" table before init all, \nuse \"list node\" to get the gtm/coordinator/datanode port information")));
		}
		/*check the parameter is right for the type node of postgresql.conf*/
		mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, &defaultvalue, &vartype, &parmunit, &parmmin, &parmmax, &effectparmstatus);
		/*check the key's value*/
		if (mgr_check_parm_value(key.data, value.data, vartype, parmunit.data, parmmin.data, parmmax.data) != 1)
		{
			return;
		}
		/*check the parm exists already in mgr_updateparm systbl*/
		insertparmstatus = mgr_check_parm_in_updatetbl(rel_updateparm, nodetype, &nodename, &key, value.data);
		if (PARM_NEED_NONE == insertparmstatus)
			continue;
		else if (PARM_NEED_UPDATE == insertparmstatus)
		{
			/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
			mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, value.data, effectparmstatus);
			continue;
		}
		datum[Anum_mgr_updateparm_parmtype-1] = CharGetDatum(parmtype);
		datum[Anum_mgr_updateparm_nodename-1] = NameGetDatum(&nodename);
		datum[Anum_mgr_updateparm_nodetype-1] = CharGetDatum(nodetype);
		datum[Anum_mgr_updateparm_key-1] = NameGetDatum(&key);
		datum[Anum_mgr_updateparm_value-1] = NameGetDatum(&value);
		/* now, we can insert record */
		newtuple = heap_form_tuple(RelationGetDescr(rel_updateparm), datum, isnull);
		simple_heap_insert(rel_updateparm, newtuple);
		CatalogUpdateIndexes(rel_updateparm, newtuple);
		heap_freetuple(newtuple);
		/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
		mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, value.data, effectparmstatus);
	}

	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
}

/*
*check the given parameter nodetype, key,value in mgr_parm, if not in, shows the parameter is not right in postgresql.conf
*/
static void mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, Name value, int *vartype, Name parmunit, Name parmmin, Name parmmax, int *effectparmstatus)
{
	HeapTuple tuple;
	char *gucconntent;
	Form_mgr_parm mgr_parm;
	Datum datumparmunit;
	Datum datumparmmin;
	Datum datumparmmax;
	bool isNull = false;
	
	/*check the name of key exist in mgr_parm system table, if the key in gtm or cn or dn, the parmtype in 
	* mgr_parm is '*'; if the key only in cn or dn, the parmtype in mgr_parm is '#', if the key only in 
	* gtm/coordinator/datanode, the parmtype in mgr_parm is PARM_TYPE_GTM/PARM_TYPE_COORDINATOR/PARM_TYPE_DATANODE
	* first: check the parmtype '*'; second: check the parmtype '#'; third check the parmtype the input parameter given
	*/
	/*check the parm in mgr_parm, type is '*'*/
	tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(PARM_IN_GTM_CN_DN), NameGetDatum(key));
	if(!HeapTupleIsValid(tuple))
	{
		/*check the parm in mgr_parm, type is '#'*/
		if (PARM_TYPE_COORDINATOR == parmtype || PARM_TYPE_DATANODE ==parmtype)
		{
			tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(PARM_IN_CN_DN), NameGetDatum(key));
			if(!HeapTupleIsValid(tuple))
			{		
					tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(parmtype), NameGetDatum(key));
					if(!HeapTupleIsValid(tuple))
						ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
							, errmsg("unrecognized configuration parameter \"%s\"", key->data)));
					mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
					Assert(mgr_parm);
					gucconntent = NameStr(mgr_parm->parmcontext);
			}
			mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
		}
		else if (PARM_TYPE_GTM == parmtype)
		{
			tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(parmtype), NameGetDatum(key));
			if(!HeapTupleIsValid(tuple))
			{
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, errmsg("unrecognized configuration parameter \"%s\"", key->data)));
			}
			mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
		}
		else
		{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the parm type \"%c\" does not exist", parmtype)));
		}
	}
	else
	{
			mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
	}

	Assert(mgr_parm);
	gucconntent = NameStr(mgr_parm->parmcontext);
	if (strcmp(NameStr(mgr_parm->parmvartype), "string") == 0)
	{
		*vartype = PGC_STRING;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "real") == 0)
	{
		*vartype = PGC_REAL;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "enum") == 0)
	{
		*vartype = PGC_ENUM;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "bool") == 0)
	{
		*vartype = PGC_BOOL;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "integer") == 0)
	{
		*vartype = PGC_INT;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("the value type \"%s\" does not exist", NameStr(mgr_parm->parmvartype))));		
	}
		
	/*get the default value*/
	namestrcpy(value, NameStr(mgr_parm->parmvalue));
	/*get parm unit*/
	datumparmunit = heap_getattr(tuple, Anum_mgr_parm_unit, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		namestrcpy(parmunit, "");
	}
	else
	{
		namestrcpy(parmunit,TextDatumGetCString(datumparmunit));
	}
	/*get parm min*/
	datumparmmin = heap_getattr(tuple, Anum_mgr_parm_minval, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		namestrcpy(parmmin, "0");
	}
	else
	{
		namestrcpy(parmmin,TextDatumGetCString(datumparmmin));
	}
	/*get parm max*/
	datumparmmax = heap_getattr(tuple, Anum_mgr_parm_maxval, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		namestrcpy(parmmax, "0");
	}
	else
	{
		namestrcpy(parmmax,TextDatumGetCString(datumparmmax));
	}	
	
	if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_USERSET]) == 0 || strcasecmp(gucconntent, GucContext_Parmnames[PGC_SUSET]) == 0 || strcasecmp(gucconntent, GucContext_Parmnames[PGC_SIGHUP]) == 0)
	{
		*effectparmstatus = PGC_SIGHUP;
	}
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_POSTMASTER]) == 0)
	{
		*effectparmstatus = PGC_POSTMASTER;
		ereport(NOTICE, (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
			, errmsg("parameter \"%s\" cannot be changed without restarting the server", key->data)));
	}
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_INTERNAL]) == 0)
	{
		*effectparmstatus = PGC_INTERNAL;
		ereport(NOTICE, (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
			, errmsg("parameter \"%s\" cannot be changed", key->data)));
	}
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_BACKEND]) == 0)
	{
		*effectparmstatus = PGC_BACKEND;
		ereport(NOTICE, (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
			, errmsg("parameter \"%s\" cannot be set after connection start", key->data)));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("unkown the content of this parameter \"%s\"", key->data)));
	}
	ReleaseSysCache(tuple);
}


/*
* check the parmeter exist in mgr_updateparm systbl or not. if it is not in , and nodename is MACRO_STAND_FOR_ALL_NODENAME, return PARM_NEED_INSERT 
* and delete the nodename which has same nodetype,key,value; if it is not in , and nodename is not MACRO_STAND_FOR_ALL_NODENAME, then check MACRO_STAND_FOR_ALL_NODENAME has 
* the same nodetype,key,value, if not return PARM_NEED_INSERT, otherwis return PARM_NEED_NONE;if it is already in and the 
* value does not need update, then return PARM_NEED_NONE; if it is already in and the value needs update, return 
* PARM_NEED_UPDATE and if the nodename is MACRO_STAND_FOR_ALL_NODENAME delete the nodename which has same nodetype,key,value who's nodename is not MACRO_STAND_FOR_ALL_NODENAME;
*/

static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value)
{
	HeapTuple tuple;
	Form_mgr_updateparm mgr_updateparm;
	Form_mgr_updateparm mgr_updateparmtmp;
	NameData nodenametmp;
	int delnum = 0;

	tuple = SearchSysCache3(MGRUPDATAPARMNODENAMENODETYPEKEY, NameGetDatum(nodename), CharGetDatum(nodetype), NameGetDatum(key));
	if(!HeapTupleIsValid(tuple))
	{
		/*if nodename is not MACRO_STAND_FOR_ALL_NODENAME*/
		if (namestrcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) != 0)
		{
			namestrcpy(&nodenametmp, MACRO_STAND_FOR_ALL_NODENAME);
			tuple = SearchSysCache3(MGRUPDATAPARMNODENAMENODETYPEKEY, NameGetDatum(&nodenametmp), CharGetDatum(nodetype), NameGetDatum(key));
			if(!HeapTupleIsValid(tuple))
			{
				return PARM_NEED_INSERT;
			}
			else
			{
				/*check the value*/
				mgr_updateparmtmp = (Form_mgr_updateparm)GETSTRUCT(tuple);
				if (strcmp(value, NameStr(mgr_updateparmtmp->updateparmvalue)) == 0)
				{
					ReleaseSysCache(tuple);
					return PARM_NEED_NONE;
				}
				else
				{
					ReleaseSysCache(tuple);
					return PARM_NEED_INSERT;
				}
			}
		}
		else
		{
			/*check the nodename in mgr_updateparm nodetype and key are not the same with MACRO_STAND_FOR_ALL_NODENAME, delete it*/
			mgr_delete_tuple_not_all(noderel, nodetype, key);
			return PARM_NEED_INSERT;
		}
	}
	/*check nodename is MACRO_STAND_FOR_ALL_NODENAME and the mgr_updateparm has the nodename which has the same nodetype and key*/
	if (namestrcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) == 0)
	{
		/*check the nodename in mgr_updateparm nodetype and key are not the same with MACRO_STAND_FOR_ALL_NODENAME, delete it*/
		delnum = mgr_delete_tuple_not_all(noderel, nodetype, key);
	}
	/*check value*/
	mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
	Assert(mgr_updateparm);	
	/*check the value, if not same, update its value*/
	if (strcmp(value, NameStr(mgr_updateparm->updateparmvalue)) != 0)
	{
		/*update parm's value*/
		namestrcpy(&(mgr_updateparm->updateparmvalue), value);
		heap_inplace_update(noderel, tuple);
		ReleaseSysCache(tuple);
		return PARM_NEED_UPDATE;
	}	
	ReleaseSysCache(tuple);
	if (delnum > 0)
	{
		return PARM_NEED_UPDATE;
	}
	
	return PARM_NEED_NONE;
}

/*
*get the parameters from mgr_updateparm, then add them to infosendparamsg,  used for initdb
*first, add the parameter which the nodename is '*' with given nodetype; second, add the parameter for given name with given nodetype 
*/
void mgr_add_parm(char *nodename, char nodetype, StringInfo infosendparamsg)
{
	Relation rel_updateparm;
	Form_mgr_updateparm mgr_updateparm;
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	char *parmkey;
	char *parmvalue;
	NameData nodenamedata;
	
	/*first: add the parameter which the nodename is '*' with given nodetype*/
	namestrcpy(&nodenamedata, MACRO_STAND_FOR_ALL_NODENAME);
	ScanKeyInit(&key[0],
		Anum_mgr_updateparm_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_updateparm_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenamedata));	
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan(rel_updateparm, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
		Assert(mgr_updateparm);
		/*get key, value*/
		parmkey = NameStr(mgr_updateparm->updateparmkey);
		parmvalue = NameStr(mgr_updateparm->updateparmvalue);
		mgr_append_pgconf_paras_str_str(parmkey, parmvalue, infosendparamsg);
	}
	heap_endscan(rel_scan);
	/*second: add the parameter for given name with given nodetype*/
	namestrcpy(&nodenamedata, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_updateparm_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_updateparm_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenamedata));
	rel_scan = heap_beginscan(rel_updateparm, SnapshotNow, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
		Assert(mgr_updateparm);
		/*get key, value*/
		parmkey = NameStr(mgr_updateparm->updateparmkey);
		parmvalue = NameStr(mgr_updateparm->updateparmvalue);
		mgr_append_pgconf_paras_str_str(parmkey, parmvalue, infosendparamsg);
	}
	heap_endscan(rel_scan);
	heap_close(rel_updateparm, RowExclusiveLock);
}

/*
* according to "set datanode|coordinator|gtm master|slave|extra (key1=value1,...)" , get the nodename, key and value, 
* then from node systbl to get ip and path, then reload the key for the node(datanode or coordinator or gtm) when 
* the type of the key does not need restart to make effective
*/

static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, char *parmkey, char *parmvalue, int effectparmstatus)
{
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumpath;
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	char *nodepath;
	char *nodetypestr;
	bool isNull;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	/*get all node, which nodetype is "nodetype" in node systbl*/
	if (strcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) == 0)
	{
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		rel_scan = heap_beginscan(noderel, SnapshotNow, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(!mgr_node->nodeincluster)
				continue;
			datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column cndnpath is null")));
			}
			nodepath = TextDatumGetCString(datumpath);
			ereport(LOG,
				(errmsg("send parameter %s=%s to %d %s", parmkey, parmvalue, mgr_node->nodehost, nodepath)));
			mgr_updateparm_send_parm(&infosendmsg, &getAgentCmdRst, mgr_node->nodehost, nodepath, parmkey, parmvalue, effectparmstatus);
		}
		heap_endscan(rel_scan);
	}
	else	/*for given nodename*/
	{
		tuple = mgr_get_tuple_node_from_name_type(noderel, nodename, nodetype);
		if(!(HeapTupleIsValid(tuple)))
		{
			nodetypestr = mgr_nodetype_str(nodetype);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", nodetypestr, nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(!mgr_node->nodeincluster)
		{
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			heap_freetuple(tuple);
			return;
		}
		/*get path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		nodepath = TextDatumGetCString(datumpath);	
		/*send the parameter to node path, then reload it*/
		ereport(LOG,
			(errmsg("send parameter %s=%s to %d %s", parmkey, parmvalue, mgr_node->nodehost, nodepath)));
		mgr_updateparm_send_parm(&infosendmsg , &getAgentCmdRst, mgr_node->nodehost, nodepath, parmkey, parmvalue, effectparmstatus);
		heap_freetuple(tuple);
	}
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

/*
* send parameter to node, refresh its postgresql.conf, if the guccontent of parameter is superuser/user/sighup, will reload the parameter
*/
static void mgr_updateparm_send_parm(StringInfo infosendmsg, GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, char *parmkey, char *parmvalue, int effectparmstatus)
{	
	/*send the parameter to node path, then reload it*/
	resetStringInfo(infosendmsg);
	resetStringInfo(&(getAgentCmdRst->description));
	mgr_append_pgconf_paras_str_str(parmkey, parmvalue, infosendmsg);
	if(effectparmstatus == PGC_SIGHUP)
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodepath, infosendmsg, hostoid, getAgentCmdRst);
	else
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodepath, infosendmsg, hostoid, getAgentCmdRst);
	if (getAgentCmdRst->ret != true)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE)
			 ,errmsg("reload parameter fail: %s", (getAgentCmdRst->description).data))); 
	}
}

static int mgr_delete_tuple_not_all(Relation noderel, char nodetype, Name key)
{
	HeapTuple looptuple;
	Form_mgr_updateparm mgr_updateparm;
	ScanKeyData scankey[1];
	HeapScanDesc rel_scan;
	int delnum = 0;
	
	/*check the nodename in mgr_updateparm nodetype and key are not the same with MACRO_STAND_FOR_ALL_NODENAME*/
	ScanKeyInit(&scankey[0],
		Anum_mgr_updateparm_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 1, scankey);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
		Assert(mgr_updateparm);
		if (strcmp(NameStr(mgr_updateparm->updateparmkey), key->data) != 0)
			continue;
		if (strcmp(NameStr(mgr_updateparm->updateparmnodename), MACRO_STAND_FOR_ALL_NODENAME) == 0)
			continue;
		/*delete the tuple which nodename is not MACRO_STAND_FOR_ALL_NODENAME and has the same nodetype and key*/
		delnum++;
		simple_heap_delete(noderel, &looptuple->t_self);
		CatalogUpdateIndexes(noderel, looptuple);
	}
	heap_endscan(rel_scan);
	return delnum;
}

/* 
* for command: reset {datanode|coordinaotr} xx {master|slave|extra} {key1,key2...} , to remove the parameter in mgr_updateparm;
*	if the reset parameters not in mgr_updateparm, report error; otherwise use the values which come from mgr_parm to replace the
*	old values;
*/
void mgr_reset_updateparm(MGRUpdateparmReset *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel_updateparm;
	Relation rel_parm;
	Relation rel_node;
	HeapTuple newtuple;
	HeapTuple looptuple;
	NameData nodename;
	NameData nodenametmp;
	NameData parmmin;
	NameData parmmax;
	Datum datum[Natts_mgr_updateparm];
	ListCell *lc;
	DefElem *def;
	NameData key;
	NameData defaultvalue;
	NameData parmunit;
	ScanKeyData scankey[3];
	HeapScanDesc rel_scan;
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	bool bget = false;
	char parmtype;			/*coordinator or datanode or gtm */
	char nodetype;			/*master/slave/extra*/
	int effectparmstatus;
	int vartype; /*the parm value type: bool, string, enum, int*/
	Assert(node && node->nodename && node->nodetype && node->parmtype);
	nodetype = node->nodetype;
	parmtype =  node->parmtype;
	/*nodename*/
	namestrcpy(&nodename, node->nodename);

	/*open systbl: mgr_parm*/
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		namestrcpy(&key, def->defname);
		if (strcmp(key.data, "port") == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				, errmsg("permission denied: \"port\" shoule be modified in \"node\" table before init all, \nuse \"list node\" to get the gtm/coordinator/datanode port information")));
		}
		/*check the parameter is right for the type node of postgresql.conf*/
		mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, &defaultvalue, &vartype, &parmunit, &parmmin, &parmmax, &effectparmstatus);
		/*if nodename is '*', delete the tuple in mgr_updateparm which nodetype is given and reload the parm if the cluster inited*/
		if (strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) == 0)
		{
			ScanKeyInit(&scankey[0],
				Anum_mgr_updateparm_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
			ScanKeyInit(&scankey[1],
				Anum_mgr_updateparm_key
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&key));
			rel_scan = heap_beginscan(rel_updateparm, SnapshotNow, 2, scankey);
			while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
			{
				/*delete the tuple which nodetype is the given nodetype*/
				simple_heap_delete(rel_updateparm, &looptuple->t_self);
				CatalogUpdateIndexes(rel_updateparm, looptuple);
			}
			heap_endscan(rel_scan);
			/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
			mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, defaultvalue.data, effectparmstatus);
		}
		/*the nodename is not MACRO_STAND_FOR_ALL_NODENAME, refresh the postgresql.conf of the node, and delete the tuple in mgr_updateparm which 
		*nodetype and nodename is given; if MACRO_STAND_FOR_ALL_NODENAME in mgr_updateparm has the same nodetype, insert one tuple to *mgr_updateparm for record
		*/
		else 
		{
			ScanKeyInit(&scankey[0],
				Anum_mgr_updateparm_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodename));
			ScanKeyInit(&scankey[1],
				Anum_mgr_updateparm_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
			ScanKeyInit(&scankey[2],
				Anum_mgr_updateparm_key
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&key));
			rel_scan = heap_beginscan(rel_updateparm, SnapshotNow, 3, scankey);
			while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
			{
				/*delete the tuple which nodetype is the given nodetype and nodename*/
				simple_heap_delete(rel_updateparm, &looptuple->t_self);
				CatalogUpdateIndexes(rel_updateparm, looptuple);
			}
			heap_endscan(rel_scan);
			
			/*check the MACRO_STAND_FOR_ALL_NODENAME has the same nodetype*/
			namestrcpy(&nodenametmp, MACRO_STAND_FOR_ALL_NODENAME);
			bget = false;
			ScanKeyInit(&scankey[0],
				Anum_mgr_updateparm_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodenametmp));
			ScanKeyInit(&scankey[1],
				Anum_mgr_updateparm_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
			ScanKeyInit(&scankey[2],
				Anum_mgr_updateparm_key
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&key));
			rel_scan = heap_beginscan(rel_updateparm, SnapshotNow, 3, scankey);
			while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
			{
				bget = true;
				break;
				
			}
			heap_endscan(rel_scan);
			if (bget)
			{
				datum[Anum_mgr_updateparm_parmtype-1] = CharGetDatum(parmtype);
				datum[Anum_mgr_updateparm_nodename-1] = NameGetDatum(&nodename);
				datum[Anum_mgr_updateparm_nodetype-1] = CharGetDatum(nodetype);
				datum[Anum_mgr_updateparm_key-1] = NameGetDatum(&key);
				datum[Anum_mgr_updateparm_value-1] = NameGetDatum(&defaultvalue);
				/* now, we can insert record */
				newtuple = heap_form_tuple(RelationGetDescr(rel_updateparm), datum, isnull);
				simple_heap_insert(rel_updateparm, newtuple);
				CatalogUpdateIndexes(rel_updateparm, newtuple);
				heap_freetuple(newtuple);
			}
			/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
			mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, defaultvalue.data, effectparmstatus);
			
		}
	}

	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);	
}

/*
* check the guc value for postgresql.conf
*/
static int mgr_check_parm_value(char *name, char *value, int vartype, char *parmunit, char *parmmin, char *parmmax)
{
	int elevel = ERROR;
	int flags;

	switch (vartype)
	{
		case PGC_BOOL:
			{
				bool		newval;

				if (value)
				{
					if (!parse_bool(value, &newval))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("parameter \"%s\" requires a Boolean value",
								 name)));
						return 0;
					}
				}
				break;
			}

		case PGC_INT:
			{
				int			newval;
				int min;
				int max;
				
				if (value)
				{
					const char *hintmsg;
					flags = mgr_get_parm_unit_type(name, parmunit);
					if (!parse_int(value, &newval, flags, &hintmsg))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for parameter \"%s\": \"%s\"",
								name, value),
								 hintmsg ? errhint("%s", _(hintmsg)) : 0));
						return 0;
					}
					if (strcmp(parmmin, "") ==0 || strcmp(parmmax, "") ==0)
					{
						return 1;
					}
					min = atoi(parmmin);
					max = atoi(parmmax);
					if (newval < min || newval > max)
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)",
										newval, name, min, max)));
						return 0;
					}
				}
				break;
			}

		case PGC_REAL:
			{
				double		newval;
				double min;
				double max;
				
				if (value)
				{
					if (!parse_real(value, &newval))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("parameter \"%s\" requires a numeric value",
								 name)));
						return 0;
					}
					
					if (strcmp(parmmin, "") == 0 || strcmp(parmmax, "") == 0)
					{
						return 1;
					}
					min = atof(parmmin);
					max = atof(parmmax);
					
					if (newval < min || newval > max)
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("%g is outside the valid range for parameter \"%s\" (%g .. %g)",
										newval, name, min, max)));
						return 0;
					}
				}
				break;
			}

		case PGC_STRING:
			{
				/*nothing to do,only need check some name will be truncated*/
				break;
			}

		case PGC_ENUM:
			{
					StringInfoData valuelist;
					initStringInfo(&valuelist);
					if (!mgr_parm_enum_lookup_by_name(name, value, &valuelist))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for parameter \"%s\": \"%s\"",
								name, value),
								 errhint("Available values: %s", _(valuelist.data))));
						pfree(valuelist.data);
						return 0;
					}
				pfree(valuelist.data);
				break;
			}
		default:
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the parm type \"d\" does not exist")));
	}
	return 1;
}

/*
* get unit type from unit and parm name(see guc.c)
*/

static int mgr_get_parm_unit_type(char *nodename, char *parmunit)
{
	if (strcmp(parmunit, "ms") == 0)
	{
		return GUC_UNIT_MS;
	}
	else if (strcmp(parmunit, "s") == 0)
	{
		if(strcmp(nodename, "post_auth_delay") ==0 || strcmp(nodename, "pre_auth_delay") ==0)
		{
			return (GUC_NOT_IN_SAMPLE | GUC_UNIT_S);
		}
		else
			return GUC_UNIT_S;
	}
	else if (strcmp(parmunit, "ms") ==0)
	{
		return GUC_UNIT_MS;
	}
	else if (strcmp(parmunit, "min") ==0)
	{
		return GUC_UNIT_MIN;
	}
	else if (strcmp(parmunit, "kB") ==0)
	{
		return GUC_UNIT_KB;
	}
	else if (strcmp(parmunit, "8kB") ==0)
	{
		if (strcmp(nodename, "wal_buffers") ==0)
		{
			return GUC_UNIT_XBLOCKS;
		}
		else if (strcmp(nodename, "wal_segment_size") ==0)
		{
			return (GUC_UNIT_XBLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE);
		}
		else
			return GUC_UNIT_KB;
		
	}
	else
		return 0;
}

/*check enum type of parm's value is right*/
static bool mgr_parm_enum_lookup_by_name(char *name, char *value, StringInfo valuelist)
{
	int iloop = 0;
	int jloop = 0;
	for(iloop=0; iloop < enumparnnum; iloop++)
	{
		/*check name*/
		if (strcmp(name, enumstruct[iloop].name) ==0)
		{
			/*check value*/
			for(jloop=0; jloop<enumstruct[iloop].valuenum; jloop++)
			{
				if (strcmp(enumstruct[iloop].value[jloop], value) == 0)
				{
					return true;
				}
			}
			
			/*get the right value list*/
			resetStringInfo(valuelist);
			for(jloop=0; jloop<enumstruct[iloop].valuenum -1; jloop++)
			{
				appendStringInfo(valuelist, "%s, ", enumstruct[iloop].value[jloop]);
			}
			appendStringInfo(valuelist, "%s", enumstruct[iloop].value[jloop]);
			return false;
		}
		
	}
	return false;
}



