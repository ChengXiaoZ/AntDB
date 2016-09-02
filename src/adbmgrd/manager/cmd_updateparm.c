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


static void mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, char *value, int *effectparmstatus);
static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value);
static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, char *key, char *value, int effectparmstatus);
static void mgr_updateparm_send_parm(StringInfo infosendmsg, GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, char *parmkey, char *parmvalue, int effectparmstatus);
static int mgr_delete_tuple_not_all(Relation noderel, char nodetype, Name key);

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
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	char parmtype;			/*coordinator or datanode or gtm */
	char nodetype;			/*master/slave/extra*/
	int insertparmstatus;
	int effectparmstatus;
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
		mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, value.data, &effectparmstatus);
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
static void mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, char *value, int *effectparmstatus)
{
	HeapTuple tuple;
	char *gucconntent;
	Form_mgr_parm mgr_parm;
	
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
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_POSTMASTER]) == 0)
	{
		*effectparmstatus = PGC_POSTMASTER;
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
