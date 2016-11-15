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

#include <stdlib.h>
#include <string.h>

#if (Natts_mgr_updateparm != 5)
#error "need change code"
#endif

static void mgr_send_show_parameters(char cmdtype, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst);
static bool mgr_recv_showparam_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
static void mgr_add_givenname_updateparm(MGRUpdateparm *node, Name nodename, char nodetype, Relation rel_node, Relation rel_updateparm, Relation rel_parm);

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

static void mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, Name value, int *vartype, Name parmunit, Name parmmin, Name parmmax, int *effectparmstatus, StringInfo enumvalue);
static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value);
static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, char *key, char *value, int effectparmstatus, bool bforce);
static void mgr_updateparm_send_parm(StringInfo infosendmsg, GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, char *parmkey, char *parmvalue, int effectparmstatus, bool bforce);
static int mgr_delete_tuple_not_all(Relation noderel, char nodetype, Name key);
static int mgr_check_parm_value(char *name, char *value, int vartype, char *parmunit, char *parmmin, char *parmmax, StringInfo enumvalue);
static int mgr_get_parm_unit_type(char *nodename, char *parmunit);
static bool mgr_parm_enum_lookup_by_name(char *value, StringInfo valuelist);

/* 
* for command: set {datanode|coordinaotr}  {master|slave|extra} {nodename|ALL} {key1=value1,key2=value2...} , 
* set datanode all {key1=value1,key2=value2...},set gtm all {key1=value1,key2=value2...}, to record the parameter
* in mgr_updateparm
*/
void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel_updateparm;
	Relation rel_parm;
	Relation rel_node;
	NameData nodename;
	ScanKeyData scankey[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char nodetype;			/*master/slave/extra*/
	Assert(node && node->nodename && node->nodetype && node->parmtype);
	nodetype = node->nodetype;
	/*nodename*/
	namestrcpy(&nodename, node->nodename);
	/*open systbl: mgr_parm*/
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);

	/*set datanode master/slave/extra all (key=value,...)*/
	if (strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) == 0 && (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype))
	{
		ScanKeyInit(&scankey[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, scankey);
		while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			if(!HeapTupleIsValid(tuple))
				break;
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			mgr_add_givenname_updateparm(node, &(mgr_node->nodename), mgr_node->nodetype, rel_node, rel_updateparm, rel_parm);
		}
		heap_endscan(rel_scan);
	}
	/*set datanode/gtm all (key=value,...), set nodetype nodname (key=value,...)*/
	else
	{
		mgr_add_givenname_updateparm(node, &nodename, nodetype, rel_node, rel_updateparm, rel_parm);
	}
	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
}

static void mgr_add_givenname_updateparm(MGRUpdateparm *node, Name nodename, char nodetype, Relation rel_node, Relation rel_updateparm, Relation rel_parm)
{
	HeapTuple newtuple;
	Datum datum[Natts_mgr_updateparm];
	ListCell *lc;
	DefElem *def;
	NameData key;
	NameData value;
	NameData defaultvalue;
	NameData parmunit;
	NameData parmmin;
	NameData parmmax;
	NameData valuetmp;
	StringInfoData enumvalue;
	bool isnull[Natts_mgr_updateparm];
	char parmtype;			/*coordinator or datanode or gtm */
	int insertparmstatus;
	int effectparmstatus;
	int vartype;  /*the parm value type: bool, string, enum, int*/
	int len = 0;
	Assert(node && node->parmtype);
	parmtype =  node->parmtype;
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	initStringInfo(&enumvalue);

	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		namestrcpy(&key, def->defname);	
		namestrcpy(&value, defGetString(def));
		if (strcmp(key.data, "port") == 0 || strcmp(key.data, "synchronous_standby_names") == 0)
		{
			pfree(enumvalue.data);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				, errmsg("permission denied: \"%s\" shoule be modified in \"node\" table before init all, \nuse \"list node\" to get information", key.data)));
		}
		if (!node->is_force)
		{
			/*check the parameter is right for the type node of postgresql.conf*/
			resetStringInfo(&enumvalue);
			mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, &defaultvalue, &vartype, &parmunit, &parmmin, &parmmax, &effectparmstatus, &enumvalue);
			if (PGC_STRING == vartype)
			{
				/*if the value of key is string, it need use signle quota*/
				len = strlen(value.data);
				if (value.data[0] != '\'' || value.data[len-1] != '\'')
				{
					valuetmp.data[0]='\'';
					strcpy(valuetmp.data+sizeof(char),value.data);
					valuetmp.data[1+len]='\'';
					valuetmp.data[2+len]='\0';
					if (len > sizeof(value.data)-2-1)
					{
						valuetmp.data[sizeof(value.data)-2]='\'';
						valuetmp.data[sizeof(value.data)-1]='\0';
					}
					namestrcpy(&value, valuetmp.data);
				}
			}
			/*check the key's value*/
			if (mgr_check_parm_value(key.data, value.data, vartype, parmunit.data, parmmin.data, parmmax.data, &enumvalue) != 1)
			{
				return;
			}
		}
		/*check the parm exists already in mgr_updateparm systbl*/
		insertparmstatus = mgr_check_parm_in_updatetbl(rel_updateparm, nodetype, nodename, &key, value.data);
		if (PARM_NEED_NONE == insertparmstatus)
			continue;
		else if (PARM_NEED_UPDATE == insertparmstatus)
		{
			/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
			mgr_reload_parm(rel_node, nodename->data, nodetype, key.data, value.data, effectparmstatus, false);
			continue;
		}
		datum[Anum_mgr_updateparm_parmtype-1] = CharGetDatum(parmtype);
		datum[Anum_mgr_updateparm_nodename-1] = NameGetDatum(nodename);
		datum[Anum_mgr_updateparm_nodetype-1] = CharGetDatum(nodetype);
		datum[Anum_mgr_updateparm_key-1] = NameGetDatum(&key);
		datum[Anum_mgr_updateparm_value-1] = NameGetDatum(&value);
		/* now, we can insert record */
		newtuple = heap_form_tuple(RelationGetDescr(rel_updateparm), datum, isnull);
		simple_heap_insert(rel_updateparm, newtuple);
		CatalogUpdateIndexes(rel_updateparm, newtuple);
		heap_freetuple(newtuple);
		/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
		mgr_reload_parm(rel_node, nodename->data, nodetype, key.data, value.data, effectparmstatus, false);
	}
	pfree(enumvalue.data);
}

/*
*check the given parameter nodetype, key,value in mgr_parm, if not in, shows the parameter is not right in postgresql.conf
*/
static void mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, Name value, int *vartype, Name parmunit, Name parmmin, Name parmmax, int *effectparmstatus, StringInfo enumvalue)
{
	HeapTuple tuple;
	char *gucconntent;
	Form_mgr_parm mgr_parm;
	Datum datumparmunit;
	Datum datumparmmin;
	Datum datumparmmax;
	Datum datumenumvalues;
	bool isNull = false;
	NameData valuetmp;
	int len = 0;
	
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
	if (PGC_STRING == *vartype)
	{
		/*if the value of key is string, it need use signle quota*/
		len = strlen(value->data);
		if (value->data[0] != '\'' || value->data[len-1] != '\'')
		{
			valuetmp.data[0]='\'';
			strcpy(valuetmp.data+sizeof(char),value->data);
			valuetmp.data[1+len]='\'';
			valuetmp.data[2+len]='\0';
			if (len > sizeof(value->data)-2-1)
			{
				valuetmp.data[sizeof(value->data)-2]='\'';
				valuetmp.data[sizeof(value->data)-1]='\0';
			}
			namestrcpy(value, valuetmp.data);
		}
	}
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
	/*get enumvalues*/
	datumenumvalues = heap_getattr(tuple, Anum_mgr_parm_enumval, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		appendStringInfo(enumvalue, "%s", "0");
	}
	else
	{
		appendStringInfo(enumvalue, "%s", TextDatumGetCString(datumenumvalues));
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
* check the parmeter exist in mgr_updateparm systbl or not. 
* 1. nodename is MACRO_STAND_FOR_ALL_NODENAME, does not in mgr_updateparm, clean the gtm or datanode param which name is not 
*	MACRO_STAND_FOR_ALL_NODENAME and has the same key, return PARM_NEED_INSERT
* 2. nodename is MACRO_STAND_FOR_ALL_NODENAME, exists in mgr_updateparm, clean the gtm or datanode param which name is not 
*	MACRO_STAND_FOR_ALL_NODENAME and has the same key, return PARM_NEED_UPDATE if the value need refresh or delnum != 0
*	(refresh value in this function if it need)
* 3. nodename is MACRO_STAND_FOR_ALL_NODENAME, exists in mgr_updateparm, clean the gtm or datanode param which name is not 
*	MACRO_STAND_FOR_ALL_NODENAME and has the same key, return PARM_NEED_DONE if the value does not need refresh and delnum == 0
* 4. nodename is not MACRO_STAND_FOR_ALL_NODENAME, MACRO_STAND_FOR_ALL_NODENAME its type include the nodename type-key, has the 
*	same value, if  nodename-type-key exists and has the same value, delete the tuple nodename-type-key return PARM_NEED_NONE
*	if  nodename-type-key exists and has not the same value, delete the tuple nodename-type-key return PARM_NEED_UPDATE,
*	if  nodename-type-key does not exists, return PARM_NEED_NONE
* 5. nodename is not MACRO_STAND_FOR_ALL_NODENAME, MACRO_STAND_FOR_ALL_NODENAME its type includes the nodename type-key, has not 
*	 the same value or not find the tuple for MACRO_STAND_FOR_ALL_NODENAME, then check the nodename-key-type exists in 
*	mgr_updateparm, if has same value, return PARM_NEED_NONE else 
*	PARM_NEED_UPDATE (refresh its value in this function)
* 6. nodename is not MACRO_STAND_FOR_ALL_NODENAME, MACRO_STAND_FOR_ALL_NODENAME its type includes the nodename type-key, has not 
*		 the same value or not find the tuple for MACRO_STAND_FOR_ALL_NODENAME, then check the nodename-key-type does not exists in 
*	mgr_updateparm, return PARM_NEED_INSERT    
*/

static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value)
{
	HeapTuple tuple;
	HeapTuple alltype_tuple;
	Form_mgr_updateparm mgr_updateparm;
	Form_mgr_updateparm mgr_updateparm_alltype;
	char allnodetype;
	int delnum = 0;
	int ret;

	if (GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
		allnodetype = CNDN_TYPE_GTM;
	else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype)
		allnodetype = CNDN_TYPE_DATANODE;
	else
		allnodetype = nodetype;
	/*nodename is MACRO_STAND_FOR_ALL_NODENAME*/
	if (namestrcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) == 0)
	{
		tuple = SearchSysCache3(MGRUPDATAPARMNODENAMENODETYPEKEY, NameGetDatum(nodename), CharGetDatum(nodetype), NameGetDatum(key));
		/*1.does not exist in mgr_updateparm*/
		if (!HeapTupleIsValid(tuple))
		{
			mgr_delete_tuple_not_all(noderel, nodetype, key);
			return PARM_NEED_INSERT;
		}
		else
		{
			/*2,3. check need update*/
			mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
			if (strcmp(value, NameStr(mgr_updateparm->updateparmvalue)) == 0)
			{
				delnum += mgr_delete_tuple_not_all(noderel, nodetype, key);
				ReleaseSysCache(tuple);
				if (delnum > 0)
					return PARM_NEED_UPDATE;
				else
					return PARM_NEED_NONE;
			}
			else
			{
				mgr_delete_tuple_not_all(noderel, nodetype, key);
				/*update parm's value*/
				namestrcpy(&(mgr_updateparm->updateparmvalue), value);
				heap_inplace_update(noderel, tuple);
				ReleaseSysCache(tuple);
				return PARM_NEED_UPDATE;
			}
		}
	}
	else
	{
		alltype_tuple = SearchSysCache3(MGRUPDATAPARMNODENAMENODETYPEKEY, NameGetDatum(MACRO_STAND_FOR_ALL_NODENAME), CharGetDatum(allnodetype), NameGetDatum(key));
		tuple = SearchSysCache3(MGRUPDATAPARMNODENAMENODETYPEKEY, NameGetDatum(nodename), CharGetDatum(nodetype), NameGetDatum(key));
		if (HeapTupleIsValid(alltype_tuple))
		{
			mgr_updateparm_alltype = (Form_mgr_updateparm)GETSTRUCT(alltype_tuple);
			Assert(mgr_updateparm_alltype);
			/*4. MACRO_STAND_FOR_ALL_NODENAME has the same type-key-value */
			if (strcmp(NameStr(mgr_updateparm_alltype->updateparmvalue), value) == 0)
			{
				if (HeapTupleIsValid(tuple))
				{
					mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
					Assert(mgr_updateparm_alltype);
					if (strcmp(NameStr(mgr_updateparm->updateparmvalue), value) != 0)
						ret = PARM_NEED_UPDATE;
					else
						ret = PARM_NEED_NONE;
					simple_heap_delete(noderel, &tuple->t_self);
					CatalogUpdateIndexes(noderel, tuple);
					ReleaseSysCache(tuple);
					ReleaseSysCache(alltype_tuple);
					return ret;
				}
				else
				{
					ReleaseSysCache(alltype_tuple);
					return PARM_NEED_NONE;
				}
			}
		}
		/*5,6*/
		if (HeapTupleIsValid(tuple))
		{
			mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
			if (strcmp(NameStr(mgr_updateparm->updateparmvalue), value) == 0)
			{
				ReleaseSysCache(tuple);
				if (HeapTupleIsValid(alltype_tuple))
					ReleaseSysCache(alltype_tuple);
				return PARM_NEED_NONE;
			}
			else
			{
				/*update parm's value*/
				namestrcpy(&(mgr_updateparm->updateparmvalue), value);
				heap_inplace_update(noderel, tuple);
				ReleaseSysCache(tuple);
				if (HeapTupleIsValid(alltype_tuple))
					ReleaseSysCache(alltype_tuple);
				return PARM_NEED_UPDATE;
			}
		}
		else
		{
			if (HeapTupleIsValid(alltype_tuple))
				ReleaseSysCache(alltype_tuple);
			return PARM_NEED_INSERT;
		}
	}
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
	char allnodetype;
	NameData nodenamedata;
	
	if (GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
		allnodetype = CNDN_TYPE_GTM;
	else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype)
		allnodetype = CNDN_TYPE_DATANODE;
	else
		allnodetype = CNDN_TYPE_COORDINATOR_MASTER;
	/*first: add the parameter which the nodename is '*' with allnodetype*/
	namestrcpy(&nodenamedata, MACRO_STAND_FOR_ALL_NODENAME);
	ScanKeyInit(&key[0],
		Anum_mgr_updateparm_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(allnodetype));
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
* according to "set datanode|coordinator|gtm master|slave|extra nodename(key1=value1,...)" , get the nodename, key and value, 
* then from node systbl to get ip and path, then reload the key for the node(datanode or coordinator or gtm) when 
* the type of the key does not need restart to make effective
*/

static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, char *parmkey, char *parmvalue, int effectparmstatus, bool bforce)
{
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumpath;
	HeapScanDesc rel_scan;
	char *nodepath;
	char *nodetypestr;
	bool isNull;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	/*nodename is MACRO_STAND_FOR_ALL_NODENAME*/
	if (strcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) == 0)
	{
		rel_scan = heap_beginscan(noderel, SnapshotNow, 0, NULL);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(!mgr_node->nodeincluster)
				continue;
			/*all gtm type: master/slave/extra*/
			if (CNDN_TYPE_GTM == nodetype)
			{
				if (mgr_node->nodetype != GTM_TYPE_GTM_MASTER && mgr_node->nodetype != GTM_TYPE_GTM_SLAVE && mgr_node->nodetype != GTM_TYPE_GTM_EXTRA)
					continue;
			}
			/*all datanode type: master/slave/extra*/
			else if (CNDN_TYPE_DATANODE == nodetype)
			{
				if (mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER && mgr_node->nodetype != CNDN_TYPE_DATANODE_SLAVE && mgr_node->nodetype != CNDN_TYPE_DATANODE_EXTRA)
					continue;
			}
			/*for coordinator all*/
			else
			{
				if (nodetype != mgr_node->nodetype)
					continue;
			}
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
			mgr_updateparm_send_parm(&infosendmsg, &getAgentCmdRst, mgr_node->nodehost, nodepath, parmkey, parmvalue, effectparmstatus, bforce);
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
		mgr_updateparm_send_parm(&infosendmsg , &getAgentCmdRst, mgr_node->nodehost, nodepath, parmkey, parmvalue, effectparmstatus, bforce);
		heap_freetuple(tuple);
	}
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

/*
* send parameter to node, refresh its postgresql.conf, if the guccontent of parameter is superuser/user/sighup, will reload the parameter
*/
static void mgr_updateparm_send_parm(StringInfo infosendmsg, GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, char *parmkey, char *parmvalue, int effectparmstatus, bool bforce)
{	
	/*send the parameter to node path, then reload it*/
	resetStringInfo(infosendmsg);
	resetStringInfo(&(getAgentCmdRst->description));
	mgr_append_pgconf_paras_str_str(parmkey, parmvalue, infosendmsg);
	if(effectparmstatus == PGC_SIGHUP)
	{
		if (!bforce)
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodepath, infosendmsg, hostoid, getAgentCmdRst);
		else
			mgr_send_conf_parameters(AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE, nodepath, infosendmsg, hostoid, getAgentCmdRst);
	}
	else
	{
		if (!bforce)
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodepath, infosendmsg, hostoid, getAgentCmdRst);
		else
			mgr_send_conf_parameters(AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE, nodepath, infosendmsg, hostoid, getAgentCmdRst);
	}

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
	HeapScanDesc rel_scan;
	int delnum = 0;
	
	/*check the nodename in mgr_updateparm nodetype and key are not the same with MACRO_STAND_FOR_ALL_NODENAME*/
	rel_scan = heap_beginscan(noderel, SnapshotNow, 0, NULL);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
		Assert(mgr_updateparm);
		if (strcmp(NameStr(mgr_updateparm->updateparmkey), key->data) != 0)
			continue;
		if (strcmp(NameStr(mgr_updateparm->updateparmnodename), MACRO_STAND_FOR_ALL_NODENAME) == 0)
			continue;
		/*all gtm type: master/slave/extra*/
		if (CNDN_TYPE_GTM == nodetype)
		{
			if (mgr_updateparm->updateparmnodetype != GTM_TYPE_GTM_MASTER && mgr_updateparm->updateparmnodetype != GTM_TYPE_GTM_SLAVE && mgr_updateparm->updateparmnodetype != GTM_TYPE_GTM_EXTRA)
				continue;
		}
		/*all datanode type: master/slave/extra*/
		else if (CNDN_TYPE_DATANODE == nodetype)
		{
			if (mgr_updateparm->updateparmnodetype != CNDN_TYPE_DATANODE_MASTER && mgr_updateparm->updateparmnodetype != CNDN_TYPE_DATANODE_SLAVE && mgr_updateparm->updateparmnodetype != CNDN_TYPE_DATANODE_EXTRA)
				continue;
		}
		/*for coordinator all*/
		else
		{
			if (mgr_updateparm->updateparmnodetype != nodetype)
				continue;
		}
		/*delete the tuple which nodename is not MACRO_STAND_FOR_ALL_NODENAME and has the same nodetype and key*/
		delnum++;
		simple_heap_delete(noderel, &looptuple->t_self);
		CatalogUpdateIndexes(noderel, looptuple);
	}
	heap_endscan(rel_scan);
	return delnum;
}

/* 
* for command: reset {datanode|coordinaotr} {master|slave|extra} {nodename | all}{key1,key2...} , reset datanode 
* all {key1,key2...}, reset gtm all{key1,key2...}.  to remove the parameter in mgr_updateparm; if the reset parameters 
* not in mgr_updateparm, report error; otherwise use the values which come from mgr_parm to replace the old values;
*/
void mgr_reset_updateparm(MGRUpdateparmReset *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel_updateparm;
	Relation rel_parm;
	Relation rel_node;
	HeapTuple newtuple;
	HeapTuple looptuple;
	HeapTuple tuple;
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
	Form_mgr_node mgr_node;
	StringInfoData enumvalue;
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	bool bget = false;
	char parmtype;			/*coordinator or datanode or gtm */
	char nodetype;			/*master/slave/extra*/
	char nodetype_original;
	char dntypearray[]={CNDN_TYPE_DATANODE_MASTER, CNDN_TYPE_DATANODE_SLAVE, CNDN_TYPE_DATANODE_EXTRA};
	char gtmtypearray[]={GTM_TYPE_GTM_MASTER, GTM_TYPE_GTM_SLAVE, GTM_TYPE_GTM_EXTRA};
	int nodearraylen = 1;		/*do not change the value 1*/
	int effectparmstatus;
	int vartype; /*the parm value type: bool, string, enum, int*/
	int iloop = 0;

	initStringInfo(&enumvalue);
	Assert(node && node->nodename && node->nodetype && node->parmtype);
	nodetype = node->nodetype;
	parmtype =  node->parmtype;
	/*nodename*/
	namestrcpy(&nodename, node->nodename);
	nodetype_original = nodetype;
	/*cmd:set datanode/gtm all (key=value,..)*/
	if (CNDN_TYPE_DATANODE == nodetype_original || CNDN_TYPE_GTM == nodetype_original)
	{
		Assert(strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) == 0);
		nodearraylen = (CNDN_TYPE_DATANODE == nodetype_original ? sizeof(dntypearray)/sizeof(dntypearray[0]): sizeof(gtmtypearray)/sizeof(gtmtypearray[0]));
	}
	
	/*open systbl: mgr_parm*/
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*get gtm name*/
	if (CNDN_TYPE_GTM == nodetype_original)
	{
		ScanKeyInit(&scankey[0]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(GTM_TYPE_GTM_MASTER));
		rel_scan = heap_beginscan(rel_node, SnapshotNow, 1, scankey);
		while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			namestrcpy(&nodename, NameStr(mgr_node->nodename));
			break;
		}
		heap_endscan(rel_scan);		
	}
	for (iloop=0; iloop<nodearraylen; iloop++)
	{
		if (CNDN_TYPE_DATANODE == nodetype_original)
			nodetype = dntypearray[iloop];
		else if (CNDN_TYPE_GTM == nodetype_original)
			nodetype = gtmtypearray[iloop];
		memset(datum, 0, sizeof(datum));
		memset(isnull, 0, sizeof(isnull));
		memset(got, 0, sizeof(got));

		foreach(lc,node->options)
		{
			def = lfirst(lc);
			Assert(def && IsA(def, DefElem));
			namestrcpy(&key, def->defname);
			if (!iloop)
			{
				if (strcmp(key.data, "port") == 0 || strcmp(key.data, "synchronous_standby_names") == 0)
				{
					ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
						, errmsg("permission denied: \"%s\" shoule be modified in \"node\" table before init all, \nuse \"list node\" to get information", key.data)));
				}
			}
			/*check the parameter is right for the type node of postgresql.conf*/
			if (!node->is_force)
			{
				resetStringInfo(&enumvalue);
				mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, &defaultvalue, &vartype, &parmunit, &parmmin, &parmmax, &effectparmstatus, &enumvalue);
			}
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
				if (!node->is_force)
					mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, defaultvalue.data, effectparmstatus, false);
				else
					mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, "force", PGC_POSTMASTER, true);
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
				if (!node->is_force)
					mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, defaultvalue.data, effectparmstatus, false);
				else
					mgr_reload_parm(rel_node, nodename.data, nodetype, key.data, "force", PGC_POSTMASTER, true);
			}
		}
		if (CNDN_TYPE_DATANODE != nodetype_original && CNDN_TYPE_GTM != nodetype_original)
			break;
	}

	pfree(enumvalue.data);
	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);	
}

/*
* check the guc value for postgresql.conf
*/
static int mgr_check_parm_value(char *name, char *value, int vartype, char *parmunit, char *parmmin, char *parmmax, StringInfo enumvalue)
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
				if (!mgr_parm_enum_lookup_by_name(value, enumvalue))
				{
					ereport(elevel,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for parameter \"%s\": \"%s\"",
						name, value),
						 errhint("Available values: %s", _(enumvalue->data))));
					return 0;
				}
				break;
			}
		default:
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the param type \"d\" does not exist")));
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
static bool mgr_parm_enum_lookup_by_name(char *value, StringInfo valuelist)
{
	char pvaluearray[256];		/*the max length of enumvals in pg_settings less than 256*/
	char *pvaluetmp;
	char *ptr;
	char *pvalue;
	bool ret = false;
	/*the content of valuelist like this "{xx,xx,xx}", so it need copy from 1 to len -2*/
	pvalue = strstr(valuelist->data, value);
	if (pvalue != NULL)
	{
		strncpy(pvaluearray, &(valuelist->data[1]), (valuelist->len-2) < 255 ? (valuelist->len-2):255);
		pvaluearray[(valuelist->len-2) < 255 ? (valuelist->len-2):255] = '\0';
		ptr = strtok_r(pvaluearray, ",", &pvaluetmp);  
		while(ptr != NULL)
		{
			if (strcmp(ptr, value) == 0)
			{
				ret = true;
				break;
			}
			ptr = strtok_r(NULL, ",", &pvaluetmp);

		}

	}

	return ret;
}

/*delete the tuple for given nodename and nodetype*/
void mgr_parmr_delete_tuple_nodename_nodetype(Relation noderel, Name nodename, char nodetype)
{
	HeapTuple looptuple;
	ScanKeyData scankey[2];
	HeapScanDesc rel_scan;
	Form_mgr_updateparm mgr_updateparm;
	
	/*for nodename is MACRO_STAND_FOR_ALL_NODENAME, only when type if master then delete the tuple*/
	if (strcmp(MACRO_STAND_FOR_ALL_NODENAME, nodename->data) == 0)
	{
		if (CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype || GTM_TYPE_GTM_MASTER == nodetype)
		{
				if (GTM_TYPE_GTM_MASTER == nodetype || GTM_TYPE_GTM_SLAVE == nodetype || GTM_TYPE_GTM_EXTRA == nodetype)
					nodetype = CNDN_TYPE_GTM;
				else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_DATANODE_EXTRA == nodetype)
					nodetype = CNDN_TYPE_DATANODE;
				else
					nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		}
		else
			return;
	}
	ScanKeyInit(&scankey[0],
		Anum_mgr_updateparm_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(nodename));
	ScanKeyInit(&scankey[1],
		Anum_mgr_updateparm_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 2, scankey);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
		Assert(mgr_updateparm);
		simple_heap_delete(noderel, &looptuple->t_self);
		CatalogUpdateIndexes(noderel, looptuple);
	}
	heap_endscan(rel_scan);
}

/*update the tuple for given nodename and nodetype*/
void mgr_parmr_update_tuple_nodename_nodetype(Relation noderel, Name nodename, char oldnodetype, char newnodetype)
{
	HeapTuple looptuple;
	ScanKeyData scankey[2];
	HeapScanDesc rel_scan;
	NameData namedatatmp;
	Form_mgr_updateparm mgr_updateparm;

	namestrcpy(&namedatatmp, MACRO_STAND_FOR_ALL_NODENAME);
	
	ScanKeyInit(&scankey[0],
		Anum_mgr_updateparm_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(nodename));
	ScanKeyInit(&scankey[1],
		Anum_mgr_updateparm_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(oldnodetype));
	rel_scan = heap_beginscan(noderel, SnapshotNow, 2, scankey);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
		Assert(mgr_updateparm);
		mgr_updateparm->updateparmnodetype = newnodetype;
		heap_inplace_update(noderel, looptuple);
		CatalogUpdateIndexes(noderel, looptuple);
	}
	heap_endscan(rel_scan);
}

/*update mgr_updateparm, change * to newmaster name and change its nodetype to mastertype*/
void mgr_update_parm_after_dn_failover(Name oldmastername, char oldmastertype, Name oldslavename,  char oldslavetype)
{
	Relation rel_updateparm;
	
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	/*delete old master parameters*/	
	mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, oldmastername, oldmastertype);
	/*update the old slave parameters to new master type*/
	mgr_parmr_update_tuple_nodename_nodetype(rel_updateparm, oldslavename, oldslavetype, oldmastertype);

	heap_close(rel_updateparm, RowExclusiveLock);
}

/*when gtm failover, the mgr_updateparm need modify: delete oldmaster parm and update slavetype to master for new master*/
void mgr_parm_after_gtm_failover_handle(Name mastername, char mastertype, Name slavename, char slavetype)
{
	Relation rel_updateparm;
	
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	/*delete old master parameters*/	
	mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, mastername, mastertype);
	/*update the old slave parameters to new master type*/
	mgr_parmr_update_tuple_nodename_nodetype(rel_updateparm, slavename, slavetype, mastertype);
	
	heap_close(rel_updateparm, RowExclusiveLock);
}

/*
* show parameter, command: SHOW NODENAME PARAMETER
*/
void mgr_showparam(MGRShowParam *node, ParamListInfo params, DestReceiver *dest)
{
	HeapTuple tuple;
	HeapTuple out;
	TupleTableSlot *slot;
	TupleDesc desc;
	MemoryContext context;
	MemoryContext oldcontext;
	Relation rel;
	StringInfoData buf;
	StringInfoData infosendmsg;
	NameData nodename;
	NameData param;
	NameData nodetypedata;
	ScanKeyData key[2];
	Form_mgr_node mgr_node;
	GetAgentCmdRst getAgentCmdRst;
	HeapScanDesc rel_scan;
	char *nodetypestr;
	/*max port is 65535,so the length of portstr is 6*/
	char portstr[6]="00000";
	
	AssertArg(node && dest && node->nodename && node->param);
	/*get node name and parameter name*/
	namestrcpy(&nodename, node->nodename);
	namestrcpy(&param, node->param);

	context = AllocSetContextCreate(CurrentMemoryContext, "showparam"
					, ALLOCSET_DEFAULT_MINSIZE
					, ALLOCSET_DEFAULT_INITSIZE
					, ALLOCSET_DEFAULT_MAXSIZE);
	desc = get_showparam_command_tuple_desc();
	(*dest->rStartup)(dest, CMD_UTILITY, desc);
	slot = MakeSingleTupleTableSlot(desc);
	initStringInfo(&buf);
	oldcontext = CurrentMemoryContext;
	PG_TRY();
	{
		TupleDesc node_desc;
		rel = heap_open(NodeRelationId, AccessShareLock);
		node_desc = CreateTupleDescCopy(RelationGetDescr(rel));
		initStringInfo(&(getAgentCmdRst.description));
		/*find the tuple ,which the node name is equal nodename*/
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(nodename.data));
		ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
		rel_scan = heap_beginscan(rel, SnapshotNow, 2, key);
		initStringInfo(&infosendmsg);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/*send the command string to agent to get the value*/
			sprintf(portstr, "%d", mgr_node->nodeport);
			resetStringInfo(&(getAgentCmdRst.description));
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, "%s", portstr);
			appendStringInfoCharMacro(&infosendmsg, '\0');
			appendStringInfo(&infosendmsg, "%s", param.data);
			appendStringInfoCharMacro(&infosendmsg, '\0');
			if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype || GTM_TYPE_GTM_EXTRA == mgr_node->nodetype)
				mgr_send_show_parameters(AGT_CMD_SHOW_AGTM_PARAM, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
			else
				mgr_send_show_parameters(AGT_CMD_SHOW_CNDN_PARAM, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
			MemoryContextSwitchTo(context);
			MemoryContextResetAndDeleteChildren(context);
			nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
			namestrcpy(&nodetypedata, nodetypestr);
			pfree(nodetypestr);
			out = build_common_command_tuple(&nodetypedata, getAgentCmdRst.ret, getAgentCmdRst.description.data);
			ExecClearTuple(slot);
			ExecStoreTuple(out, slot, InvalidBuffer, false);
			MemoryContextSwitchTo(oldcontext);
			(*dest->receiveSlot)(slot, dest);
		}
		heap_endscan(rel_scan);
		heap_close(rel, AccessShareLock);
		FreeTupleDesc(node_desc);
		pfree(infosendmsg.data);
		pfree(getAgentCmdRst.description.data);
	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();
	(*dest->rShutdown)(dest);
}

static void mgr_send_show_parameters(char cmdtype, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg
									,buf;
	bool execok;
	
	initStringInfo(&sendstrmsg);
	mgr_append_infostr_infostr(&sendstrmsg, infosendmsg);
	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	getAgentCmdRst->ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	execok = mgr_recv_showparam_msg(ma, getAgentCmdRst);
	Assert(execok == getAgentCmdRst->ret);
	ma_close(ma);	
}

/*
* get msg from agent
*/
static bool mgr_recv_showparam_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst)
{
	char			msg_type;
	StringInfoData recvbuf;
	bool initdone = false;
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
			appendStringInfoString(&(getAgentCmdRst->description), recvbuf.data);
			ereport(DEBUG1, (errmsg("receive msg: %s", recvbuf.data)));
			initdone = true;
			break;
		}
	}
	pfree(recvbuf.data);
	return initdone;
}