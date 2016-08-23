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

#if (Natts_mgr_updateparm != 5)
#error "need change code"
#endif

/*if the parmeter in gtm or coordinator or datanode pg_settins, the nodetype in mgr_parm is '*'
 , if the parmeter in coordinator or datanode pg_settings, the nodetype in mgr_parm is '#'
*/
#define PARM_IN_GTM_CN_DN '*'
#define PARM_IN_CN_DN '#'


static bool mgr_check_parm_in_pgconf(Relation noderel, char nodetype, Name key, char *value);
static bool check_parm_in_updatetbl(Relation noderel, char nodetype, char innertype, Name nodename, Name parmname, char *parmvalue);

/* 
* for command: set {datanode|coordinaotr} xx {master|slave|extra} {key1=value1,key2=value2...} , to record the parameter in mgr_updateparm
*/
void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel_updateparm;
	Relation rel_parm;
	HeapTuple newtuple;
	NameData nodename;
	Datum datum[Natts_mgr_updateparm];
	ListCell *lc;
	DefElem *def;
	NameData key;
	NameData value;
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	bool bcheck = false;
	char nodetype;			/*coordinator or datanode master/slave*/
	char innertype;			/*master/slave/extra*/
	Assert(node && node->nodename);
	Assert(node && node->nodename);
	
	nodetype = node->nodetype;
	innertype =  node->innertype;
	/*nodename*/
	namestrcpy(&nodename, node->nodename);
	
	/*open systbl: mgr_parm*/
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);
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
				, errmsg("permission denied: \"port\" shoule be modified in \"node\" table, use \"list node\" to get the gtm/coordinator/datanode port information")));
		}
		/*check the parameter is right for the type node of postgresql.conf*/
		bcheck = mgr_check_parm_in_pgconf(rel_parm, nodetype, &key, value.data);
		if (false == bcheck)
		{
			heap_close(rel_updateparm, RowExclusiveLock);
			heap_close(rel_parm, RowExclusiveLock);
			return;
		}
		/*check the parm exists already in mgr_updateparm systbl*/
		if (check_parm_in_updatetbl(rel_updateparm, nodetype, innertype, &nodename, &key, value.data))
			continue;
		datum[Anum_mgr_updateparm_nodetype-1] = nodetype;
		datum[Anum_mgr_updateparm_name-1] = NameGetDatum(&nodename);
		datum[Anum_mgr_updateparm_innertype-1] = CharGetDatum(innertype);
		datum[Anum_mgr_updateparm_key-1] = NameGetDatum(&key);
		datum[Anum_mgr_updateparm_value-1] = NameGetDatum(&value);
		/* now, we can insert record */
		newtuple = heap_form_tuple(RelationGetDescr(rel_updateparm), datum, isnull);
		simple_heap_insert(rel_updateparm, newtuple);
		CatalogUpdateIndexes(rel_updateparm, newtuple);
		heap_freetuple(newtuple);
	}

	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);	
}

/*
*check the given parameter nodetype, key,value is in mgr_parm, if not in, shows the parameter is not right in postgresql.conf then return false
*/
static bool mgr_check_parm_in_pgconf(Relation noderel, char nodetype, Name key, char *value)
{
	HeapTuple tuple;
	
	/*check the name of key exist in mgr_parm system table, if the key in gtm or cn or dn, the nodetype in 
	* mgr_parm is '*'; if the key only in cn or dn, the nodetype in mgr_parm is '#', if the key only in 
	* gtm/coordinator/datanode, the nodetype in mgr_parm is NODE_TYPE_GTM/NODE_TYPE_COORDINATOR/NODE_TYPE_DATANODE
	* first: check the nodetype '*'; second: check the nodetype '#'; third check the nodetype the input parameter given
	*/
	/*check the parm in mgr_parm, type is '*'*/
	tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(PARM_IN_GTM_CN_DN), NameGetDatum(key));
	if(!HeapTupleIsValid(tuple))
	{
		/*check the parm in mgr_parm, type is '#'*/
		if (NODE_TYPE_COORDINATOR == nodetype || NODE_TYPE_DATANODE ==nodetype)
		{
			tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(PARM_IN_CN_DN), NameGetDatum(key));
			if(!HeapTupleIsValid(tuple))
			{		
					tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(nodetype), NameGetDatum(key));
					if(!HeapTupleIsValid(tuple))
						ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
							, errmsg("unrecognized configuration parameter \"%s\"", key->data)));
					return false;
			}
		}
		else if (NODE_TYPE_GTM == nodetype)
		{
			tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(nodetype), NameGetDatum(key));
			if(!HeapTupleIsValid(tuple))
			{
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, errmsg("unrecognized configuration parameter \"%s\"", key->data)));
					return false;
			}
		}
		else
		{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the node type \"%c\" does not exist", nodetype)));
				return false;
		}
	}
	/*check the value: min <=value<=max*/
	
	ReleaseSysCache(tuple);
	return true;
}


/*
* check the parmeter exist in mgr_updateparm systbl or not, if it is already in and check the value to update, then return true; else return false
*/

static bool check_parm_in_updatetbl(Relation noderel, char nodetype, char innertype, Name nodename, Name parmname, char *parmvalue)
{
	HeapTuple tuple;
	Form_mgr_updateparm mgr_updateparm;

	tuple = SearchSysCache3(MONITORNAMEINNERTYPEKEY, NameGetDatum(nodename), CharGetDatum(innertype), NameGetDatum(parmname));
	if(!HeapTupleIsValid(tuple))
	{
		return false;
	}
	/*check value*/
	mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
	Assert(mgr_updateparm);	
	/*check the value, if not same, update its value*/
	if (strcmp(parmvalue, NameStr(mgr_updateparm->updateparmvalue)) != 0)
	{
		/*update parm's value*/
		namestrcpy(&(mgr_updateparm->updateparmvalue), parmvalue);
		heap_inplace_update(noderel, tuple);
	}
	ReleaseSysCache(tuple);
	return true;
}

void mgr_add_parm(char *nodename, char nodetype, StringInfo infosendparamsg)
{
	/*get the paremeters from mgr_updateparm, then add them to infosendparamsg*/
	Relation rel_updateparm;
	Form_mgr_updateparm mgr_updateparm;
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	char *parmkey;
	char *parmvalue;
	NameData nodenamedata;
	
	namestrcpy(&nodenamedata, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_updateparm_innertype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,NameGetDatum(nodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_updateparm_name
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenamedata));	
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	/*first, get */
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
