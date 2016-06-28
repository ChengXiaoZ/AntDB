/*
 * commands of parm
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_parm.h"
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

#if (Natts_mgr_parm != 5)
#error "need change code"
#endif

/* systbl: parm */
void mgr_alter_parm(MGRAlterParm *node, ParamListInfo params, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tuple,
	          new_tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	NameData parmkey;
	NameData parmnode;
	Datum datum[Natts_mgr_parm];
	bool isnull[Natts_mgr_parm];
	bool got[Natts_mgr_parm];
	
	TupleDesc host_dsc;
	
	Assert(node && node->parmkey && node->parmnode);
	rel = heap_open(ParmRelationId, RowExclusiveLock);
	host_dsc = RelationGetDescr(rel);
	namestrcpy(&parmkey, node->parmkey);
	namestrcpy(&parmnode, node->parmnode);

	/* check whether exists */
	tuple = SearchSysCache2(PARMPARMKEY, NameGetDatum(&parmkey), NameGetDatum(&parmnode));
	if(!SearchSysCacheExists2(PARMPARMKEY, NameGetDatum(&parmkey), NameGetDatum(&parmnode)))
	{
		if(node->if_not_exists)
		{
			heap_close(rel, RowExclusiveLock);
			return;
		}
                
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("paramter \"%s\" type \"%s\" doesnot exists", NameStr(parmkey),NameStr(parmnode))));
	}

	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* parmkey */
	datum[Anum_mgr_parm_parmkey-1] = NameGetDatum(&parmkey);
	datum[Anum_mgr_parm_parmnode-1] = NameGetDatum(&parmnode);
	foreach(lc,node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "value") == 0)
		{
			if(got[Anum_mgr_parm_parmvalue-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_parm_parmvalue-1] = true;
			str = defGetString(def);
			datum[Anum_mgr_parm_parmvalue-1] = PointerGetDatum(cstring_to_text(str));
		}else if(strcmp(def->defname, "configtype") == 0)
		{
			if(got[Anum_mgr_parm_parmconfigtype-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_parm_parmconfigtype-1] = true;
			str = defGetString(def);
			datum[Anum_mgr_parm_parmconfigtype-1] = PointerGetDatum(cstring_to_text(str));
		}else if(strcmp(def->defname, "comment") == 0)
		{
			if(got[Anum_mgr_parm_parmcomment-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_parm_parmcomment-1] = true;
			str = defGetString(def);
			datum[Anum_mgr_parm_parmcomment-1] = PointerGetDatum(cstring_to_text(str));
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is value, configtype and comment")));
		}
	}

	new_tuple = heap_modify_tuple(tuple, host_dsc, datum,isnull, got);
	simple_heap_update(rel, &tuple->t_self, new_tuple);
	CatalogUpdateIndexes(rel, new_tuple);
	ReleaseSysCache(tuple);
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
}

