/*
 * all function for ADB monitor host page.
 */
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_gtm.h"
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


typedef struct InitHostInfo
{
	Relation rel_host;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitHostInfo;

Datum
monitor_get_hostinfo(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitHostInfo *info;
    HeapTuple tup;
    HeapTuple tup_result;
    Form_mgr_host mgr_host;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execok = false;


    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        info = palloc(sizeof(*info));
        info->rel_host = heap_open(HostRelationId, AccessShareLock);
        info->rel_scan = heap_beginscan(info->rel_host, SnapshotNow, 0, NULL);
        info->lcp =NULL;
			
        /* save info */
        funcctx->user_fctx = info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Assert(funcctx);
    info = funcctx->user_fctx;
    Assert(info);

    tup = heap_getnext(info->rel_scan, ForwardScanDirection);
    if(tup == NULL)
    {
        /* end of row */
        heap_endscan(info->rel_scan);
        heap_close(info->rel_host, AccessShareLock);
        pfree(info);
        SRF_RETURN_DONE(funcctx);
    }

    mgr_host = (Form_mgr_host)GETSTRUCT(tup);
    Assert(mgr_host);

	ma = ma_connect_hostoid(HeapTupleGetOid(tup));
	
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
	}
	
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_MONITOR_GETS_HOST_INFO);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		heap_endscan(info->rel_scan);
		heap_close(info->rel_host, RowExclusiveLock);
	}

	/*check the receive msg*/
	execok = mgr_recv_msg_for_monitor(ma, &getAgentCmdRst);
	Assert(execok == getAgentCmdRst.ret);
	
	tup_result = build_common_command_tuple(
		&(mgr_host->hostname)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);

	pfree(getAgentCmdRst.description.data);
	ma_close(ma);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}
