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
#include "utils/timestamp.h"
//#include "pgtypes_timestamp.h"
// #include <pgtypes_timestamp.h>
#include "utils/inet.h"

#include "catalog/monitor_host.h"
#include "catalog/monitor_cpu.h"
#include "catalog/monitor_mem.h"
#include "catalog/monitor_net.h"
#include "catalog/monitor_disk.h"
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

/* for table: monitor_host */
typedef struct Monitor_Host
{
	Oid 		   host_oid;
	StringInfoData ip_addr;
	StringInfoData platform_type;
	StringInfoData cpu_type;
	int 		   run_state;
	StringInfoData begin_run_time;
}Monitor_Host;

/* for table: monitor_cpu */
typedef struct Monitor_Cpu
{
	StringInfoData cpu_timestamp;
	float		   cpu_usage;
}Monitor_Cpu;

/* for table: monitor_mem */
typedef struct Monitor_Mem
{
	StringInfoData  mem_timestamp;
	int64			mem_total;
	int64			mem_used;
}Monitor_Mem;

/* for table: monitor_net */
typedef struct Monitor_Net
{
	StringInfoData  net_timestamp;
	int64			net_sent;
	int64			net_recv;
}Monitor_Net;

/* for table: monitor_disk */
typedef struct Monitor_Disk
{
	StringInfoData  disk_timestamp;
	int64			disk_total;
	int64			disk_available;
	int64			disk_io_read_bytes;
	int64			disk_io_read_time;
	int64			disk_io_write_bytes;
	int64			disk_io_write_time;
}Monitor_Disk;

static void init_Monitor_Host(Monitor_Host *monitor_host);
static void init_Monitor_Cpu(Monitor_Cpu *Monitor_cpu);
static void init_Monitor_Mem(Monitor_Mem *Monitor_mem);
static void init_Monitor_Net(Monitor_Net *Monitor_net);
static void init_Monitor_Disk(Monitor_Disk *Monitor_disk);

static void pfree_Monitor_Host(Monitor_Host *monitor_host);
static void pfree_Monitor_Cpu(Monitor_Cpu *Monitor_cpu);
static void pfree_Monitor_Mem(Monitor_Mem *Monitor_mem);
static void pfree_Monitor_Net(Monitor_Net *Monitor_net);
static void pfree_Monitor_Disk(Monitor_Disk *Monitor_disk);

static void insert_into_monotor_cpu(Oid host_oid, Monitor_Cpu *monitor_cpu);

/*
 *	get the host info(host base info, cpu, disk, mem, net)
 *  insert into the table:monitor_host, monitor_cpu, monitor_mem
 *						  monitor_disk, monitor_net.
 */
Datum
monitor_get_hostinfo(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    InitHostInfo *info;
    HeapTuple tup;
    HeapTuple tup_result;
    Form_mgr_host mgr_host;
	StringInfoData buf;
	//GetAgentCmdRst getAgentCmdRst;
	bool ret;
	StringInfoData agentRstStr;
	ManagerAgent *ma;
	bool execok = false;
	// char *ptmp;
	Oid host_oid;
	
	Monitor_Host monitor_host;
	Monitor_Cpu monitor_cpu;
	Monitor_Mem monitor_mem;
	Monitor_Net monitor_net;
	Monitor_Disk monitor_disk;
	
	initStringInfo(&agentRstStr);
	init_Monitor_Host(&monitor_host);
	init_Monitor_Cpu(&monitor_cpu);
	init_Monitor_Mem(&monitor_mem);
	init_Monitor_Net(&monitor_net);
	init_Monitor_Disk(&monitor_disk);

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
    if (tup == NULL)
    {
        /* end of row */
        heap_endscan(info->rel_scan);
        heap_close(info->rel_host, AccessShareLock);
        pfree(info);
        SRF_RETURN_DONE(funcctx);
    }

    mgr_host = (Form_mgr_host)GETSTRUCT(tup);
    Assert(mgr_host);
	
	host_oid = HeapTupleGetOid(tup);
	
	ma = ma_connect_hostoid(HeapTupleGetOid(tup));
	
	if (!ma_isconnected(ma))
	{
		/* report error message */
		
		ret = false;
		appendStringInfoString(&agentRstStr, ma_last_error_msg(ma));
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_MONITOR_GETS_HOST_INFO);
	ma_endmessage(&buf, ma);
	if (!ma_flush(ma, true))
	{
		ret = false;
		appendStringInfoString(&agentRstStr, ma_last_error_msg(ma));
		ma_close(ma);
		heap_endscan(info->rel_scan);
		heap_close(info->rel_host, RowExclusiveLock);
	}

	/*check the receive msg*/
	execok = mgr_recv_msg_for_monitor(ma, &ret, &agentRstStr);
	Assert(execok == ret);

	agentRstStr.cursor = 0;
	tup_result = build_common_command_tuple(
		&(mgr_host->hostname)
		, ret
		, agentRstStr.data);
	

	//while ((ptmp = &getAgentCmdRst.description.data[getAgentCmdRst.description.cursor]) != '\0' && (getAgentCmdRst.description.cursor < getAgentCmdRst.description.len))
	//{
	/* cpu timestamp */
	appendStringInfoString(&monitor_cpu.cpu_timestamp, &agentRstStr.data[agentRstStr.cursor]);
	agentRstStr.cursor = agentRstStr.cursor + monitor_cpu.cpu_timestamp.len + 1;

	/* cpu usage */
	monitor_cpu.cpu_usage = atof(&agentRstStr.data[agentRstStr.cursor]);
	agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;
	//}

	
	insert_into_monotor_cpu(host_oid, &monitor_cpu);

	pfree_Monitor_Host(&monitor_host);
	pfree_Monitor_Cpu(&monitor_cpu);
	pfree_Monitor_Mem(&monitor_mem);
	pfree_Monitor_Net(&monitor_net);
	pfree_Monitor_Disk(&monitor_disk);
	pfree(agentRstStr.data);

	ma_close(ma);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


static void insert_into_monotor_cpu(Oid host_oid, Monitor_Cpu *monitor_cpu)
{
	Relation monitorcpu;
	HeapTuple newtuple;
	Datum datum[Natts_monitor_cpu];
	bool isnull[Natts_monitor_cpu];

	datum[Anum_monitor_cpu_host_oid - 1] = ObjectIdGetDatum(host_oid);
	datum[Anum_monitor_cpu_mc_timestamptz - 1] = DirectFunctionCall3(timestamptz_in, CStringGetDatum(monitor_cpu->cpu_timestamp.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	datum[Anum_monitor_cpu_mc_usage - 1] = Float4GetDatum(monitor_cpu->cpu_usage);

	memset(isnull, 0, sizeof(isnull));

	monitorcpu = heap_open(MonitorCpuRelationId, RowExclusiveLock);
	newtuple = heap_form_tuple(RelationGetDescr(monitorcpu), datum, isnull);
	
	simple_heap_insert(monitorcpu, newtuple);
	//heap_insert(monitorcpu, newtuple, GetCurrentCommandId(true), 0, NULL);

	heap_freetuple(newtuple);
	heap_close(monitorcpu, RowExclusiveLock);
}

static void init_Monitor_Host(Monitor_Host *monitor_host)
{
	initStringInfo(&monitor_host->ip_addr);
	initStringInfo(&monitor_host->platform_type);
	initStringInfo(&monitor_host->cpu_type);
	initStringInfo(&monitor_host->begin_run_time);
}

static void pfree_Monitor_Host(Monitor_Host *monitor_host)
{
	pfree(monitor_host->ip_addr.data);
	pfree(monitor_host->platform_type.data);
	pfree(monitor_host->cpu_type.data);
	pfree(monitor_host->begin_run_time.data);
}

static void init_Monitor_Cpu(Monitor_Cpu *Monitor_cpu)
{
	initStringInfo(&Monitor_cpu->cpu_timestamp);
}

static void pfree_Monitor_Cpu(Monitor_Cpu *Monitor_cpu)
{
	pfree(Monitor_cpu->cpu_timestamp.data);
}

static void init_Monitor_Mem(Monitor_Mem *Monitor_mem)
{
	initStringInfo(&Monitor_mem->mem_timestamp);
}

static void pfree_Monitor_Mem(Monitor_Mem *Monitor_mem)
{
	pfree(Monitor_mem->mem_timestamp.data);
}

static void init_Monitor_Net(Monitor_Net *Monitor_net)
{
	initStringInfo(&Monitor_net->net_timestamp);
}

static void pfree_Monitor_Net(Monitor_Net *Monitor_net)
{
	pfree(Monitor_net->net_timestamp.data);
}

static void init_Monitor_Disk(Monitor_Disk *Monitor_disk)
{
	initStringInfo(&Monitor_disk->disk_timestamp);
}

static void pfree_Monitor_Disk(Monitor_Disk *Monitor_disk)
{
	pfree(Monitor_disk->disk_timestamp.data);
}