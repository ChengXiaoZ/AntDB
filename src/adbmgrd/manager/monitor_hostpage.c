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
#include "catalog/mgr_cndnnode.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
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

#define strtoull(x)  ((unsigned long long int) strtoull((x), NULL, 10))

typedef struct InitHostInfo
{
    Relation rel_host;
    HeapScanDesc rel_scan;
    ListCell  **lcp;
}InitHostInfo;

/* for table: monitor_host */
typedef struct Monitor_Host
{
    int            run_state;
    StringInfoData current_time;
    int64          seconds_since_boot;
    int            cpu_core_total;
    int            cpu_core_available;
    StringInfoData system;
    StringInfoData platform_type;
}Monitor_Host;

/* for table: monitor_cpu */
typedef struct Monitor_Cpu
{
    StringInfoData cpu_timestamp;
    float          cpu_usage;
}Monitor_Cpu;

/* for table: monitor_mem */
typedef struct Monitor_Mem
{
    StringInfoData  mem_timestamp;
    int64           mem_total;
    int64           mem_used;
    float           mem_usage;
}Monitor_Mem;

/* for table: monitor_net */
typedef struct Monitor_Net
{
    StringInfoData  net_timestamp;
    int64           net_sent;
    int64           net_recv;
}Monitor_Net;

/* for table: monitor_disk */
typedef struct Monitor_Disk
{
    StringInfoData  disk_timestamptz;
    int64           disk_total;
    int64           disk_used;
    int64           disk_io_read_bytes;
    int64           disk_io_read_time;
    int64           disk_io_write_bytes;
    int64           disk_io_write_time;
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
static void insert_into_monotor_mem(Oid host_oid, Monitor_Mem *monitor_mem);
static void insert_into_monotor_disk(Oid host_oid, Monitor_Disk *monitor_disk);
static void insert_into_monotor_net(Oid host_oid, Monitor_Net *monitor_net);
static void insert_into_monotor_host(Oid host_oid, Monitor_Host *monitor_host);

/*
 *  get the host info(host base info, cpu, disk, mem, net)
 *  insert into the table:monitor_host, monitor_cpu, monitor_mem
 *                        monitor_disk, monitor_net.
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
    bool ret;
    StringInfoData agentRstStr;
    ManagerAgent *ma;
    bool execok = false;
    //char *ptmp;
    Oid host_oid;
    
    Monitor_Host monitor_host;
    Monitor_Cpu monitor_cpu;
    Monitor_Mem monitor_mem;
    Monitor_Disk monitor_disk;
    Monitor_Net monitor_net;

    initStringInfo(&agentRstStr);
    init_Monitor_Host(&monitor_host);
    init_Monitor_Cpu(&monitor_cpu);
    init_Monitor_Mem(&monitor_mem);
    init_Monitor_Disk(&monitor_disk);
    init_Monitor_Net(&monitor_net);

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

    //while ((ptmp = &agentRstStr.data[agentRstStr.cursor]) != '\0' && (agentRstStr.cursor < agentRstStr.len))
    //{
        /* cpu timestamp with timezone */
        appendStringInfoString(&monitor_cpu.cpu_timestamp, &agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + monitor_cpu.cpu_timestamp.len + 1;

        /* cpu usage */
        monitor_cpu.cpu_usage = atof(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* memory timestamp with timezone */
        appendStringInfoString(&monitor_mem.mem_timestamp, &agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + monitor_mem.mem_timestamp.len + 1;

        /* memory total size (in Bytes)*/
        monitor_mem.mem_total = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* memory used size (in Bytes) */
        monitor_mem.mem_used = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* memory usage */
        monitor_mem.mem_usage = atof(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* disk timestamp with timezone */
        appendStringInfoString(&monitor_disk.disk_timestamptz, &agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + monitor_disk.disk_timestamptz.len + 1;

        /* disk i/o read (in Bytes) */
        monitor_disk.disk_io_read_bytes = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* disk i/o read time (in milliseconds) */
        monitor_disk.disk_io_read_time = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* disk i/o write (in Bytes) */
        monitor_disk.disk_io_write_bytes = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* disk i/o write time (in milliseconds) */
        monitor_disk.disk_io_write_time = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;
        
        /* disk total size */
        monitor_disk.disk_total = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* disk used size */
        monitor_disk.disk_used = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* net timestamp with timezone */
        appendStringInfoString(&monitor_net.net_timestamp, &agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + monitor_net.net_timestamp.len + 1;

        /* net sent speed (in bytes/s) */
        monitor_net.net_sent = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* net recv speed (in bytes/s) */
        monitor_net.net_recv = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* host system */
        appendStringInfoString(&monitor_host.system, &agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + monitor_host.system.len + 1;

        /* host platform type */
        appendStringInfoString(&monitor_host.platform_type, &agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + monitor_host.platform_type.len + 1;

        /* host cpu total cores */
        monitor_host.cpu_core_total = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;

        /* host cpu available cores */
        monitor_host.cpu_core_available = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;
        
        /* host seconds since boot */
        monitor_host.seconds_since_boot = strtoull(&agentRstStr.data[agentRstStr.cursor]);
        agentRstStr.cursor = agentRstStr.cursor + strlen(&agentRstStr.data[agentRstStr.cursor]) + 1;  
    //}

    monitor_host.run_state = 1;
    
    resetStringInfo(&monitor_mem.mem_timestamp);
    resetStringInfo(&monitor_disk.disk_timestamptz);
    resetStringInfo(&monitor_net.net_timestamp);
    
    appendStringInfoString(&monitor_mem.mem_timestamp, monitor_cpu.cpu_timestamp.data);
    appendStringInfoString(&monitor_disk.disk_timestamptz, monitor_cpu.cpu_timestamp.data);
    appendStringInfoString(&monitor_net.net_timestamp, monitor_cpu.cpu_timestamp.data);
    appendStringInfoString(&monitor_host.current_time, monitor_cpu.cpu_timestamp.data);
    
    insert_into_monotor_cpu(host_oid, &monitor_cpu);
    insert_into_monotor_mem(host_oid, &monitor_mem);
    insert_into_monotor_disk(host_oid, &monitor_disk);
    insert_into_monotor_net(host_oid, &monitor_net);
    insert_into_monotor_host(host_oid, &monitor_host);

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
    datum[Anum_monitor_cpu_mc_timestamptz - 1] = 
        DirectFunctionCall3(timestamptz_in, CStringGetDatum(monitor_cpu->cpu_timestamp.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    datum[Anum_monitor_cpu_mc_usage - 1] = Float4GetDatum(monitor_cpu->cpu_usage);

    memset(isnull, 0, sizeof(isnull));

    monitorcpu = heap_open(MonitorCpuRelationId, RowExclusiveLock);
    newtuple = heap_form_tuple(RelationGetDescr(monitorcpu), datum, isnull);
    simple_heap_insert(monitorcpu, newtuple);

    heap_freetuple(newtuple);
    heap_close(monitorcpu, RowExclusiveLock);
}

static void insert_into_monotor_mem(Oid host_oid, Monitor_Mem *monitor_mem)
{
    Relation monitormem;
    HeapTuple newtuple;
    Datum datum[Natts_monitor_mem];
    bool isnull[Natts_monitor_mem];

    datum[Anum_monitor_mem_host_oid - 1] = ObjectIdGetDatum(host_oid);
    datum[Anum_monitor_mem_mm_timestamptz - 1] = 
        DirectFunctionCall3(timestamptz_in, CStringGetDatum(monitor_mem->mem_timestamp.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    datum[Anum_monitor_mem_mm_total - 1] = Int64GetDatum(monitor_mem->mem_total);
    datum[Anum_monitor_mem_mm_used - 1] = Int64GetDatum(monitor_mem->mem_used);
    datum[Anum_monitor_mem_mm_usage - 1] = Float4GetDatum(monitor_mem->mem_usage);

    memset(isnull, 0, sizeof(isnull));

    monitormem = heap_open(MonitorMemRelationId, RowExclusiveLock);
    newtuple = heap_form_tuple(RelationGetDescr(monitormem), datum, isnull);
    simple_heap_insert(monitormem, newtuple);

    heap_freetuple(newtuple);
    heap_close(monitormem, RowExclusiveLock);
}

static void insert_into_monotor_disk(Oid host_oid, Monitor_Disk *monitor_disk)
{
    Relation monitordisk;
    HeapTuple newtuple;
    Datum datum[Natts_monitor_disk];
    bool isnull[Natts_monitor_disk];

    datum[Anum_monitor_disk_host_oid - 1] = ObjectIdGetDatum(host_oid);
    datum[Anum_monitor_disk_md_timestamptz - 1] = 
        DirectFunctionCall3(timestamptz_in, CStringGetDatum(monitor_disk->disk_timestamptz.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    datum[Anum_monitor_disk_md_total - 1] = Int64GetDatum(monitor_disk->disk_total);
    datum[Anum_monitor_disk_md_used - 1] = Int64GetDatum(monitor_disk->disk_used);    
    datum[Anum_monitor_disk_md_io_read_bytes - 1] = Int64GetDatum(monitor_disk->disk_io_read_bytes);
    datum[Anum_monitor_disk_md_io_reat_time - 1] = Int64GetDatum(monitor_disk->disk_io_read_time);
    datum[Anum_monitor_disk_md_io_write_bytes - 1] = Int64GetDatum(monitor_disk->disk_io_write_bytes);
    datum[Anum_monitor_disk_md_io_write_time - 1] = Int64GetDatum(monitor_disk->disk_io_write_time);

    memset(isnull, 0, sizeof(isnull));

    monitordisk = heap_open(MonitorDiskRelationId, RowExclusiveLock);
    newtuple = heap_form_tuple(RelationGetDescr(monitordisk), datum, isnull);
    simple_heap_insert(monitordisk, newtuple);

    heap_freetuple(newtuple);
    heap_close(monitordisk, RowExclusiveLock);
}

static void insert_into_monotor_net(Oid host_oid, Monitor_Net *monitor_net)
{
    Relation monitornet;
    HeapTuple newtuple;
    Datum datum[Natts_monitor_net];
    bool isnull[Natts_monitor_net];

    datum[Anum_monitor_net_host_oid - 1] = ObjectIdGetDatum(host_oid);
    datum[Anum_monitor_net_mn_timestamptz - 1] = 
        DirectFunctionCall3(timestamptz_in, CStringGetDatum(monitor_net->net_timestamp.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    datum[Anum_monitor_net_mn_sent - 1] = Int64GetDatum(monitor_net->net_sent);
    datum[Anum_monitor_net_mn_recv - 1] = Int64GetDatum(monitor_net->net_recv);

    memset(isnull, 0, sizeof(isnull));

    monitornet = heap_open(MonitorNetRelationId, RowExclusiveLock);
    newtuple = heap_form_tuple(RelationGetDescr(monitornet), datum, isnull);
    simple_heap_insert(monitornet, newtuple);

    heap_freetuple(newtuple);
    heap_close(monitornet, RowExclusiveLock);
}

static void insert_into_monotor_host(Oid host_oid, Monitor_Host *monitor_host)
{
    Relation monitorhost;
    HeapTuple newtuple;
    Datum datum[Natts_monitor_host];
    bool isnull[Natts_monitor_host];

    datum[Anum_monitor_net_host_oid - 1] = ObjectIdGetDatum(host_oid);
    datum[Anum_monitor_host_mh_run_state - 1] = Int16GetDatum(monitor_host->run_state);
    datum[Anum_monitor_host_mh_current_time - 1] = 
        DirectFunctionCall3(timestamptz_in, CStringGetDatum(monitor_host->current_time.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    datum[Anum_monitor_host_mh_seconds_since_boot - 1] = Int64GetDatum(monitor_host->seconds_since_boot);
    datum[Anum_monitor_host_mh_cpu_core_total - 1] = Int16GetDatum(monitor_host->cpu_core_total);
    datum[Anum_monitor_host_mh_cpu_core_available - 1] = Int16GetDatum(monitor_host->cpu_core_available);
    datum[Anum_monitor_host_mh_system - 1] = CStringGetTextDatum(monitor_host->system.data);
    datum[Anum_monitor_host_mh_platform_type - 1] = CStringGetTextDatum(monitor_host->platform_type.data);

    memset(isnull, 0, sizeof(isnull));

    monitorhost = heap_open(MonitorHostRelationId, RowExclusiveLock);
    newtuple = heap_form_tuple(RelationGetDescr(monitorhost), datum, isnull);
    simple_heap_insert(monitorhost, newtuple);

    heap_freetuple(newtuple);
    heap_close(monitorhost, RowExclusiveLock);
}

static void init_Monitor_Host(Monitor_Host *monitor_host)
{
    initStringInfo(&monitor_host->current_time);
    initStringInfo(&monitor_host->system);
    initStringInfo(&monitor_host->platform_type);
}

static void pfree_Monitor_Host(Monitor_Host *monitor_host)
{
    pfree(monitor_host->current_time.data);
    pfree(monitor_host->system.data);
    pfree(monitor_host->platform_type.data);
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
    initStringInfo(&Monitor_disk->disk_timestamptz);
}

static void pfree_Monitor_Disk(Monitor_Disk *Monitor_disk)
{
    pfree(Monitor_disk->disk_timestamptz.data);
}
