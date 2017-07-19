
#ifndef MGR_CMDS_H
#define MGR_CMDS_H

#include "parser/mgr_node.h"
#include "nodes/params.h"
#include "tcop/dest.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "mgr/mgr_agent.h"
#include "utils/timestamp.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "catalog/mgr_cndnnode.h"
#include "utils/relcache.h"
#include "access/heapam.h"

#define run_success "success"
#define PRIV_GRANT        'G'
#define PRIV_REVOKE       'R'


typedef struct GetAgentCmdRst
{
	NameData nodename;
	int ret;
	StringInfoData description;
}GetAgentCmdRst;

typedef struct AppendNodeInfo
{
	char *nodename;
	char *nodepath;
	char  nodetype;
	Oid   nodehost;
	char *nodeaddr;
	int32 nodeport;
	Oid   nodemasteroid;
	char *nodeusername;
	Oid		tupleoid;
	NameData sync_state;
}AppendNodeInfo;

/* for table: monitor_alarm */
typedef struct Monitor_Alarm
{
	int16			alarm_level;
	int16			alarm_type;
	StringInfoData	alarm_timetz;
	int16			alarm_status;
	StringInfoData	alarm_source;
	StringInfoData	alarm_text;
}Monitor_Alarm;

/* for table: monitor_alarm */
typedef struct Monitor_Threshold
{
	int16			threshold_warning;
	int16			threshold_critical;
	int16			threshold_emergency;
}Monitor_Threshold;

/*cmd flag for create/drop extension*/
typedef enum
{
	EXTENSION_CREATE,
	EXTENSION_DROP
}extension_operator;

typedef struct InitNodeInfo
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitNodeInfo;

typedef struct InitAclInfo
{
	Relation rel_authid;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitAclInfo;

struct tuple_cndn
{
	List *coordiantor_list;
	List *datanode_list;
};

/* host commands, in cmd_host.c */

extern void mgr_add_host(MGRAddHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_host(MGRDropHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_host(MGRAlterHost *node, ParamListInfo params, DestReceiver *dest);
extern bool check_node_running_by_socket(char *host, int port);
extern bool port_occupancy_test(const char *ip_address, const int port);
extern bool get_node_type_str(int node_type, Name node_type_str);

extern Datum mgr_start_agent_all(PG_FUNCTION_ARGS);
extern Datum mgr_start_agent_hostnamelist(PG_FUNCTION_ARGS);

extern Datum mgr_deploy_all(PG_FUNCTION_ARGS);
extern Datum mgr_deploy_hostnamelist(PG_FUNCTION_ARGS);

extern Datum mgr_drop_host_func(PG_FUNCTION_ARGS);
extern Datum mgr_alter_host_func(PG_FUNCTION_ARGS);

extern Datum mgr_add_updateparm_func(PG_FUNCTION_ARGS);
extern Datum mgr_reset_updateparm_func(PG_FUNCTION_ARGS);
extern void mgr_stop_agent(MGRStopAgent *node,  ParamListInfo params, DestReceiver *dest);
extern void mgr_monitor_agent(MGRMonitorAgent *node,  ParamListInfo params, DestReceiver *dest);
extern int ssh2_start_agent(const char *hostname,
							unsigned short port,
					 		const char *username,
					 		const char *password,
					 		const char *commandline,
					 		StringInfo message);
extern bool ssh2_deplory_tar(const char *hostname,
							unsigned short port,
							const char *username,
							const char *password,
							const char *path,
							FILE *tar,
							StringInfo message);
extern bool mgr_check_cluster_stop(Name nodename, Name nodetypestr);

/*parm commands, in cmd_parm.c*/
extern void mgr_alter_parm(MGRAlterParm *node, ParamListInfo params, DestReceiver *dest);

/*in cmd_node.c */
extern void mgr_reload_conf(Oid hostoid, char *nodepath);
extern bool get_active_node_info(const char node_type, const char *node_name, AppendNodeInfo *nodeinfo);
/*coordinator datanode parse cmd*/
extern Datum mgr_init_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtm_slave(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtm_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_slave(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtm_extra(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtm_extra(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_extra(PG_FUNCTION_ARGS);
extern Datum mgr_failover_gtm(PG_FUNCTION_ARGS);
extern Datum mgr_init_dn_extra_all(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_extra(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_extra(PG_FUNCTION_ARGS);
 extern Datum mgr_stop_dn_extra_all(PG_FUNCTION_ARGS);
extern Datum mgr_failover_one_dn(PG_FUNCTION_ARGS);
extern void mgr_add_node(MGRAddNode *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_node(MGRAlterNode *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_node(MGRDropNode *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_drop_node_func(PG_FUNCTION_ARGS);
extern Datum mgr_init_all(PG_FUNCTION_ARGS);
extern Datum mgr_init_cn_master(PG_FUNCTION_ARGS);
extern void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *shutdown_mode);
extern Datum mgr_init_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_init_dn_slave_all(PG_FUNCTION_ARGS);
extern void mgr_init_dn_slave_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *masterhostaddress, uint32 masterport, char *mastername);

extern Datum mgr_start_cn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_master_all(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master_all(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave_all(PG_FUNCTION_ARGS);
extern Datum mgr_stop_agent_all(PG_FUNCTION_ARGS);
extern Datum mgr_stop_agent_hostnamelist(PG_FUNCTION_ARGS);
extern Datum mgr_runmode_cndn(char nodetype, char cmdtype, List *namelist, char *shutdown_mode, PG_FUNCTION_ARGS);

extern Datum mgr_monitor_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_datanode_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_gtm_all(PG_FUNCTION_ARGS);

extern Datum mgr_monitor_nodetype_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_nodetype_namelist(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_agent_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_agent_hostlist(PG_FUNCTION_ARGS);

extern Datum mgr_append_dnmaster(PG_FUNCTION_ARGS);
extern Datum mgr_append_dnslave(PG_FUNCTION_ARGS);
extern Datum mgr_append_dnextra(PG_FUNCTION_ARGS);
extern Datum mgr_append_coordmaster(PG_FUNCTION_ARGS);
extern Datum mgr_append_agtmslave(PG_FUNCTION_ARGS);
extern Datum mgr_append_agtmextra(PG_FUNCTION_ARGS);

extern Datum mgr_list_acl_all(PG_FUNCTION_ARGS);
extern Datum mgr_priv_manage(PG_FUNCTION_ARGS);
extern Datum mgr_priv_all_to_username(PG_FUNCTION_ARGS);
extern Datum mgr_priv_list_to_all(PG_FUNCTION_ARGS);

extern Datum mgr_add_host_func(PG_FUNCTION_ARGS);
extern Datum mgr_add_node_func(PG_FUNCTION_ARGS);
extern Datum mgr_alter_node_func(PG_FUNCTION_ARGS);

extern Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS);

extern bool mgr_has_priv_add(void);
extern bool mgr_has_priv_drop(void);
extern bool mgr_has_priv_alter(void);
extern bool mgr_has_priv_set(void);
extern bool mgr_has_priv_reset(void);

extern void mgr_start_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple);
extern List *get_fcinfo_namelist(const char *sepstr, int argidx, FunctionCallInfo fcinfo);
void check_dn_slave(char nodetype, List *nodenamelist, Relation rel_node, StringInfo infosendmsg);
extern bool mgr_refresh_pgxc_node_tbl(char *cndnname, int32 cndnport, char *cndnaddress, bool isprimary, Oid cndnmasternameoid, GetAgentCmdRst *getAgentCmdRst);
extern void mgr_send_conf_parameters(char filetype, char *datapath, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst);
extern void mgr_append_pgconf_paras_str_str(char *key, char *value, StringInfo infosendmsg);
extern void mgr_append_pgconf_paras_str_int(char *key, int value, StringInfo infosendmsg);
extern void mgr_get_gtm_host_port(StringInfo infosendmsg);
extern void mgr_append_infostr_infostr(StringInfo infostr, StringInfo sourceinfostr);
extern void mgr_add_parameters_pgsqlconf(Oid tupleOid, char nodetype, int cndnport, StringInfo infosendparamsg);
extern void mgr_append_pgconf_paras_str_quotastr(char *key, char *value, StringInfo infosendmsg);
extern void mgr_add_parameters_recoveryconf(char nodetype, char *slavename, Oid tupleoid, StringInfo infosendparamsg);
extern void mgr_add_parameters_hbaconf(Oid mastertupleoid, char nodetype, StringInfo infosendhbamsg);
extern void mgr_add_oneline_info_pghbaconf(int type, char *database, char *user, char *addr, int addr_mark, char *auth_method, StringInfo infosendhbamsg);
extern Datum mgr_start_one_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_dn_master(PG_FUNCTION_ARGS);
extern char *mgr_get_slavename(Oid tupleOid, char nodetype);
extern void mgr_rename_recovery_to_conf(char cmdtype, Oid hostOid, char* cndnpath, GetAgentCmdRst *getAgentCmdRst);
extern HeapTuple mgr_get_tuple_node_from_name_type(Relation rel, char *nodename, char nodetype);
extern char *mgr_nodetype_str(char nodetype);
extern Datum mgr_clean_all(PG_FUNCTION_ARGS);
extern Datum mgr_clean_node(PG_FUNCTION_ARGS);
extern int mgr_check_node_exist_incluster(Name nodename, char nodetype, bool bincluster);
extern List* mgr_get_nodetype_namelist(char nodetype);
extern Datum mgr_remove_node_func(PG_FUNCTION_ARGS);
extern void mgr_remove_node(MgrRemoveNode *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_monitor_ha(PG_FUNCTION_ARGS);
extern void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void pfree_AppendNodeInfo(AppendNodeInfo nodeinfo);
extern void mgr_lock_cluster(PGconn **pg_conn, Oid *cnoid);
extern void mgr_unlock_cluster(PGconn **pg_conn);
extern void mgr_get_master_sync_string(Oid mastertupleoid, bool bincluster, Oid excludeoid, StringInfo infostrparam);
extern bool mgr_pqexec_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst, PGconn **pg_conn, Oid cnoid);
/* mgr_common.c */
extern TupleDesc get_common_command_tuple_desc(void);
extern HeapTuple build_common_command_tuple(const Name name, bool success, const char *message);
extern int pingNode_user(char *host, char *port, char *user);
extern bool is_valid_ip(char *ip);
extern bool	mgr_check_host_in_use(Oid hostoid, bool check_inited);
extern void mgr_mark_node_in_cluster(Relation rel);
extern TupleDesc get_showparam_command_tuple_desc(void);
HeapTuple build_list_acl_command_tuple(const Name name, const char *message);
TupleDesc get_list_acl_command_tuple_desc(void);
List * DecodeTextArrayToValueList(Datum textarray);
void check_nodename_isvalid(char *nodename);
bool mgr_has_function_privilege_name(char *funcname, char *priv_type);
bool mgr_has_table_privilege_name(char *tablename, char *priv_type);
extern void mgr_recv_sql_stringvalues_msg(ManagerAgent	*ma, StringInfo resultstrdata);
/* get the host address */
char *get_hostaddress_from_hostoid(Oid hostOid);
char *get_hostname_from_hostoid(Oid hostOid);
char *get_hostuser_from_hostoid(Oid hostOid);
bool mgr_get_active_node(Name nodename, char nodetype);

/* get msg from agent */
bool mgr_recv_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
bool mgr_recv_msg_for_monitor(ManagerAgent	*ma, bool *ret, StringInfo agentRstStr);
extern List *monitor_get_dbname_list(char *user, char *address, int port);
extern void monitor_get_one_node_user_address_port(Relation rel_node, int *agentport, char **user, char **address, int *coordport, char nodetype);
extern HeapTuple build_ha_replication_tuple(const Name type, const Name nodename, const Name app, const Name client_addr, const Name state, const Name sent_location, const Name replay_location, const Name sync_state, const Name master_location, const Name sent_delay, const Name replay_delay);
extern TupleDesc get_ha_replication_tuple_desc(void);
extern bool mgr_promote_node(char cmdtype, Oid hostOid, char *path, StringInfo strinfo);
extern bool mgr_check_node_connect(char nodetype, Oid hostOid, int nodeport);
extern bool mgr_rewind_node(char nodetype, char *nodename, StringInfo strinfo);
extern bool mgr_ma_send_cmd(char cmdtype, char *cmdstr, Oid hostOid, StringInfo strinfo);
extern void mgr_get_cmd_head_word(char cmdtype, char *str);
extern bool mgr_check_node_recovery_finish(char nodetype, Oid hostoid, int nodeport, char *address);
extern char mgr_get_master_type(char nodetype);

/* monitor_hostpage.c */
extern Datum monitor_get_hostinfo(PG_FUNCTION_ARGS);
bool get_cpu_info(StringInfo hostinfostring);
extern void insert_into_monitor_alarm(Monitor_Alarm *monitor_alarm);
extern void get_threshold(int16 type, Monitor_Threshold *monitor_threshold);

/*monitor_databaseitem.c*/
extern int monitor_get_onesqlvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname);
extern int monitor_get_result_one_node(Relation rel_node, char *sqlstr, char *dbname, char nodetype);
extern int monitor_get_sqlres_all_typenode_usedbname(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int gettype);
extern Datum monitor_databaseitem_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple monitor_build_database_item_tuple(Relation rel, const TimestampTz time, char *dbname
			, int dbsize, bool archive, bool autovacuum, float heaphitrate,  float commitrate, int dbage, int connectnum
			, int standbydelay, int locksnum, int longquerynum, int idlequerynum, int preparenum, int unusedindexnum, int indexsize);
extern Datum monitor_databasetps_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple monitor_build_databasetps_qps_tuple(Relation rel, const TimestampTz time, const char *dbname, const int tps, const int qps, int pgdbruntime);
extern void monitor_get_stringvalues(char cmdtype, int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname, StringInfo resultstrdata);
extern void monitor_delete_data(MonitorDeleteData *node, ParamListInfo params, DestReceiver *dest);
extern Datum monitor_delete_data_interval_days(PG_FUNCTION_ARGS);
extern void mgr_set_init(MGRSetClusterInit *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_set_init_cluster(PG_FUNCTION_ARGS);

/*monitor_slowlog.c*/
extern char *monitor_get_onestrvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int port, char * dbname);
extern void monitor_get_onedb_slowdata_insert(Relation rel, int agentport, char *user, char *address, int port, char *dbname);
extern HeapTuple monitor_build_slowlog_tuple(Relation rel, TimestampTz time, char *dbname, char *username, float singletime, int totalnum, char *query, char *queryplan);
extern Datum monitor_slowlog_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple check_record_yestoday_today(Relation rel, int *callstoday, int *callsyestd, bool *gettoday, bool *getyesdt, char *query, char *user, char *dbname, pg_time_t ptimenow);
extern void monitor_insert_record(Relation rel, int agentport, TimestampTz time, char *dbname, char *dbuser, float singletime, int calls, char *querystr, char *user, char *address, int port);

/*monitor_dbthreshold.c*/
extern Datum get_dbthreshold(PG_FUNCTION_ARGS);
extern char *monitor_get_timestamptz_onenode(int agentport, char *user, char *address, int port);
extern bool monitor_get_sqlvalues_one_node(int agentport, char *sqlstr, char *user, char *address, int port, char * dbname, int iarray[], int len);

/*mgr_updateparm*/
extern void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_add_parm(char *nodename, char nodetype, StringInfo infosendparamsg);
extern void mgr_reset_updateparm(MGRUpdateparmReset *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_parmr_update_tuple_nodename_nodetype(Relation noderel, Name nodename, char oldnodetype, char newnodetype);
extern void mgr_update_parm_after_dn_failover(Name oldmastername, char oldmastertype, Name oldslavename, char oldslavetype);
extern void mgr_parm_after_gtm_failover_handle(Name mastername, char mastertype, Name slavename, char slavetype);
extern void mgr_parmr_delete_tuple_nodename_nodetype(Relation noderel, Name nodename, char nodetype);
extern void mgr_flushhost(MGRFlushHost *node, ParamListInfo params, DestReceiver *dest);
extern  Datum mgr_flush_host(PG_FUNCTION_ARGS);
extern Datum mgr_show_var_param(PG_FUNCTION_ARGS);
Datum mgr_update_param_gtm_failover(PG_FUNCTION_ARGS);
Datum mgr_update_param_datanode_failover(PG_FUNCTION_ARGS);

/*mgr_hba    mgr_hba.c*/

extern void mgr_clean_hba_table(char *coord_name, char *values);
extern void add_hba_table_to_file(char *coord_name);
extern void add_one_to_hba_file(const char *coord_name, const char *hba_value, GetAgentCmdRst *err_msg);
extern Datum mgr_list_hba_by_name(PG_FUNCTION_ARGS);
extern Datum mgr_drop_hba(PG_FUNCTION_ARGS);
extern Datum mgr_add_hba(PG_FUNCTION_ARGS);

/*monitor_jobitem.c*/
extern void monitor_jobitem_add(MonitorJobitemAdd *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_jobitem_alter(MonitorJobitemAlter *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_jobitem_drop(MonitorJobitemDrop *node, ParamListInfo params, DestReceiver *dest);
extern Datum monitor_jobitem_add_func(PG_FUNCTION_ARGS);
extern Datum monitor_jobitem_alter_func(PG_FUNCTION_ARGS);
extern Datum monitor_jobitem_drop_func(PG_FUNCTION_ARGS);

extern void monitor_job_add(MonitorJobAdd *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_job_alter(MonitorJobAlter *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_job_drop(MonitorJobDrop *node, ParamListInfo params, DestReceiver *dest);
extern Datum monitor_job_add_func(PG_FUNCTION_ARGS);
extern Datum monitor_job_alter_func(PG_FUNCTION_ARGS);
extern Datum monitor_job_drop_func(PG_FUNCTION_ARGS);
extern Datum adbmonitor_job(PG_FUNCTION_ARGS);

/*create/drop extension*/
Datum mgr_extension_handle(PG_FUNCTION_ARGS);
void mgr_extension(MgrExtensionAdd *node, ParamListInfo params, DestReceiver *dest);

/*mgr_manual.c*/
extern Datum mgr_failover_manual_adbmgr_func(PG_FUNCTION_ARGS);
extern Datum mgr_failover_manual_promote_func(PG_FUNCTION_ARGS);
extern Datum mgr_failover_manual_pgxcnode_func(PG_FUNCTION_ARGS);
extern Datum mgr_failover_manual_rewind_func(PG_FUNCTION_ARGS);

/*expansion calls*/
extern void	mgr_make_sure_all_running(char node_type);
extern Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange, bool bforce);
extern bool is_node_running(char *hostaddr, int32 hostport, char *user);
extern bool mgr_try_max_pingnode(char *host, char *port, char *user, const int max_times);
extern char mgr_get_master_type(char nodetype);
extern void mgr_get_nodeinfo_byname_type(char *node_name, char node_type, bool bincluster, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void get_nodeinfo(char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void mgr_add_hbaconf(char nodetype, char *dnusername, char *dnaddr);
extern void mgr_check_dir_exist_and_priv(Oid hostoid, char *dir);
extern void mgr_pgbasebackup(char nodetype, AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo);
extern void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid);
extern HeapTuple build_common_command_tuple_for_monitor(const Name name
                                                        ,char type
                                                        ,bool status
                                                        ,const char *description);
extern void mgr_get_self_address(char *server_address, int server_port, Name self_address);

#endif /* MGR_CMDS_H */
