
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

#define run_success "success"

typedef struct GetAgentCmdRst
{
	NameData nodename;
	int ret;
	StringInfoData description;
}GetAgentCmdRst;

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

/* host commands, in cmd_host.c */
extern void mgr_add_host(MGRAddHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_host(MGRDropHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_host(MGRAlterHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_deplory(MGRDeplory *node,  ParamListInfo params, DestReceiver *dest);
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

/*parm commands, in cmd_parm.c*/
extern void mgr_alter_parm(MGRAlterParm *node, ParamListInfo params, DestReceiver *dest);

/*coordinator datanode parse cmd*/
extern Datum mgr_init_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_master_f(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_master_i(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtm_slave(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtm_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_slave_f(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_slave_i(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_slave_s(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtm_extra(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtm_extra(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_extra(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_extra_f(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm_extra_i(PG_FUNCTION_ARGS);
extern Datum mgr_failover_gtm(PG_FUNCTION_ARGS);
extern Datum mgr_init_dn_extra(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_extra(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_extra(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_extra_f(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_extra_i(PG_FUNCTION_ARGS);
extern Datum mgr_init_dn_extra_all(PG_FUNCTION_ARGS);
extern Datum mgr_failover_one_dn(PG_FUNCTION_ARGS);
extern void mgr_add_node(MGRAddNode *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_node(MGRAlterNode *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_node(MGRDropNode *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_init_all(PG_FUNCTION_ARGS);
extern Datum mgr_init_cn_master(PG_FUNCTION_ARGS);
extern void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *shutdown_mode);
extern Datum mgr_init_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_init_dn_slave(PG_FUNCTION_ARGS);
extern void mgr_init_dn_slave_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *masterhostaddress, uint32 masterport, char *mastername);
extern Datum mgr_init_dn_slave_all(PG_FUNCTION_ARGS);

extern Datum mgr_start_cn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_master_i(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_master_f(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master_f(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master_i(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave_f(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave_i(PG_FUNCTION_ARGS);
extern Datum mgr_runmode_cndn(char nodetype, char cmdtype, PG_FUNCTION_ARGS, char *shutdown_mode);

extern Datum mgr_monitor_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_coord_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_dnmaster_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_dnslave_all(PG_FUNCTION_ARGS);

extern Datum mgr_append_dnmaster(PG_FUNCTION_ARGS);
extern Datum mgr_append_dnslave(PG_FUNCTION_ARGS);
extern Datum mgr_append_dnextra(PG_FUNCTION_ARGS);
extern Datum mgr_append_coordmaster(PG_FUNCTION_ARGS);
extern Datum mgr_append_agtmslave(PG_FUNCTION_ARGS);
extern Datum mgr_append_agtmextra(PG_FUNCTION_ARGS);

/* extern void mgr_configure_nodes_all(void); */
Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS);

extern Datum mgr_monitor_coord_namelist(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_dnmaster_namelist(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_dnslave_namelist(PG_FUNCTION_ARGS);

extern void mgr_start_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple);
extern List * get_fcinfo_namelist(const char *sepstr, int argidx, FunctionCallInfo fcinfo
#ifdef ADB
       , void (*check_value_func_ptr)(char*)
#endif
       );
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
extern void mgr_add_parameters_hbaconf(HeapTuple aimtuple, char nodetype, StringInfo infosendhbamsg);
void mgr_add_oneline_info_pghbaconf(int type, char *database, char *user, char *addr, int addr_mark, char *auth_method, StringInfo infosendhbamsg);
extern Datum mgr_start_one_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_dn_master(PG_FUNCTION_ARGS);
extern char *mgr_get_slavename(Oid tupleOid, char nodetype);
extern void mgr_rename_recovery_to_conf(char cmdtype, Oid hostOid, char* cndnpath, GetAgentCmdRst *getAgentCmdRst);
extern HeapTuple mgr_get_tuple_node_from_name_type(Relation rel, char *nodename, char nodetype);
extern char *mgr_nodetype_str(char nodetype);
extern Datum mgr_clean_all(PG_FUNCTION_ARGS);
extern Datum mgr_stop_agent(PG_FUNCTION_ARGS);

/* mgr_common.c */
extern TupleDesc get_common_command_tuple_desc(void);
extern HeapTuple build_common_command_tuple(const Name name, bool success, const char *message);
extern int pingNode(char *host, char *port);
extern bool	mgr_check_host_in_use(Oid hostoid);
extern void mgr_mark_node_in_cluster(Relation rel);
/* get the host address */
char *get_hostaddress_from_hostoid(Oid hostOid);
char *get_hostname_from_hostoid(Oid hostOid);
char *get_hostuser_from_hostoid(Oid hostOid);

/* get msg from agent */
bool mgr_recv_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
bool mgr_recv_msg_for_monitor(ManagerAgent	*ma, bool *ret, StringInfo agentRstStr);
extern List *monitor_get_dbname_list(char *user, char *address, int port);
extern void monitor_get_one_node_user_address_port(Relation rel_node, char **user, char **address, int *coordport, char nodetype);

/* monitor_hostpage.c */
extern Datum monitor_get_hostinfo(PG_FUNCTION_ARGS);
bool get_cpu_info(StringInfo hostinfostring);
extern void insert_into_monitor_alarm(Monitor_Alarm *monitor_alarm);
extern void get_threshold(int16 type, Monitor_Threshold *monitor_threshold);

/*monitor_databaseitem.c*/
extern int monitor_get_onesqlvalue_one_node(char *sqlstr, char *user, char *address, int port, char * dbname);
extern int monitor_get_result_one_node(Relation rel_node, char *sqlstr, char *dbname, char nodetype);
extern int monitor_get_sqlres_all_typenode_usedbname(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int gettype);
extern Datum monitor_databaseitem_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple monitor_build_database_item_tuple(Relation rel, const TimestampTz time, char *dbname
			, int dbsize, bool archive, bool autovacuum, float heaphitrate,  float commitrate, int dbage, int connectnum
			, int standbydelay, int locksnum, int longquerynum, int idlequerynum, int preparenum, int unusedindexnum, int indexsize);
extern Datum monitor_databasetps_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple monitor_build_databasetps_qps_tuple(Relation rel, const TimestampTz time, const char *dbname, const int tps, const int qps, int pgdbruntime);	

/*monitor_slowlog.c*/
extern char *monitor_get_onestrvalue_one_node(char *sqlstr, char *user, char *address, int port, char * dbname);
extern void monitor_get_onedb_slowdata_insert(Relation rel, char *user, char *address, int port, char *dbname);
extern HeapTuple monitor_build_slowlog_tuple(Relation rel, TimestampTz time, char *dbname, char *username, float singletime, int totalnum, char *query, char *queryplan);
extern Datum monitor_slowlog_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple check_record_yestoday_today(Relation rel, int *callstoday, int *callsyestd, bool *gettoday, bool *getyesdt, char *query, char *user, char *dbname, pg_time_t ptimenow);
extern void monitor_insert_record(Relation rel, TimestampTz time, char *dbname, char *dbuser, float singletime, int calls, char *querystr, char *user, char *address, int port);

/*monitor_dbthreshold.c*/
extern Datum get_dbthreshold(PG_FUNCTION_ARGS);
extern char *monitor_get_timestamptz_onenode(char *user, char *address, int port);
extern bool monitor_get_sqlvalues_one_node(char *sqlstr, char *user, char *address, int port, char * dbname, int iarray[], int len);

/*mgr_updateparm*/
extern void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_add_parm(char *nodename, char nodetype, StringInfo infosendparamsg);
extern void mgr_reset_updateparm(MGRUpdateparmReset *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_parmr_delete_tuple_nodename_nodetype(Relation noderel, Name nodename, char nodetype);
#endif /* MGR_CMDS_H */
