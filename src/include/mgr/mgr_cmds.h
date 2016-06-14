
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

#define run_success "success"

typedef struct GetAgentCmdRst
{
	NameData nodename;
	int ret;
	StringInfoData description;
}GetAgentCmdRst;

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

/*gtm commands, in cmd_gtm.c*/
extern void mgr_add_gtm(MGRAddGtm *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_gtm(MGRDropGtm *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_gtm(MGRAlterGtm *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_start_gtm(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtm(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtm(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtm_all(PG_FUNCTION_ARGS);
extern Datum mgr_runmode_gtm(const char gtmtype, const char cmdtype, PG_FUNCTION_ARGS, const char *shutdown_mode);
extern void mgr_runmode_gtm_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation gtmrel, HeapTuple aimtuple, const char *shutdown_mode);

/*parm commands, in cmd_parm.c*/
extern void mgr_alter_parm(MGRAlterParm *node, ParamListInfo params, DestReceiver *dest);

/*coordinator datanode parse cmd*/
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

/* extern void mgr_configure_nodes_all(void); */
Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS);

extern Datum mgr_monitor_coord_namelist(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_dnmaster_namelist(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_dnslave_namelist(PG_FUNCTION_ARGS);

extern void mgr_start_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple);
extern List * start_cn_master_internal(const char *sepstr, int argidx, FunctionCallInfo fcinfo
#ifdef ADB
       , void (*check_value_func_ptr)(char*)
#endif
       );
extern Datum mgr_failover_one_dn(PG_FUNCTION_ARGS);
void check_dn_slave(List *nodenamelist, Relation rel_node, StringInfo infosendmsg);
extern bool mgr_refresh_pgxc_node_tbl(char *cndnname, int32 cndnport, char *cndnaddress, bool isprimary, Oid cndnmasternameoid, GetAgentCmdRst *getAgentCmdRst);
extern void mgr_send_conf_parameters(char filetype, char *datapath, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst);
extern void mgr_append_pgconf_paras_str_str(char *key, char *value, StringInfo infosendmsg);
extern void mgr_append_pgconf_paras_str_int(char *key, int value, StringInfo infosendmsg);
extern void mgr_get_gtm_host_port(StringInfo infosendmsg);
extern void mgr_append_infostr_infostr(StringInfo infostr, StringInfo sourceinfostr);
extern void mgr_add_parameters_pgsqlconf(Oid tupleOid, char nodetype, int cndnport, char *nodename, StringInfo infosendparamsg);
extern void mgr_append_pgconf_paras_str_quotastr(char *key, char *value, StringInfo infosendmsg);
extern void mgr_add_parameters_recoveryconf(char *slavename, Oid masteroid, StringInfo infosendparamsg);
extern void mgr_add_parameters_hbaconf(char nodetype, StringInfo infosendhbamsg);
void mgr_add_oneline_info_pghbaconf(int type, char *database, char *user, char *addr, int addr_mark, char *auth_method, StringInfo infosendhbamsg);
extern Datum mgr_start_one_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_dn_master(PG_FUNCTION_ARGS);
extern char *mgr_get_dnmaster_slavename(Oid tupleOid, char nodetype);
extern void mgr_rename_recovery_to_conf(char cmdtype, Oid hostOid, char* cndnpath, GetAgentCmdRst *getAgentCmdRst);
extern HeapTuple mgr_get_tuple_node_from_name_type(Relation rel, char *nodename, char nodetype);

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
bool mgr_recv_msg_for_monitor(ManagerAgent *ma, GetAgentCmdRst *getAgentCmdRst);
/* monitor_hostpage.c */
extern Datum monitor_get_hostinfo(PG_FUNCTION_ARGS);
bool get_cpu_info(StringInfo hostinfostring);

#endif /* MGR_CMDS_H */
