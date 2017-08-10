
/*
 * agent commands
 */

#include<unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <pwd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>

#include "agent.h"
#include "agt_msg.h"
#include "agt_utility.h"
#include "mgr/mgr_msg_type.h"
#include "conf_scan.h"
#include "hba_scan.h"
#include "utils/memutils.h"
#include "c.h"
#include "postgres_fe.h"
#include "../../interfaces/libpq/libpq-fe.h"

#define BUFFER_SIZE 4096

#define GTM_CTL_VERSION "pg_ctl (PostgreSQL) " PG_VERSION "\n"
#define INITGTM_VERSION "initagtm (PostgreSQL) " PG_VERSION "\n"
#define INITDB_VERSION "initdb (PostgreSQL) " PG_VERSION "\n"
#define PG_BASEBACKUP_VERSION "pg_basebackup (PostgreSQL) " PG_VERSION "\n"
#define PG_CTL_VERSION "pg_ctl (PostgreSQL) " PG_VERSION "\n"
#define PSQL_VERSION "psql (Postgres-XC) " PGXC_VERSION "\n"
#define PG_DUMPALL_VERSION "pg_dumpall (PostgreSQL) " PG_VERSION "\n"
#define PG_REWIND_VERSION "pg_rewind 1.0.0""\n"

static void myUsleep(long microsec);
static bool parse_ping_node_msg(const StringInfo msg, Name host, Name port, Name user, char *file_path);
static int exec_ping_node(const char *host, const char *port, const char *user, const char *file_path, StringInfo err_msg);
static void cmd_ping_node(StringInfo msg);
static long get_pgpid(const char *file_path);

static void cmd_node_init(char cmdtype, StringInfo msg, char *cmdfile, char* VERSION);
static void cmd_node_refresh_pgsql_paras(char cmdtype, StringInfo msg);
static void cmd_refresh_confinfo(char *key, char *value, ConfInfo *info, bool bforce);
static void writefile(char *path, ConfInfo *info);
static void writehbafile(char *path, HbaInfo *info);
static int copyFile(const char *targetFileWithPath, const char *sourceFileWithPath);
static void pg_ltoa(int32 value, char *a);
static bool cmd_rename_recovery(StringInfo msg);
static void cmd_monitor_gets_hostinfo(void);
extern bool get_cpu_info(StringInfo hostinfostring);
extern bool get_mem_info(StringInfo hostinfostring);
extern bool get_disk_info(StringInfo hostinfostring);
extern bool get_net_info(StringInfo hostinfostring);
extern bool get_host_info(StringInfo hostinfostring);
extern bool get_disk_iops_info(StringInfo hostinfostring);
extern bool get_system_info(StringInfo hostinfostring);
extern bool get_platform_type_info(StringInfo hostinfostring);

static void cmd_rm_temp_file(StringInfo msg);
static void cmd_check_dir_exist(StringInfo msg);
static void cmd_clean_node_folder(StringInfo buf);
static void cmd_stop_agent(void);
static void cmd_get_showparam_values(char cmdtype, StringInfo buf);
static char *mgr_get_showparam(char *sqlstr, char *user, char *address, int port, char * dbname);
static void cmd_get_sqlstring_stringvalues(char cmdtype, StringInfo buf);
static void mgr_execute_sqlstring(char cmdtype, char *user, int port, char *address, char *dbname, char *sqlstring, StringInfo output);

static void cmd_node_refresh_pghba_parse(AgentCommand cmd_type, StringInfo msg);
static HbaInfo *cmd_refresh_pghba_confinfo(AgentCommand cmd_type, HbaInfo *checkinfo, HbaInfo *infohead, StringInfo err_msg);
static void add_pghba_info_list(HbaInfo *infohead, HbaInfo *checkinfo);
static HbaInfo *delete_pghba_info_from_list(HbaInfo *infohead, HbaInfo *checkinfo);
static char *get_connect_type_str(HbaType connect_type);
static bool check_pghba_exist_info(HbaInfo *checkinfo, HbaInfo *infohead);
static char *pghba_info_parse(char *ptmp, HbaInfo *newinfo, StringInfo infoparastr);
static bool check_hba_vaild(char * datapath, HbaInfo * info_head);
static void cmd_get_batch_job_result(int cmd_type, StringInfo buf);

void do_agent_command(StringInfo buf)
{
	AgentCommand cmd_type;
	AssertArg(buf);
	cmd_type = agt_getmsgbyte(buf);
	switch(cmd_type)
	{
	case AGT_CMD_GTM_INIT:
		cmd_node_init(cmd_type, buf, "initagtm", INITGTM_VERSION);
		break;
	case AGT_CMD_CNDN_CNDN_INIT:
	 	cmd_node_init(cmd_type, buf, "initdb", INITDB_VERSION);
		break;
	case AGT_CMD_GTM_SLAVE_INIT:
	case AGT_CMD_CNDN_SLAVE_INIT:
		cmd_node_init(cmd_type, buf, "pg_basebackup", PG_BASEBACKUP_VERSION);
		break;
	case AGT_CMD_CN_START:
	case AGT_CMD_CN_STOP:
	case AGT_CMD_DN_START:
	case AGT_CMD_DN_STOP:
	case AGT_CMD_DN_FAILOVER:
	case AGT_CMD_DN_RESTART:
	case AGT_CMD_CN_RESTART:
	case AGT_CMD_NODE_RELOAD:
		cmd_node_init(cmd_type, buf, "pg_ctl", PG_CTL_VERSION);
		break;
	case AGT_CMD_PGDUMPALL:
		cmd_node_init(cmd_type, buf, "pg_dumpall", PG_DUMPALL_VERSION);
		break;
	case AGT_CMD_GTM_START_MASTER:
	case AGT_CMD_GTM_STOP_MASTER:
	case AGT_CMD_GTM_START_SLAVE:
	case AGT_CMD_GTM_STOP_SLAVE:
	case AGT_CMD_GTM_SLAVE_FAILOVER:
	case AGT_CMD_AGTM_RESTART:
		cmd_node_init(cmd_type, buf, "agtm_ctl", GTM_CTL_VERSION);
		break;
	case AGT_CMD_PSQL_CMD:
		cmd_node_init(cmd_type, buf, "psql", PSQL_VERSION);
		break;
	/*modify gtm|coordinator|datanode postgresql.conf*/
	case AGT_CMD_CNDN_REFRESH_PGSQLCONF:
		cmd_node_refresh_pgsql_paras(cmd_type, buf);
		break;
	/*modify gtm|coordinator|datanode recovery.conf*/
	case AGT_CMD_CNDN_REFRESH_RECOVERCONF:
		cmd_node_refresh_pgsql_paras(cmd_type, buf);
		break;
	/*modify gtm|coordinator|datanode pg_hba.conf*/
	case AGT_CMD_CNDN_REFRESH_PGHBACONF:
		cmd_node_refresh_pghba_parse(AGT_CMD_CNDN_ADD_PGHBACONF, buf);
		break;
	case AGT_CMD_CNDN_DELETE_PGHBACONF:
		cmd_node_refresh_pghba_parse(AGT_CMD_CNDN_DELETE_PGHBACONF, buf);
		break;
	case AGT_CMD_CNDN_ALTER_PGHBACONF:
		cmd_node_refresh_pghba_parse(AGT_CMD_CNDN_DELETE_PGHBACONF, buf);
		cmd_node_refresh_pghba_parse(AGT_CMD_CNDN_ADD_PGHBACONF, buf);
		break;
	/*modify gtm|coordinator|datanode postgresql.conf and reload it*/
	case AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD:
		cmd_node_refresh_pgsql_paras(cmd_type, buf);
		break;
	/*modify gtm|coordinator|datanode postgresql.conf, delete the given parameter*/
	case AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE:
		cmd_node_refresh_pgsql_paras(cmd_type, buf);
		break;
	case AGT_CMD_CNDN_RENAME_RECOVERCONF:
		cmd_rename_recovery(buf);
		break;
	case AGT_CMD_MONITOR_GETS_HOST_INFO:
		cmd_monitor_gets_hostinfo();
		break;
	case AGT_CMD_RM:
		cmd_rm_temp_file(buf);
		break;
	case AGT_CMD_CLEAN_NODE:
		cmd_clean_node_folder(buf);
		break;
	case AGT_CMD_STOP_AGENT:
		cmd_stop_agent();
		break;
	case AGT_CMD_SHOW_AGTM_PARAM:
		cmd_get_showparam_values(cmd_type, buf);
		break;
	case AGT_CMD_SHOW_CNDN_PARAM:
		cmd_get_showparam_values(cmd_type, buf);
		break;
	case AGT_CMD_GET_SQL_STRINGVALUES:
	case AGT_CMD_GET_EXPLAIN_STRINGVALUES:
		cmd_get_sqlstring_stringvalues(cmd_type, buf);
		break;
	case AGT_CMD_GET_BATCH_JOB:
		cmd_get_batch_job_result(cmd_type, buf);
		break;
	case AGT_CMD_CHECK_DIR_EXIST:
		cmd_check_dir_exist(buf);
		break;
	case AGT_CMD_PING_NODE:
		cmd_ping_node(buf);
		break;
	case AGT_CMD_NODE_REWIND:
		cmd_node_init(cmd_type, buf, "pg_rewind", PG_REWIND_VERSION);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			,errmsg("unknown agent command %d", cmd_type)));
	}
}

/*check the node is running*/
static void cmd_ping_node(StringInfo msg)
{
	bool is_success = false;
	PGPing ping_status;
	NameData host;
	NameData port;
	NameData user;
	char file_path[MAXPGPATH] = {0};
	StringInfoData err_msg;
	initStringInfo(&err_msg);

	is_success = parse_ping_node_msg(msg, &host, &port, &user, file_path);
	if (is_success != true)
	{
		ereport(ERROR, (errmsg("funciton:cmd_ping_node, error to get values of host, port and user")));
	}
	/*
	the database of hba in slave datanode is "replication" so the client cann't connect it,
	so we use psql -p port -U username to connect, 
	because the agent and the node has the same host,
	so omit the host ip, the agent will use localhost as host ip
	*/
	ping_status = exec_ping_node(NULL, NameStr(port), NameStr(user), file_path, &err_msg);
	
	/*send msg to client */
	appendStringInfoCharMacro(&err_msg, ping_status);
	agt_put_msg(AGT_MSG_RESULT, err_msg.data, err_msg.len);
	agt_flush();
	pfree(err_msg.data);
}
static bool parse_ping_node_msg(const StringInfo msg, Name host, Name port, Name user, char *file_path)
{
	int index = msg->cursor;
	Assert(host && port && user && file_path);
	
	if (index < msg->len)
		snprintf(NameStr(*host), NAMEDATALEN, "%s", &(msg->data[index]));
	else
		return false;
	index = index + strlen(&(msg->data[index])) + 1;
	if (index < msg->len)
		snprintf(NameStr(*port), NAMEDATALEN, "%s", &(msg->data[index]));
	else
		return false;
	index = index + strlen(&(msg->data[index])) + 1;
	if (index < msg->len)
		snprintf(NameStr(*user), NAMEDATALEN, "%s", &(msg->data[index]));
	else
		return false;
	index = index + strlen(&(msg->data[index])) + 1;
	if (index < msg->len)
		snprintf(file_path, MAXPGPATH, "%s", &(msg->data[index]));
	else
		return false;
	return true;
}

static int exec_ping_node(const char *host, const char *port, const char *user, const char *file_path, StringInfo err_msg)
{
	char conninfo[NAMEDATALEN + 1] = {0};
	char editBuf[NAMEDATALEN + 1] = {0};
	int retry;
	int RETRY = 3;
	int ret = -1;
	if (host)
	{
		snprintf(editBuf, NAMEDATALEN, "host='%s' ", host);
		strncat(conninfo, editBuf, NAMEDATALEN);
	}

	if (port)
	{
		snprintf(editBuf, NAMEDATALEN, "port=%d ", atoi(port));
		strncat(conninfo, editBuf, NAMEDATALEN);
	}

	if (user)
	{
		snprintf(editBuf, NAMEDATALEN, "user=%s ", user);
		strncat(conninfo, editBuf, NAMEDATALEN);
	}
	if (get_pgpid(file_path) == 0)
		return PQPING_NO_RESPONSE;

	/*timeout set 10s, when the cluster at high press, it should enlarge the value*/
	snprintf(editBuf, NAMEDATALEN,"connect_timeout=10");
	strncat(conninfo, editBuf, NAMEDATALEN);

	if (conninfo[0])
	{
		elog(DEBUG1, "Ping node string: %s.\n", conninfo);
		for (retry = RETRY; retry; retry--)
		{
			ret = PQping(conninfo);
			switch (ret)
			{
				case PQPING_OK:
				case PQPING_REJECT:
				case PQPING_NO_ATTEMPT:
				case PQPING_NO_RESPONSE:
					return ret;
				default:
					myUsleep(100000); /*sleep 100ms*/
					continue;
			}
		}
	}
	return -1;
}
static long get_pgpid(const char *pid_file)
{
	FILE	   *pidf;
	long		pid;

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		return 0;
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		return 0;
	}
	fclose(pidf);
	return pid;
}
static void myUsleep(long microsec)
{
    struct timeval delay;

    if (microsec <= 0)
        return;

    delay.tv_sec = microsec / 1000000L;
    delay.tv_usec = microsec % 1000000L;
    (void) select(0, NULL, NULL, NULL, &delay);
}

static void cmd_check_dir_exist(StringInfo msg)
{
	const char *dir_path = NULL;
	StringInfoData output;
	struct stat stat_buf;
	DIR *chkdir;
	struct dirent *file;
	
	initStringInfo(&output);
	dir_path = agt_getmsgstring(msg);

	Assert(dir_path != NULL);

	if (stat(dir_path, &stat_buf) != 0)
	{
		/* data directory does not exist */
		if (errno == ENOENT)
		{
			appendStringInfoString(&output, "success");
			agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
			agt_flush();
			pfree(output.data);
			return ;
		}
	}
	/* data directory exist */
	if (S_ISDIR(stat_buf.st_mode))
	{
		if ((chmod(dir_path, 0700))!= 0)
			ereport(ERROR,
				(errmsg("append master node: chmod \"%s\" 0700 fail, %s", dir_path, strerror(errno))));
	}
	/* data directory exists and not empty*/
	chkdir = opendir(dir_path);
	if (chkdir == NULL)
	{
		ereport(ERROR, 
			(errmsg("append master node: open directory \"%s\" fail, %s", dir_path, strerror(errno))));
	}
	else
	{
		while ((file = readdir(chkdir)) != NULL)
		{
			if (strcmp(".", file->d_name) == 0 || strcmp("..", file->d_name) == 0)
			{
				/* skip this and parent directory */
				continue;
			}
			ereport(ERROR, 
				(errmsg("append master node: directory \"%s\" is not empty", dir_path)));
		}
	}

	appendStringInfoString(&output, "success");
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
	return ;
}

static void cmd_rm_temp_file(StringInfo msg)
{
	const char *rec_msg_string = NULL;
	StringInfoData output;
	int res;

	initStringInfo(&output);
	rec_msg_string = agt_getmsgstring(msg);

	res = system(rec_msg_string);
	if (res != 0)
	{
		appendStringInfo(&output, "do command fail: %s", rec_msg_string);
		ereport(LOG, (errmsg("do command fail: %s", rec_msg_string)));
	}
	else
		appendStringInfoString(&output, "success");

	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
}

static void cmd_node_init(char cmdtype, StringInfo msg, char *cmdfile, char* VERSION)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData exec;
	char path[MAXPGPATH];
	char recoveryfile[MAXPGPATH];
	char *ppath = NULL;
	int iloop = 0;
	const int maxchecknum = 120;

	initStringInfo(&exec);
	enlargeStringInfo(&exec, MAXPGPATH);
	if(find_other_exec(agent_argv0, cmdfile, VERSION, exec.data) != 0)
	ereport(ERROR, (errmsg("could not locate matching %s executable", cmdfile)));
	exec.len = strlen(exec.data);
	rec_msg_string = agt_getmsgstring(msg);
	appendStringInfo(&exec, " %s", rec_msg_string);
	initStringInfo(&output);
	if(exec_shell(exec.data, &output) != 0)
		ereport(ERROR, (errmsg("%s", output.data)));
	/*for datanode failover*/
	if (AGT_CMD_DN_FAILOVER == cmdtype || AGT_CMD_GTM_SLAVE_FAILOVER == cmdtype)
	{
		/*get path from msg: .... -D path ...*/
		ppath =  strstr(msg->data, " -D ");
		Assert(ppath != NULL);
		ppath = ppath + strlen(" -D");
		while(*ppath == ' ')
		{
			ppath++;
		}
		iloop = 0;
		while (*ppath != ' ' && *ppath != '\0')
		{
			path[iloop++] = *ppath++;
		}
		path[iloop++] = 0;
		/*check the path exist*/
		if (access(path, F_OK) != 0)
		{
			ereport(ERROR, (errmsg("%s does not exist", path)));
		}
		/*check path/recovery.done, if recovry.conf not update to recovery.done, sleep 3 then check it again
		* , just do this, because it should wait the pg_ctl promote execute finish, otherwise maybe get the funciton
		* which update recovery.conf to recovery.done not execute, for example: pg_ctl promote then shutdown the node
		*/
		memset(recoveryfile, 0, MAXPGPATH*sizeof(char));
		strcpy(recoveryfile, path);
		strcat(recoveryfile, "/recovery.done");
		sleep(1);
		iloop = 0;
		if (access(recoveryfile, F_OK) != 0)
		{
			/*the max check time is maxchecknum * 2 */
			while(access(recoveryfile, F_OK) != 0 && iloop++ < maxchecknum)
			{
				sleep(2);
			}
			/*check recovery.done exist*/
			if (access(recoveryfile, F_OK) != 0)
			{
				ereport(ERROR, (errmsg("could not update recovery.conf to recovery.done in %s", path)));
			}
		}

	}
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(exec.data);
	pfree(output.data);

}

/*parse the msg form manager,and write the hba msg to pg_hba.conf */
static void cmd_node_refresh_pghba_parse(AgentCommand cmd_type, StringInfo msg)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData err_msg;
	StringInfoData infoparastr;
	StringInfoData pgconffile;
	char datapath[MAXPGPATH];
	char *ptmp;
	HbaInfo *newinfo, *infohead;
	MemoryContext pgconf_context;
	MemoryContext oldcontext;
	Assert(AGT_CMD_CNDN_DELETE_PGHBACONF == cmd_type || AGT_CMD_CNDN_ADD_PGHBACONF == cmd_type);

	pgconf_context = AllocSetContextCreate(CurrentMemoryContext,
										"pghbaconf",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(pgconf_context);

	rec_msg_string = agt_getmsgstring(msg);
	initStringInfo(&infoparastr);
	initStringInfo(&pgconffile);
	initStringInfo(&output);
	initStringInfo(&err_msg);
	appendBinaryStringInfo(&infoparastr, &msg->data[msg->cursor], msg->len - msg->cursor);

	/*get datapath*/
	strcpy(datapath, rec_msg_string);
	/*check file exists*/
	appendStringInfo(&pgconffile, "%s/pg_hba.conf", datapath);
	if(access(pgconffile.data, F_OK) !=0 )
	{
		ereport(ERROR, (errmsg("could not find: %s", pgconffile.data)));
	}
	/*get the pg_hba.conf content*/
	infohead = parse_hba_file(pgconffile.data);
	newinfo = (HbaInfo *)palloc(sizeof(HbaInfo));
	while((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len))
	{
		ptmp = pghba_info_parse(ptmp, newinfo, &infoparastr);
		infohead = cmd_refresh_pghba_confinfo(cmd_type, newinfo, infohead, &err_msg);
	}
	if(check_hba_vaild(datapath, infohead) == true)
	{
		/*use the new info list to refresh the pg_hba.conf*/
		writehbafile(pgconffile.data, infohead);
	}
	else
	{
		appendStringInfoString(&err_msg, "add hba info failed in the agent");
	}
	/*send the result to the manager*/
	if(err_msg.len == 0)
	{

		appendStringInfoString(&output, "success");
		agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	}
	else
	{
/*		appendStringInfoCharMacro(&output, AGT_MSG_RESULT);
		appendStringInfoString(&output, err_msg.data);
		agt_put_msg(AGT_MSG_ERROR, output.data, output.len);
*/
		ereport(ERROR, (errmsg("%s", err_msg.data)));
	}

	agt_flush();
	pfree(output.data);
	pfree(err_msg.data);
	pfree(pgconffile.data);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(pgconf_context);
}

static HbaInfo *cmd_refresh_pghba_confinfo(AgentCommand cmd_type, HbaInfo *checkinfo, HbaInfo *infohead, StringInfo err_msg)
{
	bool is_exist;
	char *strtype;
	Assert(AGT_CMD_CNDN_DELETE_PGHBACONF == cmd_type || AGT_CMD_CNDN_ADD_PGHBACONF == cmd_type);
	Assert(checkinfo);
	Assert(infohead);

	is_exist = check_pghba_exist_info(checkinfo, infohead);
	strtype = get_connect_type_str(checkinfo->type);
	if(AGT_CMD_CNDN_ADD_PGHBACONF == cmd_type)
	{
		if(false == is_exist)
		{
			add_pghba_info_list(infohead, checkinfo);
		}
/*		else
		{
			appendStringInfo(err_msg,"\"%s %s %s %s %s\"has exist in the pg_hba.conf.\n",strtype
																						,checkinfo->database
																						,checkinfo->user
																						,checkinfo->addr
																						,checkinfo->auth_method);
		}*/
	}
	else if(AGT_CMD_CNDN_DELETE_PGHBACONF == cmd_type)
	{
		if(true == is_exist)
		{
			infohead = delete_pghba_info_from_list(infohead, checkinfo);
		}
/*		else
		{
			appendStringInfo(err_msg,"\"%s %s %s %s %s\"does not exist in the pg_hba.conf.\n",strtype
																						,checkinfo->database
																						,checkinfo->user
																						,checkinfo->addr
																						,checkinfo->auth_method);
		}	*/
	}

	pfree(strtype);
	return infohead;
}
/*append the to the info list*/
static void add_pghba_info_list(HbaInfo *infohead, HbaInfo *checkinfo)
{
	HbaInfo *infotail = infohead;
	char *strtype;
	char *database;
	char *user;
	char *addr;
	char mark[4];
	char *auth_method;
	char *line;
	int intervallen = 4;
	HbaInfo *newinfo;
	Assert(infotail);
	Assert(checkinfo);

	newinfo = (HbaInfo *)palloc0(sizeof(HbaInfo)+1);
	newinfo->type = checkinfo->type;
	/*database*/
	database = (char *)palloc(strlen(checkinfo->database)+1);
	memset(database, 0, strlen(checkinfo->database)+1);
	strncpy(database, checkinfo->database, strlen(checkinfo->database));
	/*user*/
	user = (char *)palloc(strlen(checkinfo->user)+1);
	memset(user, 0, strlen(checkinfo->user)+1);
	strncpy(user, checkinfo->user, strlen(checkinfo->user));
	/*addr*/
	addr = (char *)palloc(strlen(checkinfo->addr)+1);
	memset(addr, 0, strlen(checkinfo->addr)+1);
	strncpy(addr, checkinfo->addr, strlen(checkinfo->addr));
	/*auth_method*/
	auth_method = (char *)palloc(strlen(checkinfo->auth_method)+1);
	memset(auth_method, 0, strlen(checkinfo->auth_method)+1);
	strncpy(auth_method, checkinfo->auth_method, strlen(checkinfo->auth_method));
	newinfo->addr_mark = checkinfo->addr_mark;
	newinfo->addr_is_ipv6 = false;
	newinfo->type_loc = 0;

	strtype = get_connect_type_str(checkinfo->type);
	newinfo->type_len = strlen(strtype);
	newinfo->database = database;
	newinfo->user = user;
	newinfo->addr = addr;
	newinfo->auth_method = auth_method;
	newinfo->options = NULL;
	newinfo->db_loc = newinfo->type_len + intervallen;
	newinfo->db_len = strlen(database);
	newinfo->user_loc = newinfo->db_loc + newinfo->db_len + intervallen;
	newinfo->user_len = strlen(user);
	newinfo->addr_loc = newinfo->user_loc + newinfo->user_len + intervallen;
	newinfo->addr_len = strlen(addr);
	newinfo->mark_loc = newinfo->addr_loc + newinfo->addr_len + 1;
	pg_ltoa(newinfo->addr_mark, mark);
	newinfo->mark_len = strlen(mark);
	newinfo->method_loc = newinfo->mark_loc + newinfo->mark_len + intervallen;
	newinfo->method_len = strlen(auth_method);
	newinfo->opt_loc = newinfo->method_loc + newinfo->method_len + intervallen;
	newinfo->opt_len = 0;

	line = (char *)palloc(newinfo->method_loc+newinfo->method_len+2);
	memcpy(line, strtype, newinfo->type_len);
	memset(line + newinfo->type_len, ' ', intervallen);
	memcpy(line+newinfo->db_loc, database, newinfo->db_len);
	memset(line + newinfo->db_loc + newinfo->db_len, ' ', intervallen);
	memcpy(line+newinfo->user_loc, user, newinfo->user_len);
	memset(line + newinfo->user_loc + newinfo->user_len, ' ', intervallen);
	memcpy(line+newinfo->addr_loc, addr, newinfo->addr_len);
	line[newinfo->addr_loc+newinfo->addr_len] = '/';
	memcpy(&(line[newinfo->mark_loc]), &mark, strlen(mark));
	memset(line+newinfo->mark_loc+newinfo->mark_len, ' ', intervallen);
	memcpy(line+newinfo->method_loc, auth_method, newinfo->method_len);
	line[newinfo->method_loc+newinfo->method_len] = '\n';
	line[newinfo->method_loc+newinfo->method_len+1] = '\0';
	newinfo->line = line;
	newinfo->next = NULL;
	while(infotail->next)
	{
		infotail = infotail->next;
	}
	infotail->next = newinfo;
}
static char *get_connect_type_str(HbaType connect_type)
{
	char *strtype;
	strtype = palloc0(10);
	switch(connect_type)
	{
		case HBA_TYPE_LOCAL: //local
		    memcpy(strtype, "local", 5);
			break;
		case HBA_TYPE_HOST: //host
			memcpy(strtype, "host", 4);
			break;
		case HBA_TYPE_HOSTSSL: //hostssl
			memcpy(strtype, "hostssl", 7);
			break;
		case HBA_TYPE_HOSTNOSSL: //hostnossl
			memcpy(strtype, "hostnossl", 9);
			break;
		default:
			break;
	}
	return strtype;
}
static HbaInfo *delete_pghba_info_from_list(HbaInfo *infohead, HbaInfo *checkinfo)
{
	HbaInfo *info_cur = infohead;
	HbaInfo *info_pre = infohead;
	Assert(infohead);
	Assert(checkinfo);
	/*seek the specified elem*/
	while(info_cur)
	{
		if(checkinfo->type == info_cur->type
				&& strcmp(checkinfo->database, info_cur->database) == 0
				&& strcmp(checkinfo->user, info_cur->user) == 0
				&& strcmp(checkinfo->addr, info_cur->addr) == 0
				&& strcmp(checkinfo->auth_method, info_cur->auth_method) == 0)
		{
			break;
		}
		info_pre = info_cur;
		info_cur = info_cur->next;
	}
	/*delete from the list*/

	if(info_pre == infohead)
		infohead = NULL;
	else
		info_pre->next = info_cur->next;
	/*release HbaInfo *info_cur memory*/

	return infohead;
}

static bool check_pghba_exist_info(HbaInfo *checkinfo, HbaInfo *infohead)
{
	HbaInfo *info = infohead;
	Assert(checkinfo);
	Assert(infohead);
	while(info)
	{
		if(checkinfo->type == info->type
				&& strcmp(checkinfo->database, info->database) == 0
				&& strcmp(checkinfo->user, info->user) == 0
				&& strcmp(checkinfo->addr, info->addr) == 0
				&& (checkinfo->addr_mark == info->addr_mark)
				&& strcmp(checkinfo->auth_method, info->auth_method) == 0)
		{
			return true;
		}
		info = info->next;
	}
	return false;
}
static char *pghba_info_parse(char *ptmp, HbaInfo *newinfo, StringInfo infoparastr)
{
	/*type*/
	newinfo->type = infoparastr->data[infoparastr->cursor];
	infoparastr->cursor = infoparastr->cursor + sizeof(char) + 1;
	/*database*/
	Assert((ptmp = &infoparastr->data[infoparastr->cursor]) != '\0' && (infoparastr->cursor < infoparastr->len));
	newinfo->database = &(infoparastr->data[infoparastr->cursor]);
	infoparastr->cursor = infoparastr->cursor + strlen(newinfo->database) + 1;
	/*user*/
	Assert((ptmp = &infoparastr->data[infoparastr->cursor]) != '\0' && (infoparastr->cursor < infoparastr->len));
	newinfo->user = &(infoparastr->data[infoparastr->cursor]);
	infoparastr->cursor = infoparastr->cursor + strlen(newinfo->user) + 1;
	/*ip*/
	Assert((ptmp = &infoparastr->data[infoparastr->cursor]) != '\0' && (infoparastr->cursor < infoparastr->len));
	newinfo->addr = &(infoparastr->data[infoparastr->cursor]);
	infoparastr->cursor = infoparastr->cursor + strlen(newinfo->addr) + 1;
	/*mark*/
	Assert((ptmp = &infoparastr->data[infoparastr->cursor]) != '\0' && (infoparastr->cursor < infoparastr->len));
	newinfo->addr_mark = atoi(&(infoparastr->data[infoparastr->cursor]));
	infoparastr->cursor = infoparastr->cursor + strlen(&(infoparastr->data[infoparastr->cursor])) + 1;
	/*method*/
	Assert((ptmp = &infoparastr->data[infoparastr->cursor]) != '\0' && (infoparastr->cursor < infoparastr->len));
	newinfo->auth_method = &(infoparastr->data[infoparastr->cursor]);
	infoparastr->cursor = infoparastr->cursor + strlen(newinfo->auth_method) + 1;
	return ptmp;
}

/*
* refresh postgresql.conf, the need infomation in msg.
* msg include : datapath key value key value .., use '\0' to interval
*/
static void cmd_node_refresh_pgsql_paras(char cmdtype, StringInfo msg)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData infoparastr;
	StringInfoData pgconffile;
	StringInfoData pgconffilebak;
	char datapath[MAXPGPATH];
	char my_exec_path[MAXPGPATH];
	char pghome[MAXPGPATH];
	char *key;
	char *value;
	char *ptmp;
	char *strconf = "# PostgreSQL recovery config file";
	char buf[1024];
	bool bforce = false;
	ConfInfo *info,
			*infohead;
	FILE *create_recovery_file;
	int err;

	MemoryContext pgconf_context;
	MemoryContext oldcontext;

	pgconf_context = AllocSetContextCreate(CurrentMemoryContext,
										"pgconf",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(pgconf_context);

	rec_msg_string = agt_getmsgstring(msg);
	initStringInfo(&infoparastr);
	initStringInfo(&pgconffile);
	initStringInfo(&pgconffilebak);

	appendBinaryStringInfo(&infoparastr, &msg->data[msg->cursor], msg->len - msg->cursor);

	/*get datapath*/
	strcpy(datapath, rec_msg_string);
	/*check file exists*/
	if (AGT_CMD_CNDN_REFRESH_PGSQLCONF == cmdtype || AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD == cmdtype || AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE == cmdtype)
	{
		if (AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE == cmdtype)
			bforce = true;
		appendStringInfo(&pgconffile, "%s/postgresql.conf", datapath);
		appendStringInfo(&pgconffilebak, "%s/postgresql.conf.bak", datapath);
		if(access(pgconffile.data, F_OK) !=0 )
		{
			ereport(ERROR, (errmsg("could not find: %s", pgconffile.data)));
		}
		/*copy postgresql.conf to postgresql.conf.bak*/
		err = copyFile(pgconffilebak.data, pgconffile.data);
		if (err)
		{
			unlink(pgconffilebak.data);
			ereport(ERROR, (errmsg("could not copy %s to %s : %s", pgconffile.data, pgconffilebak.data, strerror(err))));
		}
	}
	else if (AGT_CMD_CNDN_REFRESH_RECOVERCONF == cmdtype)
	{
		appendStringInfo(&pgconffile, "%s/recovery.conf", datapath);
		/*check recovery.conf exist in */
		if(access(pgconffile.data, F_OK) !=0 )
		{
			/*cp recovery.conf from $PGHOME/share/postgresql/recovery.conf.sample to pgconffile*/
			memset(pghome, 0, MAXPGPATH);
			/* Locate the postgres executable itself */
			if (find_my_exec(agent_argv0, my_exec_path) < 0)
				elog(FATAL, "%s: could not locate my own executable path", agent_argv0);
			get_parent_directory(my_exec_path);
			get_parent_directory(my_exec_path);
			strcpy(pghome, my_exec_path);
			strcat(pghome, "/share/postgresql/recovery.conf.sample");
			/*use diffrent build method, the sample file maybe not in the same folder,so check the other folder*/
			if(access(pghome, F_OK) !=0 )
			{
				memset(pghome, 0, MAXPGPATH);
				strcpy(pghome, my_exec_path);
				strcat(pghome, "/share/recovery.conf.sample");
			}
			if(copyFile(pgconffile.data, pghome))
			{
				/*if cannot find the sample file, so make recvery.conf*/
				if ((create_recovery_file = fopen(pgconffile.data, "w")) == NULL)
				{
					fprintf(stderr, (": could not open file \"%s\" for writing: %s\n"),
						pgconffile.data, strerror(errno));
					exit(1);
				}
				memset(buf,0,1024);
				strcpy(buf, strconf);
				buf[strlen(strconf)] = '\n';
				fwrite(buf, 1, strlen(strconf)+1, create_recovery_file);
				if (fclose(create_recovery_file))
				{
					fprintf(stderr, (": could not write file \"%s\": %s\n"),
						pgconffile.data, strerror(errno));
					exit(1);
				}
			}
		}
		appendStringInfo(&pgconffilebak, "%s/recovery.conf.bak", datapath);
		/*copy recovery.conf to recovery.conf.bak*/
		err = copyFile(pgconffilebak.data, pgconffile.data);
		if (err)
		{
			unlink(pgconffilebak.data);
			ereport(ERROR, (errmsg("could not copy %s to %s : %s", pgconffile.data, pgconffilebak.data, strerror(err))));
		}

	}
	/*get the postgresql.conf content*/
	info = parse_conf_file(pgconffilebak.data);
	infohead = info;

	while((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len))
	{
		key = &(infoparastr.data[infoparastr.cursor]);
		/*refresh the infoparastr.cursor*/
		infoparastr.cursor = infoparastr.cursor + strlen(key) + 1;
		Assert((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len));
		value = &(infoparastr.data[infoparastr.cursor]);
		/*refresh the infoparastr.cursor*/
		infoparastr.cursor = infoparastr.cursor + strlen(value) + 1;
		cmd_refresh_confinfo(key, value, info, bforce);
	}

	/*use the new info list to refresh the postgresql.conf*/
	writefile(pgconffilebak.data, infohead);
	/*rename xx.conf.bak to xx.conf*/
	if (rename(pgconffilebak.data, pgconffile.data) == -1)
	{
		err = errno;
		unlink(pgconffilebak.data);
		ereport(ERROR, (errmsg("could not rename %s to %s : %s", pgconffilebak.data, pgconffile.data, strerror(err))));
	}
	pfree(pgconffile.data);
	pfree(pgconffilebak.data);
	pfree(infoparastr.data);

	initStringInfo(&output);
	if(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD == cmdtype)
	{
		/*pg_ctl reload*/
		StringInfoData exec;
		initStringInfo(&exec);
		enlargeStringInfo(&exec, MAXPGPATH);
		if(find_other_exec(agent_argv0, "pg_ctl", PG_CTL_VERSION, exec.data) != 0)
		ereport(ERROR, (errmsg("could not locate matching pg_ctl executable")));
		exec.len = strlen(exec.data);
		appendStringInfo(&exec, " reload -D %s", rec_msg_string);
		if(exec_shell(exec.data, &output) != 0)
		{
			ereport(LOG, (errmsg("%s", output.data)));
		}
		pfree(exec.data);
	}
	resetStringInfo(&output);
	appendStringInfoString(&output, "success");
	appendStringInfoCharMacro(&output, '\0');
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(pgconf_context);
}

/*
* the info is struct list for the content of postgresql , use key value to refresh info
*   list, if key not in info list, add the newlistnode to info
*/
static void cmd_refresh_confinfo(char *key, char *value, ConfInfo *info, bool bforce)
{
	bool getkey = false;
	int diffvalue;
	char *newname;
	char *newvalue;
	ConfInfo *infotmp = NULL;
	ConfInfo *infopre = info;

	/*use (key, value) to refresh info list*/
	while(info)
	{
		if(info->name != '\0' && strcmp(key, info->name) == 0 && !bforce)
		{
			getkey = true;
			diffvalue = strlen(value) - info->value_len;
			/*refresh value in info->line*/
			if(0 == diffvalue)
			{
				strncpy(info->line + info->value_loc,value,strlen(value));
			}
			else if(diffvalue < 0)
			{
				strncpy(info->line + info->value_loc, value,strlen(value));
				/*use empty space to take up the diffvalue*/
				memset(info->line + info->value_loc + strlen(value), ' ', info->value_len - strlen(value));
			}
			else
			{
				char *pline = palloc(strlen(info->line) + diffvalue + 1);
				memset(pline, 0, strlen(info->line) + diffvalue + 1);
				strncpy(pline, info->line, info->value_loc);
				/*cp value*/
				strncpy(pline + info->value_loc, value, strlen(value));
				/*cp content after old value*/
				strncpy(pline + info->value_loc + strlen(value), info->line + info->value_loc + info->value_len, strlen(info->line) - info->value_loc - info->value_len);
				info->line = pline;
				/*refresh the struct info*/
				info->value_len = strlen(value);
			}
			//break;
		}
		/*delete the parameter*/
		else if (bforce && info->name != '\0' && strcmp(key, info->name) == 0)
		{
			getkey = true;
			infopre->next = info->next;
			infotmp = info;
			info = info->next;
			pfree(infotmp);
			continue;
		}

		infopre = info;
		info = info->next;
	}

	/*append the (key,value) to the info list*/
	if (!getkey && !bforce)
	{
		ConfInfo *newinfo = (ConfInfo *)palloc(sizeof(ConfInfo)+1);
		newname = (char *)palloc(strlen(key)+1);
		memset(newname, 0, strlen(key)+1);
		newvalue = (char *)palloc(strlen(value)+1);
		memset(newvalue, 0, strlen(value)+1);
		strncpy(newname, key, strlen(key));
		strncpy(newvalue, value, strlen(value));
		newinfo->filename = NULL;
		newinfo->line = (char *)palloc(strlen(key) + strlen(value) + strlen(" = ") + 2);
		newinfo->name = newname;
		newinfo->value = newvalue;
		newinfo->name_loc = 0;
		newinfo->name_len = strlen(key);
		newinfo->value_loc = newinfo->name_len + strlen(" = ");
		newinfo->value_len = strlen(value);
		strncpy(newinfo->line, key, strlen(key));
		strncpy(newinfo->line+strlen(key), " = ", strlen(" = "));
		strncpy(newinfo->line+strlen(key) + strlen(" = "), value, strlen(value));
		newinfo->line[strlen(key) + strlen(value) + strlen(" = ")] = '\n';
		newinfo->line[strlen(key) + strlen(value) + strlen(" = ")+1] = '\0';
		newinfo->next = NULL;
		infopre->next = newinfo;
	}

}

/*
* use the struct list info to rewrite the file of path
*/
static void
writefile(char *path, ConfInfo *info)
{
	FILE *out_file;

	if ((out_file = fopen(path, "w")) == NULL)
	{
		fprintf(stderr, (": could not open file \"%s\" for writing: %s\n"),
			path, strerror(errno));
		exit(1);
	}
	while(info)
	{
		if (fputs(info->line, out_file) < 0)
		{
			fprintf(stderr, (": could not write file \"%s\": %s\n"),
				path, strerror(errno));
			exit(1);
		}
		info = info->next;
	}
	if (fclose(out_file))
	{
		fprintf(stderr, (": could not write file \"%s\": %s\n"),
			path, strerror(errno));
		exit(1);
	}
}
static bool check_hba_vaild(char * datapath, HbaInfo * info_head)
{
	FILE *fp;
	char hba_temp_file[] = "hba_temp.file";
	bool is_valid = true;
	char file_path[MAXPGPATH];
	sprintf(file_path, "%s/%s",datapath, hba_temp_file);

	if((fp = fopen(file_path, "w+")) == NULL)
	{
		is_valid = false;
		fprintf(stderr, (": could not open file \"%s\" for writing: %s\n"),
			file_path, strerror(errno));
		return is_valid;
	}
	writehbafile(file_path, info_head);
	PG_TRY();
	{
		parse_hba_file(file_path);
	}PG_CATCH();
	{
		is_valid = false;
		PG_RE_THROW();
	}PG_END_TRY();

	fclose(fp);
	remove(file_path);
	return is_valid;
}
/*
* use the struct list info to rewrite the file of path
*/
static void
writehbafile(char *path, HbaInfo *info)
{
	FILE *out_file;

	if ((out_file = fopen(path, "w")) == NULL)
	{
		fprintf(stderr, (": could not open file \"%s\" for writing: %s\n"),
			path, strerror(errno));
		exit(1);
	}
	while(info)
	{
		if (fputs(info->line, out_file) < 0)
		{
			fprintf(stderr, (": could not write file \"%s\": %s\n"),
				path, strerror(errno));
			exit(1);
		}
		info = info->next;
	}
	if (fclose(out_file))
	{
		fprintf(stderr, (": could not write file \"%s\": %s\n"),
			path, strerror(errno));
		exit(1);
	}
}

static int copyFile(const char *targetFileWithPath, const char *sourceFileWithPath)
{
	FILE *fpR, *fpW;
	char buffer[BUFFER_SIZE];
	int lenR, lenW;
	
	errno = 0;
	if ((fpR = fopen(sourceFileWithPath, "r")) == NULL)
	{
		return errno;
	}
	if ((fpW = fopen(targetFileWithPath, "w")) == NULL)
	{
		fclose(fpR);
		return errno;
	}

	memset(buffer, 0, BUFFER_SIZE);
	while ((lenR = fread(buffer, 1, BUFFER_SIZE, fpR)) > 0)
	{
		if ((lenW = fwrite(buffer, 1, lenR, fpW)) != lenR)
		{
			fclose(fpR);
			fclose(fpW);
			return errno;
		}
		memset(buffer, 0, BUFFER_SIZE);
	}

	fclose(fpR);
	fclose(fpW);
	return errno;
}

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
static void
pg_ltoa(int32 value, char *a)
{
	char	   *start = a;
	bool		neg = false;

	/*
	 * Avoid problems with the most negative integer not being representable
	 * as a positive integer.
	 */
	if (value == (-2147483647 - 1))
	{
		memcpy(a, "-2147483648", 12);
		return;
	}
	else if (value < 0)
	{
		value = -value;
		neg = true;
	}

	/* Compute the result string backwards. */
	do
	{
		int32		remainder;
		int32		oldval = value;

		value /= 10;
		remainder = oldval - value * 10;
		*a++ = '0' + remainder;
	} while (value != 0);

	if (neg)
		*a++ = '-';

	/* Add trailing NUL byte, and back up 'a' to the last character. */
	*a-- = '\0';

	/* Reverse string. */
	while (start < a)
	{
		char		swap = *start;

		*start++ = *a;
		*a-- = swap;
	}
}

static bool cmd_rename_recovery(StringInfo msg)
{
	const char *rec_msg_string;
	StringInfoData strinfoname;
	StringInfoData strinfonewname;
	StringInfoData output;

	rec_msg_string = agt_getmsgstring(msg);
	initStringInfo(&output);
	initStringInfo(&strinfoname);
	initStringInfo(&strinfonewname);
	/*check file exists*/
	appendStringInfo(&strinfoname, "%s/recovery.done", rec_msg_string);
	appendStringInfo(&strinfonewname, "%s/recovery.conf", rec_msg_string);
	if(access(strinfoname.data, F_OK) !=0 )
	{
		ereport(ERROR, (errmsg("could not find: %s", strinfoname.data)));
		return false;
	}
	/*rename recovery.done to recovery.conf*/
	if (rename(strinfoname.data, strinfonewname.data) != 0)
	{
		appendStringInfo(&output, "could not rename: %s to %s", strinfoname.data, strinfonewname.data);
		ereport(LOG, (errmsg("could not rename: %s to %s", strinfoname.data, strinfonewname.data)));
		return false;
	}
	else
		appendStringInfoString(&output, "success");
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
	pfree(strinfoname.data);
	pfree(strinfonewname.data);
	return true;
}

/*
 * this function can get host base infomation and other usage.
 * for example:
 * base information: host name, ip address,cpu type, run state
 * cpu : cpu type cpu percent
 * memory: total memory, usaged memory and memory percent.
 * disk: disk total, disk available, disk I/O
 * network: Network upload and download speed
 */
static void cmd_monitor_gets_hostinfo(void)
{
	StringInfoData hostinfostring;
	initStringInfo(&hostinfostring);

	get_cpu_info(&hostinfostring);
	get_mem_info(&hostinfostring);
	get_disk_info(&hostinfostring);
	get_net_info(&hostinfostring);
	get_system_info(&hostinfostring);
	get_platform_type_info(&hostinfostring);
	get_host_info(&hostinfostring);
	get_disk_iops_info(&hostinfostring);
	appendStringInfoCharMacro(&hostinfostring, '\0');

	agt_put_msg(AGT_MSG_RESULT, hostinfostring.data, hostinfostring.len);
	agt_flush();
	pfree(hostinfostring.data);
}

/*clean gtm/coordinator/datanode folder*/
void cmd_clean_node_folder(StringInfo buf)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData exstrinfo;

	initStringInfo(&output);
	initStringInfo(&exstrinfo);
	rec_msg_string = agt_getmsgstring(buf);
	appendStringInfo(&exstrinfo, "%s", rec_msg_string);

	if(exec_shell(exstrinfo.data, &output) != 0)
		ereport(ERROR, (errmsg("%s", output.data)));
	else
		appendStringInfoString(&output, "success");
	pfree(exstrinfo.data);
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
}

/*stop agent*/
static void cmd_stop_agent(void)
{
	pid_t pid;
	StringInfoData output;

	initStringInfo(&output);
	appendStringInfoString(&output, "receive the stop agent command");
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
	pid=getppid();
	if(kill(pid, SIGTERM) !=0)
	{
		perror("stop agent fail: ");
	}
}

/*
* get the result of command: show nodename key , and support fuzzy query,for example: show nodename "log%"
*/
static void cmd_get_showparam_values(char cmdtype, StringInfo buf)
{
	StringInfoData output;
	StringInfoData sqlstr;
	const char *rec_msg_string;
	char portstr[6];
	char param[64];
	char *valuestr;
	struct passwd *pwd;
	int resultlen = 0;

	initStringInfo(&output);
	initStringInfo(&sqlstr);
	rec_msg_string = agt_getmsgstring(buf);
	/*get port*/
	strcpy(portstr, rec_msg_string);
	strcpy(param, rec_msg_string + strlen(portstr) + 1);
	/*get param*/
	appendStringInfo(&sqlstr, "select name from pg_settings where name like '%%%s%%' order by 1", param);
	if (AGT_CMD_SHOW_CNDN_PARAM == cmdtype)
	{
		pwd = getpwuid(getuid());
		valuestr = mgr_get_showparam(sqlstr.data, pwd->pw_name, "127.0.0.1", atoi(portstr), AGTM_DBNAME);
	}
	else
	{
		valuestr = mgr_get_showparam(sqlstr.data, AGTM_USER, "127.0.0.1", atoi(portstr), AGTM_DBNAME);
	}
	pfree(sqlstr.data);
	if (valuestr != NULL)
	{
		resultlen = strlen(valuestr);
		if (resultlen > output.maxlen)
		{
			enlargeStringInfo(&output, resultlen+1);
			appendStringInfoString(&output, valuestr);
		}
		else
			appendStringInfoString(&output, valuestr);
		pfree(valuestr);
	}
	else
	{
		appendStringInfo(&output, "connect to %s to get the result failed", AGTM_DBNAME);
	}
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
}

/*given one sqlstr, return the result*/
static char *mgr_get_showparam(char *sqlstr, char *user, char *address, int port, char * dbname)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	PGresult *res_showall;
	char *oneCoordValueStr = NULL;
	char *valuestr;
	int nrow = 0;
	int nrow_all = 0;
	int iloop = 0;
	int jloop = 0;

	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, dbname);
	appendStringInfoCharMacro(&constr, '\0');
	ereport(LOG,
		(errmsg("connect info: %s, sql: %s",constr.data, sqlstr)));
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		pfree(constr.data);
		ereport(ERROR,
		(errmsg("%s", PQerrorMessage(conn))));
	}
	res = PQexec(conn, sqlstr);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQfinish(conn);
		pfree(constr.data);
		ereport(ERROR,
		(errmsg("%s" , PQresultErrorMessage(res))));
	}
	/*check column number*/
	Assert(1 == PQnfields(res));
	/*get row num*/
	nrow = PQntuples(res);
	if (nrow)
		oneCoordValueStr = (char *)palloc(nrow*64);
	else
		oneCoordValueStr = (char *)palloc(64);
	/*get none*/
	if (!nrow)
	{
		strcat(oneCoordValueStr,"no parameter be found");
	}
	/* get "show all" result */
	res_showall = PQexec(conn, "show all");
	if(PQresultStatus(res_showall) != PGRES_TUPLES_OK)
	{
		PQfinish(conn);
		pfree(constr.data);
		ereport(ERROR,
		(errmsg("%s" , PQresultErrorMessage(res_showall))));
	}
	nrow_all = PQntuples(res_showall);
	Assert(nrow_all >= nrow);
	for (iloop=0; iloop<nrow; iloop++)
	{
		for (jloop=0; jloop<nrow_all; jloop++)
		{
			if (strcmp(PQgetvalue(res, iloop, 0 ), PQgetvalue(res_showall, jloop, 0 )) != 0)
				continue;
			strcat(oneCoordValueStr, PQgetvalue(res_showall, jloop, 0 ));
			strcat(oneCoordValueStr, " = ");
			valuestr = PQgetvalue(res_showall, jloop, 1 );
			if (strcmp(valuestr, "") == 0)
				strcat(oneCoordValueStr, "''");
			else
				strcat(oneCoordValueStr, valuestr);
			if(iloop != nrow-1)
				strcat(oneCoordValueStr, "\t\n");
			else
				strcat(oneCoordValueStr, "\0");
			break;
		}
	}
	PQclear(res);
	PQclear(res_showall);
	PQfinish(conn);
	pfree(constr.data);
	return oneCoordValueStr;
}

/*given the sql_string, and return the column value string, which delimiter by '\0'*/
static void cmd_get_sqlstring_stringvalues(char cmdtype, StringInfo buf)
{
	StringInfoData output;
	StringInfoData sqlstr;
	const char *rec_msg_string;
	char user[64];
	char port[64];
	char dbname[64];
	char *address = "127.0.0.1";
	int i = 0;

	initStringInfo(&output);
	initStringInfo(&sqlstr);
	rec_msg_string = agt_getmsgstring(buf);
	/*sequence:user port dbname sqlstring, delimiter by '\0'*/
	while(i < 4)
	{
		if (!rec_msg_string)
		{
			ereport(ERROR, (errmsg("agent receive the cmd string not match \"user port dbname sqlstring\", which delimiter by \'\\0\'")));
		}
		switch(i++)
		{
			case 0:
				strcpy(user, rec_msg_string);
				user[strlen(user)] = 0;
				rec_msg_string = rec_msg_string + strlen(user) + 1;
				break;
			case 1:
				strcpy(port, rec_msg_string);
				port[strlen(port)] = 0;
				rec_msg_string = rec_msg_string + strlen(port) + 1;
				break;
			case 2:
				strcpy(dbname, rec_msg_string);
				dbname[strlen(dbname)] = 0;
				rec_msg_string = rec_msg_string + strlen(dbname) + 1;
				break;
			case 3:
				appendStringInfoString(&sqlstr, rec_msg_string);
				sqlstr.data[sqlstr.len] = 0;
				break;
			default:
				/*never come here*/
				ereport(WARNING, (errmsg("get the sqlstring values fail, this is %d string, 0 start", i)));
				break;
		}
	}
	/*get the sqlstring values*/
	mgr_execute_sqlstring(cmdtype, user, atoi(port), address, dbname, sqlstr.data, &output);
	pfree(sqlstr.data);
	if (output.len == 0)
	{
		appendStringInfo(&output, "connect to %s to get the result failed", AGTM_DBNAME);
		agt_put_msg(AGT_MSG_ERROR, output.data, output.len);
	}
	else
	{
		agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	}
	agt_flush();
	pfree(output.data);
}

/*given one sqlstr, return the result*/
static void mgr_execute_sqlstring(char cmdtype, char *user, int port, char *address, char *dbname, char *sqlstring, StringInfo output)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	int nrow = 0;
	int ncolumn = 0;
	int iloop = 0;
	int jloop = 0;

	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", user, address, port, dbname);
	appendStringInfoCharMacro(&constr, '\0');
	ereport(LOG,
		(errmsg("connect info: %s, sql: %s",constr.data, sqlstring)));
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		pfree(constr.data);
		ereport(ERROR,
		(errmsg("%s", PQerrorMessage(conn))));
		/*PQfinish(conn);*/
	}
	res = PQexec(conn, sqlstring);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQfinish(conn);
		pfree(constr.data);
		ereport(ERROR,
		(errmsg("%s" , PQresultErrorMessage(res))));
		/*PQclear(res);*/
		/*return NULL;*/
	}

	/*get row num*/
	nrow = PQntuples(res);
	ncolumn = PQnfields(res);
	/*get null*/
	if (!nrow || !ncolumn)
	{
		ereport(WARNING,
		(errmsg("adress=%s port=%d, the result is null, the sql string: %s" , address, port, sqlstring)));
	}
	for (iloop=0; iloop<nrow; iloop++)
	{
		for (jloop=0; jloop<ncolumn; jloop++)
		{
			appendStringInfo(output, "%s", PQgetvalue(res, iloop, jloop ));
			if (AGT_CMD_GET_EXPLAIN_STRINGVALUES == cmdtype)
				appendStringInfoCharMacro(output, '\n');
			else
				appendStringInfoCharMacro(output, '\0');
		}
	}
	appendStringInfoCharMacro(output, '\0');
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);
}

/*
* get monitor job result, the job type is batch
*/
static void cmd_get_batch_job_result(int cmd_type, StringInfo buf)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData exec;
	char *userpath;
	char scriptpath[256];
	char inputargs[1024];
	char *resultHeadP = "{\"result\":\"";

	userpath = getenv("HOME");
  if (NULL != userpath)
		chdir(userpath);
	initStringInfo(&exec);
	rec_msg_string = agt_getmsgstring(buf);
	/*script path*/
	strcpy(scriptpath, rec_msg_string);
	scriptpath[strlen(scriptpath)] = '\0';
	strcpy(inputargs, rec_msg_string + strlen(scriptpath)+1);
	inputargs[strlen(inputargs)] = '\0';
	appendStringInfo(&exec, "%s", scriptpath);
	appendStringInfo(&exec, " %s ", inputargs);
	initStringInfo(&output);
	appendStringInfo(&output, "%s", resultHeadP);

	if(exec_shell(exec.data, &output) != 0)
	{
		pfree(exec.data);
		ereport(ERROR, (errmsg("execute fail, %s", output.data + strlen(resultHeadP))));
	}
	/*json format*/
	if ('\n' == output.data[output.len-2])
		output.len = output.len -2;
	appendStringInfo(&output, "%s", "\"}");
	appendStringInfoCharMacro(&output, '\0');
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(exec.data);
	pfree(output.data);

}

