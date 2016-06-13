
/*
 * agent commands
 */
 
#include<unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "agent.h"

#include "agt_msg.h"
#include "agt_utility.h"
#include "mgr/mgr_msg_type.h"
#include "conf_scan.h"
#include "hba_scan.h"
#include "utils/memutils.h"

#define BUFFER_SIZE 4096

#define GTM_CTL_VERSION "gtm_ctl (ADB) " PGXC_VERSION "\n"
#define INITGTM_VERSION "initgtm (Postgres-XC) " PGXC_VERSION "\n"
#define INITDB_VERSION "initdb (PostgreSQL) " PG_VERSION "\n"
#define PG_BASEBACKUP_VERSION "pg_basebackup (PostgreSQL) " PG_VERSION "\n"
#define PG_CTL_VERSION "pg_ctl (PostgreSQL) " PG_VERSION "\n"
#define PSQL_VERSION "psql (Postgres-XC) " PGXC_VERSION "\n"

static void cmd_node_init(StringInfo msg, char *cmdfile, char* VERSION);
static void cmd_node_refresh_pgsql_paras(char cmdtype, StringInfo msg);
static void cmd_refresh_confinfo(char *key, char *value, ConfInfo *info);
static void writefile(char *path, ConfInfo *info);
static void writehbafile(char *path, HbaInfo *info);
static bool copyFile(const char *targetFileWithPath, const char *sourceFileWithPath);
static void cmd_refresh_pghba_confinfo(HbaInfo *checkinfo, HbaInfo *info);
static void cmd_node_refresh_pghba_paras(StringInfo msg);
static void pg_ltoa(int32 value, char *a);
static bool cmd_rename_recovery(StringInfo msg);
static void cmd_monitor_gets_hostinfo(void);
extern bool get_cpu_info(StringInfo hostinfostring);
extern bool get_mem_info(StringInfo hostinfostring);
extern bool get_disk_info(StringInfo hostinfostring);
extern bool get_net_info(StringInfo hostinfostring);

void do_agent_command(StringInfo buf)
{
	int cmd_type;
	AssertArg(buf);
	cmd_type = agt_getmsgbyte(buf);
	switch(cmd_type)
	{
	case AGT_CMD_GTM_INIT:
	case AGT_CMD_GTM_PROXY_INIT:
		cmd_node_init(buf, "initgtm", INITGTM_VERSION);
		break;
	case AGT_CMD_CNDN_CNDN_INIT:
	 	cmd_node_init(buf, "initdb", INITDB_VERSION);
		break;
	case AGT_CMD_CNDN_SLAVE_INIT:
		cmd_node_init(buf, "pg_basebackup", PG_BASEBACKUP_VERSION);
		break;
	case AGT_CMD_CN_START:
	case AGT_CMD_CN_STOP:
	case AGT_CMD_DN_START:
	case AGT_CMD_DN_STOP:
	case AGT_CMD_DN_FAILOVER:
	case AGT_CMD_DN_RESTART:
		cmd_node_init(buf, "pg_ctl", PG_CTL_VERSION);
		break;
	case AGT_CMD_GTM_START_MASTER:
	case AGT_CMD_GTM_STOP_MASTER:
	case AGT_CMD_GTM_START_SLAVE:
	case AGT_CMD_GTM_STOP_SLAVE:
	case AGT_CMD_GTM_START_PROXY:
	case AGT_CMD_GTM_STOP_PROXY:
		cmd_node_init(buf, "gtm_ctl", GTM_CTL_VERSION);
		break;
	case AGT_CMD_PSQL_CMD:
		cmd_node_init(buf, "psql", PSQL_VERSION);
		break;
	case AGT_CMD_CNDN_REFRESH_PGSQLCONF:
		cmd_node_refresh_pgsql_paras(AGT_CMD_CNDN_REFRESH_PGSQLCONF, buf);
		break;
	case AGT_CMD_CNDN_REFRESH_RECOVERCONF:
		cmd_node_refresh_pgsql_paras(AGT_CMD_CNDN_REFRESH_RECOVERCONF, buf);
		break;
	case AGT_CMD_CNDN_REFRESH_PGHBACONF:
		cmd_node_refresh_pghba_paras(buf);
		break;
	case AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD:
		cmd_node_refresh_pgsql_paras(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, buf);
		break;
	case AGT_CMD_CNDN_RENAME_RECOVERCONF:
		cmd_rename_recovery(buf);
		break;
	case AGT_CMD_MONITOR_GETS_HOST_INFO:
		cmd_monitor_gets_hostinfo();
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			,errmsg("unknown agent command %d", cmd_type)));
	}
}

static void cmd_node_init(StringInfo msg, char *cmdfile, char* VERSION)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData exec;

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
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(exec.data);
	pfree(output.data);
	
}

static void cmd_node_refresh_pghba_paras(StringInfo msg)
{
	const char *rec_msg_string;
	StringInfoData output;
	StringInfoData infoparastr;
	StringInfoData pgconffile;
	char datapath[MAXPGPATH];
	char *ptmp;
	HbaInfo *info,
			*newinfo,
			*infohead;
	MemoryContext pgconf_context;
	MemoryContext oldcontext;

	pgconf_context = AllocSetContextCreate(CurrentMemoryContext,
										"pghbaconf",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(pgconf_context);
	
	rec_msg_string = agt_getmsgstring(msg);		 
	initStringInfo(&infoparastr);
	initStringInfo(&pgconffile);
	if(msg->len > infoparastr.maxlen)
		enlargeStringInfo(&infoparastr, msg->len - infoparastr.maxlen);
	memcpy(infoparastr.data, &msg->data[msg->cursor], msg->len - msg->cursor);
	infoparastr.cursor = 0;
	infoparastr.len = msg->len - msg->cursor;
	/*get datapath*/
	strcpy(datapath, rec_msg_string);
	/*check file exists*/
	appendStringInfo(&pgconffile, "%s/pg_hba.conf", datapath);
	if(access(pgconffile.data, F_OK) !=0 )
	{
		ereport(ERROR, (errmsg("could not find: %s", pgconffile.data)));
	}
	/*get the postgresql.conf content*/
	info = parse_hba_file(pgconffile.data);
	infohead = info;
	newinfo = (HbaInfo *)palloc(sizeof(HbaInfo));
	while((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len))
	{
		/*type*/
		newinfo->type = infoparastr.data[infoparastr.cursor];
		infoparastr.cursor = infoparastr.cursor + sizeof(char) + 1;
		/*database*/
		Assert((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len));
		newinfo->database = &(infoparastr.data[infoparastr.cursor]);
		infoparastr.cursor = infoparastr.cursor + strlen(newinfo->database) + 1;
		/*user*/
		Assert((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len));
		newinfo->user = &(infoparastr.data[infoparastr.cursor]);
		infoparastr.cursor = infoparastr.cursor + strlen(newinfo->user) + 1;
		/*ip*/
		Assert((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len));
		newinfo->addr = &(infoparastr.data[infoparastr.cursor]);
		infoparastr.cursor = infoparastr.cursor + strlen(newinfo->addr) + 1;
		/*mark*/
		Assert((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len));
		newinfo->addr_mark = atoi(&(infoparastr.data[infoparastr.cursor]));
		infoparastr.cursor = infoparastr.cursor + strlen(&(infoparastr.data[infoparastr.cursor])) + 1;
		/*method*/
		Assert((ptmp = &infoparastr.data[infoparastr.cursor]) != '\0' && (infoparastr.cursor < infoparastr.len));
		newinfo->auth_method = &(infoparastr.data[infoparastr.cursor]);
		infoparastr.cursor = infoparastr.cursor + strlen(newinfo->auth_method) + 1;
		cmd_refresh_pghba_confinfo(newinfo, info);
	}
	
	/*use the new info list to refresh the postgresql.conf*/
	writehbafile(pgconffile.data, infohead);
	initStringInfo(&output);
	appendStringInfoString(&output, "success");
	appendStringInfoCharMacro(&output, '\0');
	agt_put_msg(AGT_MSG_RESULT, output.data, output.len);
	agt_flush();
	pfree(output.data);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(pgconf_context);	

}

static void cmd_refresh_pghba_confinfo(HbaInfo *checkinfo, HbaInfo *info)
{
	bool getkey = false;
	char *strtype;
	char *database;
	char *user;
	char *addr;
	char mark[4];
	char *auth_method;
	char *line;
	int intervallen = 4;
	HbaInfo *infopre = info;

	/*use newinfo to refresh info list*/
	while(checkinfo && info)
	{
		if(checkinfo->type == info->type
				&& strcmp(checkinfo->database, info->database) == 0
				&& strcmp(checkinfo->user, info->user) == 0
				&& strcmp(checkinfo->addr, info->addr) == 0
				&& strcmp(checkinfo->auth_method, info->auth_method) == 0)
		{
			getkey = true;
			break;
		}
		infopre = info;
		info = info->next;
	}

	/*append the to the info list*/
	if (!getkey)
	{		
		HbaInfo *newinfo = (HbaInfo *)palloc(sizeof(HbaInfo)+1);
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
		switch(checkinfo->type)
		{
			case 1: //local
			  strtype = "local";
				newinfo->type_len = 5;
				break;
			case 2: //host
				strtype = "host";
				newinfo->type_len = 4;
				break;
			case 3: //hostssl
				strtype = "hostssl";
				newinfo->type_len = 7;
				break;
			case 4: //hostnossl
				strtype = "hostnossl";
				newinfo->type_len = 9;
				break;
			default:
				newinfo->type_len = 0;
				break;
		}

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
		infopre->next = newinfo;
	}

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
	char datapath[MAXPGPATH];
	char my_exec_path[MAXPGPATH];
	char pghome[MAXPGPATH];
	char *key;
	char *value;
	char *ptmp;
	char *strconf = "# PostgreSQL recovery config file";
	char buf[1024];
	ConfInfo *info,
			*infohead;
	FILE *create_recovery_file;

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
	if(msg->len > infoparastr.maxlen)
		enlargeStringInfo(&infoparastr, msg->len - infoparastr.maxlen);
	memcpy(infoparastr.data, &msg->data[msg->cursor], msg->len - msg->cursor);
	infoparastr.cursor = 0;
	infoparastr.len = msg->len - msg->cursor;
	/*get datapath*/
	strcpy(datapath, rec_msg_string);
	/*check file exists*/
	if (AGT_CMD_CNDN_REFRESH_PGSQLCONF == cmdtype || AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD == cmdtype)
	{
		appendStringInfo(&pgconffile, "%s/postgresql.conf", datapath);
		if(access(pgconffile.data, F_OK) !=0 )
		{
			ereport(ERROR, (errmsg("could not find: %s", pgconffile.data)));
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
			if(!copyFile(pgconffile.data, pghome))
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
		
	}
	/*get the postgresql.conf content*/
	info = parse_conf_file(pgconffile.data);
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
		cmd_refresh_confinfo(key, value, info);
	}
	
	/*use the new info list to refresh the postgresql.conf*/
	writefile(pgconffile.data, infohead);
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
static void cmd_refresh_confinfo(char *key, char *value, ConfInfo *info)
{
	bool getkey = false;
	int diffvalue;
	char *newname,
		*newvalue;
	ConfInfo *infopre = info;

	/*use (key, value) to refresh info list*/
	while(info)
	{
		if(info->name != '\0' && strcmp(key, info->name) == 0)
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
		}
		infopre = info;
		info = info->next;
	}

	/*append the (key,value) to the info list*/
	if (!getkey)
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

static bool copyFile(const char *targetFileWithPath, const char *sourceFileWithPath)
{
	FILE *fpR, *fpW;
	char buffer[BUFFER_SIZE];
	int lenR, lenW;
	if ((fpR = fopen(sourceFileWithPath, "r")) == NULL)
	{
		return false;
	}
	if ((fpW = fopen(targetFileWithPath, "w")) == NULL)
	{
		fclose(fpR);
		return false;
	}

	memset(buffer, 0, BUFFER_SIZE);
	while ((lenR = fread(buffer, 1, BUFFER_SIZE, fpR)) > 0)
	{
		if ((lenW = fwrite(buffer, 1, lenR, fpW)) != lenR)
		{
			fclose(fpR);
			fclose(fpW);
			return false;
		}
		memset(buffer, 0, BUFFER_SIZE);
	}

	fclose(fpR);
	fclose(fpW);
	return true;
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
	appendStringInfoCharMacro(&hostinfostring, '\0');
	
	agt_put_msg(AGT_MSG_RESULT, hostinfostring.data, hostinfostring.len);
	agt_flush();
	pfree(hostinfostring.data);
}
