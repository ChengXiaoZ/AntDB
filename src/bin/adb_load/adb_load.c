#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>

#include "postgres_fe.h"
#include "catalog/pg_type.h"
#include "compute_hash.h"
#include "loadsetting.h"
#include "linebuf.h"
#include "msg_queue.h"
#include "msg_queue_pipe.h"
#include "log_summary_fd.h"
#include "log_process_fd.h"
#include "log_detail_fd.h"
#include "read_producer.h"
#include "dispatch.h"
#include "lib/ilist.h"
#include "utility.h"
#include "properties.h"
#include "log_summary.h"

typedef struct tables
{
	int table_nums;
	struct TableInfo *info;
}Tables;

typedef struct FileLocation
{
	char * location;
	slist_node next;
} FileLocation;

struct special_table
{
	int table_nums;
	struct TableName *tb_ptr;
};
struct TableName
{
	char *name;
	struct TableName *next;
};

struct special_table *special_table_list = NULL;

static char get_distribute_by(const char *conninfo, const char *tablename);
static void get_all_datanode_info(ADBLoadSetting *setting);
static void get_conninfo_for_alldatanode(ADBLoadSetting *setting);
static int  adbloader_cmp_nodes(const void *a, const void *b);
static void get_table_attribute(ADBLoadSetting *setting, TableInfo *table_info);
static void get_hash_field(const char *conninfo, TableInfo *table_info);
static void get_table_loc_count_and_loc(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void get_func_args_count_and_type(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void get_create_and_drop_func_sql(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void get_userdefined_funcinfo(const char *conninfo, TableInfo *table_info);
static void create_func_to_server(char *serverconninfo, char *creat_func_sql);
static void reset_hash_field(TableInfo *table_info);
static void get_node_count_and_node_list(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void drop_func_to_server(char *serverconninfo, char *drop_func_sql);
static void get_use_datanodes(ADBLoadSetting *setting, TableInfo *table_info);
static void get_use_datanodes_from_conf(ADBLoadSetting *setting, TableInfo *table_info);
static void check_connect(ADBLoadSetting *setting, TableInfo *table_info_ptr);
//static void check_get_enough_connect_num(TableInfo *table_info_ptr);
static int get_max_connect(NodeInfoData *use_datanodes);
static void check_max_connections(int threads_num_per_datanode, NodeInfoData *node_info);
static void check_queue_num_valid(ADBLoadSetting *setting, TableInfo *table_info);

static bool is_create_in_adb_cluster(char *table_name);
static Tables* get_file_info(char *input_dir);
static bool update_file_info(char *input_dir, Tables *tables);

static char *rename_file_name(char *file_name, char *input_dir, char *suffix);
static void rename_file_suffix(char *old_file_path, char *suffix);

static char *get_full_path(char *file_name, char *input_dir);
static char *get_table_name(char *file_name);
static bool is_suffix(char *str, char *suffix);

static void send_data_to_datanode(DISTRIBUTE distribute_by,
                                ADBLoadSetting *setting,
                                TableInfo *table_info_ptr);

static bool do_replaciate_roundrobin(DISTRIBUTE distribute_by,
									char *filepath,
									TableInfo *table_info);

static void clean_replaciate_roundrobin(void);

static bool do_hash_module(char *filepath, const TableInfo *table_info);
static void clean_hash_module(void);
static char *get_outqueue_name (int datanode_num);
static void exit_nicely(PGconn *conn);
static void deep_copy(HashField *dest, HashField *src);

/*--------------------------------------------------------*/

void file_name_print(char **file_list, int file_num);
void tables_print(Tables *tables_ptr);

static void tables_list_free(Tables *tables);
static void table_free(TableInfo *table_info);
static void clean_hash_field (HashField *hash_field);
static char * conver_type_to_fun (Oid type, DISTRIBUTE loactor);
static void free_datanode_info (DatanodeInfo *datanode_info);
static void free_dispatch_info (DispatchInfo *dispatch_info);
static void free_hash_info(HashComputeInfo *hash_info);
static void free_read_info(ReadInfo *read_info);
static LOG_LEVEL covert_loglevel_from_char_type (char *type);
static char *covert_distribute_type_to_string (DISTRIBUTE type);
static void main_write_error_message(DISTRIBUTE distribute, char * message, char *start_cmd);
static int get_file_total_line_num (char *file_path);
static void process_bar(int total, int send);
static void process_bar_multi(int total, int * send , int num);
static void show_process(int total,	int datanodes, int * thread_send_total, DISTRIBUTE type);

/*--------------------------------------------------------*/
#define is_digit(c) ((unsigned)(c) - '0' <= 9)

#define MAXPGPATH               1024
#define QUERY_MAXLEN            1024
#define DEFAULT_USER            "postgres"

#define PROVOLATILE_IMMUTABLE   'i'   /* never changes for given input */
#define PROVOLATILE_STABLE      's'   /* does not change within a scan */
#define PROVOLATILE_VOLATILE    'v'   /* can change even within a scan */

#define SUFFIX_SQL              ".sql"
#define SUFFIX_SCAN             ".scan"
#define SUFFIX_DOING            ".doing"
#define SUFFIX_ADBLOAD          ".adbload"
#define SUFFIX_ERROR            ".error"
#define END_FILE_NAME           "adbload_end"

#define NO_USE_PATH_IN_STREAM_MODE "no_use_path_in_stream_mode"

#define CHECK_FILE_INFO_ALWAYS \
do \
{ \
	for (;;) \
	{ \
		tables_ptr = get_file_info(setting->input_directory); \
		if (tables_ptr->table_nums == 0 && tables_ptr->info == NULL) \
		{ \
			if (update_tables) \
			{ \
				sleep(2); \
				continue; \
			} \
			else \
			{ \
			    break; \
			} \
		} \
		else \
		{ \
			break; \
		} \
	} \
} while (0)


static ADBLoadSetting *setting = NULL;

/*update dir for tables info */
static bool update_tables = true;

void
clean_hash_field (HashField *hash_field)
{
	if (hash_field == NULL)
		return;

	pg_free(hash_field->field_loc);
	hash_field->field_loc = NULL;

	pg_free(hash_field->field_type);
	hash_field->field_type = NULL;

	pg_free(hash_field->func_name);
	hash_field->func_name = NULL;

	pg_free(hash_field->node_list);
	hash_field->node_list = NULL;

	pg_free(hash_field->text_delim);
	hash_field->text_delim = NULL;

	pg_free(hash_field->copy_options);
	hash_field->copy_options = NULL;

	pg_free(hash_field->hash_delim);
	hash_field->hash_delim = NULL;

	pg_free(hash_field);
	hash_field = NULL;

	return;
}

LOG_LEVEL
covert_loglevel_from_char_type (char *type)
{
	Assert(type != NULL);

	if (strcasecmp(type, "DEBUG") == 0)
		return LOG_DEBUG;
	else if (strcasecmp(type, "INFO") == 0)
		return LOG_INFO;
	else if (strcasecmp(type, "WARNING") == 0)
		return LOG_WARN;
	else if (strcasecmp(type, "ERROR") == 0)
		return LOG_ERROR;
	else
		fprintf(stderr, "log leve error : %s \n", type);

	return LOG_INFO;
}

char *
covert_distribute_type_to_string (DISTRIBUTE type)
{
	LineBuffer * buff = get_linebuf();
	char *result;

	switch(type)
	{
		case DISTRIBUTE_BY_REPLICATION:
			appendLineBufInfoString(buff, "DISTRIBUTE_BY_REPLICATION");
			break;
		case DISTRIBUTE_BY_ROUNDROBIN:
			appendLineBufInfoString(buff, "DISTRIBUTE_BY_ROUNDROBIN");
			break;
		case DISTRIBUTE_BY_USERDEFINED:
			appendLineBufInfoString(buff, "DISTRIBUTE_BY_USERDEFINED");
			break;
		case DISTRIBUTE_BY_DEFAULT_HASH:
			appendLineBufInfoString(buff, "DISTRIBUTE_BY_DEFAULT_HASH");
			break;
		case DISTRIBUTE_BY_DEFAULT_MODULO:
			appendLineBufInfoString(buff, "DISTRIBUTE_BY_DEFAULT_MODULO");
			break;
		default:
			break;
	}

	result = pg_strdup(buff->data);
	release_linebuf(buff);

	return result;
}

int
main(int argc, char **argv)
{
	Tables *tables_ptr = NULL;
	TableInfo *table_info_ptr = NULL;
	int table_count = 0;

	/* get cmdline param. */
	setting = cmdline_adb_load_setting(argc, argv);
	if (setting == NULL)
	{
		fprintf(stderr, "Error: Command-line processing error.\n");
		exit(EXIT_FAILURE);
	}

	/* get var value from config file. */
	get_settings_by_config_file(setting);

	get_node_conn_info(setting);

	/* make sure threads_num_per_datanode < max_connect for agtm */
	check_max_connections(setting->threads_num_per_datanode, setting->agtm_info);

	/* make sure threads_num_per_datanode < max_connect for coordinator */
	check_max_connections(setting->threads_num_per_datanode, setting->coordinator_info);

	/* open log file if not exist create. */
	if (setting->output_directory != NULL)
	{
		open_log_process_fd(setting->output_directory,
			covert_loglevel_from_char_type(setting->log_field->log_level));
		open_log_summary_fd(setting->output_directory);
	}else if (setting->log_field->log_path != NULL)
	{
		open_log_process_fd(setting->log_field->log_path,
			covert_loglevel_from_char_type(setting->log_field->log_level));
		open_log_summary_fd(setting->log_field->log_path);
	}

	if (setting->single_file_mode || setting->stream_mode)
	{
		FileLocation *file_location = (FileLocation*)palloc0(sizeof(FileLocation));
		tables_ptr = (Tables *)palloc0(sizeof(Tables));
		tables_ptr->table_nums = 1;/* just one table in single mode */

		table_info_ptr = (TableInfo *)palloc0(sizeof(TableInfo));
		if (!is_create_in_adb_cluster(setting->table_name))
		{
			fprintf(stderr, "Error: table \"%s\" does not exist in adb cluster. \n", setting->table_name);
			ADBLOADER_LOG(LOG_ERROR, "[main][thread main ] table \"%s\" does not exist in adb cluster. \n", setting->table_name);
			exit(EXIT_FAILURE);
		}

		table_info_ptr->table_name = pg_strdup(setting->table_name);
		table_info_ptr->file_nums = 1; /* just one file in single node */
		table_info_ptr->next = NULL;

		if (setting->stream_mode)
		{
			/* don't use input file in stream mode */
			file_location->location = pg_strdup(NO_USE_PATH_IN_STREAM_MODE);
		}
		else if (setting->single_file_mode)
		{
			file_location->location = pg_strdup(setting->input_file);
		}

		slist_push_head(&table_info_ptr->file_head, &file_location->next);
	}
	else if (setting->static_mode || setting->dynamic_mode)
	{
		tables_ptr = get_file_info(setting->input_directory);

		/* static mode : input_directory is empty */
		if (setting->static_mode        &&
			tables_ptr->table_nums == 0 &&
			tables_ptr->info == NULL)
		{
			fprintf(stderr, "Error: the folder is empty in path \"%s\".\n", setting->input_directory);
			exit(EXIT_FAILURE);
		}

		/* dynamic mode : input_directory is empty */
		if (setting->dynamic_mode       &&
			tables_ptr->table_nums == 0 &&
			tables_ptr->info == NULL)
		{
			CHECK_FILE_INFO_ALWAYS;
		}

		table_info_ptr = tables_ptr->info;
	}
	else
	{
		fprintf(stderr, "Error: unrecognized mode. \n");
		exit(EXIT_FAILURE);
	}

	if (!setting->config_datanodes_valid)
	{
		get_all_datanode_info(setting);
		get_conninfo_for_alldatanode(setting);
	}

	/* init linebuf */
	init_linebuf(setting->datanodes_num);

	while(table_info_ptr)
	{
		++table_count;

		if (setting->dynamic_mode)
		{
			if (table_info_ptr->file_nums == 0 && table_info_ptr->next != NULL)
			{
				table_info_ptr = table_info_ptr->next;
				continue;
			}

			if (table_info_ptr->file_nums == 0 && table_info_ptr->next == NULL)
			{
				if (update_tables)
				{
					update_file_info(setting->input_directory, tables_ptr);
					table_info_ptr = tables_ptr->info;
					continue;
				}
				else
				{
					break;
				}
			}
		}

		/* get table info to store in the table_info_ptr*/
		get_table_attribute(setting, table_info_ptr);

		if (setting->config_datanodes_valid)
			get_use_datanodes_from_conf(setting, table_info_ptr);
		else
			get_use_datanodes(setting, table_info_ptr);

		/* check -Q valid*/
		check_queue_num_valid(setting, table_info_ptr);

		open_log_detail_fd(setting->log_field->log_path, table_info_ptr->table_name);

		/* create all threads and send data to datanode */
		send_data_to_datanode(table_info_ptr->distribute_type, setting, table_info_ptr);

		close_log_detail_fd();
		check_log_detail_fd(setting->log_field->log_path, table_info_ptr->table_name);

		if (setting->dynamic_mode && update_tables)
		{
			update_file_info(setting->input_directory, tables_ptr);
			table_info_ptr = tables_ptr->info;
		}
		else
		{
			table_info_ptr = table_info_ptr->next;
		}
	}

	close_log_summary_fd();

	/* check again */
	if(table_count != tables_ptr->table_nums)
	{
		ADBLOADER_LOG(LOG_ERROR,"[main] The number of imported tables does not match the number of calcuate.");
		return 0;
	}

	tables_list_free(tables_ptr);
	close_log_process_fd();
	pg_free_adb_load_setting(setting);
	end_linebuf();
	DestoryConfig();

	return 0;
}
/*------------------------end main-------------------------------------------------------*/
static void
send_data_to_datanode(DISTRIBUTE distribute_by,
                                ADBLoadSetting *setting,
                                TableInfo *table_info_ptr)
{
	slist_mutable_iter siter;
	LineBuffer    *linebuff = NULL;
	FileLocation  *file_location = NULL;
	bool           sent_ok = false;

	/* the table may be split to more than one*/
	slist_foreach_modify (siter, &table_info_ptr->file_head)
	{
		/* check connect to adb_load server, agtm, coordinator and all datanode. */
		check_connect(setting, table_info_ptr);

		/* make sure threads_num_per_datanode < max_connect */
		//check_get_enough_connect_num(table_info_ptr);

		file_location = slist_container(FileLocation, next, siter.cur);
		Assert(file_location->location != NULL);

		/* begin record error file */
		linebuff = format_error_begin(file_location->location,
							covert_distribute_type_to_string(table_info_ptr->distribute_type));
		write_log_summary_fd(linebuff);
		write_log_detail_fd(linebuff->data);
		release_linebuf(linebuff);

		if (setting->stream_mode)
		{
			fprintf(stderr, "table: %s -------------> ", table_info_ptr->table_name);
		}
		else
		{
			fprintf(stderr, "file : %s -------------> ", file_location->location);
		}

		switch (distribute_by)
		{
			case DISTRIBUTE_BY_REPLICATION:
			case DISTRIBUTE_BY_ROUNDROBIN:
				{
					sent_ok = do_replaciate_roundrobin(distribute_by, file_location->location, table_info_ptr);

					/* stop module and clean resource */
					clean_replaciate_roundrobin();
					break;
				}
			case DISTRIBUTE_BY_DEFAULT_HASH:
			case DISTRIBUTE_BY_DEFAULT_MODULO:
			case DISTRIBUTE_BY_USERDEFINED:
				{
					sent_ok = do_hash_module(file_location->location, table_info_ptr);

					/* stop module and clean resource */
					clean_hash_module();
					break;
				}
			default:
				break;
		}

		/* end record error file */
		linebuff = format_error_end(file_location->location);
		write_log_detail_fd(linebuff->data);
		write_log_summary_fd(linebuff);

		release_linebuf(linebuff);

		/* print exec status */
		if (sent_ok)
			fprintf(stderr, "success\n");
		else
			fprintf(stderr, "failed\n");

		/* rename file name */
		if (setting->static_mode || setting->dynamic_mode)
		{
			if (sent_ok)
				rename_file_suffix(file_location->location, SUFFIX_ADBLOAD);
			else
				rename_file_suffix(file_location->location, SUFFIX_ERROR);
		}

		/* remove file from file list */
		slist_delete_current(&siter);

		/* file num subtract 1 */
		table_info_ptr->file_nums--;

		/* free location */
		if (file_location->location)
			pg_free(file_location);
		file_location = NULL;
	}

	return;
}

static void
check_queue_num_valid(ADBLoadSetting *setting, TableInfo *table_info)
{
	int flag = 0;
	int threads_total = 0;

	threads_total = table_info->use_datanodes_num * table_info->threads_num_per_datanode;

	for (flag = 0 ; flag < setting->redo_queue_total; flag++)
	{
		if (setting->redo_queue_index[flag] <= 0 || setting->redo_queue_index[flag] >= threads_total)
		{
			fprintf(stderr, "Error: value for option \"-Q\" is invalid, must be between 0 and THREADS_NUM_PER_DATANODE * DATANODE NUMBER -1.\n");
			exit(EXIT_FAILURE);
		}
	}

	return;
}

static void
check_max_connections(int threads_num_per_datanode, NodeInfoData *node_info)
{
	int max_connect = 0;

	max_connect = get_max_connect(node_info);
	if (threads_num_per_datanode >= max_connect)
	{
		fprintf(stderr, "Error: \"THREADS_NUM_PER_DATANODE\"(%d) should be "
						"less than max_connect(%d) for %s:%s.\n",
						threads_num_per_datanode,
						max_connect,
						node_info->node_host,
						node_info->node_port);
		exit(EXIT_FAILURE);
	}

	return ;
}

#if 0
static void
check_get_enough_connect_num(TableInfo *table_info_ptr)
{
	int i = 0;
	int max_connect = 0;

	Assert(table_info_ptr != NULL);

	for (i = 0; i < table_info_ptr->use_datanodes_num; i++)
	{
		max_connect = get_max_connect(table_info_ptr->use_datanodes[i]);
		if (table_info_ptr->threads_num_per_datanode >= max_connect)
		{
			fprintf(stderr, "\"THREADS_NUM_PER_DATANODE\"(%d) should be less than max_connect(%d) for %s.\n",
							table_info_ptr->threads_num_per_datanode,
							max_connect,
							table_info_ptr->use_datanodes[i]->node_name);
			exit(EXIT_FAILURE);
		}
	}

	return;
}
#endif

static int
get_max_connect(NodeInfoData *node_info)
{
	char      query[QUERY_MAXLEN] = {0};
	PGconn   *conn = NULL;
	PGresult *res = NULL;
	int       max_connect = 0;

	conn = PQconnectdb(node_info->connection);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to datanode failed:%s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select setting "
					"from pg_settings "
					"where name = \'max_connections\';");

	res = PQexec(conn, query);
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to datanode failed:%s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	if (PQntuples(res) != 1)
	{
		PQclear(res);
		exit_nicely(conn);
	}

	max_connect = atoi(PQgetvalue(res, 0, PQfnumber(res, "setting")));

	PQclear(res);
	PQfinish(conn);

	return max_connect;
}

static void
check_connect(ADBLoadSetting *setting, TableInfo *table_info_ptr)
{
	int i = 0;

	Assert(setting != NULL);
	Assert(table_info_ptr != NULL);

	/* check adb_load server can connect.*/
	check_node_connection_valid(setting->server_info->node_host,
								setting->server_info->node_port,
								setting->server_info->connection);

	/* check ADB agtm can connect. */
	check_node_connection_valid(setting->agtm_info->node_host,
								setting->agtm_info->node_port,
								setting->agtm_info->connection);

	/* check ADB coordinator can connect. */
	check_node_connection_valid(setting->coordinator_info->node_host,
								setting->coordinator_info->node_port,
								setting->coordinator_info->connection);

	/* check all use datanode can connect. */
	for (i = 0; i < table_info_ptr->use_datanodes_num; i++)
	{
		check_node_connection_valid(table_info_ptr->use_datanodes[i]->node_host,
									table_info_ptr->use_datanodes[i]->node_port,
									table_info_ptr->use_datanodes[i]->connection);
	}

	return ;
}

static bool
update_file_info(char *input_dir, Tables *tables)
{
	TableInfo * table_info_dynamic = NULL;
	struct dirent *dirent_ptr = NULL;
	char *new_file_name = NULL;
	char *table_name = NULL;
	char *file_full_path = NULL;
	FileLocation * file_location = NULL;
	DIR *dir = NULL;
	TableInfo *ptr = NULL;
	TableInfo *tail = NULL;

	if ((dir=opendir(input_dir)) == NULL)
	{
		fprintf(stderr, "can not open directory \"%s\".\n", input_dir);
		return false;
	}

	while ((dirent_ptr = readdir(dir)) != NULL)
	{
		if (strcmp(dirent_ptr->d_name, ".") == 0          ||
			strcmp(dirent_ptr->d_name, "..") == 0         ||
			is_suffix(dirent_ptr->d_name, SUFFIX_ADBLOAD) ||
			is_suffix(dirent_ptr->d_name, SUFFIX_DOING)   ||
			is_suffix(dirent_ptr->d_name, SUFFIX_ERROR)   ||
			is_suffix(dirent_ptr->d_name, SUFFIX_SCAN))
			continue;

		if (strcmp(dirent_ptr->d_name, END_FILE_NAME) == 0)
		{
			update_tables = false;
			continue;
		}

		if (!is_suffix(dirent_ptr->d_name, SUFFIX_SQL))
		{
			fprintf(stderr, "invalid file name :\"%s/%s\"\n", input_dir, dirent_ptr->d_name);
			ADBLOADER_LOG(LOG_ERROR, "[main][thread main ] invalid file name :\"%s/%s\"\n", input_dir, dirent_ptr->d_name);
			continue;
		}

		table_name  = get_table_name(dirent_ptr->d_name);
		if (!is_create_in_adb_cluster(table_name))
		{
			fprintf(stderr, "Error: table \"%s\" does not exist in adb cluster.\n", table_name);
			ADBLOADER_LOG(LOG_ERROR, "[main][thread main ] table \"%s\" does not exist in adb cluster.\n", table_name);

			new_file_name  = rename_file_name(dirent_ptr->d_name, input_dir, SUFFIX_ERROR);
			if (new_file_name != NULL)
			{
				pfree(new_file_name);
				new_file_name = NULL;
			}

			continue;
		}

		new_file_name  = rename_file_name(dirent_ptr->d_name, input_dir, SUFFIX_SCAN);
		file_full_path = get_full_path(new_file_name, input_dir);

		ptr = tables->info;
		tail = tables->info;

		while(ptr != NULL)
		{
			if (strcmp(ptr->table_name, table_name) == 0)
			{
				file_location = (FileLocation*)palloc0(sizeof(FileLocation));
				file_location->location = file_full_path;
				slist_push_head(&ptr->file_head, &file_location->next);

				ptr->file_nums ++;
				break;
			}

			if (ptr->next == NULL)
				tail = ptr;

			ptr = ptr->next;
		}

		if (ptr == NULL)
		{
			table_info_dynamic = (TableInfo *)palloc0(sizeof(TableInfo));
			table_info_dynamic->file_nums ++;
			table_info_dynamic->table_name = table_name;

			slist_init(&table_info_dynamic->file_head);

			file_location = (FileLocation*)palloc0(sizeof(FileLocation));
			file_location->location = file_full_path;
			slist_push_head(&table_info_dynamic->file_head, &file_location->next);

			tables->table_nums ++;

			if (tables == NULL)
				tables->info = table_info_dynamic;
			else
				tail->next = table_info_dynamic;
		}
	}

	closedir((DIR*)dir);

	return true;
}

static Tables*
get_file_info(char *input_dir)
{
	Tables * tables_dynamic = NULL;
	TableInfo * table_info_dynamic = NULL;
	DIR *dir = NULL;
	struct dirent *dirent_ptr = NULL;
	char *new_file_name = NULL;
	char *table_name = NULL;
	char *file_full_path = NULL;
	FileLocation * file_location = NULL;
	TableInfo *ptr = NULL;
	TableInfo *tail = NULL;

	if ((dir=opendir(input_dir)) == NULL)
	{
		fprintf(stderr, "open dir %s error...\n", input_dir);
		exit(EXIT_FAILURE);
	}

	tables_dynamic = (Tables *)palloc0(sizeof(Tables));
	tables_dynamic->table_nums = 0;
	tables_dynamic->info = NULL;

	while ((dirent_ptr = readdir(dir)) != NULL)
	{
		if(strcmp(dirent_ptr->d_name, ".") == 0           ||
			strcmp(dirent_ptr->d_name, "..") == 0         ||
			is_suffix(dirent_ptr->d_name, SUFFIX_ERROR)   ||
			is_suffix(dirent_ptr->d_name, SUFFIX_ADBLOAD))
			continue;

		if (setting->dynamic_mode)
		{
			/*stop update file info if have a file END_FILE_NAME(adbload_end) */
			if (strcmp(dirent_ptr->d_name, END_FILE_NAME) == 0)
			{
				 update_tables = false;
				 continue;
			}
		}

		if (setting->static_mode || setting->dynamic_mode)
		{
			if (!is_suffix(dirent_ptr->d_name, SUFFIX_SQL))
			{
				fprintf(stderr, "invalid file name :\"%s/%s\"\n", input_dir, dirent_ptr->d_name);
				ADBLOADER_LOG(LOG_ERROR, "[main][thread main ] invalid file name :\"%s/%s\"\n", input_dir, dirent_ptr->d_name);
				continue;
			}
		}

		table_name = get_table_name(dirent_ptr->d_name);
		if (!is_create_in_adb_cluster(table_name))
		{
			fprintf(stderr, "Error: table \"%s\" does not exist in adb cluster.\n", table_name);
			ADBLOADER_LOG(LOG_ERROR, "[main][thread main ] table \"%s\" does not exist in adb cluster.\n", table_name);

			new_file_name  = rename_file_name(dirent_ptr->d_name, input_dir, SUFFIX_ERROR);
			if (new_file_name != NULL)
			{
				pfree(new_file_name);
				new_file_name = NULL;
			}

			continue;
		}

		new_file_name  = rename_file_name(dirent_ptr->d_name, input_dir, SUFFIX_SCAN);
		file_full_path = get_full_path(new_file_name, input_dir);

		ptr = tables_dynamic->info;
		tail = tables_dynamic->info;

		while(ptr != NULL)
		{
			if (strcmp(ptr->table_name, table_name) == 0)
			{
				file_location = (FileLocation*)palloc0(sizeof(FileLocation));
				file_location->location = file_full_path;
				slist_push_head(&ptr->file_head, &file_location->next);

				ptr->file_nums ++;
				break;
			}

			if (ptr->next == NULL)
				tail = ptr;

			ptr = ptr->next;
		}

		if (ptr == NULL)
		{
			table_info_dynamic = (TableInfo *)palloc0(sizeof(TableInfo));
			table_info_dynamic->file_nums ++;
			table_info_dynamic->table_name = table_name;

			slist_init(&table_info_dynamic->file_head);

			file_location = (FileLocation*)palloc0(sizeof(FileLocation));
			file_location->location = file_full_path;
			slist_push_head(&table_info_dynamic->file_head, &file_location->next);

			tables_dynamic->table_nums ++;

			if (tables_dynamic->info == NULL)
				tables_dynamic->info = table_info_dynamic;
			else
				tail->next = table_info_dynamic;
		}
	}

	closedir((DIR*)dir);

	return tables_dynamic;
}

static bool
is_create_in_adb_cluster(char *table_name)
{
	char query[QUERY_MAXLEN] = {0};
	PGconn       *conn = NULL;
	PGresult     *res = NULL;
	int          numtuples = 0;

	conn = PQconnectdb(setting->coordinator_info->connection);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select pcrelid "
					"from pgxc_class "
					"where pcrelid = ( "
						"select oid "
						"from pg_class "
						"where relname = \'%s\');", table_name);

	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	numtuples = PQntuples(res);

	PQclear(res);
	PQfinish(conn);

	if (numtuples == 1) /* create table in adb cluster */
		return true;
	else                /* no create table in adb cluster */
		return false;
}

static char *
get_table_name(char *file_name)
{
	char * file_name_local = NULL;
	int file_name_local_len;

	file_name_local = pg_strdup(file_name);
	file_name_local_len = strlen(file_name_local);

	while (file_name_local[file_name_local_len] != '_')
		file_name_local_len --;
	file_name_local[file_name_local_len] = '\0';

	return adb_load_tolower(file_name_local);
}

static char *
rename_file_name(char *file_name, char *input_dir, char *suffix)
{
	char old_file_name_path[1024] = {0};
	char new_file_name_path[1024] = {0};
	char *new_fiel_name = NULL;

	sprintf(old_file_name_path, "%s/%s", input_dir, file_name);
	sprintf(new_file_name_path, "%s/%s%s", input_dir, file_name, suffix);

	if (rename(old_file_name_path, new_file_name_path) < 0)
	{
		fprintf(stderr, "could not rename file \"%s\" to \"%s\" : %s \n",
				old_file_name_path, new_file_name_path, strerror(errno));
	}

	new_fiel_name = (char *)palloc0(strlen(file_name) + strlen(suffix) + 1);
	sprintf(new_fiel_name, "%s%s", file_name, suffix);

	return new_fiel_name;
}

static void
rename_file_suffix(char *old_file_path, char *suffix)
{
	char new_file_path[1024] = {0};
	char tmp[1024] = {0};
	int tmp_len;

	strcpy(tmp, old_file_path);

	tmp_len = strlen(tmp);
	while(tmp[tmp_len] != '.')
		tmp_len --;

	tmp[tmp_len] = '\0';
	sprintf(new_file_path, "%s%s", tmp, suffix);

	if (rename(old_file_path, new_file_path) < 0)
	{
		fprintf(stderr, "could not rename file \"%s\" to \"%s\" : %s \n",
				old_file_path, new_file_path, strerror(errno));
	}

	return;
}

static char *
get_full_path(char *file_name, char *input_dir)
{
	int file_name_len;
	int input_dir_len;
	char *full_path = NULL;

	file_name_len = strlen(file_name);
	input_dir_len = strlen(input_dir);

	full_path = (char *)palloc0(file_name_len + input_dir_len + 2); // 2 = '\0' and '/'
	sprintf(full_path, "%s/%s", input_dir, file_name);

	return full_path;
}

static bool
is_suffix(char *str, char *suffix)
{
	char *ptr = NULL;

	Assert(str != NULL);

	ptr = strrchr(str, '.');
	if (ptr == NULL)
		return false;

	if (strcmp(ptr, suffix) == 0)
		return true;
	else
		return false;
}

char *
get_outqueue_name (int datanode_num)
{
	char *name = NULL;
	LineBuffer * lineBuffer;

	lineBuffer = get_linebuf();
	appendLineBufInfo(lineBuffer, "queue%d", datanode_num);

	name = (char*)palloc0(lineBuffer->len + 1);
	memcpy(name, lineBuffer->data, lineBuffer->len);
	name[lineBuffer->len] = '\0';

	release_linebuf(lineBuffer);
	return name;
}

static bool
do_replaciate_roundrobin(DISTRIBUTE distribute_by, char *filepath, TableInfo *table_info)
{
	MessageQueuePipe **output_queue = NULL;
	DispatchInfo      *dispatch = NULL;
	ReadInfo          *read_info = NULL;
	DatanodeInfo      *datanode_info = NULL;
	char              *queue_name = NULL;
	int                res;
	int                file_total_line = 0;
	bool               read_finish = false;
	bool               dispatch_finish = false;
	bool               do_success = true;
	bool               need_redo = false;
	char              *start = NULL;
	int               *thread_send_total;
	int                output_queue_total;
	int                i = 0;
	LineBuffer        *linebuf = NULL;
	LogSummaryThreadInfo * log_summary_thread_info = NULL;

	Assert(filepath != NULL);
	Assert(table_info != NULL);

	linebuf = get_linebuf();

	if (setting->process_bar)
	{
		file_total_line = get_file_total_line_num(filepath);
		thread_send_total = palloc0(sizeof(int) * table_info->use_datanodes_num);
	}

	if (!is_create_in_adb_cluster(table_info->table_name))
	{
		char error_str[1024] = {0};
		sprintf(error_str, "Error: table \"%s\" does not exist in adb cluster." , table_info->table_name);
		main_write_error_message(table_info->distribute_type, error_str, NULL);

		do_success = false;
		return do_success;
	}

	start = create_start_command(filepath, setting, table_info);
	if (start == NULL)
	{
		main_write_error_message(table_info->distribute_type, "create start cmd error", NULL);
		exit(EXIT_FAILURE);
	}

	/*get datanode info from table_info struct*/
	datanode_info = (DatanodeInfo*)palloc0(sizeof(DatanodeInfo));
	datanode_info->node_nums = table_info->use_datanodes_num;
	datanode_info->datanode = (Oid *)palloc0(sizeof(Oid) * table_info->use_datanodes_num);
	datanode_info->conninfo = (char **)palloc0(sizeof(char *) * table_info->use_datanodes_num);
	for (i = 0; i < table_info->use_datanodes_num; i++)
	{
		datanode_info->datanode[i] = table_info->use_datanodes[i]->node_oid;
		datanode_info->conninfo[i] = pg_strdup(table_info->use_datanodes[i]->connection);
	}

	/*get dispatch info. */
	dispatch = (DispatchInfo *)palloc0(sizeof(DispatchInfo));
	dispatch->datanodes_num = table_info->use_datanodes_num;
	dispatch->threads_num_per_datanode = table_info->threads_num_per_datanode;
	output_queue_total = dispatch->datanodes_num * dispatch->threads_num_per_datanode;

	output_queue = (MessageQueuePipe**)palloc0(sizeof(MessageQueuePipe*) * output_queue_total);
	for (i = 0; i < output_queue_total; i++)
	{
		output_queue[i] =  (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));

		output_queue[i]->write_lock = false;
		queue_name = get_outqueue_name(i);
		mq_pipe_init(output_queue[i], queue_name);

		pfree(queue_name);
		queue_name = NULL;
	}

	dispatch->output_queue = output_queue;
	dispatch->conninfo_agtm = pg_strdup(setting->agtm_info->connection);
	dispatch->datanode_info = datanode_info;
	dispatch->table_name = pg_strdup(table_info->table_name);

	if (setting->hash_config->copy_option != NULL)
	{
		dispatch->copy_options = pg_strdup(setting->hash_config->copy_option);
	}

	dispatch->process_bar = setting->process_bar;
	dispatch->just_check = setting->just_check;
	dispatch->copy_cmd_comment = setting->copy_cmd_comment;
	dispatch->copy_cmd_comment_str = pg_strdup(setting->copy_cmd_comment_str);
	set_dispatch_file_start_cmd(start);

	/* start dispatch module  */
	if ((res = init_dispatch_threads(dispatch, TABLE_REPLICATION)) != DISPATCH_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "start dispatch module failed");
		main_write_error_message(table_info->distribute_type, "start dispatch module failed", start);
		exit(EXIT_FAILURE);
	}

	/* start read_producer module last */
	read_info = (ReadInfo *)palloc0(sizeof(ReadInfo));
	read_info->filepath = pg_strdup(filepath);
	read_info->input_queue = NULL;
	read_info->output_queue = output_queue;
	read_info->output_queue_num = output_queue_total;
	read_info->datanodes_num = table_info->use_datanodes_num;
	read_info->threads_num_per_datanode = setting->threads_num_per_datanode;

	if (distribute_by == DISTRIBUTE_BY_REPLICATION)
	{
		read_info->replication = true;
	}
	else if (distribute_by == DISTRIBUTE_BY_ROUNDROBIN)
	{
		read_info->roundrobin = true;
	}else
	{
		ADBLOADER_LOG(LOG_ERROR, "distribute by for table is wrong.\n");
		main_write_error_message(table_info->distribute_type, "distribute by for table is wrong.", start);
		exit(EXIT_FAILURE);
	}

	log_summary_thread_info = (LogSummaryThreadInfo *)palloc0(sizeof(LogSummaryThreadInfo));
	log_summary_thread_info->threshold_value = setting->error_threshold;
	if ((res = init_log_summary_thread(log_summary_thread_info)) != LOG_SUMMARY_RES_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,"start log summary module failed");
		main_write_error_message(table_info->distribute_type, "start log summary module failed", start);
		exit(EXIT_FAILURE);
	}

	read_info->end_flag_num = 0;
	read_info->start_cmd = start;
	read_info->read_file_buffer = setting->read_file_buffer;
	read_info->redo_queue_index = setting->redo_queue_index;
	read_info->redo_queue_total = setting->redo_queue_total;
	read_info->redo_queue = setting->redo_queue;
	read_info->filter_first_line = setting->filter_first_line;
	read_info->stream_mode = setting->stream_mode;
	if ((res = init_read_thread(read_info)) != READ_PRODUCER_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,"start read_producer module failed");
		main_write_error_message(table_info->distribute_type, "start read_producer module failed", start);
		exit(EXIT_FAILURE);
	}

	/* check module state every 1s */
	for (;;)
	{
		ReadProducerState  state = READ_PRODUCER_PROCESS_DEFAULT;
		DispatchThreads   *dispatch_exit = NULL;
		int                i = 0;

		if (setting->process_bar)
		{
			memset(thread_send_total, 0 , sizeof(int) * table_info->use_datanodes_num);
			get_sent_conut(thread_send_total);
			show_process(file_total_line, table_info->use_datanodes_num,
								thread_send_total, DISTRIBUTE_BY_REPLICATION);
		}

		/* all modules complete */
		if (read_finish && dispatch_finish)
		{
			ADBLOADER_LOG(LOG_INFO, "all modules complete");
			break;
		}

		/*read data file have some error*/
		if (!read_finish)
		{
			state = GetReadModule();
			if (state == READ_PRODUCER_PROCESS_ERROR)
			{
				stop_read_thread();
				/* read module error, stop dispatch */
				stop_dispatch_threads();
				read_finish = true;
				dispatch_finish = true;
				do_success = false;

				/* need redo for all threads */
				ADBLOADER_LOG(LOG_ERROR,"read data file module failed");
				main_write_error_message(table_info->distribute_type, "read data file module failed", start);

				break;
			}
			else if (state == READ_PRODUCER_PROCESS_COMPLETE)
			{
				read_finish = true;
			}

		}

		dispatch_exit = get_dispatch_exit_threads();
		if (dispatch_exit->send_thread_cur != dispatch_exit->send_thread_count)
		{
			sleep(1);
			continue;
		}
		else
		{
			appendLineBufInfo(linebuf, "%s", start);
			appendLineBufInfo(linebuf, "%s", " -Q ");

			for (i = 0; i < dispatch_exit->send_thread_count; i++)
			{
				DispatchThreadInfo *dispatch_thread = NULL;
				dispatch_thread = dispatch_exit->send_threads[i];
				if (dispatch_thread != NULL)
				{
					if (dispatch_thread->state != DISPATCH_THREAD_EXIT_NORMAL)
					{
						need_redo = true;
						appendLineBufInfo(linebuf, "%d,", i);
					}
				}
			}

			if (need_redo)
			{
				char *local_char = NULL;

				local_char = strrchr(linebuf->data, ',');
				*local_char = ' ';

				do_success = false;
			}
			else
			{
				do_success = true;
			}

			dispatch_finish = true;

		}

		sleep(1);
	}

	if (!stop_log_summary_thread())
	{
		ADBLOADER_LOG(LOG_ERROR, "stop log summary thread failed");
		main_write_error_message(table_info->distribute_type, "stop log summary module failed", start);
		exit(EXIT_FAILURE);
	}

	if (need_redo && !setting->just_check)
	{
		ADBLOADER_LOG(LOG_ERROR,"dispatch module failed");
		main_write_error_message(table_info->distribute_type, "dispatch module failed", linebuf->data);
	}

	release_linebuf(linebuf);
	clean_dispatch_resource();

    /* just free output queeu once , read threads and dispatch threads all used */
	if (dispatch->output_queue != NULL)
	{
	    int output_queue_total = 0;
        int flag = 0;

		output_queue_total = dispatch->datanodes_num * dispatch->threads_num_per_datanode;
		for (flag = 0; flag < output_queue_total; flag++)
		{
			mq_pipe_destory(dispatch->output_queue[flag]);
		}
		pg_free(dispatch->output_queue);
		dispatch->output_queue = NULL;
	}

	/* free dispatch */
	free_dispatch_info(dispatch);
	pg_free(dispatch);
	dispatch = NULL;

	free_read_info(read_info);
	pg_free(read_info);
	read_info = NULL;

	return do_success;
}

void
clean_replaciate_roundrobin(void)
{ return;}

static bool
do_hash_module(char *filepath, const TableInfo *table_info)
{
	MessageQueuePipe  *input_queue = NULL;
	MessageQueuePipe **output_queue = NULL;
	HashField          *field = NULL;
	DispatchInfo       *dispatch = NULL;
	ReadInfo           *read_info = NULL;
	HashComputeInfo    *hash_info = NULL;
	DatanodeInfo       *datanode_info = NULL;
	char               *queue_name = NULL;
	char               *func_name = NULL;
	char               *start = NULL;
	int                *thread_send_total = NULL;
	LineBuffer         *linebuf = NULL;
	int                 flag = 0;
	int                 res = 0;
	int                 file_total_line = 0;
	bool                read_finish = false;
	bool                hash_finish = false;
	bool                dispatch_finish = false;
	bool                do_success = true;
	bool                do_read_file_success = true;
	bool                do_hash_success = true;
	bool                do_dispatch_success = true;
	bool                need_redo = false;
	int                 output_queue_total = 0;
	int                 i = 0;
	LogSummaryThreadInfo *log_summary_thread_info = NULL;

	if (setting->process_bar)
	{
		file_total_line = get_file_total_line_num(filepath);
		thread_send_total = palloc0(sizeof(int) * table_info->use_datanodes_num);
	}

	start = create_start_command(filepath, setting, table_info);
	if (start == NULL)
	{
		main_write_error_message(table_info->distribute_type, "creat start command failed", NULL);
		exit(EXIT_FAILURE);
	}

	if (!is_create_in_adb_cluster(table_info->table_name))
	{
		char error_str[1024] = {0};
		sprintf(error_str, "Error: table \"%s\" does not exist in adb cluster." , table_info->table_name);
		main_write_error_message(table_info->distribute_type, error_str, NULL);

		do_success = false;
		return do_success;
	}

	/* init input queue */
	input_queue = (MessageQueuePipe *)palloc0(sizeof(MessageQueuePipe));
	mq_pipe_init(input_queue, "input_queue");

	datanode_info = (DatanodeInfo*)palloc0(sizeof(DatanodeInfo));
	datanode_info->node_nums = table_info->use_datanodes_num;
	datanode_info->datanode = (Oid *)palloc0(sizeof(Oid) * table_info->use_datanodes_num);
	datanode_info->conninfo = (char **)palloc0(sizeof(char *) * table_info->use_datanodes_num);
	for (flag = 0; flag < table_info->use_datanodes_num; flag++)
	{
		datanode_info->datanode[flag] = table_info->use_datanodes[flag]->node_oid;
		datanode_info->conninfo[flag] = pg_strdup(table_info->use_datanodes[flag]->connection);
	}

	/* init output queue */
	output_queue_total = table_info->use_datanodes_num * table_info->threads_num_per_datanode;
	output_queue = (MessageQueuePipe **)palloc0(sizeof(MessageQueuePipe *) * output_queue_total);
	for (i = 0; i < output_queue_total; i++)
	{
		output_queue[i] = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
		output_queue[i]->write_lock = true;
		queue_name = get_outqueue_name(i);
		mq_pipe_init(output_queue[i], queue_name);

		pfree(queue_name);
		queue_name = NULL;
	}

	/* get table base attribute */
	field = (HashField *)palloc0(sizeof(HashField));
	deep_copy(field, table_info->table_attribute);

	/* start hash module first */
	if (table_info->distribute_type == DISTRIBUTE_BY_DEFAULT_HASH ||
		table_info->distribute_type == DISTRIBUTE_BY_DEFAULT_MODULO)
	{
		Assert(field->field_nums == 1);
		func_name = conver_type_to_fun(field->field_type[0], DISTRIBUTE_BY_DEFAULT_HASH);
	}
	else if (table_info->distribute_type == DISTRIBUTE_BY_USERDEFINED)
	{
		Assert(field->field_nums > 0);
		func_name = pg_strdup(field->func_name);
	}

	set_hash_file_start_cmd(start);

	hash_info = (HashComputeInfo *)palloc0(sizeof(HashComputeInfo));
	hash_info->datanodes_num = table_info->use_datanodes_num;
	hash_info->threads_num_per_datanode = table_info->threads_num_per_datanode;
	hash_info->conninfo = pg_strdup(setting->server_info->connection);
	hash_info->input_queue = input_queue;
	hash_info->output_queue = output_queue;
	hash_info->output_queue_num = output_queue_total;
	hash_info->func_name = pg_strdup(func_name);

	pfree(func_name);
	func_name = NULL;

	hash_info->hash_field = field;
	hash_info->redo_queue = setting->redo_queue;
	hash_info->redo_queue_total = setting->redo_queue_total;
	hash_info->redo_queue_index = setting->redo_queue_index;
	hash_info->filter_queue_file = setting->filter_queue_file;
	hash_info->table_name = pg_strdup(table_info->table_name);
	hash_info->filter_queue_file_path = pg_strdup(setting->filter_queue_file_path);
	hash_info->copy_cmd_comment = setting->copy_cmd_comment;
	hash_info->copy_cmd_comment_str = pg_strdup(setting->copy_cmd_comment_str);
	if ((res = init_hash_compute(hash_info)) != HASH_COMPUTE_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "start hash module failed");
			main_write_error_message(table_info->distribute_type, "start hash module failed", start);
		exit(EXIT_FAILURE);
	}

	/* start dispatch module */
	dispatch = (DispatchInfo *)palloc0(sizeof(DispatchInfo));
	dispatch->conninfo_agtm = pg_strdup(setting->agtm_info->connection);
	dispatch->datanodes_num = table_info->use_datanodes_num;
	dispatch->threads_num_per_datanode = table_info->threads_num_per_datanode;
	dispatch->output_queue = output_queue;
	dispatch->datanode_info = datanode_info;
	dispatch->table_name = pg_strdup(table_info->table_name);

	if (setting->hash_config->copy_option != NULL)
	{
		dispatch->copy_options = pg_strdup(setting->hash_config->copy_option);
	}

	dispatch->process_bar = setting->process_bar;
	dispatch->just_check = setting->just_check;
	dispatch->copy_cmd_comment = setting->copy_cmd_comment;
	dispatch->copy_cmd_comment_str = pg_strdup(setting->copy_cmd_comment_str);

	set_dispatch_file_start_cmd(start);
	if ((res = init_dispatch_threads(dispatch, TABLE_DISTRIBUTE)) != DISPATCH_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "start dispatch module failed");
		main_write_error_message(table_info->distribute_type, "start dispatch module failed", start);
		exit(EXIT_FAILURE);
	}

	log_summary_thread_info = (LogSummaryThreadInfo *)palloc0(sizeof(LogSummaryThreadInfo));
	log_summary_thread_info->threshold_value = setting->error_threshold;
	if ((res = init_log_summary_thread(log_summary_thread_info)) != LOG_SUMMARY_RES_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,"start log summary module failed");
		main_write_error_message(table_info->distribute_type, "start log summary module failed", start);
		exit(EXIT_FAILURE);
	}

	/* start read_producer module last */
	read_info = (ReadInfo *)palloc0(sizeof(ReadInfo));
	read_info->filepath = pg_strdup(filepath);
	read_info->input_queue = input_queue;
	read_info->output_queue = NULL;
	read_info->output_queue_num = 0;
	read_info->datanodes_num = 0;
	read_info->threads_num_per_datanode = 0;
	read_info->replication = false;
	read_info->end_flag_num = setting->hash_config->hash_thread_num;
	read_info->start_cmd = start;
	read_info->read_file_buffer = setting->read_file_buffer;
	read_info->redo_queue_index = setting->redo_queue_index;
	read_info->redo_queue_total = setting->redo_queue_total;
	read_info->redo_queue = setting->redo_queue;
	read_info->filter_first_line = setting->filter_first_line;
	read_info->stream_mode = setting->stream_mode;
	if ((res = init_read_thread(read_info)) != READ_PRODUCER_OK)

	{
		ADBLOADER_LOG(LOG_ERROR, "start read module failed");
		main_write_error_message(table_info->distribute_type, "start read module failed", start);
		exit(EXIT_FAILURE);
	}

	/* check module state every 1s */
	for (;;)
	{
		ReadProducerState  state = READ_PRODUCER_PROCESS_DEFAULT;
		DispatchThreads   *dispatch_exit = NULL;
		HashThreads       *hash_exit = NULL;
		int                flag = 0;

		if (setting->process_bar)
		{
			memset(thread_send_total, 0 , sizeof(int) * table_info->use_datanodes_num);
			get_sent_conut(thread_send_total);
			show_process(file_total_line, table_info->use_datanodes_num,
								thread_send_total, DISTRIBUTE_BY_DEFAULT_HASH);
		}

		/* all modules complete*/
		if (read_finish && hash_finish && dispatch_finish)
		{
			ADBLOADER_LOG(LOG_INFO, "all modules complete");
			break;
		}

		/* check read data file module status */
		if (!read_finish)
		{
			state = GetReadModule();
			if (state == READ_PRODUCER_PROCESS_ERROR)
			{
				stop_read_thread();
				stop_hash_threads(); /* stop hash threads and then dispatch threads */
				stop_dispatch_threads();

				read_finish = true;
				hash_finish = true;
				dispatch_finish = true;
				do_read_file_success = false;

				/* need redo for all threads */
				ADBLOADER_LOG(LOG_ERROR,"read data file module failed");
				main_write_error_message(table_info->distribute_type, "read data file module failed", start);

				break;
			}
			else if (state == READ_PRODUCER_PROCESS_COMPLETE)
			{
				do_read_file_success = true;
				read_finish = true;
			}
		}

		/* check hash module status */
		hash_exit = get_exit_threads_info();
		if (hash_exit->hs_thread_cur == 0)
		{
			sleep(2);
			continue;
		}

		if (!hash_finish && hash_exit->hs_thread_cur == hash_exit->hs_thread_count)
		{
			hash_finish = true;

			if (setting->filter_queue_file)
				fclose_filter_queue_file_fd(setting->redo_queue_total);

			pthread_mutex_lock(&hash_exit->mutex);
			for (flag =0; flag < hash_exit->hs_thread_count; flag++)
			{
				ComputeThreadInfo * hash_thread_info = hash_exit->hs_threads[flag];
				Assert(hash_thread_info != NULL);

				if (hash_thread_info->state != THREAD_EXIT_NORMAL)
				{
					do_hash_success = false;

					pthread_mutex_unlock(&hash_exit->mutex);
					break;
				}
				else
				{
					do_hash_success = true;
				}
			}

			pthread_mutex_unlock(&hash_exit->mutex);
		}

		/* dispatch data module */
		dispatch_exit = get_dispatch_exit_threads();
		if (dispatch_exit->send_thread_cur != dispatch_exit->send_thread_count)
		{
			sleep(1);
			continue;
		}
		else
		{
			linebuf = get_linebuf();
			appendLineBufInfo(linebuf, "%s", start);
			appendLineBufInfo(linebuf, "%s", " -Q ");

			pthread_mutex_lock(&dispatch_exit->mutex);
			for (flag = 0; flag < dispatch_exit->send_thread_count; flag++)
			{
				DispatchThreadInfo *dispatch_thread = NULL;
				dispatch_thread = dispatch_exit->send_threads[flag];
				if (dispatch_thread != NULL)
				{
					if (dispatch_thread->state != DISPATCH_THREAD_EXIT_NORMAL)
					{
						need_redo = true;
						appendLineBufInfo(linebuf, "%d,", flag);
					}
				}
			}

			if (need_redo)
			{
				char *local_char = NULL;
				local_char = strrchr(linebuf->data, ',');
				*local_char = ' ';

				do_dispatch_success = false;
			}
			else
			{
				do_dispatch_success = true;
			}

			dispatch_finish = true;

			pthread_mutex_unlock(&dispatch_exit->mutex);
		}

		sleep(2);
	}

	if (!stop_log_summary_thread())
	{
		ADBLOADER_LOG(LOG_ERROR, "stop log summary thread failed");
		main_write_error_message(table_info->distribute_type, "stop log summary module failed", start);
		exit(EXIT_FAILURE);
	}

	if (need_redo && !setting->just_check)
	{
		ADBLOADER_LOG(LOG_ERROR,"dispatch module failed");
		main_write_error_message(table_info->distribute_type, "dispatch module failed", linebuf->data);
	}

	release_linebuf(linebuf);

	do_success = (do_read_file_success && do_hash_success && do_dispatch_success);

	clean_hash_resource();
	clean_dispatch_resource();

	/* free input queue */
	if (input_queue != NULL)
	{
		mq_pipe_destory(input_queue);
		pg_free(input_queue);
		input_queue = NULL;
	}

	/* free output queue memory */
	if (read_info->output_queue != NULL)
	{
		int output_queue_total = 0;
	    int i = 0;

		output_queue_total = read_info->datanodes_num * read_info->threads_num_per_datanode;
		for (i = 0; i < output_queue_total; i++)
		{
			mq_pipe_destory(read_info->output_queue[i]);
		}

		pg_free(read_info->output_queue);
		read_info->output_queue = NULL;
	}

	/* free dispatch */
	free_dispatch_info(dispatch);
	pg_free(dispatch);
	dispatch = NULL;

	/*free hash info */
	free_hash_info(hash_info);
	pg_free(hash_info);
	hash_info = NULL;

	/* free read_info */
	free_read_info(read_info);
	pg_free(read_info);
	read_info = NULL;

	return do_success;
}

void
clean_hash_module(void)
{ return;}

static void
get_table_attribute(ADBLoadSetting *setting, TableInfo *table_info)
{
	char distribute_by;
	char *coord_conn_info = NULL;

	coord_conn_info = setting->coordinator_info->connection;

	distribute_by = get_distribute_by(coord_conn_info, table_info->table_name);
	switch (distribute_by)
	{
		case 'R':
			table_info->distribute_type = DISTRIBUTE_BY_REPLICATION;
			break;
		case 'N':
			table_info->distribute_type = DISTRIBUTE_BY_ROUNDROBIN;
			break;
		case 'H':
			table_info->distribute_type = DISTRIBUTE_BY_DEFAULT_HASH;
			break;
		case 'M':
			table_info->distribute_type = DISTRIBUTE_BY_DEFAULT_MODULO;
			break;
		case 'U':
			table_info->distribute_type = DISTRIBUTE_BY_USERDEFINED;
			break;
		default:
			break;
	}

	if (table_info->distribute_type == DISTRIBUTE_BY_DEFAULT_HASH)
	{
		get_hash_field(coord_conn_info, table_info);
	}
	else if (table_info->distribute_type == DISTRIBUTE_BY_USERDEFINED)
	{
		get_userdefined_funcinfo(coord_conn_info, table_info);
		reset_hash_field(table_info);

		drop_func_to_server(setting->server_info->connection, table_info->funcinfo->drop_func_sql);
		create_func_to_server(setting->server_info->connection, table_info->funcinfo->creat_func_sql);
	}

	return;
}

static void
get_use_datanodes(ADBLoadSetting *setting, TableInfo *table_info)
{
	char query[QUERY_MAXLEN];
	PGconn       *conn;
	PGresult     *res;
	int          numtuples;
	int          i = 0;
	int          j = 0;
	NodeInfoData **use_datanodes_info = NULL;
	Oid          *datanodes_oid = NULL;

	conn = PQconnectdb(setting->coordinator_info->connection);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Error: Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select unnest(nodeoids) as nodeoids "
					"from pgxc_class "
					"where pcrelid = ("
						"select oid from pg_class where relname ='%s');", table_info->table_name);

	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Error: Connection return value failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	if (PQntuples(res) == 0)
	{
		fprintf(stderr, "Error: table information is wrong. \n");
		PQclear(res);
		exit_nicely(conn);
	}

	numtuples = PQntuples(res);
	if (numtuples > setting->datanodes_num)
	{
		fprintf(stderr, "Error: table : \'%s\' use datanode num is wrong. \n", table_info->table_name);
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	datanodes_oid = (Oid *)palloc0(numtuples * sizeof(Oid));

	for (i = 0; i< numtuples; i++)
	{
		datanodes_oid[i] = atoi(PQgetvalue(res, i, PQfnumber(res, "nodeoids")));
	}

	use_datanodes_info = (NodeInfoData **)palloc0(numtuples * sizeof(NodeInfoData *));
	for (i = 0; i < numtuples; i++)
	{
		use_datanodes_info[i] = (NodeInfoData *)palloc0(sizeof(NodeInfoData));
	}

	for (i = 0; i < numtuples; i++)
	{
		for(j = 0; j < setting->datanodes_num; j++)
		{
			if (datanodes_oid[i] == setting->datanodes_info[j]->node_oid)
			{
				use_datanodes_info[i]->node_oid = setting->datanodes_info[j]->node_oid;
				use_datanodes_info[i]->node_name = pg_strdup(setting->datanodes_info[j]->node_name);
				use_datanodes_info[i]->node_host = pg_strdup(setting->datanodes_info[j]->node_host);
				use_datanodes_info[i]->node_port = pg_strdup(setting->datanodes_info[j]->node_port);
				use_datanodes_info[i]->user_name = pg_strdup(setting->datanodes_info[j]->user_name);
				use_datanodes_info[i]->database_name = pg_strdup(setting->datanodes_info[j]->database_name);
				use_datanodes_info[i]->connection = pg_strdup(setting->datanodes_info[j]->connection);
				break;
			}
			else
			{
				continue;
			}
		}
	}

	table_info->threads_num_per_datanode = setting->threads_num_per_datanode;
	table_info->use_datanodes_num = numtuples;
	table_info->use_datanodes = use_datanodes_info;

	qsort(table_info->use_datanodes,
					table_info->use_datanodes_num,
					sizeof(NodeInfoData*),
					adbloader_cmp_nodes);

	if (datanodes_oid != NULL)
	{
		free(datanodes_oid);
		datanodes_oid = NULL;
	}

	PQclear(res);
	PQfinish(conn);

	return;
}

static void
get_use_datanodes_from_conf(ADBLoadSetting *setting, TableInfo *table_info)
{
	NodeInfoData **use_datanodes_info = NULL;
	int i = 0;
	int use_datanode_num = 0;

	Assert(setting != NULL);
	Assert(table_info != NULL);
	Assert(setting->datanodes_num > 0);

	use_datanode_num = setting->datanodes_num;
	use_datanodes_info = (NodeInfoData **)palloc0(use_datanode_num * sizeof(NodeInfoData *));
	for (i = 0; i < use_datanode_num; i++)
	{
		use_datanodes_info[i] = (NodeInfoData *)palloc0(sizeof(NodeInfoData));
	}

	for (i = 0; i < use_datanode_num; i++)
	{
		use_datanodes_info[i]->node_oid = setting->datanodes_info[i]->node_oid;
		use_datanodes_info[i]->node_name = pg_strdup(setting->datanodes_info[i]->node_name);
		use_datanodes_info[i]->node_host = pg_strdup(setting->datanodes_info[i]->node_host);
		use_datanodes_info[i]->node_port = pg_strdup(setting->datanodes_info[i]->node_port);
		use_datanodes_info[i]->user_name = pg_strdup(setting->datanodes_info[i]->user_name);
		use_datanodes_info[i]->database_name = pg_strdup(setting->datanodes_info[i]->database_name);
		use_datanodes_info[i]->connection = pg_strdup(setting->datanodes_info[i]->connection);
	}

	table_info->threads_num_per_datanode = setting->threads_num_per_datanode;
	table_info->use_datanodes_num = use_datanode_num;
	table_info->use_datanodes = use_datanodes_info;

	qsort(table_info->use_datanodes,
					use_datanode_num,
					sizeof(NodeInfoData*),
					adbloader_cmp_nodes);

	return;
}

static void
reset_hash_field(TableInfo *table_info)
{
	HashField *hashfield = NULL;

	hashfield = (HashField *)palloc0(sizeof(HashField));

	hashfield->datanodes_num = table_info->funcinfo->node_count;
	hashfield->node_list = (Oid *)palloc0(hashfield->datanodes_num * sizeof(Oid));
	memcpy(hashfield->node_list, table_info->funcinfo->node_list, (hashfield->datanodes_num * sizeof(Oid)));

	hashfield->field_nums = table_info->funcinfo->func_args_count;
	hashfield->field_loc  = (int *)palloc0(hashfield->field_nums * sizeof(int));
	hashfield->field_type = (Oid *)palloc0(hashfield->field_nums * sizeof(Oid));
	memcpy(hashfield->field_loc, table_info->funcinfo->table_loc,       hashfield->field_nums * sizeof(int));
	memcpy(hashfield->field_type, table_info->funcinfo->func_args_type, hashfield->field_nums * sizeof(Oid));

	hashfield->func_name = pg_strdup(table_info->funcinfo->func_name);

	hashfield->hash_threads_num = setting->hash_config->hash_thread_num;
	hashfield->text_delim   = pg_strdup(setting->hash_config->text_delim);
	hashfield->copy_options = pg_strdup(setting->hash_config->copy_option);
	hashfield->quotec       = setting->hash_config->copy_quotec[0];
	hashfield->has_qoute    = false;
	hashfield->hash_delim   = pg_strdup(setting->hash_config->hash_delim);

	table_info->table_attribute = hashfield;
}

static void
drop_func_to_server(char *serverconninfo, char *drop_func_sql)
{
	PGconn   *conn = NULL;
	PGresult *res = NULL;

	conn = PQconnectdb(serverconninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to server failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	res = PQexec(conn, drop_func_sql);
	if ( !res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Connection to server failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	PQclear(res);
	PQfinish(conn);

	return ;
}

static void
create_func_to_server(char *serverconninfo, char *creat_func_sql)
{
	PGconn   *conn = NULL;
	PGresult *res = NULL;

	conn = PQconnectdb(serverconninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to server failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	res = PQexec(conn, creat_func_sql);
	if ( !res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Connection to server failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	PQclear(res);
	PQfinish(conn);

	return ;
}

static void
get_userdefined_funcinfo(const char *conninfo, TableInfo *table_info)
{
	UserFuncInfo *userdefined_funcinfo = NULL;

	userdefined_funcinfo = (UserFuncInfo *)palloc0(sizeof(UserFuncInfo));
	get_func_args_count_and_type(conninfo, table_info->table_name, userdefined_funcinfo);
	get_table_loc_count_and_loc(conninfo,  table_info->table_name, userdefined_funcinfo);
	get_create_and_drop_func_sql(conninfo, table_info->table_name, userdefined_funcinfo);
	get_node_count_and_node_list(conninfo, table_info->table_name, userdefined_funcinfo);

	table_info->funcinfo = userdefined_funcinfo;

	return;
}

static void
get_node_count_and_node_list(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
{
	char query[QUERY_MAXLEN];
	PGconn     *conn = NULL;
	PGresult   *res = NULL;
	int i = 0;
	int local_node_nums = 0;

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select unnest(nodeoids) as nodeoids "
					"from pgxc_class "
					"where pcrelid = (select oid from pg_class where relname = \'%s\');",
					table_name);

	res = PQexec(conn, query);

	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	if ((local_node_nums = PQntuples(res)) == 0)
	{
		fprintf(stderr, "could not find the table \'%s\' in ADB cluster. \n", table_name);
		PQclear(res);
		exit_nicely(conn);
	}

	userdefined_funcinfo->node_count= local_node_nums;
	userdefined_funcinfo->node_list= (Oid *)palloc0(local_node_nums * sizeof(Oid));

	for (i = 0; i < local_node_nums; i++)
	{
		userdefined_funcinfo->node_list[i] = atoi(PQgetvalue(res, i, PQfnumber(res, "nodeoids")));
	}

	PQclear(res);
	PQfinish(conn);

	return ;
}

static void
get_hash_field(const char *conninfo, TableInfo *table_info)
{
	char query[QUERY_MAXLEN];
	PGconn     *conn;
	PGresult   *res;
	int i;
	HashField *hashfield = NULL;
	int local_node_nums;
	int local_field_nums;

	hashfield = (HashField *)palloc0(sizeof(HashField));

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select pcattnum, b.atttypid, unnest(nodeoids) as nodeoids "
					"from (select * from pgxc_class where pcrelid =(select oid from pg_class where relname = \'%s\')) as a,  pg_attribute as b "
					"where attrelid = (select oid from pg_class where relname = \'%s\') and b.attnum = a.pcattnum;",
					table_info->table_name, table_info->table_name);

	res = PQexec(conn, query);

	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	if ((local_node_nums = PQntuples(res)) == 0)
	{
		fprintf(stderr, "could not find the table \'%s\' in ADB cluster. \n", table_info->table_name);
		PQclear(res);
		exit_nicely(conn);
	}

	hashfield->datanodes_num = local_node_nums;
	hashfield->node_list = (Oid *)palloc0(local_node_nums * sizeof(Oid));

	for (i = 0; i < local_node_nums; i++)
	{
		hashfield->node_list[i] = atoi(PQgetvalue(res, i, PQfnumber(res, "nodeoids")));
	}

	local_field_nums = 1;
	hashfield->field_nums = local_field_nums;
	hashfield->field_loc  = (int *)palloc0(local_field_nums * sizeof(int));
	hashfield->field_type = (Oid *)palloc0(local_field_nums * sizeof(Oid));

	for (i = 0; i < local_field_nums; i++)
	{
		hashfield->field_loc[i]  = atoi(PQgetvalue(res, 0, PQfnumber(res, "pcattnum")));
		hashfield->field_type[i] = atoi(PQgetvalue(res, 0, PQfnumber(res, "atttypid")));
	}

	/*get other hashfield info*/
	hashfield->hash_threads_num = setting->hash_config->hash_thread_num;
	hashfield->text_delim        = pg_strdup(setting->hash_config->text_delim);
	hashfield->copy_options = pg_strdup(setting->hash_config->copy_option);
	hashfield->quotec       = setting->hash_config->copy_quotec[0];
	hashfield->has_qoute    = false;
	hashfield->hash_delim   = pg_strdup(setting->hash_config->hash_delim);
	table_info->table_attribute = hashfield;

	PQclear(res);
	PQfinish(conn);
	return ;
}

static char
get_distribute_by(const char *conninfo, const char *tablename)
{
	char query[QUERY_MAXLEN];
	PGconn     *conn;
	PGresult   *res;
	char distribute = '\0';

	conn = PQconnectdb(conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select pclocatortype "
					"from pgxc_class "
					"where pcrelid=( "
						"select oid from pg_class where relname = \'%s\');", tablename);

	res = PQexec(conn, query);
	if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
	{
		distribute = *(PQgetvalue(res, 0, 0));
	}
	else
	{
		fprintf(stderr, "get_distribute_by failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	PQclear(res);
	PQfinish(conn);
	return distribute;
}

static void
get_table_loc_count_and_loc(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
{
	char query[QUERY_MAXLEN];
	PGconn       *conn;
	PGresult     *res;
	int          numtuples;
	int          *table_loc;
	int          i = 0;

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select pcfuncid, unnest(pcfuncattnums) "
					"from pgxc_class "
					"where pcrelid = ("
						"select oid from pg_class where relname ='%s');", table_name);
	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	Assert(PQntuples(res) != 0);

	numtuples = PQntuples(res);
	table_loc = (int *)palloc0(sizeof(int)* numtuples);
	if (table_loc == NULL)
	{
		fprintf(stderr, "out of memory. \n");
		exit_nicely(conn);
	}

	for (i = 0; i < numtuples; i++)
		table_loc[i]  = atoi(PQgetvalue(res, i, 1));

	userdefined_funcinfo->table_loc = table_loc;
	userdefined_funcinfo->table_loc_count= numtuples;

	PQclear(res);
	PQfinish(conn);

}

#if 0
static void
get_table_loc_for_hash_modulo(const char *conninfo, const char *tablename, UserFuncInfo *funcinfo)
{
	char query[QUERY_MAXLEN];
	PGconn       *conn;
	PGresult     *res;
	int          *table_loc;

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select pcattnum "
					"from pgxc_class "
					"where pcrelid = ("
						"select oid from pg_class where relname ='%s');", tablename);
	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	Assert(PQntuples(res) == 1);
	table_loc = (int *)palloc0(sizeof(int));
	if (table_loc == NULL)
	{
		fprintf(stderr, "out of memory. \n");
		exit_nicely(conn);
	}

	table_loc[0]  = atoi(PQgetvalue(res, 0, 0));

	funcinfo->table_loc = table_loc;
	funcinfo->table_loc_count= PQntuples(res);

	PQclear(res);
	PQfinish(conn);
}
#endif

static void
get_func_args_count_and_type(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
{
	char query[QUERY_MAXLEN];
	PGconn       *conn;
	PGresult     *res;
	int          numtuples;
	Oid         *arg_type;
	int         i = 0;

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select pronargs, unnest(proargtypes) "
					"from pg_proc "
					"where oid = ("
						"select pcfuncid "
						"from pgxc_class "
						"where pcrelid = ("
							"select oid from pg_class where relname ='%s'));", table_name);
	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	Assert(PQntuples(res) != 0);

	numtuples = PQntuples(res);
	arg_type = (Oid *)palloc0(sizeof(Oid)* numtuples);

	for (i = 0; i < numtuples; i++)
		arg_type[i]  = atoi(PQgetvalue(res, i, 1));

	userdefined_funcinfo->func_args_type = arg_type;
	userdefined_funcinfo->func_args_count = numtuples;

	PQclear(res);
	PQfinish(conn);
}

static void
get_create_and_drop_func_sql(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
{
	char        query[QUERY_MAXLEN];
	LineBuffer *create_func_sql = NULL;
	char       *create_func = NULL;
	LineBuffer *drop_func_sql = NULL;
	char       *drop_func = NULL;
	PGconn     *conn = NULL;
	PGresult   *res = NULL;
	char       *proretset = NULL;
	char       *proname = NULL;
	char       *prosrc = NULL;
	//char     *probin = NULL;
	char       *funcargs = NULL;
	//char *funciargs = NULL;
	char       *funcresult = NULL;
	char       *proiswindow = NULL;
	char       *provolatile = NULL;
	char       *proisstrict = NULL;
	char       *prosecdef = NULL;
	char       *proleakproof = NULL;
	//char     *proconfig = NULL;
	char       *procost = NULL;
	char       *prorows = NULL;
	char       *lanname = NULL;

	create_func_sql = get_linebuf();
	drop_func_sql = get_linebuf();

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "SELECT proretset,"
							"proname,"
							"prosrc,"
							"probin,"
							"pg_catalog.pg_get_function_arguments(oid) AS funcargs,"
							"pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs,"
							"pg_catalog.pg_get_function_result(oid) AS funcresult,"
							"proiswindow,"
							"provolatile,"
							"proisstrict,"
							"prosecdef,"
							"proleakproof,"
							"proconfig,"
							"procost,"
							"prorows,"
							"(SELECT lanname FROM pg_catalog.pg_language WHERE oid =prolang) AS lanname "
					"FROM pg_catalog.pg_proc "
					"WHERE oid = ( "
						"select pcfuncid from pgxc_class where pcrelid = ( "
							"select oid from pg_class where relname ='%s'));", table_name);

	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	Assert(PQntuples(res) == 1);

	proretset = PQgetvalue(res, 0, PQfnumber(res, "proretset"));
	proname = PQgetvalue(res, 0, PQfnumber(res, "proname"));
	prosrc = PQgetvalue(res, 0, PQfnumber(res, "prosrc"));
	//probin = PQgetvalue(res, 0, PQfnumber(res, "probin"));
	funcargs = PQgetvalue(res, 0, PQfnumber(res, "funcargs"));
	//funciargs = PQgetvalue(res, 0, PQfnumber(res, "funciargs"));
	funcresult = PQgetvalue(res, 0, PQfnumber(res, "funcresult"));
	proiswindow = PQgetvalue(res, 0, PQfnumber(res, "proiswindow"));
	provolatile = PQgetvalue(res, 0, PQfnumber(res, "provolatile"));
	proisstrict = PQgetvalue(res, 0, PQfnumber(res, "proisstrict"));
	prosecdef = PQgetvalue(res, 0, PQfnumber(res, "prosecdef"));
	proleakproof = PQgetvalue(res, 0, PQfnumber(res, "proleakproof"));
	//proconfig = PQgetvalue(res, 0, PQfnumber(res, "proconfig"));
	procost = PQgetvalue(res, 0, PQfnumber(res, "procost"));
	prorows = PQgetvalue(res, 0, PQfnumber(res, "prorows"));
	lanname = PQgetvalue(res, 0, PQfnumber(res, "lanname"));

	appendLineBufInfoString(create_func_sql, "CREATE FUNCTION ");

	if (proname != NULL && funcargs != NULL)
	{
		appendLineBufInfo(create_func_sql, "%s(%s) ", proname, funcargs);
		appendLineBufInfo(drop_func_sql, "DROP FUNCTION IF EXISTS %s(%s);", proname, funcargs);
	}
	else
	{
		fprintf(stderr, "get user define function failed. \n");
		PQclear(res);
		exit_nicely(conn);
	}

	if (funcresult != NULL)
	{
		appendLineBufInfo(create_func_sql, "RETURNS %s ", funcresult);
	}
	else
	{
		fprintf(stderr, "get user define function failed. \n");
		PQclear(res);
		exit_nicely(conn);
	}

	appendLineBufInfo(create_func_sql, "LANGUAGE %s ", lanname);

	if (proiswindow[0] == 't')
		appendLineBufInfoString(create_func_sql, " WINDOW ");

	if (provolatile[0] != PROVOLATILE_VOLATILE)
	{
		if (provolatile[0] == PROVOLATILE_IMMUTABLE)
			appendLineBufInfo(create_func_sql, " IMMUTABLE");
		else if (provolatile[0] == PROVOLATILE_STABLE)
			appendLineBufInfo(create_func_sql, " STABLE");
		else if (provolatile[0] != PROVOLATILE_VOLATILE)
		{
			fprintf(stderr, "unrecognized provolatile value for function: %s. \n", proname);
			exit(1);
		}
	}

	if (proisstrict[0] == 't')
		appendLineBufInfo(create_func_sql, " STRICT");

	if (prosecdef[0] == 't')
		appendLineBufInfo(create_func_sql, " SECURITY DEFINER");

	if (proleakproof[0] == 't')
		appendLineBufInfo(create_func_sql, " LEAKPROOF");

	if (strcmp(procost, "0") != 0)
	{
		if (strcmp(lanname, "internal") == 0 || strcmp(lanname, "c") == 0)
		{
			/* default cost is 1 */
			if (strcmp(procost, "1") != 0)
				appendLineBufInfo(create_func_sql, " COST %s", procost);
		}
		else
		{
			/* default cost is 100 */
			if (strcmp(procost, "100") != 0)
				appendLineBufInfo(create_func_sql, " COST %s", procost);
		}
	}

	if (proretset[0] == 't' &&
		strcmp(prorows, "0") != 0 &&
		strcmp(prorows, "1000") != 0)
		appendLineBufInfo(create_func_sql, " ROWS %s ", prorows);

	if (strcmp(prosrc, "-") != 0)
		appendLineBufInfo(create_func_sql, " AS $_$ %s $_$ ;", prosrc);

	create_func = (char *)palloc0((create_func_sql->len) +1);
	drop_func = (char *)palloc0((drop_func_sql->len) +1);

	memcpy(create_func, create_func_sql->data, ((create_func_sql->len)));
	create_func[create_func_sql->len] = '\0';
	memcpy(drop_func, drop_func_sql->data, ((drop_func_sql->len)));
	drop_func[drop_func_sql->len] = '\0';

    userdefined_funcinfo->func_name      = pg_strdup(proname);
	userdefined_funcinfo->creat_func_sql = create_func;
	userdefined_funcinfo->drop_func_sql = drop_func;

	release_linebuf(create_func_sql);
	release_linebuf(drop_func_sql);

	PQclear(res);
	PQfinish(conn);
	return;
}

static void
get_all_datanode_info(ADBLoadSetting *setting)
{
	char query[QUERY_MAXLEN];
	PGconn       *conn;
	PGresult     *res;
	int          numtuples;
	NodeInfoData **datanodeinfo;
	int i = 0;

	conn = PQconnectdb(setting->coordinator_info->connection);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select oid, node_name,node_port,node_host "
					"from pgxc_node "
					"where node_type= 'D' order by oid;");

	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection return value failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	Assert(PQntuples(res) != 0);
	numtuples = PQntuples(res);

	datanodeinfo = (NodeInfoData **)palloc0(numtuples * sizeof(NodeInfoData*));
	for (i = 0; i < numtuples; i++)
	{
		datanodeinfo[i] = (NodeInfoData *)palloc0(sizeof(NodeInfoData));
	}
	for (i = 0; i < numtuples; i++)
	{
		datanodeinfo[i]->node_oid  = atoi(PQgetvalue(res, i, PQfnumber(res, "oid")));
		datanodeinfo[i]->node_name = pg_strdup(PQgetvalue(res, i, PQfnumber(res, "node_name")));
		datanodeinfo[i]->node_host = pg_strdup(PQgetvalue(res, i, PQfnumber(res, "node_host")));
		datanodeinfo[i]->node_port = pg_strdup(PQgetvalue(res, i, PQfnumber(res, "node_port")));

		datanodeinfo[i]->user_name = pg_strdup(setting->user_name);
		datanodeinfo[i]->database_name = pg_strdup(setting->database_name);
	}

	setting->datanodes_info = datanodeinfo;
	setting->datanodes_num = numtuples;

	PQclear(res);
	PQfinish(conn);

	/* sort the datanode info by datanode name using qsort. */
	qsort(setting->datanodes_info,
					setting->datanodes_num,
					sizeof(NodeInfoData*),
					adbloader_cmp_nodes);

	return;
}

static void
get_conninfo_for_alldatanode(ADBLoadSetting *setting)
{
	char conn_info_string[1024] = {0};
	int i;

	if (setting->password != NULL) //cmd special password option
	{
		for(i = 0; i < setting->datanodes_num; ++i)
		{
			sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s password=%s "
								"options=\'-c grammar=postgres -c remotetype=coordinator -c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
								,setting->datanodes_info[i]->user_name
								,setting->datanodes_info[i]->node_host
								,setting->datanodes_info[i]->node_port
								,setting->datanodes_info[i]->database_name
								,setting->password);
			setting->datanodes_info[i]->connection = pstrdup(conn_info_string);
		}

	}
	else //cmd not special password option
	{
		for(i = 0; i < setting->datanodes_num; ++i)
		{
			sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s "
								"options=\'-c grammar=postgres -c remotetype=coordinator -c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
								,setting->datanodes_info[i]->user_name
								,setting->datanodes_info[i]->node_host
								,setting->datanodes_info[i]->node_port
								,setting->datanodes_info[i]->database_name);
			setting->datanodes_info[i]->connection = pstrdup(conn_info_string);
		}

	}

	/*check the connection is valid*/
	for(i = 0; i < setting->datanodes_num; ++i)
	{
		check_node_connection_valid(setting->datanodes_info[i]->node_host, setting->datanodes_info[i]->node_port, setting->datanodes_info[i]->connection);
	}

}

static int
adbloader_cmp_nodes(const void *a, const void *b)
{
	char *nodename1 = (*(NodeInfoData **)a)->node_name;
	char *nodename2 = (*(NodeInfoData **)b)->node_name;

	if (strcmp(nodename1, nodename2) < 0)
		return -1;

	if (strcmp(nodename1, nodename2) == 0)
		return 0;

	return 1;
}

static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

void
file_name_print(char **file_list, int file_num)
{
	int file_index = 0;
	for(file_index = 0; file_index < file_num; ++file_index)
	{
		printf("%s\n",file_list[file_index]);
	}
}

void
tables_print(Tables *tables_ptr)
{
	TableInfo * table_info_ptr;
	slist_iter	iter;
	if(tables_ptr == NULL)
		return;
	printf("table nums:%d\n", tables_ptr->table_nums);
	table_info_ptr = tables_ptr->info;
	while(table_info_ptr)
	{
		printf("table_split_num:%d\n", table_info_ptr->file_nums);
		printf("table_split_files:%s\n", table_info_ptr->table_name);
		slist_foreach(iter, &table_info_ptr->file_head)
		{
			FileLocation * file_location = slist_container(FileLocation, next, iter.cur);
			printf("\t table_split_files:%s", file_location->location);

		}
		printf("\n");
		table_info_ptr = table_info_ptr->next;
	}
}

static void
tables_list_free(Tables *tables)
{
	TableInfo *table_info;
	TableInfo *table_info_next;

	table_info = tables->info;

	while(table_info != NULL)
	{
		table_info_next = table_info->next;
		table_free(table_info);
		table_info = table_info_next;
	}

	pg_free(tables);
	tables = NULL;
}

static void
table_free(TableInfo *table_info)
{
	int i = 0;
	slist_mutable_iter siter;

	Assert(table_info != NULL);

	pg_free(table_info->table_name);
	table_info->table_name = NULL;

	clean_hash_field(table_info->table_attribute);
	table_info->table_attribute = NULL;

	if (table_info->funcinfo != NULL)
	{
		pg_free(table_info->funcinfo->creat_func_sql);
		table_info->funcinfo->creat_func_sql = NULL;

		pg_free(table_info->funcinfo->drop_func_sql);
		table_info->funcinfo->drop_func_sql = NULL;

		pg_free(table_info->funcinfo->func_name);
		table_info->funcinfo->func_name = NULL;

		pg_free(table_info->funcinfo->func_args_type);
		table_info->funcinfo->func_args_type = NULL;

		pg_free(table_info->funcinfo->table_loc);
		table_info->funcinfo->table_loc = NULL;

		pg_free(table_info->funcinfo);
		table_info->funcinfo = NULL;
	}

	slist_foreach_modify(siter, &table_info->file_head)
	{
		FileLocation * file_location = slist_container(FileLocation, next, siter.cur);

		pg_free(file_location->location);
		file_location->location = NULL;

		pg_free(file_location);
		file_location = NULL;
	}

	for (i = 0; i < table_info->use_datanodes_num; i++)
	{
		pg_free_NodeInfoData(table_info->use_datanodes[i]);
		pg_free(table_info->use_datanodes[i]);
		table_info->use_datanodes[i] = NULL;
	}

	pg_free(table_info);
	table_info = NULL;

	return;
}

char *
conver_type_to_fun (Oid type, DISTRIBUTE loactor)
{
	char        *func = NULL;
	LineBuffer  *buf = NULL;

	buf = get_linebuf();
	switch (type)
	{

		case INT8OID:
			/* This gives added advantage that
			 * a = 8446744073709551359
			 * and a = 8446744073709551359::int8 both work*/
			appendLineBufInfoString(buf, "hashint8");
			break;
		case INT2OID:
			appendLineBufInfoString(buf, "hashint2");
			break;
		case OIDOID:
			appendLineBufInfoString(buf, "hashoid");
			break;
		case INT4OID:
			appendLineBufInfoString(buf, "hashint4");
			break;
		case BOOLOID:
			appendLineBufInfoString(buf, "hashchar");
			break;
		case CHAROID:
			appendLineBufInfoString(buf, "hashchar");
			break;
		case NAMEOID:
			appendLineBufInfoString(buf, "hashname");
			break;
		case INT2VECTOROID:
			appendLineBufInfoString(buf, "hashint2vector");
			break;
		case VARCHAR2OID:
		case NVARCHAR2OID:
		case VARCHAROID:
		case TEXTOID:
			appendLineBufInfoString(buf, "hashtext");
			break;
		case OIDVECTOROID:
			appendLineBufInfoString(buf, "hashoidvector");
			break;
		case FLOAT4OID:
			appendLineBufInfoString(buf, "hashfloat4");
			break;
		case FLOAT8OID:
			appendLineBufInfoString(buf, "hashfloat8");
			break;
		case ABSTIMEOID:
			if (loactor == DISTRIBUTE_BY_DEFAULT_HASH)
				appendLineBufInfoString(buf, "hashint4");
			else
				appendLineBufInfoString(buf, "DatumGetInt32");
			break;
		case RELTIMEOID:
			if (loactor == DISTRIBUTE_BY_DEFAULT_HASH)
				appendLineBufInfoString(buf, "hashint4");
			else
				appendLineBufInfoString(buf, "DatumGetRelativeTime");
			break;
		case CASHOID:
			appendLineBufInfoString(buf, "hashint8");
			break;
		case BPCHAROID:
			appendLineBufInfoString(buf, "hashbpchar");
			break;
		case BYTEAOID:
			appendLineBufInfoString(buf, "hashvarlena");
			break;
		case DATEOID:
			if (loactor == DISTRIBUTE_BY_DEFAULT_HASH)
				appendLineBufInfoString(buf, "hashint4");
			else
				appendLineBufInfoString(buf, "DatumGetDateADT");
			break;
		case TIMEOID:
			appendLineBufInfoString(buf, "time_hash");
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			appendLineBufInfoString(buf, "timestamp_hash");
			break;
		case INTERVALOID:
			appendLineBufInfoString(buf, "interval_hash");
			break;
		case TIMETZOID:
			appendLineBufInfoString(buf, "timetz_hash");
			break;
		case NUMERICOID:
			appendLineBufInfoString(buf, "hash_numeric");
			break;
		default:
			ADBLOADER_LOG(LOG_ERROR, "[Main] field type error");
			release_linebuf(buf);
			exit(1);
			break;
	}
	Assert(buf->len > 0);
	func = (char*)palloc0(buf->len + 1);
	memcpy(func, buf->data, buf->len);
	func[buf->len] = '\0';

	release_linebuf(buf);
	return func;
}

void
free_datanode_info (DatanodeInfo *datanode_info)
{
	int i = 0;

	Assert(datanode_info != NULL);
	Assert(datanode_info->datanode != NULL);
	Assert(datanode_info->conninfo != NULL);

	pg_free(datanode_info->datanode);
	datanode_info->datanode = NULL;

	for (i = 0; i < datanode_info->node_nums; i++)
	{
		pg_free(datanode_info->conninfo[i]);
		datanode_info->conninfo[i] = NULL;
	}
	pg_free(datanode_info->conninfo);
	datanode_info->conninfo = NULL;

	return;
}

void
free_dispatch_info (DispatchInfo *dispatch)
{
	Assert(dispatch != NULL);
	Assert(dispatch->table_name != NULL);
	Assert(dispatch->conninfo_agtm != NULL);

	pg_free(dispatch->conninfo_agtm);
	dispatch->conninfo_agtm = NULL;

	/* free output queue memory */
	// to do nothing

	free_datanode_info(dispatch->datanode_info);
	pg_free(dispatch->datanode_info);
	dispatch->datanode_info = NULL;

	pg_free(dispatch->table_name);
	dispatch->table_name = NULL;

	pg_free(dispatch->copy_options);
	dispatch->copy_options = NULL;

	return ;
}

static void
free_hash_info(HashComputeInfo *hash_info)
{
	Assert(hash_info != NULL);

	pg_free(hash_info->func_name);
	hash_info->func_name = NULL;

	pg_free(hash_info->table_name);
	hash_info->table_name = NULL;

	pg_free(hash_info->conninfo);
	hash_info->conninfo = NULL;

	/*free input queue */
	// to do nothing

	/* free input queue */
	// to do nothing

	pg_free(hash_info->redo_queue_index);
	hash_info->redo_queue_index = NULL;

	pg_free(hash_info->filter_queue_file_path);
	hash_info->filter_queue_file_path = NULL;

	clean_hash_field(hash_info->hash_field);
	hash_info->hash_field = NULL;

	return ;
}

static void
free_read_info(ReadInfo *read_info)
{
	Assert(read_info != NULL);

	/* free filepath */
	pg_free(read_info->filepath);
	read_info->filepath = NULL;

	/* free input queue for hash table */
	/* to do nothing */

	/* free output queue for replication table */
	/* to do nothing */

	/* free start_cmd */
	pg_free(read_info->start_cmd);
	read_info->start_cmd = NULL;

	/* free redo_queue_index */
	pg_free(read_info->redo_queue_index);
	read_info->redo_queue_index = NULL;

	return ;
}

static void
main_write_error_message(DISTRIBUTE distribute, char * message, char *start_cmd)
{
	LineBuffer *error_buffer = NULL;

	error_buffer = format_error_info(message, MAIN, NULL, 0, NULL);

	if (start_cmd != NULL)
	{
		appendLineBufInfoString(error_buffer, "Suggest       : ");
		appendLineBufInfoString(error_buffer, "Please choose one of the following options according to if it is a primary key error\n");
		appendLineBufInfoString(error_buffer, "Information   : ");
		appendLineBufInfoString(error_buffer, "If it is a non-primary key error, please proceed as follows\n");
		appendLineBufInfoString(error_buffer, "                   1,Manually modify the original data\n");
		appendLineBufInfoString(error_buffer, "                   2,Execute the following command\n");
		appendLineBufInfoString(error_buffer, "                     ");
		appendLineBufInfoString(error_buffer, start_cmd);
		appendLineBufInfoString(error_buffer, "\n");
		appendLineBufInfoString(error_buffer, "                If it is a primary key error, please proceed as follows\n");
		appendLineBufInfoString(error_buffer, "                   1,Execute the following command, gets the data in each queue into file\n");
		appendLineBufInfoString(error_buffer, "                     ");
		appendLineBufInfoString(error_buffer, start_cmd);
		appendLineBufInfoString(error_buffer, " -e \n");
		appendLineBufInfoString(error_buffer, "                   2,Manually modify the error data in the queue data file\n");
		appendLineBufInfoString(error_buffer, "                   3,Import data using single file mode or static folder mode\n");
	}

	write_log_summary_fd(error_buffer);
	release_linebuf(error_buffer);
}

static int
get_file_total_line_num (char *file_path)
{
	FILE *fp;
	int n = 0;
	int ch;

	if((fp = fopen(file_path,"r+")) == NULL)
	{
		fprintf(stderr,"open file \"%s\" error: %s\n", file_path, strerror(errno));
		exit(EXIT_FAILURE);
	}

	while((ch = fgetc(fp)) != EOF)
	{
		if(ch == '\n')
		{
			n++;
		}
	}

	n++;

	fclose(fp);
	return n;
}

static void
process_bar(int total, int send)
{
	char buf[103];
	char index[6] = "-\\|/\0";
	double precent = 0;
	int i = 0;
	int flag = 0;

	memset(buf, ' ', sizeof(buf));
	buf[0] = '[';
	buf[101] = ']';
	buf[102] = '\0';

	precent = (double)send/(double)total * 100;
	i = (int)precent;

	for (flag = 1; flag < i; flag++)
	{
		buf[flag] = '=';
	}
	printf("%s [%d/%d][%d%%][%c]\r", buf, send, total,(int)precent, index[send % 4]);
	/* flush cache buffer */
	fflush(stdout);
	//printf("\n");
}

static void
process_bar_multi(int total, int * send , int num)
{
	int flag;
	int loc;
	char **buf;
	double precent = 0;
	int i = 0;

	buf = (char**)palloc0(sizeof(char*) * num);
	for (flag = 0; flag < num; flag++)
	{
		buf[flag] = (char*)palloc0(sizeof(char) * 103);
		memset(buf[flag], ' ', 103);
		buf[flag][0] = '[';
		buf[flag][101] = ']';
		buf[flag][102] = '\0';
	}

	for (flag = 0; flag < num; flag++)
	{
		precent = (double)send[flag]/(double)total * 100;
		i = (int)precent;
		for (loc = 1; loc < i; loc++)
		{
			buf[flag][loc] = '=';
		}
		printf("%s [%d][%d/%d][%d%%]\r", buf[flag], flag, send[flag], total, (int)precent);
		fflush(stdout);
		printf("\n");
	}

	for (flag = 0; flag < num; flag++)
	{
		pfree(buf[flag]);
		buf[flag] = NULL;
	}
	pfree(buf);
	buf = NULL;
}

static void
show_process(int total, int datanodes, int * thread_send_total, DISTRIBUTE type)
{
	int flag;
	Assert(datanodes > 0);
	Assert(thread_send_total != NULL);

	switch(type)
	{
		case DISTRIBUTE_BY_REPLICATION:
		{
			 process_bar_multi(total, thread_send_total, datanodes);
		}
			break;
		case DISTRIBUTE_BY_DEFAULT_HASH:
		{
			long sum = 0;
			for (flag = 0; flag < datanodes; flag++)
			{
				sum += thread_send_total[flag];
			}
			process_bar(total, sum);
		}
			break;
		default:
			break;
	}
}

static void
deep_copy(HashField *dest, HashField *src)
{
	memcpy(dest, src, sizeof(HashField));

	dest->node_list = (Oid *)palloc0(src->datanodes_num * sizeof(Oid));
	memcpy(dest->node_list, src->node_list, (src->datanodes_num * sizeof(Oid)));

	dest->field_loc  = (int *)palloc0(src->field_nums * sizeof(int));
	memcpy(dest->field_loc, src->field_loc, src->field_nums * sizeof(int));

	dest->field_type = (Oid *)palloc0(src->field_nums * sizeof(Oid));
	memcpy(dest->field_type, src->field_type, src->field_nums * sizeof(Oid));

	dest->text_delim = pg_strdup(src->text_delim);
	dest->hash_delim = pg_strdup(src->hash_delim);

	dest->copy_options = pg_strdup(src->copy_options);

	return;
}

