#include "postgres_fe.h"
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

#include "catalog/pg_type.h"

#include "compute_hash.h"
#include "loadsetting.h"
#include "linebuf.h"
#include "msg_queue.h"
#include "msg_queue_pipe.h"
#include "read_write_file.h"
#include "adbloader_log.h"
#include "read_producer.h"
#include "dispatch.h"
#include "lib/ilist.h"
#include "utility.h"
#include "properties.h"

typedef struct tables
{
	int table_nums;
	struct TableInfo *info;
}Tables;

typedef enum DISTRIBUTE
{
	DISTRIBUTE_BY_REPLICATION,
	DISTRIBUTE_BY_ROUNDROBIN,
	DISTRIBUTE_BY_USERDEFINED,
	DISTRIBUTE_BY_DEFAULT_HASH,
	DISTRIBUTE_BY_DEFAULT_MODULO,
	DISTRIBUTE_BY_ERROR
} DISTRIBUTE;

typedef struct FileLocation
{
	char * location;
	slist_node next;
} FileLocation;

typedef struct TableInfo
{
	int           file_nums;        /*the num of files which own to one table */
	char         *table_name;       /*the name of table*/
	HashField   *table_attribute;  /*the attribute of table*/
	DISTRIBUTE    distribute_type;
	UserFuncInfo *funcinfo;
	slist_head	   file_head;
	NodeInfoData **use_datanodes;
	int   		   use_datanodes_num; // use datanode num for special table
	struct TableInfo *next;/* */
} TableInfo;

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
// static void get_func_info(const char *conninfo, TableInfo *table_info);
static void get_table_loc_count_and_loc(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void get_func_args_count_and_type(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
//static void get_table_loc_for_hash_modulo(const char *conninfo, const char *tablename, UserFuncInfo *funcinfo);
static void get_create_and_drop_func_sql(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void get_userdefined_funcinfo(const char *conninfo, TableInfo *table_info);
static void create_func_to_server(char *serverconninfo, char *creat_func_sql);
static void reset_hash_field(TableInfo *table_info);
static void get_node_count_and_node_list(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo);
static void drop_func_to_server(char *serverconninfo, char *drop_func_sql);
static void get_use_datanodes(ADBLoadSetting *setting, TableInfo *table_info);
static bool file_exists(char *file);
static Tables* get_file_info(char *input_dir);
static bool update_file_info(char *input_dir, Tables *tables);

static char *rename_file_name(char *file_name, char *input_dir);
static void rename_file_suffix(char *old_file_path, char *suffix);

static char *get_full_path(char *file_name, char *input_dir);
static char *get_table_name(char *file_name);
static bool is_suffix(char *str, char *suffix);

static bool do_replaciate_roundrobin(char *filepath, TableInfo *table_info);
static void clean_replaciate_roundrobin(void);

static bool do_hash_module(char *filepath, const TableInfo *table_info);
static void clean_hash_module(void);
static char *get_outqueue_name (int datanode_num);
static void exit_nicely(PGconn *conn);
/*--------------------------------------------------------*/

void file_name_print(char **file_list, int file_num);
void tables_print(Tables *tables_ptr);

static Tables *read_table_info(char *fullpath);
static Tables *get_tables(char **file_name_list, char **file_path_list, int file_num);
static TableInfo *get_table_info(char**file_name_list, int file_num);
static void get_special_table_by_sql(void);
static struct special_table *add_special_table(struct special_table *table_list, char *table_name);
static int has_special_file(char **file_name_list, int file_nums);
static int is_special_table(char *table_name);
static int is_special_file_name(char *file_name);
static void str_list_sort(char **file_list, int file_num);
static int get_file_num(char *fullpath);
static int is_type_file(char *fullpath);
static int is_type_dir(char *fullpath);
static void tables_list_free(Tables *tables);
static void table_free(TableInfo *table_info);
static void clean_hash_field (HashField *hash_field);
static char * conver_type_to_fun (Oid type, DISTRIBUTE loactor);
static void free_datanode_info (DatanodeInfo *datanode_info);
static void free_dispatch_info (DispatchInfo *dispatch_info);
static LOG_TYPE covert_loglevel_from_char_type (char *type);
static char * covert_distribute_type_to_string (DISTRIBUTE type);
static void main_write_error_message(char * message, char *start_cmd);
static int get_file_total_line_num (char *file_path);
static void process_bar(int total, int send);
static void process_bar_multi(int total, int * send , int num);
static void show_process(int total,	int datanodes, int * thread_send_total, DISTRIBUTE type);

/*--------------------------------------------------------*/
#define is_digit(c) ((unsigned)(c) - '0' <= 9)

#define MAXPGPATH          1024
#define QUERY_MAXLEN       1024
#define DEFAULT_USER       "postgres"

#define PROVOLATILE_IMMUTABLE   'i'   /* never changes for given input */
#define PROVOLATILE_STABLE      's'   /* does not change within a scan */
#define PROVOLATILE_VOLATILE    'v'   /* can change even within a scan */

#define SUFFIX_SQL     ".sql"
#define SUFFIX_DOING   ".doing"
#define SUFFIX_ADBLOAD ".adbload"
#define SUFFIX_ERROR   ".error"
#define END_FILE_NAME  "adbload_end"

static ADBLoadSetting *setting;

/*update dir for tables info */
bool update_tables = true;

void
clean_hash_field (HashField *hash_field)
{
	if (NULL == hash_field)
		return;
	Assert(NULL != hash_field);
	if (NULL != hash_field->field_loc)
		pfree(hash_field->field_loc);

	if (NULL != hash_field->field_type)
		pfree(hash_field->field_type);

	if (NULL != hash_field->node_list)
		pfree(hash_field->node_list);

	if (NULL != hash_field->delim)
		pfree(hash_field->delim);

	if (NULL != hash_field->copy_options)
		pfree(hash_field->copy_options);

	if (NULL != hash_field->hash_delim)
		pfree(hash_field->hash_delim);

	hash_field->field_loc = NULL;
	hash_field->field_type = NULL;
	hash_field->node_list = NULL;
	hash_field->delim = NULL;
	hash_field->copy_options = NULL;
	hash_field->hash_delim = NULL;

	pfree(hash_field);
	hash_field = NULL;
}

LOG_TYPE
covert_loglevel_from_char_type (char *type)
{
	Assert(NULL != type);
	if (strcmp(type, "DEBUG") == 0)
		return LOG_DEBUG;
	else if (strcmp(type, "INFO") == 0)
		return LOG_INFO;
	else if (strcmp(type, "WARN") == 0)
		return LOG_WARN;
	else if (strcmp(type, "ERROR") == 0)
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

int main(int argc, char **argv)
{
	Tables *tables_ptr = NULL;
	TableInfo    *table_info_ptr = NULL;
	int table_count = 0;

	/* get cmdline param. */
	setting = cmdline_adb_load_setting(argc, argv, setting);

	get_settings_by_config_file(setting);
	get_node_conn_info(setting);

	/* open log file if not exist create. */
	adbLoader_log_init(setting->log_field->log_path,
					covert_loglevel_from_char_type(setting->log_field->log_level));

	setting->program = pg_strdup(argv[0]);

	/*static mode */
	if (setting->static_mode)
	{
		if (setting->input_file != NULL && setting->table_name != NULL) // sigle file
		{
			FileLocation *file_location = (FileLocation*)palloc0(sizeof(FileLocation));
			tables_ptr = (Tables *)palloc0(sizeof(Tables));
			tables_ptr->table_nums = 1;

			table_info_ptr = (TableInfo *)palloc0(sizeof(TableInfo));
			table_info_ptr->table_name = pg_strdup(setting->table_name);
			table_info_ptr->file_nums = 1;
			table_info_ptr->next = NULL;
			file_location->location = pg_strdup(setting->input_file);
			slist_push_head(&table_info_ptr->file_head, &file_location->next);
		}
		else // static mode ,multi file in one directry
		{
			/*get tables info from data directory file*/
			tables_ptr = read_table_info(setting->input_directory);
			if(tables_ptr == NULL)
			{
				ADBLOADER_LOG(LOG_ERROR,"[main] there is no table to store");
				return 0;
			}

			/*get special table which the tail of name is Glide line add digit num*/
			get_special_table_by_sql();

			table_info_ptr = tables_ptr->info;
			table_count = 0;
		}
	}
	else if (setting->dynamic_mode)// dysnamic mode
	{
		tables_ptr = get_file_info(setting->input_directory);
		if (tables_ptr == NULL)
		{
			fprintf(stderr, "cannot get file information in \"%s\" \n", setting->input_directory);
			exit(EXIT_FAILURE);
		}
		table_info_ptr = tables_ptr->info;
	}
	else
	{
		fprintf(stderr, "mode is not recognized \n");
		exit(EXIT_FAILURE);
	}

	if (!setting->config_datanodes_valid)
	{
		get_all_datanode_info(setting);
		get_conninfo_for_alldatanode(setting);
	}
	/* init linebuf */
	init_linebuf(setting->datanodes_num);

	/* open error file */
	fopen_error_file(NULL);

	while(table_info_ptr)
	{
		LineBuffer *linebuff = NULL;
		slist_mutable_iter siter;
		++table_count;

		if (table_info_ptr->file_nums == 0 && table_info_ptr->next != NULL)
		{
			table_info_ptr = table_info_ptr->next;
			continue;
		}

		if (table_info_ptr->file_nums == 0 && table_info_ptr->next == NULL)
		{
			if (update_tables) //need update
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

		/*get table info to store in the table_info_ptr*/
		get_table_attribute(setting, table_info_ptr);

		get_use_datanodes(setting, table_info_ptr);		

		switch(table_info_ptr->distribute_type)
		{
			case DISTRIBUTE_BY_REPLICATION:
			case DISTRIBUTE_BY_ROUNDROBIN:
			{
				/*the table may be split to more than one*/
				slist_foreach_modify (siter, &table_info_ptr->file_head) 
				{
					FileLocation * file_location = slist_container(FileLocation, next, siter.cur);
					Assert(NULL != file_location->location);
					/* begin record error file */
					linebuff = format_error_begin(file_location->location,
										covert_distribute_type_to_string(table_info_ptr->distribute_type));
					write_error(linebuff);
					release_linebuf(linebuff);
					fprintf(stderr, "-----------------------------------------begin file : %s ----------------------------------------------\n",
						file_location->location);

					/* start module */
					do_replaciate_roundrobin(file_location->location, table_info_ptr);
					/* stop module and clean resource */
					clean_replaciate_roundrobin();

					/* end record error file */
					linebuff = format_error_end(file_location->location);
					write_error(linebuff);
					release_linebuf(linebuff);
					fprintf(stderr, "\n-----------------------------------------end file : %s ------------------------------------------------\n\n",
						file_location->location);
					//rename file name
					rename_file_suffix(file_location->location, SUFFIX_ADBLOAD);

					/* remove  file from file list*/
					slist_delete_current(&siter);
					/* file num  subtract 1 */
					table_info_ptr->file_nums--;

					/* free location */
					if (file_location->location)
						pfree(file_location);
					file_location = NULL;
				}
				break;
			}
			case DISTRIBUTE_BY_DEFAULT_HASH:
			case DISTRIBUTE_BY_DEFAULT_MODULO:
			case DISTRIBUTE_BY_USERDEFINED:
			{
				/*the table may be split to more than one*/
				slist_foreach_modify (siter, &table_info_ptr->file_head) 
				{					
					FileLocation * file_location = slist_container(FileLocation, next, siter.cur);
					Assert(NULL != file_location->location);
					/* begin record error file */
					linebuff = format_error_begin(file_location->location,
										covert_distribute_type_to_string(table_info_ptr->distribute_type));
					write_error(linebuff);
					release_linebuf(linebuff);
					fprintf(stderr, "-----------------------------------------begin file : %s ----------------------------------------------\n",
						file_location->location);

					/* start module */
					do_hash_module(file_location->location,table_info_ptr);
					/* stop module and clean resource */
					clean_hash_module();

					/* end record error file */
					linebuff = format_error_end(file_location->location);
					write_error(linebuff);
					release_linebuf(linebuff);
					fprintf(stderr, "\n-----------------------------------------end file : %s ------------------------------------------------\n\n",
						file_location->location);
					//rename file name
					rename_file_suffix(file_location->location, SUFFIX_ADBLOAD);

					/* remove  file from file list*/
					slist_delete_current(&siter);
					/* file num  subtract 1 */
					table_info_ptr->file_nums--;

					/* free location */
					if (file_location->location)
						pfree(file_location);
					file_location = NULL;
				}
				break;
			}
			default:
			{
				LineBuffer * error_buffer = NULL;
				ADBLOADER_LOG(LOG_ERROR,"[main] unrecognized distribute by type: %d",
						table_info_ptr->distribute_type);
				error_buffer = format_error_info("table type error ,file need to redo",
												MAIN,
												NULL,
												0,
												NULL);
				write_error(error_buffer);
				release_linebuf(error_buffer);
			}
				break; 
		}

		if (update_tables)
		{
			update_file_info(setting->input_directory, tables_ptr);
			table_info_ptr = tables_ptr->info;
		}
		else
		{
			table_info_ptr = table_info_ptr->next;
		}
	}


	fclose_error_file(); /* close error file */

	if(table_count != tables_ptr->table_nums) /*check again*/
	{
		ADBLOADER_LOG(LOG_ERROR,"[main] The number of imported tables does not match the number of calcuate: %d");
		return 0;
	}
	tables_list_free(tables_ptr);

	adbLoader_log_end(); /*close log file. */
	free_adb_load_setting(setting);
	end_linebuf();       /* end line buffer */
	DestoryConfig();     /* destory config  */
	return 0;
}
/*------------------------end main-------------------------------------------------------*/


static bool update_file_info(char *input_dir, Tables *tables)
{
	TableInfo * table_info_dynamic = NULL;
	DIR *dir;
	struct dirent *dirent_ptr;
	char *new_file_name = NULL;
	char *table_name = NULL;
	char *file_full_path = NULL;
	FileLocation * file_location;

	TableInfo *ptr = NULL;
	TableInfo *tail = NULL;

	if ((dir=opendir(input_dir)) == NULL)
	{
		fprintf(stderr, "open dir %s error...\n", input_dir);
		return false;
	}

	while ((dirent_ptr = readdir(dir)) != NULL)
	{
		if(strcmp(dirent_ptr->d_name, ".") == 0 ||
		strcmp(dirent_ptr->d_name, "..") == 0||
		is_suffix(dirent_ptr->d_name, SUFFIX_ADBLOAD) ||
		is_suffix(dirent_ptr->d_name, SUFFIX_DOING) ||
		is_suffix(dirent_ptr->d_name, SUFFIX_ERROR))
			continue;

		if (strcmp(dirent_ptr->d_name, END_FILE_NAME) == 0)
		{
			update_tables = false;
			continue;
		}

		if (!is_suffix(dirent_ptr->d_name, SUFFIX_SQL))
		{
			fprintf(stderr, "invalid file name :\"%s/%s\"\n", input_dir, dirent_ptr->d_name);
			continue;
		}

		new_file_name  = rename_file_name(dirent_ptr->d_name, input_dir);
		table_name     = get_table_name(dirent_ptr->d_name);
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
	return true;
}

static Tables* get_file_info(char *input_dir)
{
	Tables * tables_dynamic = NULL;
	TableInfo * table_info_dynamic = NULL;
	DIR *dir;
	struct dirent *dirent_ptr;
	char *new_file_name = NULL;
	char *table_name = NULL;
	char *file_full_path = NULL;
	FileLocation * file_location;
	TableInfo *ptr = NULL;
	TableInfo *tail = NULL;

	if ((dir=opendir(input_dir)) == NULL)
	{
		fprintf(stderr, "open dir %s error...\n", input_dir);
		return NULL;
	}

	tables_dynamic = (Tables *)palloc0(sizeof(Tables));
	tables_dynamic->table_nums = 0;
	tables_dynamic->info = NULL;

	while ((dirent_ptr = readdir(dir)) != NULL)
	{
		if(strcmp(dirent_ptr->d_name, ".") == 0 || strcmp(dirent_ptr->d_name, "..") == 0)
			continue;

		/*stop update file info if have a file END_FILE_NAME(adbload_end) */
		if (strcmp(dirent_ptr->d_name, END_FILE_NAME) == 0)
		{
			update_tables = false;
			continue;
		}

		new_file_name  = rename_file_name(dirent_ptr->d_name, input_dir);
		table_name     = get_table_name(dirent_ptr->d_name);
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
	return tables_dynamic;
}

static char *get_table_name(char *file_name)
{
	char * file_name_local = NULL;
	int file_name_local_len;

	file_name_local = pg_strdup(file_name);
	file_name_local_len = strlen(file_name_local);

	while (file_name_local[file_name_local_len] != '_')
		file_name_local_len --;
	file_name_local[file_name_local_len] = '\0';

	return file_name_local;
}

static char *rename_file_name(char *file_name, char *input_dir)
{
	char old_file_name_path[1024] = {0};
	char new_file_name_path[1024] = {0};
	char *new_fiel_name = NULL;

	sprintf(old_file_name_path, "%s/%s", input_dir, file_name);
	sprintf(new_file_name_path, "%s/%s%s", input_dir, file_name, SUFFIX_DOING);

	if (rename(old_file_name_path, new_file_name_path) < 0)
	{
		fprintf(stderr, "could not rename file \"%s\" to \"%s\" : %s \n",
				old_file_name_path, new_file_name_path, strerror(errno));
	}

	new_fiel_name = (char *)palloc0(strlen(file_name) + strlen(SUFFIX_DOING) + 1);
	sprintf(new_fiel_name, "%s%s", file_name, SUFFIX_DOING);

	return new_fiel_name;
}

static void rename_file_suffix(char *old_file_path, char *suffix)
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
}

static char *get_full_path(char *file_name, char *input_dir)
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

static bool is_suffix(char *str, char *suffix)
{
	char *ptr = NULL;

	ptr = strrchr(str, '.');
	if (ptr == NULL)
	{
		fprintf(stderr, "The character \"%c\" is at position:%s\n", '.', str);
		return false;
	}

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
	appendLineBufInfoString(lineBuffer, "datanode");
	appendLineBufInfo(lineBuffer, "%d", datanode_num);
	name = (char*)palloc0(lineBuffer->len + 1);
	memcpy(name, lineBuffer->data, lineBuffer->len);
	name[lineBuffer->len] = '\0';
	release_linebuf(lineBuffer);
	return name;
}

static bool
do_replaciate_roundrobin(char *filepath, TableInfo *table_info)
{
	MessageQueuePipe 	 **output_queue;
	DispatchInfo 		  *dispatch = NULL;
	DatanodeInfo		  *datanode_info = NULL;
	char 				  *queue_name;
	int					   flag;
	int					   res;
	int					   file_total_line = 0;
	bool				   read_finish = false;
	bool				   dispatch_finish = false;
	bool				   dispatch_failed = false;
	bool				   do_sucess = true;
	char				  *start = NULL;
	int					  *thread_send_total;

	Assert(NULL != filepath && NULL != table_info);

	if (setting->process_bar)
	{
		file_total_line = get_file_total_line_num(filepath);
		thread_send_total = palloc0(sizeof(int) * table_info->use_datanodes_num);
	}
		
	start = create_start_command(setting->program, filepath, (char*)table_info->table_name,
		setting->config_file_path, setting->database_name, setting->user_name, setting->password);
	if (!start)
	{
		main_write_error_message("creat start cmd error", NULL);
		exit(1);
	}
	output_queue = (MessageQueuePipe**)palloc0(sizeof(MessageQueuePipe*) * table_info->use_datanodes_num);
	datanode_info = (DatanodeInfo*)palloc0(sizeof(DatanodeInfo));
	datanode_info->node_nums = table_info->use_datanodes_num;
	datanode_info->datanode = (Oid *)palloc0(sizeof(Oid) * table_info->use_datanodes_num);
	datanode_info->conninfo = (char **)palloc0(sizeof(char *) * table_info->use_datanodes_num);
	for (flag = 0; flag < table_info->use_datanodes_num; flag++)
	{
		datanode_info->datanode[flag] = table_info->use_datanodes[flag]->node_oid;
		datanode_info->conninfo[flag] = pg_strdup(table_info->use_datanodes[flag]->connection);

		output_queue[flag] =  (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
		output_queue[flag]->write_lock = false;
		queue_name = get_outqueue_name(flag);
		mq_pipe_init(output_queue[flag], queue_name);
		pfree(queue_name);
		queue_name = NULL;
	}

	dispatch = (DispatchInfo *)palloc0(sizeof(DispatchInfo));
	dispatch->conninfo_agtm = pg_strdup(setting->agtm_info->connection);
	dispatch->thread_nums = table_info->use_datanodes_num;
	dispatch->output_queue = output_queue;
	dispatch->datanode_info = datanode_info;
	dispatch->table_name = pg_strdup(table_info->table_name);
	if (NULL != setting->hash_config->copy_option)		
		dispatch->copy_options = pg_strdup(setting->hash_config->copy_option);

	dispatch->start_cmd = pg_strdup(start);
	dispatch->process_bar = setting->process_bar;
	/* start dispatch module  */
	if ((res = InitDispatch(dispatch, TABLE_REPLICATION)) != DISPATCH_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,"start dispatch module failed");
		main_write_error_message("start dispatch module failed", start);
		exit(1);
	}

	/* start read_producer module last */
	if ( (res = InitReadProducer(filepath, NULL, output_queue,
										table_info->use_datanodes_num, true, 0, start)) != READ_PRODUCER_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,"start read_producer module failed");
		main_write_error_message("start read_producer module failed", start);
		exit(1);
	}

	/* check module state every 10s */
	for (;;)
	{
		ReadProducerState  state;
		DispatchThreads *dispatch_exit = NULL;
		int              flag;

		if (setting->process_bar)
		{
			memset(thread_send_total, 0 , sizeof(int) * table_info->use_datanodes_num);
			GetSendCount(thread_send_total);
			show_process(file_total_line, table_info->use_datanodes_num,
								thread_send_total, DISTRIBUTE_BY_REPLICATION);
		}

		/* all modules complete */
		if (read_finish && dispatch_finish)
		{
			ADBLOADER_LOG(LOG_INFO, "all modules complete");
			break;
		}

		if (!read_finish)
		{
			state = GetReadModule();
			if (state == READ_PRODUCER_PROCESS_ERROR)
			{
				SetReadProducerExit();
				/* read module error, stop dispatch */
				StopDispatch();
				read_finish = true;
				dispatch_finish = true;
				break;
			}
			else if (state == READ_PRODUCER_PROCESS_COMPLETE)
				read_finish = true;
		}		
		
		/* if read module state is ok, check dispatch state */
		dispatch_exit = GetDispatchExitThreads();
		if (dispatch_exit->send_thread_cur == 0)
		{
			sleep(1);
			continue;
		}
		else
		{
			/* all threads exited ? */
			if (dispatch_exit->send_thread_cur == dispatch_exit->send_thread_count)
			{
				int i;
				bool all_error = true;
				for (i = 0; i < dispatch_exit->send_thread_count; i++)
				{
					if (dispatch_exit->send_threads[i]->state == DISPATCH_THREAD_EXIT_NORMAL)
						all_error = false;
					else
						do_sucess = false;
				}
				if (all_error)
				{
					ADBLOADER_LOG(LOG_ERROR,
						"[MAIN][Tthread main] dispatch module all threads error, tabel name :%s ", table_info->table_name);
					dispatch_failed = true;
					StopDispatch();
					dispatch_finish = true;
					goto FAILED;
				}
			}

			for (flag = 0 ;flag < dispatch_exit->send_thread_count; flag ++)
			{
				DispatchThreadInfo *dispatch_thread = NULL;
				dispatch_thread = dispatch_exit->send_threads[flag];
				if (NULL != dispatch_thread)
				{
					if (dispatch_thread->state != DISPATCH_THREAD_EXIT_NORMAL)
						ADBLOADER_LOG(LOG_WARN,
							"[MAIN][Tthread main] dispatch thread error, table name :%s, connection : %s",
							dispatch_thread->thread_id, dispatch_thread->table_name, dispatch_thread->conninfo_datanode);
				}
			}
			if (dispatch_exit->send_thread_cur == dispatch_exit->send_thread_count)
				dispatch_finish = true;
		}

		sleep(1);
	}
FAILED:
	if (dispatch_failed)
	{
		if (!read_finish)
		{
			SetReadProducerExit();
			read_finish = true;
		}
			
	}
	CleanDispatchResource();

	/* free  memory */
	for (flag = 0; flag < table_info->use_datanodes_num; flag++)
	{
		/* free mesage queue */
		mq_pipe_destory(output_queue[flag]);
	}

	pfree(output_queue);
	output_queue = NULL;
	/* free datanode info */
	free_datanode_info(datanode_info);
	datanode_info = NULL;
	/* free dispatch */
	free_dispatch_info(dispatch);
	dispatch = NULL;
	pfree(start);
	start = NULL;
	return do_sucess;
}

void 
clean_replaciate_roundrobin(void)
{

}

static bool
do_hash_module(char *filepath, const TableInfo *table_info)
{
	MessageQueuePipe 	  *	input_queue;
	MessageQueuePipe 	 **	output_queue;
	HashField		 	  * field;
	DispatchInfo 		  * dispatch = NULL;
	DatanodeInfo		  * datanode_info = NULL;
	int					 	flag = 0;
	char 				  * queue_name = NULL;
	char				  * func_name = NULL;
	int						res;
	int						file_total_line;
	bool					hash_failed = false;
	bool					dispatch_failed = false;
	bool					read_finish = false;
	bool					hash_finish = false;
	bool					dispatch_finish = false;
	bool				   do_sucess = true;
	char				  * start = NULL;
	int					  *thread_send_total;

	if (setting->process_bar)
	{
		file_total_line = get_file_total_line_num(filepath);
		thread_send_total = palloc0(sizeof(int) * table_info->use_datanodes_num);
	}

	start = create_start_command(setting->program, filepath, table_info->table_name,
				setting->config_file_path, setting->database_name, setting->user_name, setting->password);
	if (!start)
	{
		main_write_error_message("start read_producer module failed", NULL);
		exit(1);
	}
	/* init queue */
	input_queue = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	output_queue = (MessageQueuePipe**)palloc0(sizeof(MessageQueuePipe*) * table_info->use_datanodes_num);
	mq_pipe_init(input_queue, "input_queue");

	datanode_info = (DatanodeInfo*)palloc0(sizeof(DatanodeInfo));
	datanode_info->node_nums = table_info->use_datanodes_num;
	datanode_info->datanode = (Oid *)palloc0(sizeof(Oid) * table_info->use_datanodes_num);
	datanode_info->conninfo = (char **)palloc0(sizeof(char *) * table_info->use_datanodes_num);
	for (flag = 0; flag < table_info->use_datanodes_num; flag++)
	{
		datanode_info->datanode[flag] = table_info->use_datanodes[flag]->node_oid;
		datanode_info->conninfo[flag] = pg_strdup(table_info->use_datanodes[flag]->connection);

		output_queue[flag] =  (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
		output_queue[flag]->write_lock = true;
		queue_name = get_outqueue_name(flag);
		mq_pipe_init(output_queue[flag], queue_name);
	}
	/* start hash module first */
	field = table_info->table_attribute;

	/*start hash module */
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

	res = InitHashCompute(setting->hash_config->hash_thread_num, func_name, setting->server_info->connection,
						input_queue, output_queue, table_info->use_datanodes_num, field, start);
	pfree(func_name);
	func_name = NULL;
	if (res != HASH_COMPUTE_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "start hash module failed");
		main_write_error_message("start hash module failed", start);
		exit(1);
	}

	/* start dispatch module */
	dispatch = (DispatchInfo *)palloc0(sizeof(DispatchInfo));
	dispatch->conninfo_agtm = pg_strdup(setting->agtm_info->connection);
	dispatch->thread_nums = table_info->use_datanodes_num;
	dispatch->output_queue = output_queue;
	dispatch->datanode_info = datanode_info;
	dispatch->table_name = pg_strdup(table_info->table_name);
	if (NULL != setting->hash_config->copy_option)		
		dispatch->copy_options = pg_strdup(setting->hash_config->copy_option);
	dispatch->start_cmd = pg_strdup(start);
	dispatch->process_bar = setting->process_bar;

	/* start dispatch module */
	if ((res = InitDispatch(dispatch, TABLE_DISTRIBUTE)) != DISPATCH_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "start dispatch module failed");
		main_write_error_message("start dispatch module failed", start);
		exit(1);
	}

	/* start read_producer module last */
	if ((res =InitReadProducer(filepath, input_queue, NULL, 0, 
						false, setting->hash_config->hash_thread_num, start))!= READ_PRODUCER_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "start read module failed");
		main_write_error_message("start read module failed", start);
		exit(1);
	}

	/* check module state every 1s */
	for (;;)
	{
		ReadProducerState  state = READ_PRODUCER_PROCESS_DEFAULT;
		DispatchThreads	*dispatch_exit = NULL;
		HashThreads		*hash_exit = NULL;
		int					 flag;

		if (setting->process_bar)
		{
			memset(thread_send_total, 0 , sizeof(int) * table_info->use_datanodes_num);
			GetSendCount(thread_send_total);
			show_process(file_total_line, table_info->use_datanodes_num,
								thread_send_total, DISTRIBUTE_BY_DEFAULT_HASH);
		}
		/* all modules complete*/
		if (read_finish && hash_finish && dispatch_finish)
		{
			ADBLOADER_LOG(LOG_INFO, "all modules complete");
			break;
		}

		if (!read_finish)
		{
			state = GetReadModule();
			if (state == READ_PRODUCER_PROCESS_ERROR)
			{
				SetReadProducerExit();
				/* read module error, stop hash and dispatch */
				StopHash();
				StopDispatch();
				read_finish = true;
				hash_finish = true;
				dispatch_finish = true;
				break;
			}
			else if (state == READ_PRODUCER_PROCESS_COMPLETE)
				read_finish = true;
		}		

		/* if read module state is ok, check hash and dispatch state */
		dispatch_exit = GetDispatchExitThreads();
		hash_exit = GetExitThreadsInfo();

		if (hash_exit->hs_thread_cur == 0 && dispatch_exit->send_thread_cur == 0)
		{
			/* all threads are running */
			sleep(1);
			continue;
		}

		if (!hash_finish && hash_exit->hs_thread_cur > 0)
		{
			/* check hash module state */
			pthread_mutex_lock(&hash_exit->mutex);
			for (flag = 0; flag < hash_exit->hs_thread_count; flag++)
			{
				ComputeThreadInfo * thread_info = hash_exit->hs_threads[flag];
				if (NULL != thread_info && thread_info->state != THREAD_EXIT_NORMAL) /* ERROR happened */
				{
					pthread_mutex_unlock(&hash_exit->mutex);
					ADBLOADER_LOG(LOG_ERROR,
						"[MAIN][thread main] hash module error, thread id :%ld, table name :%s, filepath : %s",
						thread_info->thread_id, table_info->table_name, filepath);
					/* stop hash other threads */
					StopHash();
					hash_failed = true;
					hash_finish = true;
					do_sucess = false;
					goto FAILED;
				}
			}
			if (hash_exit->hs_thread_cur == hash_exit->hs_thread_count)
				hash_finish = true;
			pthread_mutex_unlock(&hash_exit->mutex);
		}

		if (!dispatch_finish && dispatch_exit->send_thread_cur > 0)
		{
			pthread_mutex_lock(&dispatch_exit->mutex);
			for (flag = 0 ;flag < dispatch_exit->send_thread_count; flag ++)
			{
				DispatchThreadInfo *dispatch_thread = NULL;
				dispatch_thread = dispatch_exit->send_threads[flag];
				if (NULL != dispatch_thread && dispatch_thread->state != DISPATCH_THREAD_EXIT_NORMAL)
				{
					pthread_mutex_unlock(&dispatch_exit->mutex);
					ADBLOADER_LOG(LOG_ERROR,
					"[MAIN][Tthread main] dispatch module error, thread id :%ld table name :%s, connection : %s, filepath :%s",
					dispatch_thread->thread_id, dispatch_thread->table_name, dispatch_thread->conninfo_datanode, filepath);
					/* stop dispatch other threads */
					StopDispatch();
					dispatch_failed = true;
					dispatch_finish = true;
					do_sucess = false;
					goto FAILED;
				}
			}
			if (dispatch_exit->send_thread_cur == dispatch_exit->send_thread_count)
				dispatch_finish = true;
			pthread_mutex_unlock(&dispatch_exit->mutex);
		}
	
	sleep(1);
	}

FAILED:
	if (hash_failed)
	{
		/* check read module is finish */
		if (!read_finish)
			SetReadProducerExit();

		if (!dispatch_finish)
			StopDispatch();
	}
	else if (dispatch_failed)
	{
		if (!read_finish)
			SetReadProducerExit();

		if (!hash_finish)
			StopHash();
	}

	CleanHashResource();
	CleanDispatchResource();
	/* free  memory */
	for (flag = 0; flag < table_info->use_datanodes_num; flag++)
	{
		/* free mesage queue */
		mq_pipe_destory(output_queue[flag]);
	}
	pfree(output_queue);
	output_queue = NULL;
	/* free inner queue */
	mq_pipe_destory(input_queue);
	/* free datanode info */
	free_datanode_info(datanode_info);
	datanode_info = NULL;
	/* free dispatch */
	free_dispatch_info(dispatch);
	dispatch = NULL;
	pfree(start);
	start = NULL;

	return do_sucess;
}

void
clean_hash_module(void)
{

}

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

		drop_func_to_server(setting->server_info->connection, table_info->funcinfo->drop_frunc_sql);
		create_func_to_server(setting->server_info->connection, table_info->funcinfo->creat_func_sql);
	}

	return;
}
static void get_use_datanodes(ADBLoadSetting *setting, TableInfo *table_info)
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
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select unnest(nodeoids) as nodeoids "
					"from pgxc_class "
					"where pcrelid = ("
						"select oid from pg_class where relname ='%s');", table_info->table_name);

	res = PQexec(conn, query);
	if ( !res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Connection return value failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	//Assert(PQntuples(res) != 0);
	if (PQntuples(res) == 0)
	{
		fprintf(stderr, "table information is wrong. \n");
		PQclear(res);
		exit_nicely(conn);
	}

	numtuples = PQntuples(res);
	if (numtuples > setting->datanodes_num)
	{
		fprintf(stderr, "table : \'%s\' use datanode num is wrong. \n", table_info->table_name);
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
				continue;
		}
	}

	table_info->use_datanodes_num = numtuples;
	table_info->use_datanodes = use_datanodes_info;
	
	qsort(table_info->use_datanodes,
					table_info->use_datanodes_num,
					sizeof(NodeInfoData*),
					adbloader_cmp_nodes);

	if (datanodes_oid != NULL)
		free(datanodes_oid);

	PQclear(res);
	PQfinish(conn);

}

static void reset_hash_field(TableInfo *table_info)
{
	HashField *hashfield = NULL;

	hashfield = (HashField *)palloc0(sizeof(HashField));

	hashfield->node_nums = table_info->funcinfo->node_count;
	hashfield->node_list = (Oid *)palloc0(hashfield->node_nums * sizeof(Oid));
	memcpy(hashfield->node_list, table_info->funcinfo->node_list, (hashfield->node_nums * sizeof(Oid)));

	hashfield->field_nums = table_info->funcinfo->func_args_count;
	hashfield->field_loc  = (int *)palloc0(hashfield->field_nums * sizeof(int));
	hashfield->field_type = (Oid *)palloc0(hashfield->field_nums * sizeof(Oid));
	memcpy(hashfield->field_loc, table_info->funcinfo->table_loc,       hashfield->field_nums * sizeof(int));
	memcpy(hashfield->field_type, table_info->funcinfo->func_args_type, hashfield->field_nums * sizeof(Oid));

	hashfield->func_name = pg_strdup(table_info->funcinfo->func_name);

	hashfield->delim        = pg_strdup(setting->hash_config->test_delim);
	hashfield->copy_options = pg_strdup(setting->hash_config->copy_option);
	hashfield->quotec       = setting->hash_config->copy_quotec[0];
	hashfield->has_qoute    = false;
	hashfield->hash_delim   = pg_strdup(setting->hash_config->hash_delim);

	table_info->table_attribute = hashfield;
}

static void drop_func_to_server(char *serverconninfo, char *drop_func_sql)
{
	PGconn     *conn;
	PGresult   *res;

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
}

static void create_func_to_server(char *serverconninfo, char *creat_func_sql)
{
	PGconn     *conn;
	PGresult   *res;

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
}

static void get_userdefined_funcinfo(const char *conninfo, TableInfo *table_info)
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

static void get_node_count_and_node_list(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
{
	char query[QUERY_MAXLEN];
	PGconn     *conn;
	PGresult   *res;
	int i;
	int local_node_nums;

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

static void get_hash_field(const char *conninfo, TableInfo *table_info)
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

	hashfield->node_nums = local_node_nums;
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

	//get other hashfield info
	hashfield->delim        = pg_strdup(setting->hash_config->test_delim);
	hashfield->copy_options = pg_strdup(setting->hash_config->copy_option);
	hashfield->quotec       = setting->hash_config->copy_quotec[0];
	hashfield->has_qoute    = false;
	hashfield->hash_delim   = pg_strdup(setting->hash_config->hash_delim);
	table_info->table_attribute = hashfield;
	PQclear(res);
	PQfinish(conn);
	return ;
}

static void get_special_table_by_sql(void)
{
	char query[QUERY_MAXLEN];
	PGconn     *conn;
	PGresult   *res;
	char *table_name;
	int table_nums = 0;
	int i = 0;
	conn = PQconnectdb(setting->coordinator_info->connection);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to coordinator failed: %s \n", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	sprintf(query, "select tablename from pg_tables where schemaname=\'public\'");

	res = PQexec(conn, query);
	if(PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
	{
		table_nums = PQntuples(res);
		for(i=0; i< table_nums; ++i)
		{
			table_name = PQgetvalue(res, i, 0);
			if(is_special_table(table_name) == 0)
			{
				special_table_list = add_special_table(special_table_list, table_name);
			}
		}
	}
	else
	{
		fprintf(stderr, "get_distribute_by failed: %s \n", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	PQclear(res);
	PQfinish(conn);
	
}
static struct special_table *add_special_table(struct special_table *table_list, char *table_name)
{
	struct TableName *table_ptr;
	if(table_list == NULL)
	{
		table_list = (struct special_table *)palloc0(sizeof(struct special_table));
		table_list->table_nums = 0;
		table_list->tb_ptr = NULL;
	}
	else if(table_name != NULL)
	{
		table_ptr = (struct TableName *)palloc0(sizeof(struct TableName));
		table_ptr->name = pstrdup(table_name);
		table_ptr->next = table_list->tb_ptr;
		table_list->tb_ptr = table_ptr;
		table_list->table_nums = table_list->table_nums + 1;
	}
	return table_list;
}

static char get_distribute_by(const char *conninfo, const char *tablename)
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

/*
static void get_func_info(const char *conninfo, TableInfo *table_info)
{
	UserFuncInfo *funcinfodata;

	funcinfodata = (UserFuncInfo *)palloc0(sizeof(UserFuncInfo));
	if (funcinfodata == NULL)
	{
		fprintf(stderr, "out of memory. \n");
		exit(1);
	}

	if (table_info->distribute_type == DISTRIBUTE_BY_USERDEFINED)
	{
		//get_func_args_count_and_type(conninfo, table_info->table_name, funcinfodata);
		//get_table_loc_count_and_loc(conninfo, table_info->table_name, funcinfodata);
		//get_create_and_drop_func_sql(conninfo, table_info->table_name, funcinfodata);
	}else if (table_info->distribute_type == DISTRIBUTE_BY_DEFAULT_HASH ||
			table_info->distribute_type == DISTRIBUTE_BY_DEFAULT_MODULO )
	{
		get_table_loc_for_hash_modulo(conninfo, table_info->table_name, funcinfodata);
	}else
	{
		fprintf(stderr, "unrecognized distribute by type: %c.\n", table_info->distribute_type);
		exit(1);
	}

	table_info->funcinfo = funcinfodata;
	return;
}
*/
static void get_table_loc_count_and_loc(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
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

/*
static void get_table_loc_for_hash_modulo(const char *conninfo, const char *tablename, UserFuncInfo *funcinfo)
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
*/
static void get_func_args_count_and_type(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo) 
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

static void get_create_and_drop_func_sql(const char *conninfo, char *table_name, UserFuncInfo *userdefined_funcinfo)
{
    char query[QUERY_MAXLEN];
    LineBuffer *create_func_sql;
	char *create_func;
	LineBuffer *drop_func_sql;
	char *drop_func;
	PGconn *conn;
	PGresult *res;
	char *proretset;
	char *proname;
	char *prosrc;
	char *probin;
	char *funcargs;
	char *funciargs;
	char *funcresult;
	char *proiswindow;
	char *provolatile;
	char *proisstrict;
	char *prosecdef;
	char *proleakproof;
	char *proconfig;
	char *procost;
	char *prorows;
	char *lanname;

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
	probin = PQgetvalue(res, 0, PQfnumber(res, "probin"));
	funcargs = PQgetvalue(res, 0, PQfnumber(res, "funcargs"));
	funciargs = PQgetvalue(res, 0, PQfnumber(res, "funciargs"));
	funcresult = PQgetvalue(res, 0, PQfnumber(res, "funcresult"));
	proiswindow = PQgetvalue(res, 0, PQfnumber(res, "proiswindow"));
	provolatile = PQgetvalue(res, 0, PQfnumber(res, "provolatile"));
	proisstrict = PQgetvalue(res, 0, PQfnumber(res, "proisstrict"));
	prosecdef = PQgetvalue(res, 0, PQfnumber(res, "prosecdef"));
	proleakproof = PQgetvalue(res, 0, PQfnumber(res, "proleakproof"));
	proconfig = PQgetvalue(res, 0, PQfnumber(res, "proconfig"));
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
	userdefined_funcinfo->drop_frunc_sql = drop_func;

	release_linebuf(create_func_sql);
	release_linebuf(drop_func_sql);

	PQclear(res);
	PQfinish(conn);
}

static void get_all_datanode_info(ADBLoadSetting *setting)
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


	/* get conninfo for all datanodes. */
	
	return;
}

static void get_conninfo_for_alldatanode(ADBLoadSetting *setting)
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

static void exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

/*
 * According to the directory file to get the table infomation from the name of file 
 */
static Tables* read_table_info(char *fullpath)
{
	/*check the file_path is directory*/
	DIR *dir;
	struct dirent *ptr;
	char *file_path;
	char *file_name;
	char **file_name_list;
	char **file_path_list;
	int file_index = 0;
	int file_num = 0;
	char file_path_temp[1024];
	Tables *tables_ptr = NULL;
	
	if(is_type_dir(fullpath) == 0)
	{
		fprintf(stderr, "\"%s\" has directory file", fullpath);
		return NULL;
	}
	
	file_num = get_file_num(fullpath);
	if ((dir=opendir(fullpath)) == NULL)
	{
		fprintf(stderr, "open dir %s error...\n", fullpath);
		return NULL;
	}

	file_name_list = (char **)palloc0(file_num * sizeof(char *));
	file_index = 0;
	while ((ptr=readdir(dir)) != NULL)
	{
		if(strcmp(ptr->d_name,".")==0 || strcmp(ptr->d_name,"..")==0)
			continue;

		sprintf(file_path_temp, "%s/%s", fullpath, ptr->d_name);
		if(file_exists(file_path_temp))
		{
			file_name_list[file_index] = pg_strdup(ptr->d_name);
			file_index = file_index + 1;
		}
	}

	if(has_special_file(file_name_list, file_num) == 0)
	{
		if (file_name_list != NULL)
		{
			int i;
			for (i = 0; i < file_num; i++)
			{
				pfree(file_name_list[i]);
				file_name_list[i] = NULL;
			}
		}
		pfree(file_name_list);
		file_name_list = NULL;

		return NULL;
	}

	//no need sort
	//str_list_sort(file_name_list, file_num);

	file_name_print(file_name_list, file_num);

	file_path_list = (char **)palloc0(file_num * sizeof(char *));
	for(file_index = 0; file_index < file_num; ++file_index)
	{
		file_name = file_name_list[file_index];
		file_path = (char*)palloc0(strlen(fullpath) + strlen(file_name) + 2);/* 2 = '\0' + '/' */
		sprintf(file_path, "%s/%s", fullpath, file_name);
		file_path_list[file_index] = file_path;
	}

	/*get tables info */
	tables_ptr = get_tables(file_name_list, file_path_list, file_num);

	tables_print(tables_ptr);
	closedir(dir);

	/* pfree file_name_list */
	if (file_name_list != NULL)
	{
		int i;
		for (i = 0; i < file_num; i++)
		{
			pfree(file_name_list[i]);
			file_name_list[i] = NULL;
		}
	}
	pfree(file_name_list);
	file_name_list = NULL;

	/*pfree file_path_list*/
	if (file_path_list != NULL)
	{
		int i;
		for (i = 0; i < file_num; i++)
		{
			pfree(file_path_list[i]);
			file_path_list[i] = NULL;
		}
	}
	pfree(file_path_list);
	file_path_list = NULL;

	return tables_ptr;
}

static Tables * get_tables(char **file_name_list, char **file_path_list, int file_num)
{
	TableInfo *table_info_ptr = NULL;
	TableInfo *table_info_prev_ptr = NULL;
	TableInfo *table_info_head_ptr = NULL;
	Tables *tables_ptr = NULL;
	int file_index = 0;
	int table_sum = 0;

	if(file_num <=0)
		return NULL;

	table_info_ptr = get_table_info(file_name_list, file_num);
	if(table_info_ptr != NULL)
	{
		table_info_head_ptr = table_info_ptr;
		table_info_prev_ptr = table_info_head_ptr;
	}

	file_index =0;
	table_sum = 0;

	while(table_info_ptr)
	{
		int i;
		for (i = 0; i < table_info_ptr->file_nums; i++)
		{
			FileLocation * file_location = (FileLocation*)palloc0(sizeof(FileLocation));
			file_location->location = pg_strdup(file_path_list[file_index]);
			slist_push_head(&table_info_ptr->file_head, &file_location->next);
			file_index++;
		}

		table_sum = table_sum + 1;
		table_info_ptr = get_table_info(&(file_name_list[file_index]), file_num - file_index);
		table_info_prev_ptr->next = table_info_ptr;
		table_info_prev_ptr = table_info_ptr;
	}

	if(table_sum > 0)
	{
		tables_ptr = (Tables *)palloc0(sizeof(Tables));
		tables_ptr->table_nums = table_sum;
		tables_ptr->info = table_info_head_ptr;
	}

	return tables_ptr;
}
static TableInfo *get_table_info(char **file_name_list, int file_num)
{
	char *table_name;
	char table_name_temp[256];
	int file_index;
	int file_name_len = 0;
	int table_split_num = 0;
	TableInfo *table_info_ptr;
	if(file_name_list == NULL || file_num <= 0)
		return NULL;
	table_info_ptr = (TableInfo *)palloc0(sizeof(TableInfo));
	slist_init(&table_info_ptr->file_head);

	for(file_index = 0; file_index < file_num; ++file_index)
	{
		strcpy(table_name_temp,file_name_list[file_index]);
		file_name_len = strlen(table_name_temp) - 1;

		while(table_name_temp[file_name_len] != '_')
			--file_name_len;
		table_name_temp[file_name_len] = '\0';

		if(table_info_ptr->table_name == NULL)
		{
			table_name = (char*)palloc0(strlen(table_name_temp) + 1);
			memcpy(table_name, table_name_temp, strlen(table_name_temp) + 1);
			table_info_ptr->table_name = table_name;
			table_split_num = 1;
		}

		else if(strcmp(table_name_temp ,table_info_ptr->table_name) == 0)
		{
			++table_split_num;
		}
		else
		{ 
			if(table_split_num == 1)
				
			break;
		}
	}

	table_info_ptr->file_nums = table_split_num;
	table_info_ptr->next = NULL;
	return table_info_ptr;
}

static int has_special_file(char **file_name_list, int file_nums)
{
	int i = 0;
	int is_valid = 0;

	for(i = 0; i < file_nums; ++i)
	{
		is_valid = is_special_file_name(file_name_list[i]);
		if(!is_valid)
		{	
			fprintf(stderr, "the format of file name %s is error\n", file_name_list[i]);
			return 0;
		}
	}	
	return 1;
}

static int is_special_table(char *table_name)
{
	return is_special_file_name(table_name);
}

/* if the tail of file_name is underline add digit   reutrn true
   else   return false
*/

static int is_special_file_name(char *file_name)
{
	int file_name_len = 0;
	file_name_len = strlen(file_name) - 5;
	while(is_digit(file_name[file_name_len])) 
		--file_name_len;

	if(file_name[file_name_len] == '_')
		return 1;
	else
		return 0;
}
static int get_file_num(char *fullpath)
{
	DIR *dir;
	struct dirent *ptr;
	char file_path[1024];
	int file_num = 0;
	
	if ((dir=opendir(fullpath)) == NULL)
    {
	//	fprintf(stderr, "open dir %s error...\n", fullpath);
        return 0;
    }
	/*get file of directory*/
	file_num = 0;
	while ((ptr=readdir(dir)) != NULL)
    {
        if(strcmp(ptr->d_name,".")==0 || strcmp(ptr->d_name,"..")==0)    ///current dir OR parrent dir
            continue;
		
		sprintf(file_path, "%s/%s", fullpath, ptr->d_name);
		if(is_type_file(file_path))
		{
			++file_num;
		}
	}
	closedir(dir);
	return file_num;
}
static void str_list_sort(char **file_list, int file_num)
{
	int i=0;
	int j=0;
	int min_index  = 0;
    for(i=0; i < file_num; ++i)
    {
        min_index = i; 
        for(j=i+1; j < file_num; j++)
        {
			if(strcmp(file_list[j], file_list[min_index]) < 0)
			{
                min_index = j;
            }
        }
        if( i != min_index)
        {
            char *temp = file_list[i];
            file_list[i] = file_list[min_index];
            file_list[min_index] = temp;
        }
    }
}

void file_name_print(char **file_list, int file_num)
{
	int file_index = 0;
	for(file_index = 0; file_index < file_num; ++file_index)
	{
		printf("%s\n",file_list[file_index]);
	}
}

void tables_print(Tables *tables_ptr)
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

static void tables_list_free(Tables *tables)
{
	TableInfo *table_info;
	TableInfo *table_info_next;
	
	table_info = tables->info;
	
	while(table_info)
	{
		table_info_next = table_info->next;
		table_free(table_info);
		table_info = table_info_next;	
	}
	
	pfree(tables);
	tables = NULL;
}

static void table_free(TableInfo *table_info)
{
	int i = 0;
	slist_mutable_iter siter;
	Assert(NULL != table_info && NULL != table_info->table_name);

	pg_free(table_info->table_name);
	table_info->table_name = NULL;

	clean_hash_field(table_info->table_attribute);

	if(table_info->funcinfo != NULL)
	{
		if(table_info->funcinfo->creat_func_sql)
		{
			pfree(table_info->funcinfo->creat_func_sql);
			table_info->funcinfo->creat_func_sql = NULL;
		}

		if(table_info->funcinfo->drop_frunc_sql)
		{
			pfree(table_info->funcinfo->drop_frunc_sql);
			table_info->funcinfo->drop_frunc_sql = NULL;
		}
		
		if(table_info->funcinfo->func_args_type)
		{
			pfree(table_info->funcinfo->func_args_type);
			table_info->funcinfo->func_args_type = NULL;
		}
		
		if(table_info->funcinfo->table_loc)
		{
			pfree(table_info->funcinfo->table_loc);
			table_info->funcinfo->table_loc = NULL;
		}
		
		pfree(table_info->funcinfo);
		table_info->funcinfo = NULL;
	}

	slist_foreach_modify(siter, &table_info->file_head)
	{
		FileLocation * file_location = slist_container(FileLocation, next, siter.cur);
		if (file_location->location)
			pfree(file_location->location);
		file_location->location = NULL;
		pfree(file_location);
	}
	
	for (i = 0; i < table_info->use_datanodes_num; i++)
	{
		free_NodeInfoData(table_info->use_datanodes[i]);
		pg_free(table_info->use_datanodes[i]);
		table_info->use_datanodes[i] = NULL;
	}

	pfree(table_info);
	table_info = NULL;
}

static int is_type_file(char *fullpath)
{
	struct stat statbuf;
	if(lstat(fullpath, &statbuf)<0)/*stat error*/
		return 0;
	if(S_ISDIR(statbuf.st_mode) == 0)
		return 1;
	return 0;
}

static int
is_type_dir(char *fullpath)
{
	struct stat statbuf;
	if(lstat(fullpath, &statbuf)<0)/*stat error*/
	{	
		return 0;
	}
	if(S_ISDIR(statbuf.st_mode) == 0)
		return 0;
	return 1;
}

char *
conver_type_to_fun (Oid type, DISTRIBUTE loactor)
{
	char		* func;
	LineBuffer  * buf;

	buf = get_linebuf();
	switch (type)
	{

		case INT8OID:
			/* This gives added advantage that
			 *	a = 8446744073709551359
			 * and	a = 8446744073709551359::int8 both work*/			
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
	int flag;
	Assert(NULL != datanode_info && NULL != datanode_info->datanode && NULL != datanode_info->conninfo);
	pfree(datanode_info->datanode);
	datanode_info->datanode = NULL;

	for (flag = 0; flag < datanode_info->node_nums; flag++)
	{
		pfree(datanode_info->conninfo[flag]);
		datanode_info->conninfo[flag] = NULL;
	}
	pfree(datanode_info->conninfo);
	datanode_info->conninfo = NULL;

	pfree(datanode_info);		
}

void
free_dispatch_info (DispatchInfo *dispatch_info)
{
	Assert(NULL != dispatch_info && NULL != dispatch_info->table_name && NULL != dispatch_info->conninfo_agtm);

	pfree(dispatch_info->table_name);
	dispatch_info->table_name = NULL;
	pfree(dispatch_info->conninfo_agtm);
	dispatch_info->conninfo_agtm = NULL;
	if (dispatch_info->copy_options)
	{
		pfree(dispatch_info->copy_options);
		dispatch_info->copy_options = NULL;
	}

	dispatch_info->output_queue = NULL;
	pfree(dispatch_info->start_cmd);
	dispatch_info->start_cmd = NULL;
	pfree(dispatch_info);
}

static void
main_write_error_message(char * message, char *start_cmd)
{
	LineBuffer *error_buffer = NULL;
	error_buffer = format_error_info(message, MAIN, NULL, 0, NULL);
	if (start_cmd)
	{
		appendLineBufInfoString(error_buffer, "\n");
		appendLineBufInfoString(error_buffer, "suggest : ");
		appendLineBufInfoString(error_buffer, start_cmd);
		appendLineBufInfoString(error_buffer, "\n");
	}
	appendLineBufInfoString(error_buffer, "-------------------------------------------------------\n");
	appendLineBufInfoString(error_buffer, "\n");
	write_error(error_buffer);
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
	   fprintf(stderr,"open file 1.c error! %s\n",strerror(errno));
	}

	 while((ch = fgetc(fp)) != EOF)
     {
         if(ch == '\n')
         {
             n++;
         }
     }
	 fclose(fp);
	 return n;
}

bool file_exists(char *file)
{
    FILE  *f = fopen(file, "r");

    if (!f)
        return false;

    fclose(f);
    return true;
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
	printf("%s [%d/%d][%f%%][%c]\r", buf, send, total, precent, index[send % 4]);
	/* flush cache buffer */
	fflush(stdout);
//	printf("\n");
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
		printf("%s [%d][%d/%d][%f%%]\r", buf[flag], flag, send[flag], total, precent);
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
show_process(int total,
			int datanodes, int * thread_send_total, DISTRIBUTE type)
{
	int flag;
	Assert(datanodes > 0 && thread_send_total != NULL);
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

