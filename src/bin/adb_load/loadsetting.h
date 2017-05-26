#ifndef ADB_LOAD_SETTING_H
#define ADB_LOAD_SETTING_H

FILE *fp_hash_error;

typedef struct NodeInfo
{
	Oid   node_oid;
	char *node_name;
	char *node_host;
	char *node_port;
	char *user_name;
	char *database_name;
	char *connection;
} NodeInfoData;

typedef NodeInfoData ServerNodeInfo;

typedef struct HashConfig
{
	int   hash_thread_num;
	char *text_delim;
	char *hash_delim;
	bool  copy_header;
	char *copy_quotec;
	char *copy_escapec;
	char *copy_option;
}HashConfig;

typedef struct LogField
{
	char *log_level;
	char *log_path;
}LogField;

typedef struct ModuleErrFile
{
	char *hash_err_file;
	char *dispatch_err_file;
	char *read_err_file;
	char *err_file_dir;
}HashLogField;

typedef struct UserFuncInfo
{
	char *creat_func_sql;
	char *drop_func_sql;
	char *func_name;
	int   func_args_count;
	Oid  *func_args_type;
	int  *table_loc;
	int   table_loc_count;
	Oid  *node_list;
	int   node_count;
} UserFuncInfo;

typedef struct ADBLoadSetting
{
	char *input_file;
	char *input_directory;
	char *config_file_path;
	char *output_directory;
	char *database_name;
	char *user_name;
	char *password;
	char *table_name;
	char *program;

	bool  dynamic_mode;
	bool  static_mode;
	bool  single_file_mode;
	bool  stream_mode;
	bool  process_bar;
	bool  just_check;
	bool  config_datanodes_valid;
	int   threads_num_per_datanode;
	int   hash_thread_num;
	int   read_file_buffer;

	NodeInfoData    *server_info;
	NodeInfoData    *agtm_info;
	NodeInfoData    *coordinator_info;

	NodeInfoData    **datanodes_info;
	int               datanodes_num;

	HashConfig      *hash_config;
	LogField        *log_field;

	int             *redo_queue_index;
	int              redo_queue_total;
	bool             redo_queue;

	char            *filter_queue_file_path;
	bool             filter_queue_file;

	char            *error_data_file_path;

	bool             copy_cmd_comment;
	char            *copy_cmd_comment_str;

	int              error_threshold;
	bool             filter_first_line;
} ADBLoadSetting;

extern ADBLoadSetting *cmdline_adb_load_setting(int argc, char **argv);
extern void pg_free_adb_load_setting(ADBLoadSetting *setting);
extern void pg_free_NodeInfoData(NodeInfoData *pt);
extern void get_node_conn_info(ADBLoadSetting *setting);
extern void get_settings_by_config_file(ADBLoadSetting *setting);
extern void check_node_connection_valid(const char *host_ip, const char *host_port, const char *connection_str);

#endif /* ADB_LOAD_SETTING_H */
