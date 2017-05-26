#ifndef ADB_LOAD_UTILITY_H
#define ADB_LOAD_UTILITY_H

#include "linebuf.h"
#include "compute_hash.h"
#include "dispatch.h"
#include "loadsetting.h"

typedef enum DISTRIBUTE
{
	DISTRIBUTE_BY_REPLICATION,
	DISTRIBUTE_BY_ROUNDROBIN,
	DISTRIBUTE_BY_USERDEFINED,
	DISTRIBUTE_BY_DEFAULT_HASH,
	DISTRIBUTE_BY_DEFAULT_MODULO,
	DISTRIBUTE_BY_ERROR
} DISTRIBUTE;

struct enum_name
{
	int   type;
	char *name;
};

typedef enum Module
{
	MAIN,
	READER,
	QUEUE,
	HASHMODULE,
	DISPATCH,
	DEFAULT
} Module;

typedef struct ConnectionNode
{
	char *database;
	char *user;
	char *passw;
} ConnectionNode;

typedef struct FileInfo
{
	char *file_path;
	char *table_name;
	char *config_path;
	ConnectionNode *connection;
} FileInfo;

typedef struct TableInfo
{
	int            file_nums;        /*the num of files which own to one table */
	char          *table_name;       /*the name of table*/
	HashField     *table_attribute;  /*the attribute of table*/
	DISTRIBUTE     distribute_type;
	UserFuncInfo  *funcinfo;
	slist_head     file_head;
	NodeInfoData **use_datanodes;
	int            use_datanodes_num; /*use datanode num for special table */
	int            threads_num_per_datanode;
	struct TableInfo *next;
} TableInfo;

/* module error file format */
extern LineBuffer *format_error_begin (char *file_name, char *table_type);
extern LineBuffer *format_error_info (char *message, Module type, char *error_message,
								      int line_no, char *line_data);

extern LineBuffer *format_error_end (char *file_name);

extern ConnectionNode *create_connectionNode(char *database, char *user, char *passw);

extern FileInfo *create_fileInfo (char *file_path, char *table_name, char *config_path,
							char *database, char *user, char *passw);

extern void free_conenctionNode (ConnectionNode *connection);

extern void free_fileInfo (FileInfo *fileInfo);

extern char * create_start_command(char *file_path, ADBLoadSetting *setting, const TableInfo *table_info);

extern char* get_current_time(void);

extern bool directory_exists(char *dir);
extern void make_directory(const char *dir);

extern unsigned long file_size(const char *file);
extern bool file_exists(const char *file);
extern bool remove_file(const char *file);

extern char *adb_load_tolower(char *str);
extern bool check_copy_comment_str_valid(char *copy_cmd_comment_str);

#endif /* ADB_LOAD_UTILITY_H */