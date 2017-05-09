#ifndef UTILITY_H
#define UTILITY_H

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
LineBuffer *format_error_begin (char *file_name, char *table_type);

LineBuffer *format_error_info (char *message, Module type, char *error_message,
								int line_no, char *line_data);

LineBuffer *format_error_end (char *file_name);

ConnectionNode *create_connectionNode(char *database, char *user, char *passw);

FileInfo *create_fileInfo (char *file_path, char *table_name, char *config_path,
							char *database, char *user, char *passw);

void free_conenctionNode (ConnectionNode *connection);

void free_fileInfo (FileInfo *fileInfo);

char * create_start_command(char *file_path, ADBLoadSetting *setting, const TableInfo *table_info);

char* get_current_time(void);

bool directory_exists(char *dir);
void make_directory(const char *dir);

unsigned long file_size(const char *file);
bool file_exists(const char *file);
bool remove_file(const char *file);
#endif