#ifndef UTILITY_H
#define UTILITY_H

#include "linebuf.h"

struct enum_name
{
	int type;
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
	char * database;
	char * user;
	char * passw;
} ConnectionNode;

typedef struct FileInfo
{
	char * file_path;
	char * table_name;
	char * config_path;
	ConnectionNode * connection;
} FileInfo;

/* module error file format */
LineBuffer * format_error_begin (char * file_name, char*  table_type);
LineBuffer * format_error_info (char * message, Module type, char * error_message,
								int line_no, char *line_data);
LineBuffer * format_error_end (char * file_name);
ConnectionNode * create_connectionNode(char * database, char * user, char * passw);
FileInfo   * create_fileInfo (char * file_path, char * table_name, char * config_path,
							char * database, char * user, char * passw);
void		 free_conenctionNode (ConnectionNode * connection);
void		 free_fileInfo (FileInfo *fileInfo);
char 	  *	 create_start_command (char * program, char * file_path, char * table_name, char * config_path,
							char * database, char * user, char * passw);	
#endif