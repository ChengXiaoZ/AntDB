#include "postgres_fe.h"

#include <stdio.h>
#include "utility.h"


LineBuffer *
format_error_begin (char * file_name, char* table_type)
{
	LineBuffer * linebuf = get_linebuf();
	Assert(NULL != file_name && NULL != table_type);
	appendLineBufInfoString(linebuf, "-------------------------------------------------------");
	appendLineBufInfoString(linebuf, file_name);
	appendLineBufInfoString(linebuf, "-------------------------------------------------------\n");
	appendLineBufInfoString(linebuf, "TABLE_TYPE : ");
	appendLineBufInfoString(linebuf, table_type);
	appendLineBufInfoString(linebuf, "\n");
	return linebuf;
}

LineBuffer *
format_error_info (char * message, Module type, char * error_message,
								int line_no, char *line_data)
{
	LineBuffer * linebuf = get_linebuf();
	Assert(NULL != message);

	appendLineBufInfoString(linebuf, "Failed module : ");


	switch (type)
	{
		case MAIN:
			appendLineBufInfoString(linebuf, "MAIN \n");
			break;
		case READER:
			appendLineBufInfoString(linebuf, "READER \n");
			break;
		case QUEUE:
			appendLineBufInfoString(linebuf, "QUEUE \n");
			break;
		case HASHMODULE:
			appendLineBufInfoString(linebuf, "HASH \n");
			break;
		case DISPATCH:
			appendLineBufInfoString(linebuf, "DISPATCH \n");
			break;
		default:
			break;
	}

	appendLineBufInfoString(linebuf, "Explain       : ");
	appendLineBufInfoString(linebuf, message);
	appendLineBufInfoString(linebuf, "\n");

	appendLineBufInfoString(linebuf, "Failed Reason : ");
	if (NULL != error_message)		
		appendLineBufInfoString(linebuf, error_message);
	appendLineBufInfoString(linebuf, "\n");

	appendLineBufInfoString(linebuf, "Failed line loc  : ");
	appendLineBufInfo(linebuf, "%d", line_no);
	appendLineBufInfoString(linebuf, "\n");

	appendLineBufInfoString(linebuf, "Failed line data : ");
	if (NULL != line_data)		
		appendLineBufInfoString(linebuf, line_data);
	appendLineBufInfoString(linebuf, "\n");	
	return linebuf;
}

LineBuffer * format_error_end (char * file_name)
{
	LineBuffer * linebuf = get_linebuf();
	Assert(NULL != file_name);
	appendLineBufInfoString(linebuf, "--------------------------------------------------------------------------------------------------------------\n");
	return linebuf;
}

ConnectionNode *
create_connectionNode(char * database, char * user, char * passw)
{
	ConnectionNode *connection = NULL;
	Assert(NULL != database && NULL != user);

	connection = (ConnectionNode*)palloc0(sizeof(ConnectionNode));
	connection->database = pg_strdup(database);
	connection->user = pg_strdup(user);
	if (passw)
		connection->passw = pg_strdup(passw);
	return connection;
}

FileInfo   *
create_fileInfo (char * file_path, char * table_name, char * config_path,
							char * database, char * user, char * passw)
{
	FileInfo *fileInfo = NULL;
	Assert(NULL != file_path && NULL != table_name &&
				NULL != config_path && NULL != database && NULL != user);

	fileInfo = (FileInfo*)palloc0(sizeof(FileInfo));
	fileInfo->file_path = pg_strdup(file_path);
	fileInfo->table_name = pg_strdup(table_name);
	fileInfo->config_path = pg_strdup(config_path);
	fileInfo->connection = create_connectionNode(database, user, passw);
	return fileInfo;
}

void
free_conenctionNode (ConnectionNode * connection)
{
	Assert(NULL != connection);
	if (connection->database)
	{
		pfree(connection->database);
		connection->database = NULL;
	}

	if (connection->user)
	{
		pfree(connection->user);
		connection->user = NULL;
	}

	if (connection->passw)
	{
		pfree(connection->passw);
		connection->passw = NULL;
	}
	pfree(connection);
}


void
free_fileInfo(FileInfo *fileInfo)
{
	Assert(NULL != fileInfo);

	if (fileInfo->file_path)
	{
		pfree(fileInfo->file_path);
		fileInfo->file_path = NULL;
	}

	if (fileInfo->table_name)
	{
		pfree(fileInfo->table_name);
		fileInfo->table_name = NULL;
	}

	if (fileInfo->config_path)
	{
		pfree(fileInfo->config_path);
		fileInfo->config_path = NULL;
	}

	if (fileInfo->connection)
	{
		free_conenctionNode(fileInfo->connection);
		fileInfo->connection = NULL;
	}
	pfree(fileInfo);
	
}

char *
create_start_command (char * program, char * file_path, char * table_name, char * config_path,
							char * database, char * user, char * passw)
{
	LineBuffer * linebuf = get_linebuf();
	char	   * start = NULL;
	Assert(NULL != program && NULL != file_path && NULL != table_name &&
						NULL != config_path && NULL != database && NULL != user);

	appendLineBufInfoString(linebuf, program);
	appendLineBufInfoString(linebuf, " -s ");
	appendLineBufInfoString(linebuf, " -d ");
	appendLineBufInfoString(linebuf, database);
	appendLineBufInfoString(linebuf, " -U ");
	appendLineBufInfoString(linebuf, user);
	if (passw)
	{
		appendLineBufInfoString(linebuf, " -W ");
		appendLineBufInfoString(linebuf, passw);
	}
	appendLineBufInfoString(linebuf, " -c ");
	appendLineBufInfoString(linebuf, config_path);
	appendLineBufInfoString(linebuf, " -f ");
	appendLineBufInfoString(linebuf, file_path);
	appendLineBufInfoString(linebuf, " -t ");
	appendLineBufInfoString(linebuf, table_name);

	start = (char*)palloc0(linebuf->len+1);
	memcpy(start, linebuf->data, linebuf->len);
	start[linebuf->len] = '\0';
	release_linebuf(linebuf);
	return start;	
}

