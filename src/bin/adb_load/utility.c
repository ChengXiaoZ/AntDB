#include <stdio.h>
#include <time.h>

#include "postgres_fe.h"
#include "utility.h"
#include <sys/types.h>
#include <sys/stat.h>

static char *rename_string_suffix(char *file_path, char *suffix);

LineBuffer *
format_error_begin(char *file_name, char*table_type)
{
	LineBuffer * linebuf = get_linebuf();

	Assert(file_name != NULL);
	Assert(table_type != NULL);

	appendLineBufInfoString(linebuf, "==================");
	appendLineBufInfo(linebuf, "[%s]begin file: ", get_current_time());
	appendLineBufInfoString(linebuf, file_name);
	appendLineBufInfoString(linebuf, "==================\n");

	return linebuf;
}

LineBuffer *
format_error_info(char *message, Module type, char *error_message,
								int line_no, char *line_data)
{
	LineBuffer *linebuf = get_linebuf();
	Assert(message != NULL);

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
	if (error_message != NULL)
	{
		appendLineBufInfoString(linebuf, error_message);
	}
	else
	{
		appendLineBufInfoString(linebuf, "\n");
	}

	if (type == HASHMODULE)
	{
		appendLineBufInfoString(linebuf, "Failed line loc  : ");
		appendLineBufInfo(linebuf, "%d", line_no);
		appendLineBufInfoString(linebuf, "\n");

		appendLineBufInfoString(linebuf, "Failed line data : ");
		if (line_data != NULL)
			appendLineBufInfoString(linebuf, line_data);
	}

	return linebuf;
}

LineBuffer *
format_error_end (char *file_name)
{
	LineBuffer * linebuf = get_linebuf();

	Assert(file_name != NULL);

	appendLineBufInfoString(linebuf, "==================");
	appendLineBufInfo(linebuf, "[%s]end file  : ", get_current_time());
	appendLineBufInfoString(linebuf,file_name);
	appendLineBufInfoString(linebuf, "==================\n\n");

	return linebuf;
}

ConnectionNode *
create_connectionNode(char *database, char *user, char *passw)
{
	ConnectionNode *connection = NULL;
	Assert(database != NULL);
	Assert(user != NULL);

	connection = (ConnectionNode*)palloc0(sizeof(ConnectionNode));
	connection->database = pg_strdup(database);
	connection->user = pg_strdup(user);
	if (passw != NULL)
		connection->passw = pg_strdup(passw);
	return connection;
}

FileInfo *
create_fileInfo (char * file_path, char * table_name, char * config_path,
							char * database, char * user, char * passw)
{
	FileInfo *fileInfo = NULL;

	Assert(file_path != NULL);
	Assert(table_name != NULL);
	Assert(config_path != NULL);
	Assert(database != NULL);
	Assert(user != NULL);

	fileInfo = (FileInfo*)palloc0(sizeof(FileInfo));
	fileInfo->file_path = pg_strdup(file_path);
	fileInfo->table_name = pg_strdup(table_name);
	fileInfo->config_path = pg_strdup(config_path);
	fileInfo->connection = create_connectionNode(database, user, passw);
	return fileInfo;
}

void
free_conenctionNode (ConnectionNode *connection)
{
	Assert(connection != NULL);
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
	connection = NULL;
}

void
free_fileInfo(FileInfo *fileInfo)
{
	Assert(fileInfo != NULL);

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
create_start_command(char *file_path, ADBLoadSetting *setting, const TableInfo *table_info)
{
	char       *start = NULL;
	char       *new_file_path_suffix = NULL;
	LineBuffer *linebuf = get_linebuf();

	Assert(setting->program != NULL);
	Assert(file_path != NULL);
	Assert(table_info->table_name != NULL);
	Assert(setting->config_file_path != NULL);
	Assert(setting->database_name != NULL);
	Assert(setting->user_name != NULL);

	appendLineBufInfoString(linebuf, setting->program);
	appendLineBufInfoString(linebuf, " -g");
	appendLineBufInfoString(linebuf, " -d ");
	appendLineBufInfoString(linebuf, setting->database_name);
	appendLineBufInfoString(linebuf, " -U ");
	appendLineBufInfoString(linebuf, setting->user_name);
	if (setting->password != NULL)
	{
		appendLineBufInfoString(linebuf, " -W ");
		appendLineBufInfoString(linebuf, setting->password);
	}
	appendLineBufInfoString(linebuf, " -c ");
	appendLineBufInfoString(linebuf, setting->config_file_path);
	appendLineBufInfoString(linebuf, " -f ");

	if (setting->static_mode || setting->dynamic_mode)
	{
		new_file_path_suffix = rename_string_suffix(file_path, ".error");
		appendLineBufInfoString(linebuf, new_file_path_suffix);
		pg_free(new_file_path_suffix);
		new_file_path_suffix = NULL;
	}else if (setting->single_file_mode)
	{
		appendLineBufInfoString(linebuf, setting->input_file);
	}

	appendLineBufInfoString(linebuf, " -t ");
	appendLineBufInfoString(linebuf, table_info->table_name);

	if (table_info->distribute_type == DISTRIBUTE_BY_REPLICATION ||
		table_info->distribute_type == DISTRIBUTE_BY_ROUNDROBIN)
	{
		appendLineBufInfo(linebuf, " -r %d", table_info->threads_num_per_datanode);
	}
	else if (table_info->distribute_type == DISTRIBUTE_BY_USERDEFINED ||
			table_info->distribute_type == DISTRIBUTE_BY_DEFAULT_HASH ||
			table_info->distribute_type  == DISTRIBUTE_BY_DEFAULT_MODULO)
	{
		appendLineBufInfo(linebuf, " -h %d", table_info->table_attribute->hash_threads_num);
		appendLineBufInfo(linebuf, " -r %d", table_info->threads_num_per_datanode);
	}

	start = (char*)palloc0(linebuf->len + 1);
	memcpy(start, linebuf->data, linebuf->len);
	start[linebuf->len] = '\0';

	release_linebuf(linebuf);

	return start;
}


static char *
rename_string_suffix(char *file_path, char *suffix)
{
	char new_string_suffix[1024];
	char tmp[1024] ;
	int  tmp_len = 0;

	strcpy(tmp, file_path);

	tmp_len = strlen(tmp);
	while (tmp[tmp_len] != '.')
		tmp_len--;

	tmp[tmp_len] = '\0';

	sprintf(new_string_suffix, "%s%s", tmp, suffix);
	return pstrdup(new_string_suffix);
}

char *
get_current_time()
{
	static char nowtime[20];
	time_t rawtime;
	struct tm result = {0};

	time(&rawtime); // rawtime = time(NULL)
	localtime_r(&rawtime, &result); // threads safe
	strftime(nowtime, sizeof(nowtime), "%Y-%m-%d %H:%M:%S", &result);

	return nowtime;
}

bool
directory_exists(char *dir)
{
	struct stat st;

	if (stat(dir, &st) != 0)
		return false;
	if (S_ISDIR(st.st_mode))
		return true;
	return false;
}

/* make a directory */
void
make_directory(const char *dir)
{
	if (mkdir(dir, S_IRWXU | S_IRWXG | S_IRWXO) < 0)
	{
		fprintf(stderr, _("Error: could not create directory \"%s\": %s\n"),
				dir, strerror(errno));
		exit(2);
	}
}

/*
 * Count bytes in file
 */
unsigned long
file_size(const char *file)
{
	unsigned long filesize = -1;
	struct stat statbuff;

	if(stat(file, &statbuff) < 0)
	{
		return filesize;
	}
	else
	{
		filesize = statbuff.st_size;
	}

	return filesize;
}

bool
file_exists(const char *file)
{
	struct stat st;

	Assert(file != NULL);

	if (stat(file, &st) == 0)
	{
		return S_ISDIR(st.st_mode) ? false : true;
	}
	else if (!(errno == ENOENT   ||
				errno == ENOTDIR ||
				errno == EACCES))
	{
		fprintf(stderr, "Error: could not access file \"%s\".\n", file);
	}

	return false;
}

bool
remove_file(const char *file)
{
	int res = 0;

	res = remove(file);
	if (res == -1)
	{
		fprintf(stderr, "remove fie \"%s\" failed.\n", file);
		return false;
	}

	return true;
}

char *
adb_load_tolower(char *str)
{
	char *orign = str;

	for (; *str != '\0'; str++)
	{
		*str = pg_tolower(*str);
	}

	return orign;
}

/*
 * Fold a character to lower case.
 *
 * Unlike some versions of tolower(), this is safe to apply to characters
 * that aren't upper case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
unsigned char
pg_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	else if (IS_HIGHBIT_SET(ch) && isupper(ch))
		ch = tolower(ch);
	return ch;
}

bool
check_copy_comment_str_valid(char *copy_cmd_comment_str)
{
	int str_len = 0;
	char first_char = '\0';
	char second_char = '\0';

	/* copy command comment string length must be 2. */
	str_len = strlen(copy_cmd_comment_str);
	if (str_len != 2)
		return false;

	first_char = *copy_cmd_comment_str;
	second_char = *(copy_cmd_comment_str + 1);

	/*first char and second char must be the same. */
	if (first_char != second_char)
		return false;

	return true;
}

