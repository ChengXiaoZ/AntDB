#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "utility.h"
#include "postgres_fe.h"
#include "loadsetting.h"
#include "adbloader_log.h"

FILE *adbloader_log_file_fd = NULL;
FILE *adb_load_error_data_fd = NULL;
LOG_TYPE log_output_type = LOG_INFO;

static char *get_file_name(const char *file_path);
static char *get_file_path(char *path, char *tablename);
static void remove_duplicate_lines(char *file_path);

void adbLoader_log_init(char *logfilepath, LOG_TYPE type)
{
	char logpath[MAXPGPATH];
	log_output_type = type;

	if (logfilepath == NULL)
	{
		snprintf(logpath, sizeof(logpath), "./adb_load_log_file.txt");
	}
	else
	{
		if (logfilepath[strlen(logfilepath) - 1] == '/')
			snprintf(logpath, sizeof(logpath), "%s%s", logfilepath, "adb_load_log_file.txt");
		else
			snprintf(logpath, sizeof(logpath), "%s/%s", logfilepath, "adb_load_log_file.txt");
	}

	adbloader_log_file_fd = fopen(logpath, "a+");
	if (adbloader_log_file_fd == NULL)
	{
		fprintf(stderr, "could not open log file: %s.\n",  logpath);
		exit(1);
	}

	return;
}

void adbLoader_log_end(void)
{
	if (fclose(adbloader_log_file_fd))
	{
		fprintf(stderr, "could not close log file.\n");
	}
}

void adbloader_log_type(LOG_TYPE type, const char *file, const char *fun, int line, const char *fmt, ...)
{
	char buffer[1024] = {0};
	char new_fmt[1024] = {0};
	char *head_fmt = NULL;
	va_list args;
	int cnt;
	int n;
	char *p_file_name = NULL;

	if (type < log_output_type)
		return;

	switch (type)
	{
		case LOG_ERROR:
			head_fmt = "[file:%s][ERROR][function:%s][line:%d][time:%s]";
			break;
		case LOG_WARN:
			head_fmt = "[file:%s][WARN ][function:%s][line:%d][time:%s]";
			break;
		case LOG_INFO:
			head_fmt = "[file:%s][INFO ][function:%s][line:%d][time:%s]";
			break;
		case LOG_DEBUG:
			head_fmt = "[file:%s][DEBUG][function:%s][line:%d][time:%s]";
			break;
		default:
			fprintf(stderr, "unrecognized log type: %d \n", type);
			exit(1);
			break;
	}

	p_file_name = get_file_name(file);

	n = sprintf(new_fmt, head_fmt, p_file_name, fun, line, get_current_time());
	free(p_file_name);

	strcat(new_fmt + n, fmt);

	va_start(args, fmt);
	cnt = vsnprintf(buffer, sizeof(buffer), new_fmt, args);

	if (adbloader_log_file_fd == NULL)
	{
		adbLoader_log_init(NULL, LOG_INFO);
	}

	buffer[strlen(buffer)] = '\n';
	fwrite(buffer, strlen(buffer), 1, adbloader_log_file_fd);
	fflush(adbloader_log_file_fd);
	va_end(args);

	return;
}

static char *get_file_name(const char *file_path)
{
	char *ptr = NULL;

	ptr = strrchr(file_path, '/');
	if (ptr == NULL)
		fprintf(stderr, "The character \'/\' was not found.\n");

	return strdup(ptr + 1);
}

//===================================================================

void fopen_adb_load_error_data(char *path, char *tablename)
{
	char *file_path = NULL;

	Assert(tablename != NULL);

	file_path = get_file_path(path, tablename);
	adb_load_error_data_fd = fopen(file_path, "a+");
	if (adb_load_error_data_fd == NULL)
	{
		fprintf(stderr, "could not open log file: %s.\n",  file_path);
		pfree(file_path);
		file_path = NULL;
		exit(EXIT_FAILURE);
	}

	pfree(file_path);
	file_path = NULL;

	return;
}

void check_db_load_error_data(char *path, char *tablename)
{
	char *file_path = NULL;

	file_path = get_file_path(path, tablename);
	if (file_exists(file_path))
	{
		if (file_size(file_path) > 0)
		{
			remove_duplicate_lines(file_path);
			pfree(file_path);
			file_path = NULL;
			return;
		}
		else // file is empty
		{
			remove_file(file_path);
		}
	}

	pfree(file_path);
	file_path = NULL;

	return;    
}

void fwrite_adb_load_error_data(char *linedata)
{
	int ret = 0;

	if (linedata == NULL)
		return;

	ret = fwrite(linedata, 1, strlen(linedata), adb_load_error_data_fd);
	if (ret != strlen(linedata))
	{
		fprintf(stderr, "could not write data to error data file.\n");
		exit(EXIT_FAILURE);
	}

	fflush(adb_load_error_data_fd);

	return;
}

void fclose_adb_load_error_data(void)
{
	if (fclose(adb_load_error_data_fd))
	{
		fprintf(stderr, "could not close error data file.\n");
		exit(EXIT_FAILURE);
	}

	return;
}

static char *get_file_path(char *path, char *tablename)
{
	char error_path[1024] = {0};

	Assert(path != NULL);
	Assert(tablename != NULL);

	if (path == NULL)
	{
		snprintf(error_path, sizeof(error_path), "./%s.txt", tablename);
	}
	else
	{
		if (path[strlen(path) - 1] == '/')
			snprintf(error_path, sizeof(error_path), "%s%s.txt", path, tablename);
		else
			snprintf(error_path, sizeof(error_path), "%s/%s.txt", path, tablename);
	}

	return pstrdup(error_path);
}

static void
remove_duplicate_lines(char *file_path)
{
	int res = 0;
	char cmd[1024] = {0};

	sprintf(cmd, "sort -u %s -o %s", file_path, file_path);

	res = system(cmd);
	if (res == 127 || res == -1)
	{
		fprintf(stderr, "remove duplicate line failed.\n");
	}

	return;
}
