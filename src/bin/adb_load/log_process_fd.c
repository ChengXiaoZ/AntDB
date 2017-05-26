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
#include "log_process_fd.h"
#include "log_detail_fd.h"

FILE *log_process_fd = NULL;
LOG_LEVEL g_log_level = LOG_INFO;

static char *get_file_name(const char *file_path);

void
open_log_process_fd(char *logfilepath, LOG_LEVEL log_level)
{
	char logpath[MAXPGPATH];
	g_log_level = log_level;

	if (logfilepath == NULL)
	{
		snprintf(logpath, sizeof(logpath), "./log_process.log");
	}
	else
	{
		if (logfilepath[strlen(logfilepath) - 1] == '/')
			snprintf(logpath, sizeof(logpath), "%s%s", logfilepath, "log_process.log");
		else
			snprintf(logpath, sizeof(logpath), "%s/%s", logfilepath, "log_process.log");
	}

	log_process_fd = fopen(logpath, "a+");
	if (log_process_fd == NULL)
	{
		fprintf(stderr, "could not open log file: %s.\n",  logpath);
		exit(1);
	}

	return;
}

void
close_log_process_fd()
{
	if (fclose(log_process_fd))
	{
		fprintf(stderr, "could not close log file.\n");
	}

	return;
}

void
write_log_process_fd(LOG_LEVEL log_level, const char *file, const char *fun, int line, const char *fmt, ...)
{
	char buffer[1024] = {0};
	char new_fmt[1024] = {0};
	char *head_fmt = NULL;
	va_list args;
	int count = 0;
	char *p_file_name = NULL;

	if (log_level < g_log_level)
		return;

	switch (log_level)
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
			fprintf(stderr, "unrecognized log type: %d \n", log_level);
			exit(1);
			break;
	}

	p_file_name = get_file_name(file);

	count = sprintf(new_fmt, head_fmt, p_file_name, fun, line, get_current_time());
	free(p_file_name);

	strcat(new_fmt + count, fmt);

	va_start(args, fmt);
	vsnprintf(buffer, sizeof(buffer), new_fmt, args);

	if (log_process_fd == NULL)
	{
		open_log_process_fd(NULL, LOG_INFO);
	}

	buffer[strlen(buffer)] = '\n';
	fwrite(buffer, strlen(buffer), 1, log_process_fd);
	fflush(log_process_fd);
	va_end(args);

	return;
}

static char *
get_file_name(const char *file_path)
{
	char *ptr = NULL;

	ptr = strrchr(file_path, '/');
	if (ptr == NULL)
		fprintf(stderr, "The character \'/\' was not found.\n");

	return pg_strdup(ptr + 1);
}

