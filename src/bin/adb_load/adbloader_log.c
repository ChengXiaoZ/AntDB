#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>

#include "postgres_fe.h"
#include "loadsetting.h"
#include "adbloader_log.h"

FILE *adbloader_log_file_fd = NULL;
LOG_TYPE log_output_type = LOG_INFO;


void adbLoader_log_init(char *logfilepath, LOG_TYPE type)
{
	char logpath[MAXPGPATH];
	log_output_type = type;
	if (logfilepath == NULL)
	{
		snprintf(logpath, sizeof(logpath), "./adbloader_log_file.txt");
	}
	else
	{
		if (logfilepath[strlen(logfilepath) - 1] == '/')
			snprintf(logpath, sizeof(logpath), "%s%s", logfilepath, "adbloader_log_file.txt");
		else
			snprintf(logpath, sizeof(logpath), "%s/%s", logfilepath, "adbloader_log_file.txt");
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

	if (type < log_output_type)
		return;

	switch (type)
	{
		case LOG_ERROR:
			head_fmt = "[file:%s]    [ERROR][function:%s][line:%d]";
			break;
		case LOG_WARN:
			head_fmt = "[file:%s]    [WARN ][function:%s][line:%d]";
			break;
		case LOG_INFO:
			head_fmt = "[file:%s]    [INFO ][function:%s][line:%d]";
			break;
		case LOG_DEBUG:
			head_fmt = "[file:%s]    [DEBUG][function:%s][line:%d]";
			break;
		default:
			fprintf(stderr, "unrecognized log type: %d \n", type);
			exit(1);
			break;
	}

	n = sprintf(new_fmt, head_fmt, file, fun, line);
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

