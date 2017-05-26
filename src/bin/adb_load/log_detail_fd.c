#include <stdio.h>

#include "postgres_fe.h"
#include "log_detail_fd.h"
#include "utility.h"

FILE *log_detail_fd = NULL;

static char *get_file_path(char *path, char *tablename);

void
open_log_detail_fd(char *path, char *tablename)
{
	char *file_path = NULL;

	Assert(tablename != NULL);

	file_path = get_file_path(path, tablename);
	log_detail_fd = fopen(file_path, "a+");
	if (log_detail_fd == NULL)
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

void
check_log_detail_fd(char *path, char *tablename)
{
	char *file_path = NULL;

	file_path = get_file_path(path, tablename);
	if (file_exists(file_path))
	{
		if (file_size(file_path) > 0)
		{
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

void
write_log_detail_fd(char *linedata)
{
	int ret = 0;

	if (linedata == NULL)
		return;

	ret = fwrite(linedata, 1, strlen(linedata), log_detail_fd);
	if (ret != strlen(linedata))
	{
		fprintf(stderr, "could not write data to error data file.\n");
		exit(EXIT_FAILURE);
	}

	fflush(log_detail_fd);

	return;
}

void
close_log_detail_fd()
{
	if (fclose(log_detail_fd))
	{
		fprintf(stderr, "could not close error data file.\n");
		exit(EXIT_FAILURE);
	}

	return;
}

static char *
get_file_path(char *path, char *tablename)
{
	char error_path[1024] = {0};

	Assert(path != NULL);
	Assert(tablename != NULL);

	if (path == NULL)
	{
		snprintf(error_path, sizeof(error_path), "./%s.log", tablename);
	}
	else
	{
		if (path[strlen(path) - 1] == '/')
			snprintf(error_path, sizeof(error_path), "%s%s.log", path, tablename);
		else
			snprintf(error_path, sizeof(error_path), "%s/%s.log", path, tablename);
	}

	return pstrdup(error_path);
}

