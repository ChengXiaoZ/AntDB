#include "postgres_fe.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>

#include "loadsetting.h"
#include "linebuf.h"
#include "msg_queue.h"
#include "read_write_file.h"

#define MAXPGPATH          1024
#define READFILEBUFSIZE    (8 * BLCKSZ)
#define MSGQUEUE_MAXSIZE   100

static char path[MAXPGPATH] = {0};

FILE *err_data_fd = NULL;
//static pthread_mutex_t write_file = PTHREAD_MUTEX_INITIALIZER;

int fopen_error_file(char *adb_load_error_path)
{

	if (adb_load_error_path == NULL)
	{
		snprintf(path, sizeof(path), "./adb_load_error_data.log");
	}
	else
	{
		if (adb_load_error_path[strlen(adb_load_error_path) - 1] == '/')
			snprintf(path, sizeof(path), "%s%s", adb_load_error_path, "adb_load_error_data.log");
		else
			snprintf(path, sizeof(path), "%s/%s", adb_load_error_path, "adb_load_error_data.log");
	}

	if ((err_data_fd = fopen(path, "a")) == NULL)
	{
		fprintf(stderr, "could not open file: %s.\n", path);
		exit(1);
	}
    return 0;
}

int write_error(LineBuffer *lineBuffer)
{
	char buffer[READFILEBUFSIZE];
	AssertArg(lineBuffer);

	sprintf(buffer, "%s\n", lineBuffer->data);
	fwrite(buffer, strlen(buffer), 1, err_data_fd);
    fflush(err_data_fd);
	return 0;
}

void fclose_error_file(void)
{
    if (fclose(err_data_fd))
    {
        fprintf(stderr, "could not close file \"%s\" \n", path);
    }
}

int sent_to_datanode_error(char *error)
{
	return 0;
}
