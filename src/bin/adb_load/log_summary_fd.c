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
#include "log_summary_fd.h"

#define MAXPGPATH          1024
#define READFILEBUFSIZE    (8 * BLCKSZ)
#define MSGQUEUE_MAXSIZE   100

static char path[MAXPGPATH] = {0};

FILE *log_summary_fd = NULL;

int
open_log_summary_fd(char *log_summary_path)
{
	if (log_summary_path == NULL)
	{
		snprintf(path, sizeof(path), "./log_summary.log");
	}
	else
	{
		if (log_summary_path[strlen(log_summary_path) - 1] == '/')
			snprintf(path, sizeof(path), "%s%s", log_summary_path, "log_summary.log");
		else
			snprintf(path, sizeof(path), "%s/%s", log_summary_path, "log_summary.log");
	}

	if ((log_summary_fd = fopen(path, "a")) == NULL)
	{
		fprintf(stderr, "could not open file: %s.\n", path);
		exit(EXIT_FAILURE);
	}

    return 0;
}

int
write_log_summary_fd(LineBuffer *lineBuffer)
{
	AssertArg(lineBuffer);

	fwrite(lineBuffer->data, strlen(lineBuffer->data), 1, log_summary_fd);
    fflush(log_summary_fd);

	return 0;
}

void
close_log_summary_fd(void)
{
    if (fclose(log_summary_fd))
    {
        fprintf(stderr, "could not close file \"%s\"\n", path);
    }

	return;
}

