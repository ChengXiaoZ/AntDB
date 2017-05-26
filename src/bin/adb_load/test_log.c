#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <pthread.h>

#include "postgres_fe.h"
#include "loadsetting.h"
#include "log_process_fd.h"
#include "log_detail_fd.h"

static void *pthread_start(void *args);

int main(int argc, char **argv)
{
	char *logfilepath = NULL;

	pthread_t thread_id[10];
	int i;

	adbLoader_log_init(logfilepath, LOG_INFO);

	for (i = 0; i < 10; i++)
	{
		if (pthread_create(&thread_id[i], NULL, pthread_start, NULL) != 0)
		{
			fprintf(stderr, "create thread failed! \n");
		}
	}

	for (i = 0; i < 10; i++)
	{
		if (pthread_join(thread_id[i], NULL) != 0)
		{
			fprintf(stderr, "pthread_join() failed! \n");
		}
	}

	close_log_process_fd();
	return 0;
}

static void *pthread_start(void *args)
{
	int i;

	for (i = 0; i < 100; i++)
	{
		ADBLOADER_LOG(LOG_ERROR, "%s%u", "pthread_id = ", pthread_self());
	}

	return NULL;
}

