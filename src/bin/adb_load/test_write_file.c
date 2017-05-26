#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <pthread.h>

#include "postgres_fe.h"
#include "linebuf.h"
#include "read_write_file.h"

static void *pthread_start_write_file(void *args);

int main(int argc, char **argv)
{
	char *logfilepath = NULL;

	pthread_t thread_id[10];

	fopen_error_file(logfilepath);

    if (pthread_create(&thread_id[1], NULL, pthread_start_write_file, NULL) != 0)
    {
        fprintf(stderr, "create thread failed! \n");
    }

    if (pthread_join(thread_id[1], NULL) != 0)
    {
        fprintf(stderr, "pthread_join() failed! \n");
    }


    /*
	for (i = 0; i < 10; i++)
	{
		if (pthread_create(&thread_id[i], NULL, pthread_start_write_file, NULL) != 0)
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
    */
    printf("sssssss");

	fclose_error_file();
	return 0;
}

static void *pthread_start_write_file(void *args)
{
	int i;
	char pthread_id[1024] = {0};
	LineBuffer *linebuffer;

	sprintf(pthread_id, "%lu", (unsigned long int)pthread_self());
	init_linebuf(3);
	linebuffer = get_linebuf();
	linebuffer->lineno = 1234;
	linebuffer->data = pthread_id;

	for (i = 0; i < 2; i++)
    {
		write_log_summary_fd(linebuffer);
	}

	return NULL;
}

