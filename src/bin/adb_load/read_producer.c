#include "postgres_fe.h"

#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

#include "adbloader_log.h"
#include "read_producer.h"
#include "utility.h"
#include "read_write_file.h"

typedef pthread_t	Read_ThreadID;

#define READFILEBUFSIZE    (8 * BLCKSZ)

typedef struct Read_ThreadInfo
{
	Read_ThreadID 		 thread_id;
	bool				 replication;
	MessageQueuePipe  	*input_queue;
	MessageQueuePipe   **output_queue;
	int					 out_queue_size;
	int					 end_flag_num;
	char				*file_path;
	char				*start_cmd;
	void 				* (* thr_startroutine)(void *);
} Read_ThreadInfo;

static void	* read_ThreadWrapper (void *argp);
static void	  read_ThreadCleanup (void * argp);
static void	* read_threadMain (void *argp);
static void	  read_write_error_message(Read_ThreadInfo  *thrinfo, char * message,			char * error_message,
													int line_no, char *line_data, bool redo);


static ReadProducerState	STATE = READ_PRODUCER_PROCESS_OK;
static bool				NEED_EXIT = false;
static bool				EXITED = false;

void *
read_ThreadWrapper (void *argp)
{
	Read_ThreadInfo  *thrinfo = (Read_ThreadInfo*) argp;
	pthread_detach(thrinfo->thread_id);
	pthread_cleanup_push(read_ThreadCleanup, thrinfo);
	thrinfo->thr_startroutine(thrinfo);
	pthread_cleanup_pop(1);

	return thrinfo;
}

void
read_ThreadCleanup (void * argp)
{
	Read_ThreadInfo  *thrinfo = (Read_ThreadInfo*) argp;
	if (thrinfo->file_path)
	{
		pfree(thrinfo->file_path);
		thrinfo->file_path = NULL;
	}

	if (thrinfo->start_cmd)
	{
		pfree(thrinfo->start_cmd);
		thrinfo->start_cmd = NULL;
	}
	pfree(thrinfo);
	thrinfo = NULL;

	EXITED = true;
	return;
}

void *
read_threadMain (void *argp)
{
	struct 			 stat statbuf;
	Read_ThreadInfo  *thrinfo = (Read_ThreadInfo*) argp;
	FILE 			 *fp;
	char 			 buf[READFILEBUFSIZE];
	LineBuffer		 *linebuf;
	long			 lineno = 0;
	int 			 flag;

	stat(thrinfo->file_path, &statbuf);
	if (S_ISDIR(statbuf.st_mode))
	{
		ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] %s is not a file",
				thrinfo->thread_id, thrinfo->file_path);
		read_write_error_message(thrinfo, "not a file", NULL,	0,
									NULL, TRUE);
		pthread_exit(thrinfo);
	}

	if ((fp = fopen(thrinfo->file_path, "r")) == NULL)
	{
		ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] could not open file: %s",
				thrinfo->thread_id, thrinfo->file_path);
		read_write_error_message(thrinfo, "could not open file, check file", NULL,	0,
									NULL, TRUE);
		pthread_exit(thrinfo);
	}

	if (thrinfo->replication)
	{		
		while ((fgets(buf, sizeof(buf), fp)) != NULL)
		{
			int res;			
			lineno++;

			if (NEED_EXIT)
			{
				STATE = READ_PRODUCER_PROCESS_EXIT_BY_CALLER;
				pthread_exit(thrinfo);
			}

			for (flag = 0; flag < thrinfo->out_queue_size; flag++)
			{	
				linebuf = get_linebuf();
				appendLineBufInfoString(linebuf, buf);
				linebuf->fileline = lineno;
				res = mq_pipe_put(thrinfo->output_queue[flag], linebuf);

				if (res < 0)
				{
					ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] put linebuf to messagequeue failed, data :%s, lineno :%ld, filepath :%s",
						thrinfo->thread_id, linebuf->data, lineno, thrinfo->file_path);
					STATE = READ_PRODUCER_PROCESS_ERROR;
					read_write_error_message(thrinfo, "put linebuf to messagequeue failed", NULL, lineno,
									linebuf->data, TRUE);
					fclose(fp);
					pthread_exit(thrinfo);
				}
			}		
		}
		for (flag = 0; flag < thrinfo->out_queue_size; flag++)
		{
			mq_pipe_put(thrinfo->output_queue[flag], NULL);
		}
	}
	else
	{
		while ((fgets(buf, sizeof(buf), fp)) != NULL)
		{
			int res;
			lineno++;

			if (NEED_EXIT)
			{
				STATE = READ_PRODUCER_PROCESS_EXIT_BY_CALLER;
				pthread_exit(thrinfo);
			}

			linebuf = get_linebuf();
			appendLineBufInfoString(linebuf, buf);
			linebuf->fileline = lineno;
			res = mq_pipe_put(thrinfo->input_queue, linebuf);

			if (res < 0)
			{
				ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] put linebuf to messagequeue failed, data :%s, lineno :%ld, filepath :%s",
					thrinfo->thread_id, linebuf->data, lineno, thrinfo->file_path);
				STATE = READ_PRODUCER_PROCESS_ERROR;
				read_write_error_message(thrinfo, "put linebuf to messagequeue failed", NULL, lineno,
									linebuf->data, TRUE);
				fclose(fp);
				pthread_exit(thrinfo);
			}
		}
		for (flag = 0; flag < thrinfo->end_flag_num; flag++)
		{
			mq_pipe_put(thrinfo->input_queue, NULL);
		}
	}
	ADBLOADER_LOG(LOG_INFO, "[thread id : %ld ] read file complete, lineno  :%ld, , filepath :%s",
				thrinfo->thread_id, lineno, thrinfo->file_path);
	fclose(fp);
	STATE = READ_PRODUCER_PROCESS_COMPLETE;
	return NULL;
}

int
InitReadProducer(char *filepath, MessageQueuePipe *input_queue, MessageQueuePipe **output_queue,
								int out_queue_size, bool replication, int end_flag_num, char * start_cmd)
{
	Read_ThreadInfo *read_threadInfo = (Read_ThreadInfo*)palloc0(sizeof(Read_ThreadInfo));
	int error;
	Assert(NULL != filepath);

	read_threadInfo->file_path = pg_strdup(filepath);
	read_threadInfo->replication = replication;
	read_threadInfo->start_cmd = pg_strdup(start_cmd);
	if (replication)
	{
		Assert(NULL != output_queue && out_queue_size > 0);
		read_threadInfo->out_queue_size = out_queue_size;
		read_threadInfo->output_queue = output_queue;
	}
	else
	{
		Assert(NULL != input_queue && end_flag_num > 0);
		read_threadInfo->input_queue = input_queue;
		read_threadInfo->end_flag_num = end_flag_num;
	}
	read_threadInfo->thr_startroutine = read_threadMain;
	if ((error = pthread_create(&read_threadInfo->thread_id, NULL, read_ThreadWrapper, read_threadInfo)) < 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[thread main ] create thread error");
		return READ_PRODUCER_ERROR;
	}
	return READ_PRODUCER_OK;
}

ReadProducerState
GetReadModule(void)
{
	return STATE;
}

void
SetReadProducerExit(void)
{
	NEED_EXIT = true;
	for (;;)
	{
		if (EXITED)
			break;
		/* sleep 5s to wait thread exit */
		sleep(5);
	}
}

static void	 
read_write_error_message(Read_ThreadInfo  *thrinfo, char * message,			char * error_message, int line_no,
								char *line_data, bool redo)
{
	LineBuffer *error_buffer = NULL;
	error_buffer = format_error_info(message, READER, error_message,
								line_no, line_data);
	appendLineBufInfoString(error_buffer, "-------------------------------------------------------\n");
	appendLineBufInfoString(error_buffer, "\n");
	if (redo)
	{
		appendLineBufInfoString(error_buffer, "\n");
		appendLineBufInfoString(error_buffer, "suggest : ");
		if (thrinfo)
			appendLineBufInfoString(error_buffer, thrinfo->start_cmd);
		else if (thrinfo == NULL && error_message != NULL)
			appendLineBufInfoString(error_buffer, error_message);
		appendLineBufInfoString(error_buffer, "\n");	
	}
	else
	{
		appendLineBufInfoString(error_buffer, "\n");
		appendLineBufInfoString(error_buffer, "suggest : ");
		appendLineBufInfoString(error_buffer, "must deal this data alone");
		appendLineBufInfoString(error_buffer, "\n");
	}
	write_error(error_buffer);
	release_linebuf(error_buffer);
}

