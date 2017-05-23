#include "postgres_fe.h"

#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

#include "adbloader_log.h"
#include "read_producer.h"
#include "utility.h"
#include "read_write_file.h"

typedef pthread_t Read_ThreadID;

pthread_t g_read_thread_id;

#define READFILEBUFSIZE    (8 * BLCKSZ)

typedef struct Read_ThreadInfo
{
	Read_ThreadID        thread_id;
	bool                 replication;
	MessageQueuePipe    *input_queue;
	MessageQueuePipe   **output_queue;
	int                  output_queue_num;
	int                  datanodes_num;
	int                  threads_num_per_datanode;
	int                  end_flag_num;

	int                 *redo_queue_index;
	int                  redo_queue_total;
	bool                 redo_queue;

	bool                filter_first_line;

	char                *file_path;
	char                *start_cmd;
	int                  read_file_buffer; //unit is KB
	bool                 stream_node;
	FILE                *fp;
	void                *(* thr_startroutine)(void *);
} Read_ThreadInfo;

static void *read_ThreadWrapper (void *argp);
static void  read_ThreadCleanup (void * argp);
static void *read_threadMain (void *argp);
static bool is_replication_table(Read_ThreadInfo *thrinfo);
static bool need_redo_queue(Read_ThreadInfo *thrinfo);
static void read_data_file_and_no_need_redo(Read_ThreadInfo *thrinfo);
static void read_data_file_and_need_redo(Read_ThreadInfo *thrinfo);
static void read_data_file_for_hash_table(Read_ThreadInfo *thrinfo);
static void  read_write_error_message(Read_ThreadInfo *thrinfo,
								char * message,
								char * error_message,
								int line_no,
								char *line_data,
								bool redo);

static ReadProducerState    STATE = READ_PRODUCER_PROCESS_OK;
static bool                 NEED_EXIT = false;
static bool                 EXITED = false;

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

static bool
is_replication_table(Read_ThreadInfo *thrinfo)
{
	Assert(thrinfo != NULL);

	return thrinfo->replication;
}

static bool
need_redo_queue(Read_ThreadInfo *thrinfo)
{
	Assert(thrinfo != NULL);

	return thrinfo->redo_queue;
}

void
read_ThreadCleanup (void * argp)
{
	Read_ThreadInfo  *thrinfo = (Read_ThreadInfo*) argp;

	pg_free(thrinfo->file_path);
	thrinfo->file_path = NULL;

	pg_free(thrinfo->start_cmd);
	thrinfo->start_cmd = NULL;

	pg_free(thrinfo);
	thrinfo = NULL;

	EXITED = true;
	return;
}

int
get_filter_queue_file_fd_index(int redo_queue_total, int *redo_queue_index, int flag)
{
	int i = 0;

	for (i = 0; i < redo_queue_total; i++)
	{
		if (redo_queue_index[i] == flag)
			return i;
	}

	return -1;
}

bool
check_need_redo_queue(int redo_queue_total, int *redo_queue_index, int flag)
{
	int i = 0;

	for (i = 0; i < redo_queue_total; i++)
	{
		if (redo_queue_index[i] == flag)
			return true;
	}

	return false;
}

static void read_data_file_and_no_need_redo(Read_ThreadInfo *thrinfo)
{
	char        buf[READFILEBUFSIZE];
	int         lineno = 0;
	int         datanodes_num = 0;
	int         threads_num_per_datanode= 0;
	LineBuffer *linebuf = NULL;
	int         i = 0;
	int         flag = 0;
	bool        filter_first_line = false;

	filter_first_line = thrinfo->filter_first_line;
	datanodes_num = thrinfo->datanodes_num;
	threads_num_per_datanode = thrinfo->threads_num_per_datanode;

	while ((fgets(buf, sizeof(buf), thrinfo->fp)) != NULL)
	{
		int res = 0;
		int j = 0;
		lineno++;

		if (filter_first_line)
		{
			filter_first_line = false;
			continue;
		}

		pthread_testcancel();

		if (NEED_EXIT)
		{
			STATE = READ_PRODUCER_PROCESS_EXIT_BY_CALLER;
			fclose(thrinfo->fp);
			pthread_exit(thrinfo);
		}

		for (j = 0; j < datanodes_num; j++)
		{
			linebuf = get_linebuf();
			appendLineBufInfoString(linebuf, buf);
			linebuf->fileline = lineno;

			res = mq_pipe_put(thrinfo->output_queue[i + j * threads_num_per_datanode], linebuf);
			ADBLOADER_LOG(LOG_DEBUG,
				"[READ_PRODUCER][thread id : %ld ] send data: %s TO queue num:%s \n",
				thrinfo->thread_id, linebuf->data, thrinfo->output_queue[i + j * threads_num_per_datanode]->name);
			if (res < 0)
			{
				ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] put linebuf to messagequeue failed, data :%s, lineno :%d, filepath :%s",
					thrinfo->thread_id, linebuf->data, lineno, thrinfo->file_path);
				STATE = READ_PRODUCER_PROCESS_ERROR;
				read_write_error_message(thrinfo, "put linebuf to messagequeue failed", NULL, lineno,
								linebuf->data, TRUE);
				fclose(thrinfo->fp);
				pthread_exit(thrinfo);
			}
		}

		i++;
		if (i == threads_num_per_datanode)
			i = 0;
	}

	/* insert NULL flag, it means read data file over. */
	for (flag = 0; flag < thrinfo->output_queue_num; flag++)
	{
		mq_pipe_put(thrinfo->output_queue[flag], NULL);
	}

	return;
}

static void read_data_file_and_need_redo(Read_ThreadInfo *thrinfo)
{
	bool         need_redo_queue = false;
	int          datanodes_num = 0;
	int          threads_num_per_datanode= 0;
	int          threads_total = 0;
	int          lineno = 0;
	char         buf[READFILEBUFSIZE];
	LineBuffer  *linebuf = NULL;
	int          output_queue_index = 0;
	int          flag = 0;
	int          i = 0;
	bool         filter_first_line = false;

	filter_first_line = thrinfo->filter_first_line;
	datanodes_num = thrinfo->datanodes_num;
	threads_num_per_datanode = thrinfo->threads_num_per_datanode;
	threads_total = datanodes_num * threads_num_per_datanode;

	for (flag = 0; flag < threads_total; flag++)
	{
		need_redo_queue = check_need_redo_queue(thrinfo->redo_queue_total,
												thrinfo->redo_queue_index,
												flag);
		if (!need_redo_queue)
		{
			mq_pipe_put(thrinfo->output_queue[flag], NULL);
		}
	}

	while ((fgets(buf, sizeof(buf), thrinfo->fp)) != NULL)
	{
		int res = 0;
		int j = 0;
		lineno++;

		if (filter_first_line)
		{
			filter_first_line = false;
			continue;
		}

		pthread_testcancel();

		if (NEED_EXIT)
		{
			STATE = READ_PRODUCER_PROCESS_EXIT_BY_CALLER;
			fclose(thrinfo->fp);
			pthread_exit(thrinfo);
		}

		for (j = 0; j < datanodes_num; j++)
		{
			linebuf = get_linebuf();
			appendLineBufInfoString(linebuf, buf);
			linebuf->fileline = lineno;

			output_queue_index = i + j * threads_num_per_datanode;

			need_redo_queue = check_need_redo_queue(thrinfo->redo_queue_total,
													thrinfo->redo_queue_index,
													output_queue_index);
			if (need_redo_queue)
			{
				res = mq_pipe_put(thrinfo->output_queue[output_queue_index], linebuf);
				ADBLOADER_LOG(LOG_DEBUG,
					"[READ_PRODUCER][thread id : %ld ] send data: %s TO queue num:%s \n",
					thrinfo->thread_id, linebuf->data, thrinfo->output_queue[i + j * threads_num_per_datanode]->name);
				if (res < 0)
				{
					ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] put linebuf to messagequeue failed, data :%s, lineno :%d, filepath :%s",
						thrinfo->thread_id, linebuf->data, lineno, thrinfo->file_path);
					STATE = READ_PRODUCER_PROCESS_ERROR;
					read_write_error_message(thrinfo, "put linebuf to messagequeue failed", NULL, lineno,
									linebuf->data, TRUE);
					fclose(thrinfo->fp);
					pthread_exit(thrinfo);
				}
			}
		}

		i++;
		if (i == threads_num_per_datanode)
			i = 0;
	}

	/* insert NULL flag, it means read data file over. */
	for (flag = 0; flag < thrinfo->redo_queue_total; flag++)
	{
		mq_pipe_put(thrinfo->output_queue[thrinfo->redo_queue_index[flag]], NULL);
	}

	return;
}

static void read_data_file_for_hash_table(Read_ThreadInfo *thrinfo)
{
	int         lineno = 0;
	char        buf[READFILEBUFSIZE];
	LineBuffer *linebuf = NULL;
	int         flag = 0;
	bool        filter_first_line = false;

	filter_first_line = thrinfo->filter_first_line;
	while ((fgets(buf, sizeof(buf), thrinfo->fp)) != NULL)
	{
		int res;
		lineno++;

		if (filter_first_line)
		{
			filter_first_line = false;
			continue;
		}

		pthread_testcancel();

		if (NEED_EXIT)
		{
			STATE = READ_PRODUCER_PROCESS_EXIT_BY_CALLER;
			fclose(thrinfo->fp);
			pthread_exit(thrinfo);
		}

		linebuf = get_linebuf();
		appendLineBufInfoString(linebuf, buf);
		linebuf->fileline = lineno;
		res = mq_pipe_put(thrinfo->input_queue, linebuf);

		if (res < 0)
		{
			ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] put linebuf to messagequeue failed, data :%s, lineno :%d, filepath :%s",
				thrinfo->thread_id, linebuf->data, lineno, thrinfo->file_path);
			STATE = READ_PRODUCER_PROCESS_ERROR;
			read_write_error_message(thrinfo, "put linebuf to messagequeue failed", NULL, lineno,
								linebuf->data, TRUE);
			fclose(thrinfo->fp);
			pthread_exit(thrinfo);
		}
	}

	for (flag = 0; flag < thrinfo->end_flag_num; flag++)
	{
		mq_pipe_put(thrinfo->input_queue, NULL);
	}

	return;
}

void *
read_threadMain (void *argp)
{
	Read_ThreadInfo  *thrinfo = (Read_ThreadInfo*) argp;
	char             *vbuf = NULL;
	unsigned long int vbuf_size = 0;

	/* enble cancel this thread. */
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

	if (!thrinfo->stream_node && !file_exists(thrinfo->file_path))
	{
		ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] %s is not a file",
				thrinfo->thread_id, thrinfo->file_path);
		read_write_error_message(thrinfo, "not a file", NULL, 0, NULL, TRUE);
		pthread_exit(thrinfo);
	}

	if (thrinfo->stream_node)
	{
		thrinfo->fp = stdin;
	}
	else
	{
		if ((thrinfo->fp = fopen(thrinfo->file_path, "r")) == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR, "[thread id : %ld ] could not open file: %s",
					thrinfo->thread_id, thrinfo->file_path);
			read_write_error_message(thrinfo, "could not open file, check file",
									NULL, 0, NULL, TRUE);
			pthread_exit(thrinfo);
		}

		/*set vbuf*/
		/*use default value 512 byte, if thrinfo->read_file_buffer <= 0*/
		if (thrinfo->read_file_buffer > 0)
		{
			vbuf_size = thrinfo->read_file_buffer * 1024 * sizeof(char);
			vbuf = (char *)palloc0(vbuf_size);
			setvbuf(thrinfo->fp, vbuf, _IOFBF, vbuf_size);
		}
	}

	if (is_replication_table(thrinfo) && !need_redo_queue(thrinfo))
	{
		read_data_file_and_no_need_redo(thrinfo);
	}
	else if (is_replication_table(thrinfo) && need_redo_queue(thrinfo))
	{
		read_data_file_and_need_redo(thrinfo);
	}
	else /* for hash table */
	{
		read_data_file_for_hash_table(thrinfo);
	}

	STATE = READ_PRODUCER_PROCESS_COMPLETE;
	ADBLOADER_LOG(LOG_INFO, "[thread id : %ld ] read file complete, filepath :%s",
				thrinfo->thread_id, thrinfo->file_path);

	fclose(thrinfo->fp);

	return NULL;
}

int init_read_thread(ReadInfo *read_info)
{
	Read_ThreadInfo *read_threadInfo = (Read_ThreadInfo*)palloc0(sizeof(Read_ThreadInfo));
	int error = 0;

	Assert(read_info->filepath != NULL);
	Assert(read_info->start_cmd != NULL);

	read_threadInfo->file_path = pg_strdup(read_info->filepath);
	read_threadInfo->replication = read_info->replication;
	read_threadInfo->start_cmd = pg_strdup(read_info->start_cmd);
	read_threadInfo->filter_first_line = read_info->filter_first_line;
	read_threadInfo->stream_node = read_info->stream_mode;
	if (read_info->replication)
	{

		Assert(read_info->output_queue != NULL);
		Assert(read_info->output_queue_num > 0);

		read_threadInfo->output_queue_num = read_info->output_queue_num;
		read_threadInfo->output_queue = read_info->output_queue;
		read_threadInfo->datanodes_num = read_info->datanodes_num;
		read_threadInfo->threads_num_per_datanode = read_info->threads_num_per_datanode;
		read_threadInfo->read_file_buffer = read_info->read_file_buffer;

		if (read_info->redo_queue)
		{
			read_threadInfo->redo_queue = true;
			read_threadInfo->redo_queue_total = read_info->redo_queue_total;
			read_threadInfo->redo_queue_index = read_info->redo_queue_index;
		}
	}
	else
	{
		Assert(read_info->input_queue != NULL && read_info->end_flag_num > 0);

		read_threadInfo->input_queue = read_info->input_queue;
		read_threadInfo->end_flag_num = read_info->end_flag_num;
		read_threadInfo->datanodes_num = read_info->datanodes_num;
		read_threadInfo->threads_num_per_datanode = read_info->threads_num_per_datanode;
		read_threadInfo->read_file_buffer = read_info->read_file_buffer;
	}
	read_threadInfo->thr_startroutine = read_threadMain;
	if ((error = pthread_create(&read_threadInfo->thread_id, NULL, read_ThreadWrapper, read_threadInfo)) < 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[thread main ] create thread error");
		return READ_PRODUCER_ERROR;
	}

	g_read_thread_id = read_threadInfo->thread_id;

    ADBLOADER_LOG(LOG_INFO, "[Read][thread main ] create read data file thread : %ld ", read_threadInfo->thread_id);

	return READ_PRODUCER_OK;
}

ReadProducerState
GetReadModule(void)
{
	return STATE;
}

void
stop_read_thread(void)
{
	int i =0;
	int pthread_res = 0;

	NEED_EXIT = true;

	/*try 3 times*/
	for(i = 0; i < 3; i++)
	{
		if (EXITED)
			break;

		sleep(2);
	}

	if (!EXITED)
	{
		pthread_res = pthread_cancel(g_read_thread_id);
		if (pthread_res != 0)
		{
			fprintf(stderr, "pthread cancel failed.\n");
		}
	}
}


static void
read_write_error_message(Read_ThreadInfo  *thrinfo, char * message, char * error_message, int line_no,
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

