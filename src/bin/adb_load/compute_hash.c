#include "postgres_fe.h"

#include <pthread.h>
#include <unistd.h>

#include "adbloader_log.h"
#include "compute_hash.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "read_write_file.h"
#include "utility.h"

static const unsigned int xc_mod_m[] =
{
  0x00000000, 0x55555555, 0x33333333, 0xc71c71c7,
  0x0f0f0f0f, 0xc1f07c1f, 0x3f03f03f, 0xf01fc07f,
  0x00ff00ff, 0x07fc01ff, 0x3ff003ff, 0xffc007ff,
  0xff000fff, 0xfc001fff, 0xf0003fff, 0xc0007fff,
  0x0000ffff, 0x0001ffff, 0x0003ffff, 0x0007ffff,
  0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff,
  0x00ffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff,
  0x0fffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff
};

static const unsigned int xc_mod_q[][6] =
{
  { 0,  0,  0,  0,  0,  0}, {16,  8,  4,  2,  1,  1}, {16,  8,  4,  2,  2,  2},
  {15,  6,  3,  3,  3,  3}, {16,  8,  4,  4,  4,  4}, {15,  5,  5,  5,  5,  5},
  {12,  6,  6,  6 , 6,  6}, {14,  7,  7,  7,  7,  7}, {16,  8,  8,  8,  8,  8},
  { 9,  9,  9,  9,  9,  9}, {10, 10, 10, 10, 10, 10}, {11, 11, 11, 11, 11, 11},
  {12, 12, 12, 12, 12, 12}, {13, 13, 13, 13, 13, 13}, {14, 14, 14, 14, 14, 14},
  {15, 15, 15, 15, 15, 15}, {16, 16, 16, 16, 16, 16}, {17, 17, 17, 17, 17, 17},
  {18, 18, 18, 18, 18, 18}, {19, 19, 19, 19, 19, 19}, {20, 20, 20, 20, 20, 20},
  {21, 21, 21, 21, 21, 21}, {22, 22, 22, 22, 22, 22}, {23, 23, 23, 23, 23, 23},
  {24, 24, 24, 24, 24, 24}, {25, 25, 25, 25, 25, 25}, {26, 26, 26, 26, 26, 26},
  {27, 27, 27, 27, 27, 27}, {28, 28, 28, 28, 28, 28}, {29, 29, 29, 29, 29, 29},
  {30, 30, 30, 30, 30, 30}, {31, 31, 31, 31, 31, 31}
};

static const unsigned int xc_mod_r[][6] =
{
  {0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000},
  {0x0000ffff, 0x000000ff, 0x0000000f, 0x00000003, 0x00000001, 0x00000001},
  {0x0000ffff, 0x000000ff, 0x0000000f, 0x00000003, 0x00000003, 0x00000003},
  {0x00007fff, 0x0000003f, 0x00000007, 0x00000007, 0x00000007, 0x00000007},
  {0x0000ffff, 0x000000ff, 0x0000000f, 0x0000000f, 0x0000000f, 0x0000000f},
  {0x00007fff, 0x0000001f, 0x0000001f, 0x0000001f, 0x0000001f, 0x0000001f},
  {0x00000fff, 0x0000003f, 0x0000003f, 0x0000003f, 0x0000003f, 0x0000003f},
  {0x00003fff, 0x0000007f, 0x0000007f, 0x0000007f, 0x0000007f, 0x0000007f},
  {0x0000ffff, 0x000000ff, 0x000000ff, 0x000000ff, 0x000000ff, 0x000000ff},
  {0x000001ff, 0x000001ff, 0x000001ff, 0x000001ff, 0x000001ff, 0x000001ff},
  {0x000003ff, 0x000003ff, 0x000003ff, 0x000003ff, 0x000003ff, 0x000003ff},
  {0x000007ff, 0x000007ff, 0x000007ff, 0x000007ff, 0x000007ff, 0x000007ff},
  {0x00000fff, 0x00000fff, 0x00000fff, 0x00000fff, 0x00000fff, 0x00000fff},
  {0x00001fff, 0x00001fff, 0x00001fff, 0x00001fff, 0x00001fff, 0x00001fff},
  {0x00003fff, 0x00003fff, 0x00003fff, 0x00003fff, 0x00003fff, 0x00003fff},
  {0x00007fff, 0x00007fff, 0x00007fff, 0x00007fff, 0x00007fff, 0x00007fff},
  {0x0000ffff, 0x0000ffff, 0x0000ffff, 0x0000ffff, 0x0000ffff, 0x0000ffff},
  {0x0001ffff, 0x0001ffff, 0x0001ffff, 0x0001ffff, 0x0001ffff, 0x0001ffff},
  {0x0003ffff, 0x0003ffff, 0x0003ffff, 0x0003ffff, 0x0003ffff, 0x0003ffff},
  {0x0007ffff, 0x0007ffff, 0x0007ffff, 0x0007ffff, 0x0007ffff, 0x0007ffff},
  {0x000fffff, 0x000fffff, 0x000fffff, 0x000fffff, 0x000fffff, 0x000fffff},
  {0x001fffff, 0x001fffff, 0x001fffff, 0x001fffff, 0x001fffff, 0x001fffff},
  {0x003fffff, 0x003fffff, 0x003fffff, 0x003fffff, 0x003fffff, 0x003fffff},
  {0x007fffff, 0x007fffff, 0x007fffff, 0x007fffff, 0x007fffff, 0x007fffff},
  {0x00ffffff, 0x00ffffff, 0x00ffffff, 0x00ffffff, 0x00ffffff, 0x00ffffff},
  {0x01ffffff, 0x01ffffff, 0x01ffffff, 0x01ffffff, 0x01ffffff, 0x01ffffff},
  {0x03ffffff, 0x03ffffff, 0x03ffffff, 0x03ffffff, 0x03ffffff, 0x03ffffff},
  {0x07ffffff, 0x07ffffff, 0x07ffffff, 0x07ffffff, 0x07ffffff, 0x07ffffff},
  {0x0fffffff, 0x0fffffff, 0x0fffffff, 0x0fffffff, 0x0fffffff, 0x0fffffff},
  {0x1fffffff, 0x1fffffff, 0x1fffffff, 0x1fffffff, 0x1fffffff, 0x1fffffff},
  {0x3fffffff, 0x3fffffff, 0x3fffffff, 0x3fffffff, 0x3fffffff, 0x3fffffff},
  {0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff}
};

static int 	  		adbLoader_hashThreadCreate (HashComputeInfo *hash_info, HashField *field);

static void			adbLoader_ThreadCleanup (void * argp);

static void   		prepare_hash_field (LineBuffer **element_batch, int size,
							ComputeThreadInfo *thrinfo, MessageQueue *inner_queue);

static int	  		max_value (int * values, int size);

static int			compute_hash_modulo (unsigned int numerator, unsigned int denominator);

static int			calc_send_datanode (uint32 hash, HashField *hash_field);

static void			read_data (PGconn *conn, char *read_buff, ComputeThreadInfo	*thrinfo);

static void			restart_hash_stream (ComputeThreadInfo	*thrinfo);

static void			check_restart_hash_stream (ComputeThreadInfo	*thrinfo);

static void   		free_buff (char ** buff, int len);

static void			print_struct (HashComputeInfo * hash_computeInfo, HashField * hash_field);

static void		 	* adbLoader_ThreadMainWrapper (void *argp);

static char		  	* create_copy_string (char *func, int parameter_nums, char *copy_options);

//static LineBuffer 	* get_field (char **fields, char *line, int * loc, char *delim, int size);

static LineBuffer	* get_field_quote (char **fields, char *line, int * loc, char *delim, char quotec,
								char escapec, int size, ComputeThreadInfo * thrinfo);

static void		 	* hash_threadMain (void *argp);

static LineBuffer 	* package_field (LineBuffer *lineBuffer, ComputeThreadInfo *thrinfo);

static PGconn		* reconnect (ComputeThreadInfo *thrinfo);

static void 		  set_other_threads_exit(void); 

static void		  	  hash_write_error_message(ComputeThreadInfo	*thrinfo, char * message,
										char * hash_error_message, int line_no, char *line_data, bool redo);


static const int 	THREAD_QUEUE_SIZE = 200;
static bool			RECV_END_FLAG = FALSE;
static bool			ALL_THREADS_EXIT = FALSE;
static HashThreads	HashThreadsData;
static HashThreads	*RunThreads = &HashThreadsData;
static HashThreads	HashThreadsFinishData;
static HashThreads	*FinishThreads = &HashThreadsFinishData;

static char 		*g_start_cmd = NULL;	

int
InitHashCompute(int thread_nums, char * func, char * conninfo, MessageQueuePipe * message_queue_in,
	MessageQueuePipe ** message_queue_out, int queue_size, HashField * field)
{
	HashComputeInfo * hash_info;
	Assert(NULL != func && NULL != conninfo &&
		NULL != message_queue_in && NULL != message_queue_out && NULL != field);

	hash_info = (HashComputeInfo *)palloc0(sizeof(HashComputeInfo));
	if(NULL == hash_info)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] Init_Hash_ComputeInfo function palloc memory error");
		return HASH_COMPUTE_ERROR;
	}

	hash_info->thread_nums = thread_nums;
	hash_info->conninfo = pg_strdup(conninfo);
	hash_info->input_queue = message_queue_in;
	hash_info->output_queue = message_queue_out;
	hash_info->output_queue_size = queue_size;
	hash_info->func_name = pg_strdup(func);
	return (adbLoader_hashThreadCreate(hash_info, field));
}

int
CheckComputeState(void)
{
	int		running = 0;
	pthread_mutex_lock(&RunThreads->mutex);
	running = RunThreads->hs_thread_cur;
	pthread_mutex_unlock(&RunThreads->mutex);
	return running;
}

HashThreads *
GetExitThreadsInfo(void)
{
	return FinishThreads;
}

int
StopHash(void)
{
	int flag;
	pthread_mutex_lock(&RunThreads->mutex);
	for (flag = 0; flag < RunThreads->hs_thread_count; flag++)
	{
		ComputeThreadInfo *thrinfo = RunThreads->hs_threads[flag];
		if (NULL != thrinfo)
		{
			thrinfo->exit = true;
			ADBLOADER_LOG(LOG_INFO, "[HASH][thread main ] hash function set thread exit : %ld", thrinfo->thread_id);
		}
	}
	pthread_mutex_unlock(&RunThreads->mutex);

	for (;;)
	{
		pthread_mutex_lock(&RunThreads->mutex);
		if (RunThreads->hs_thread_cur == 0)
		{
			pthread_mutex_unlock(&RunThreads->mutex);
			break;
		}
		pthread_mutex_unlock(&RunThreads->mutex);		
		sleep(5);
	}
	return 0;
}

static void
set_other_threads_exit(void)
{
	int flag;
	pthread_mutex_lock(&RunThreads->mutex);
	for (flag = 0; flag < RunThreads->hs_thread_count; flag++)
	{
		ComputeThreadInfo *thrinfo = RunThreads->hs_threads[flag];
		if (NULL != thrinfo)
		{
			thrinfo->exit = true;
			ADBLOADER_LOG(LOG_INFO, "[HASH] current thread error , set other threads exit");
		}
	}
	pthread_mutex_unlock(&RunThreads->mutex);
}

static void
hash_write_error_message(ComputeThreadInfo	*thrinfo, char * message,
										char * hash_error_message, int line_no, char *line_data, bool redo)
{
	LineBuffer *error_buffer = NULL;

	if (thrinfo)
		error_buffer = format_error_info(message, HASHMODULE, hash_error_message,
								line_no, line_data);
	else
		error_buffer = format_error_info(message, HASHMODULE, NULL, line_no, line_data);

	if (redo)
	{
		appendLineBufInfoString(error_buffer, "\n");
		appendLineBufInfoString(error_buffer, "suggest : ");
		if (thrinfo)
			appendLineBufInfoString(error_buffer, g_start_cmd);
		else if (thrinfo == NULL && hash_error_message != NULL)
			appendLineBufInfoString(error_buffer, hash_error_message);
		appendLineBufInfoString(error_buffer, "\n");
	}
	else
	{
		if (line_data)
		{
			appendLineBufInfoString(error_buffer, "\n");
			appendLineBufInfoString(error_buffer, "suggest : ");
			appendLineBufInfoString(error_buffer, "must deal this data alone");
			appendLineBufInfoString(error_buffer, "\n");
		}
	}
	appendLineBufInfoString(error_buffer, "-------------------------------------------------------\n");
	appendLineBufInfoString(error_buffer, "\n");
	write_error(error_buffer);
	release_linebuf(error_buffer);
}

void 
CleanHashResource(void)
{
	int flag;
	Assert(RunThreads->hs_thread_cur == 0);
	if (NULL != RunThreads->hs_threads)
		pfree(RunThreads->hs_threads);

	for (flag = 0; flag < FinishThreads->hs_thread_count; flag++)
	{
		ComputeThreadInfo *thrinfo = FinishThreads->hs_threads[flag];
		if (NULL != thrinfo)
		{
			Assert(NULL == thrinfo->conn && NULL == thrinfo->inner_queue);
			if (thrinfo->func_name)
			{
				pfree(thrinfo->func_name);
				thrinfo->func_name = NULL;
				
			}
			if (thrinfo->conninfo)
			{
				pfree(thrinfo->conninfo);
				thrinfo->conninfo = NULL;
			}
			if (thrinfo->copy_str)
			{
				pfree(thrinfo->copy_str);
				thrinfo->copy_str = NULL;
			}

			pfree(thrinfo);		
			thrinfo = NULL;
		}
	}
	pfree(FinishThreads->hs_threads);

	RunThreads->hs_threads = NULL;
	RunThreads->hs_thread_count = 0;
	RunThreads->hs_thread_cur = 0;
	pthread_mutex_destroy(&RunThreads->mutex);

	FinishThreads->hs_threads = NULL;
	FinishThreads->hs_thread_count = 0;
	FinishThreads->hs_thread_cur = 0;
	pthread_mutex_destroy(&FinishThreads->mutex);

	if (g_start_cmd)
		pfree(g_start_cmd);
	g_start_cmd = NULL;
}

void 
SetHashFileStartCmd(char * start_cmd)
{
	Assert(NULL != start_cmd);
	 if(g_start_cmd)
	 	pfree(g_start_cmd);
	 g_start_cmd = NULL;
	 g_start_cmd = pg_strdup(start_cmd);
}

int
adbLoader_hashThreadCreate(HashComputeInfo *hash_info, HashField *field)
{
	int 		error;
	int			thread_flag;
	ComputeThreadInfo *thread_info;

	Assert(field->field_nums > 0 && field->node_nums && NULL != field->field_loc &&
		NULL != field->field_type && NULL != field->node_list && NULL != field->delim && NULL != field->hash_delim);
	if(pthread_mutex_init(&RunThreads->mutex, NULL) != 0 ||
		pthread_mutex_init(&FinishThreads->mutex, NULL) != 0 )
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] Can not initialize mutex: %s", strerror(errno));

		hash_write_error_message(NULL, 
								"Can not initialize mutex, file need to redo",
								g_start_cmd, 0, NULL, TRUE);	
		exit(1);
	}
	/*print HashComputeInfo and HashField */
	print_struct(hash_info, field);

	RunThreads->hs_thread_count = hash_info->thread_nums;
	RunThreads->hs_threads = (ComputeThreadInfo **)palloc0(sizeof(ComputeThreadInfo *) * hash_info->thread_nums);	

	FinishThreads->hs_thread_count = hash_info->thread_nums;
	FinishThreads->hs_threads = (ComputeThreadInfo **)palloc0(sizeof(ComputeThreadInfo *) * hash_info->thread_nums);

	for (thread_flag = 0; thread_flag < hash_info->thread_nums; thread_flag++)
	{	
		thread_info = (ComputeThreadInfo *)palloc0(sizeof(ComputeThreadInfo));
		/* copy func name */
		thread_info->func_name = pg_strdup(hash_info->func_name);
		if(NULL == thread_info->func_name)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] copy function name error");
			return HASH_COMPUTE_ERROR;
		}
		/* copy conninfo */
		thread_info->conninfo = pg_strdup(hash_info->conninfo);
		if(NULL == thread_info->conninfo)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] copy conninfo error");
			return HASH_COMPUTE_ERROR;
		}

		thread_info->input_queue = hash_info->input_queue;
		thread_info->output_queue = hash_info->output_queue;
		thread_info->output_queue_size = hash_info->output_queue_size;
		thread_info->hash_field = (HashField *)palloc0(sizeof(HashField));
		if (NULL == thread_info->hash_field)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] palloc HashField error");
			return HASH_COMPUTE_ERROR;
		}
		thread_info->hash_field->field_nums = field->field_nums;
		thread_info->hash_field->node_nums = field->node_nums;
		thread_info->hash_field->field_loc = (int *)palloc0(sizeof(int) * field->field_nums);
		thread_info->hash_field->field_type = (Oid *)palloc0(sizeof(Oid) * field->field_nums);
		thread_info->hash_field->node_list = (Oid *)palloc0(sizeof(Oid) * field->node_nums);
		if(NULL == thread_info->hash_field->field_loc || NULL ==  thread_info->hash_field->field_type ||
			NULL == thread_info->hash_field->node_list)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] copy conninfo error");
			return HASH_COMPUTE_ERROR;
		}
		memcpy(thread_info->hash_field->field_loc, field->field_loc, sizeof(int) * field->field_nums);
		memcpy(thread_info->hash_field->field_type, field->field_type, sizeof(Oid) * field->field_nums);
		memcpy(thread_info->hash_field->node_list, field->node_list, sizeof(Oid) * field->node_nums);
		thread_info->hash_field->delim = pg_strdup(field->delim);
		thread_info->hash_field->hash_delim = pg_strdup(field->hash_delim);
		if(NULL != field->copy_options)
			thread_info->hash_field->copy_options = pg_strdup(field->copy_options);
		else
			thread_info->hash_field->copy_options = NULL;
		thread_info->hash_field->quotec = field->quotec;
		thread_info->hash_field->has_qoute = field->has_qoute;
		thread_info->thr_startroutine = hash_threadMain;
		thread_info->state = THREAD_DEFAULT;
		RunThreads->hs_threads[thread_flag] = thread_info;

		if ((error = pthread_create(&thread_info->thread_id, NULL, adbLoader_ThreadMainWrapper, thread_info)) < 0)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] create thread error");
			return HASH_COMPUTE_ERROR;
		}
		ADBLOADER_LOG(LOG_INFO, "[HASH][thread main ] create thread : %ld ",thread_info->thread_id);
		RunThreads->hs_thread_cur++;
	}
	/* free HashComputeInfo */
	if (hash_info->func_name)
	{
		pfree(hash_info->func_name);
		hash_info->func_name = NULL;
	}
	if (hash_info->conninfo)
	{
		pfree(hash_info->conninfo);
		hash_info->conninfo = NULL;
	}
	pfree(hash_info);
	hash_info = NULL;
	return HASH_COMPUTE_OK;
}

static void	*
adbLoader_ThreadMainWrapper(void *argp)
{
	ComputeThreadInfo  *thrinfo = (ComputeThreadInfo*) argp;
	pthread_detach(thrinfo->thread_id);
	pthread_cleanup_push(adbLoader_ThreadCleanup, thrinfo);
	thrinfo->thr_startroutine(thrinfo);
	pthread_cleanup_pop(1);

	return thrinfo;
}

static void
adbLoader_ThreadCleanup(void * argp)
{
	ComputeThreadInfo  *thrinfo = (ComputeThreadInfo*) argp;
	MessageQueue *inner_queue = thrinfo->inner_queue;
	int flag, loc;

	flag = loc = 0;
	while (!mq_empty(inner_queue))
	{
		QueueElement * element;
		element = mq_poll(inner_queue);
//		write_error(element->lineBuffer);
		ADBLOADER_LOG(LOG_WARN, "[HASH][thread id : %ld ]thread exit, inner queue is not empty, data : %s",
					thrinfo->thread_id, element->lineBuffer->data);
		release_linebuf(element->lineBuffer);
		element->lineBuffer = NULL;
		pfree(element);
	}
	/* close socket */
	if (thrinfo->conn)
	{
		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;
	}
	/* free copy string */
	if (thrinfo->copy_str)
	{
		pfree(thrinfo->copy_str);
		thrinfo->copy_str = NULL;
	}

	/* check current thread state */
	if (thrinfo->state == THREAD_MEMORY_ERROR || thrinfo->state == THREAD_CONNECTION_ERROR ||
		thrinfo->state == THREAD_SELECT_ERROR || thrinfo->state == THREAD_COPY_STATE_ERROR ||
		thrinfo->state == THREAD_MESSAGE_CONFUSION_ERROR || thrinfo->state == THREAD_FIELD_ERROR)
		set_other_threads_exit();

	/* remove thread info  */
	pthread_mutex_lock(&RunThreads->mutex);
	for (flag = 0; flag < RunThreads->hs_thread_count; flag++)
	{
		ComputeThreadInfo *thread_info = RunThreads->hs_threads[flag];
		if(thread_info == thrinfo)
		{
			RunThreads->hs_threads[flag] = NULL;
			RunThreads->hs_thread_cur--;
			break;
		}
	}
	if (flag >= RunThreads->hs_thread_count)
	{
		/* error happen */
	}
	else
		loc = flag;
	pthread_mutex_unlock(&RunThreads->mutex);
	if (RunThreads->hs_thread_cur == 0)
		ALL_THREADS_EXIT = true;

	/* destory inner_queue */
	Assert(NULL != inner_queue);
	mq_destory(inner_queue);
	thrinfo->inner_queue = NULL;

	if (ALL_THREADS_EXIT)
	{
		bool copy_end = false;
		bool hapend_error = false;
		ComputeThreadInfo * exit_thread = NULL;
		/* check all other threads state */
		for (flag = 0; flag < FinishThreads->hs_thread_count; flag++)
		{
			exit_thread = FinishThreads->hs_threads[flag];
			if (NULL != exit_thread)
			{
				if (exit_thread->state == THREAD_MEMORY_ERROR || exit_thread->state == THREAD_CONNECTION_ERROR ||
					exit_thread->state == THREAD_SELECT_ERROR || exit_thread->state == THREAD_COPY_STATE_ERROR ||
					exit_thread->state == THREAD_MESSAGE_CONFUSION_ERROR || exit_thread->state == THREAD_FIELD_ERROR)
					hapend_error = true;
			}	
		}

		if (hapend_error)
			copy_end = false;
		else
			copy_end = true;
		/* check current thread state */	
		if (thrinfo->state == THREAD_MEMORY_ERROR || thrinfo->state == THREAD_CONNECTION_ERROR ||
			thrinfo->state == THREAD_SELECT_ERROR || thrinfo->state == THREAD_COPY_STATE_ERROR ||
			thrinfo->state == THREAD_MESSAGE_CONFUSION_ERROR || thrinfo->state == THREAD_FIELD_ERROR)
			copy_end = false;

		if (RunThreads->hs_thread_count == 1 &&
			((thrinfo->state == THREAD_EXIT_NORMAL) || (thrinfo->state == THREAD_DEAL_COMPLETE) ||
			(thrinfo->state == THREAD_DEFAULT)))
			copy_end = true;

		/* if all hash threads exit, put over flag to output queue to notice dispatch */
		if (copy_end)
		{
			for (flag = 0; flag < thrinfo->output_queue_size; flag++)
			{
				mq_pipe_put(thrinfo->output_queue[flag], NULL);
			}
		}
	}

	if (thrinfo->state != THREAD_EXIT_NORMAL &&
		thrinfo->state != THREAD_DEFAULT && thrinfo->state != THREAD_DEAL_COMPLETE)
			hash_write_error_message(thrinfo, 
									"thread exit state not right, file need to redo",
									NULL, 0, NULL, TRUE);
	/* record exit thread */
	pthread_mutex_lock(&FinishThreads->mutex);
	FinishThreads->hs_threads[loc] = thrinfo;
	FinishThreads->hs_thread_cur++;
	if (thrinfo->state == THREAD_DEFAULT || thrinfo->state == THREAD_DEAL_COMPLETE)
		thrinfo->state= THREAD_EXIT_NORMAL;		
	pthread_mutex_unlock(&FinishThreads->mutex);
	return;
}

static void *
hash_threadMain(void *argp)
{
	//int					poll_size;
	ComputeThreadInfo	*thrinfo = (ComputeThreadInfo*) argp;
	MessageQueuePipe	*input_queue;
	MessageQueue		*inner_queue;
	//MessageQueuePipe	**output_queue;
	//QueueElementPipe 	**element_batch;
	char				*copy;
	PGresult 	 		*res;
	bool 				mq_read = false;
	bool 				conn_read = false;
	bool 				conn_write = false;
	LineBuffer 			*lineBuffer = NULL;
	char 				*read_buff = NULL;

	fd_set read_fds;
	fd_set write_fds;
	int max_fd;
	int select_res;

	//poll_size = 0;
	//element_batch = NULL;
	input_queue = thrinfo->input_queue;
	//output_queue = thrinfo->output_queue;
	inner_queue = (MessageQueue*)palloc0(sizeof(MessageQueue));
	mq_init(inner_queue, THREAD_QUEUE_SIZE, "inner_queue");
	thrinfo->inner_queue = inner_queue;

	thrinfo->conn = PQconnectdb(thrinfo->conninfo);
	if (PQstatus(thrinfo->conn) != CONNECTION_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH]Connection to database failed: %s, flag is : %ld",
					PQerrorMessage(thrinfo->conn), thrinfo->thread_id);

		thrinfo->state = THREAD_CONNECTION_ERROR;
		hash_write_error_message(thrinfo,
								"Connection to database failed",
								PQerrorMessage(thrinfo->conn),
								0, NULL, TRUE);
		pthread_exit(thrinfo);
	}

	copy = create_copy_string(thrinfo->func_name, thrinfo->hash_field->field_nums,
									thrinfo->hash_field->copy_options);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %ld ] compute hash copy string is : %s ",
				thrinfo->thread_id, copy);

	res = PQexec(thrinfo->conn, copy);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH]PQresultStatus is not PGRES_COPY_BOTH, thread id : %ld, thread exit",
					thrinfo->thread_id);
		thrinfo->state = THREAD_COPY_STATE_ERROR;
		hash_write_error_message(thrinfo,
								"PQresultStatus is not PGRES_COPY_BOTH",
								PQerrorMessage(thrinfo->conn),
								0, NULL, TRUE);
		pthread_exit(thrinfo);
	}

	thrinfo->copy_str = copy;
	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %ld ] input queue read fd : %d, input queue write fd : %d, server connection fd : %d",
		thrinfo->thread_id, input_queue->fd[0],	input_queue->fd[1], thrinfo->conn->sock);

	for(;;)
	{
		if (thrinfo->exit)
		{
			ADBLOADER_LOG(LOG_INFO, "[HASH]hash thread : %d exit", thrinfo->thread_id);
			thrinfo->state = THREAD_EXIT_BY_OTHERS;
			pthread_exit(thrinfo);
		}

		mq_read 	= false;
		conn_read 	= false;
		conn_write	= false;
		read_buff	= NULL;
		lineBuffer 	= NULL;

		FD_ZERO(&read_fds);	
		FD_SET(thrinfo->conn->sock, &read_fds);
		FD_SET(input_queue->fd[0], &read_fds);
		FD_ZERO(&write_fds);
		FD_SET(thrinfo->conn->sock, &write_fds);

		if (input_queue->fd[0] > thrinfo->conn->sock)
			max_fd = input_queue->fd[0] + 1;
		else
			max_fd = thrinfo->conn->sock + 1;
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %ld ] max fd : %d", thrinfo->thread_id, max_fd);
		select_res = select(max_fd, &read_fds, &write_fds, NULL, NULL);
		if (select_res > 0)
		{
			if (FD_ISSET(thrinfo->conn->sock, &write_fds) > 0)
			{
				conn_write = true;
				if (FD_ISSET(input_queue->fd[0], &read_fds))
				{
					mq_read = true;
					/* need consider one question , input mode put last one line into queue, current thread select 
					   input_queue->fd[0] is ok, now other thread also detector input_queue->fd[0] is ok too and get last value before 
					   current thread, this case will lock this postion.
					   to solve question, input mode put last line, then wait some time, last send signal to all threads
					*/
					lineBuffer = mq_pipe_poll(input_queue);
					if (NULL != lineBuffer)
					{
						prepare_hash_field (&lineBuffer, 1, thrinfo, inner_queue);
					}
					else
					{
						int end_copy;
						int end_times = 0;
						RECV_END_FLAG = true;
						ADBLOADER_LOG(LOG_DEBUG,
							"[HASH][thread id : %ld ] file is complete, befor exit thread do something",
							thrinfo->thread_id);
ENDCOPY:
						end_times++;
						if (end_times > 3)
						{
							ADBLOADER_LOG(LOG_DEBUG,
							"[HASH][thread id : %ld ] send end copy error", thrinfo->thread_id);
							thrinfo->state = THREAD_SEND_ERROR;
							hash_write_error_message(thrinfo,
													"send end copy error, more than three times",
													PQerrorMessage(thrinfo->conn),
													0, NULL, TRUE);
							pthread_exit(thrinfo);
						}

						/* send copy end to server */
						if ((end_copy = PQputCopyEnd(thrinfo->conn, NULL)) == 1)
						{
							res = PQgetResult(thrinfo->conn);
							if (PQresultStatus(res) == PGRES_COPY_OUT)
							{
								int result;
						 		while (!mq_empty(inner_queue))
								{
									/* wait server send message */
									read_data(thrinfo->conn, read_buff, thrinfo);						
								}
								result = PQgetCopyData(thrinfo->conn, &read_buff, 0);
								if (result == - 1)
								{
									ADBLOADER_LOG(LOG_INFO,
											"[HASH][thread id : %ld ] file is complete", thrinfo->thread_id);
									PQclear(res);
									thrinfo->state = THREAD_DEAL_COMPLETE;
								}
							}
							else if (PQresultStatus(res) != PGRES_COMMAND_OK)
							{
								ADBLOADER_LOG(LOG_ERROR,
										"[HASH][thread id : %ld ] put copy end error, message : %s",
										thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
								thrinfo->state = THREAD_COPY_END_ERROR;

								hash_write_error_message(thrinfo, 
														"copy end error, file need to redo",
														PQerrorMessage(thrinfo->conn), 0,
														NULL, TRUE);
								PQclear(res);								
							}
							else if (PQresultStatus(res) == PGRES_COMMAND_OK)
							{
								ADBLOADER_LOG(LOG_INFO,
											"[HASH][thread id : %ld ] file is complete", thrinfo->thread_id);
								PQclear(res);
								thrinfo->state = THREAD_DEAL_COMPLETE;
							}
						}
						else if (end_copy == 0)
							goto ENDCOPY;

						else if (end_copy == -1)
							{
								ADBLOADER_LOG(LOG_ERROR,
											"[HASH][thread id : %ld ] send copy end to server error, message : %s",
											thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
								thrinfo->state = THREAD_COPY_END_ERROR;
							}
					pthread_exit(thrinfo);
					}			
				}
				else
					mq_read = false;
			}
			else
				conn_write = false;

			if(FD_ISSET(thrinfo->conn->sock, &read_fds))
			{
				conn_read = true;
				read_data(thrinfo->conn, read_buff, thrinfo);
				/* buffer is empty */
			}
			else
				conn_read = false;

			if (conn_write && !mq_read && !conn_read)
			{
				FD_ZERO(&read_fds);
				FD_SET(thrinfo->conn->sock, &read_fds);
				FD_SET(input_queue->fd[0], &read_fds);
				select_res = select(max_fd, &read_fds, NULL, NULL, NULL);
				if (select_res > 0)
					continue;
				else
				{
					/* exception handling */
					if (errno != EINTR && errno != EWOULDBLOCK)
					{
						ADBLOADER_LOG(LOG_ERROR,
								"[HASH][thread id : %ld ] select connect server socket return < 0, network may be not well, exit thread",
									thrinfo->thread_id);
						thrinfo->state = THREAD_SELECT_ERROR;
						hash_write_error_message(thrinfo,
												"select connect server socket return < 0, network may be not well",
												PQerrorMessage(thrinfo->conn),
												0, NULL, TRUE);
						pthread_exit(thrinfo);
					}
				}
			}

			/* now conn can't read and can't write */
			if (!conn_read && !conn_write)
			{
				if (PQstatus(thrinfo->conn) != CONNECTION_OK)
				{	
					/* reconnect */
					reconnect(thrinfo);
					continue;
				}

				FD_ZERO(&read_fds);	
				FD_SET(thrinfo->conn->sock, &read_fds);
				FD_ZERO(&write_fds);
				FD_SET(thrinfo->conn->sock, &write_fds);
				select_res = select(max_fd, &read_fds, &write_fds, NULL, NULL);
				if (select_res > 0)
					continue;
				else
				{
					/* exception handling */
					if (errno != EINTR && errno != EWOULDBLOCK)
					{
						ADBLOADER_LOG(LOG_ERROR,
								"[HASH][thread id : %ld ] select connect server socket return < 0, network may be not well, exit thread",
									thrinfo->thread_id);
						thrinfo->state = THREAD_SELECT_ERROR;
						hash_write_error_message(thrinfo,
												"select connect server socket return < 0, network may be not well",
												PQerrorMessage(thrinfo->conn),
												0, NULL, TRUE);
						pthread_exit(thrinfo);
					}
				}
			}
			
		}
		if (select_res < 0)
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				/* exception handling */
					ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] select connect server socket return < 0, network may be not well, exit thread",
								thrinfo->thread_id);
					thrinfo->state = THREAD_SELECT_ERROR;
					hash_write_error_message(thrinfo,
											"select connect server socket return < 0, network may be not well",
											PQerrorMessage(thrinfo->conn),
											0, NULL, TRUE);
					pthread_exit(thrinfo);
			}

			if (PQstatus(thrinfo->conn) != CONNECTION_OK)
			{	
				/* reconnect */
				reconnect(thrinfo);
				continue;				
			}
		}
	}
	return NULL;
}

static void
prepare_hash_field (LineBuffer **	element_batch, int size,
				ComputeThreadInfo * thrinfo,  MessageQueue *inner_queue)
{
	int 		  element_flag;
	LineBuffer	  *lineBuffer = NULL;
	LineBuffer	  *hash_field;

	for (element_flag = 0; element_flag < size; element_flag++)
	{
		lineBuffer = element_batch[element_flag];
		hash_field = package_field(lineBuffer, thrinfo);
		if(hash_field == NULL)
		{
			hash_write_error_message(thrinfo,
									"extract hash field error",
									"extract hash field error", lineBuffer->fileline,
									lineBuffer->data, FALSE);
			/* release linebuffer */
			release_linebuf(lineBuffer);
		}
		else
		{
			int send;			
			/* send data */
			send = PQputCopyData(thrinfo->conn, hash_field->data, hash_field->len);
			PQflush(thrinfo->conn);
			if(send < 0)
			{
				ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] send copy data error : %s ",
								thrinfo->thread_id, lineBuffer->data);

				hash_write_error_message(thrinfo,
										"send hash field to server error",
										PQerrorMessage(thrinfo->conn), lineBuffer->fileline,
										lineBuffer->data, FALSE);
				/* release linebuffer */
				release_linebuf(lineBuffer);
			}
			else
			{
				/* put data to inner queue */
				QueueElement *element_inner = NULL;			
				element_inner = (QueueElement *)palloc0(sizeof(QueueElement));
				if (NULL == element_inner)
				{
					ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] palloc0 QueueElement error, may be memory not enough",
								thrinfo->thread_id);
					release_linebuf(hash_field);
					thrinfo->state = THREAD_MEMORY_ERROR;
					hash_write_error_message(thrinfo,
											"palloc0 QueueElement error, may be memory not enough",
											"palloc0 QueueElement error, may be memory not enough",
											0, NULL, TRUE);
					pthread_exit(thrinfo);
				}
				element_inner->lineBuffer = lineBuffer;
				/* lineno begin from zero */
				element_inner->lineBuffer->lineno = thrinfo->send_seq;
				thrinfo->send_seq++;				
				mq_put_element(inner_queue, element_inner);
			}
		}
		release_linebuf(hash_field);
	}
}

static LineBuffer *
package_field (LineBuffer *lineBuffer, ComputeThreadInfo * thrinfo)
{
	LineBuffer  *buf;
	char 	   **fields = (char **)palloc0(sizeof(char*) * thrinfo->hash_field->field_nums);
	char        *line = (char*)palloc0(lineBuffer->len + 1);
	
	if (NULL == fields || NULL == line)
	{
		ADBLOADER_LOG(LOG_ERROR,
			"[HASH][thread id : %ld ] distribute function palloc memory error",
			thrinfo->thread_id);
		return 	NULL;
	}
	memcpy(line, lineBuffer->data, lineBuffer->len);
	line[lineBuffer->len] = '\0';
	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %ld ] get line : %s , thread id : %ld ",
						thrinfo->thread_id, line);
	buf = get_field_quote(fields, line,thrinfo->hash_field->field_loc, thrinfo->hash_field->delim,
		thrinfo->hash_field->quotec, thrinfo->hash_field->escapec, thrinfo->hash_field->field_nums, thrinfo);
//	buf = get_field(fields, line, thrinfo->hash_field->field_loc,
//							thrinfo->hash_field->delim, thrinfo->hash_field->field_nums);

	if(buf == NULL)
	{	
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %ld ] get hash field failed : %s ",
			thrinfo->thread_id, line);
		pfree(line);
		line = NULL;
		return NULL;
	}

	pfree(line);
	line = NULL;
	free_buff(fields, thrinfo->hash_field->field_nums);

	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %ld ] hash line : %s , thread id : %ld ",
						thrinfo->thread_id, buf->data);
	return buf;
}

/*
static LineBuffer *
get_field (char **fields, char *line, int * loc, char *delim, int size)
{

	int			flag = 0;
	int			split = 0;
	char		*field = NULL;
	char		*tmp = NULL;
	int			max_loc;
	int			len = 0;
	int			cur;
	int			tmp_loc;
	LineBuffer	*buf;

	max_loc = max_value(loc, size);
	split++;
	if (split <= max_loc)
	{
		field = strtok(line, delim);
		if (NULL == field)
			return NULL;
	}

	len = strlen(field);
	tmp = (char*)palloc0(len + 1);
	memcpy(tmp, field, len);
	tmp[len] = '\0';
	fields[flag] = tmp;
	flag++;

	while (NULL != field)
	{
		split++;
		if (split > max_loc)
			break;
		field = strtok(NULL, delim);
		if (NULL != field)
		{
			len = strlen(field);
			tmp = (char*)palloc0(len + 1);
			memcpy(tmp, field, len);
			tmp[len] = '\0';
			fields[flag] = tmp;
			flag++;
		}
	}
	if (split <= max_loc)
	{

		for (cur = 0; cur < flag; cur++)
		{
			pfree(fields[cur]);
			fields[cur] = NULL;
		}		
		return NULL;
	}
	buf = get_linebuf();
	for (tmp_loc = 0; tmp_loc < size -1; tmp_loc++)
	{
		int field_loc;
		field_loc = loc[tmp_loc]-1;
		appendLineBufInfo(buf, "%s", fields[field_loc]);
		appendLineBufInfo(buf, "%c", ',');
	}
	appendLineBufInfo(buf, "%s", fields[loc[size -1]-1]);
	return buf;
}
*/
static LineBuffer	*
get_field_quote (char **fields, char *line, int * loc, char *delim, char quotec, char escapec,
							int size, ComputeThreadInfo * thrinfo)
{
	int			max_loc;	
	int			split = 0;
	char	    *line_end_ptr;
	char	    *cur_ptr;
	char		*result;
	char	    *output_ptr;
	char		delimc;
	LineBuffer	*buf;
	int			tmp_loc;
	int			input_len;

	line_end_ptr = line + strlen(line);
	max_loc = max_value(loc, size);
	cur_ptr = line;
	delimc = delim[0];
	result = (char*)palloc0(strlen(line) + 1);
	output_ptr = result;
	for(;;)
	{
		//bool		found_delim = false;
		bool		saw_quote = false;
		char	   *start_ptr;
		char	   *end_ptr;
		
		if (split + 1 > max_loc)
			break;
		start_ptr = cur_ptr;
		fields[split] = output_ptr;
		for (;;)
		{
			char c;			
			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (c == delimc)
				{
					//found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					*output_ptr++ = c;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}
			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
				{
					ADBLOADER_LOG(LOG_ERROR,				
								"[HASH][thread id : %ld] unterminated CSV quoted field ",
								thrinfo->thread_id);
					return NULL;
				}
				c = *cur_ptr++;
				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char		nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}
				if (c == quotec)
				{
					*output_ptr++ = c;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
endfield:
		*output_ptr++ = '\0';
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == 0)
		{
			fields[split] = "";
		}
		split++;
	}
	for (tmp_loc = 0; tmp_loc < size; tmp_loc++)
	{
		char		*field_value;
		int			value_len;
		value_len = strlen(fields[tmp_loc]);
		field_value	= (char*)palloc0(value_len + 1);
		memcpy(field_value, fields[tmp_loc], value_len);
		field_value[value_len] = '\0';
		fields[tmp_loc] = field_value;
	}
	buf = get_linebuf();
	for (tmp_loc = 0; tmp_loc < size -1; tmp_loc++)
	{
		
		int		field_loc;

		field_loc = loc[tmp_loc]-1;
		appendLineBufInfo(buf, "%s", fields[field_loc]);
		appendLineBufInfo(buf, "%c", ',');
	}
	appendLineBufInfo(buf, "%s", fields[loc[size -1]-1]);
	pfree(result);
	return buf;
}

static char *
create_copy_string(char *func, int parameter_nums, char *copy_options)
{
	LineBuffer	*buf;
	int			flag;
	char		*result;
	buf = get_linebuf();
	appendLineBufInfo(buf, "%s", "copy function ");
	appendLineBufInfo(buf, "%s", func);
	appendLineBufInfo(buf, "%c", '(');
	for (flag = 1; flag < parameter_nums; flag++)
	{
		appendLineBufInfo(buf, "%c", '$');
		appendLineBufInfo(buf, "%d", flag);
		appendLineBufInfo(buf, "%c", ',');		
	}
	appendLineBufInfo(buf, "%c", '$');
	appendLineBufInfo(buf, "%d", flag);
	appendLineBufInfo(buf, "%c", ')');
	appendLineBufInfo(buf, "%s", " from stdin to stdout ");
	if (NULL != copy_options)
		appendLineBufInfo(buf, "%s", copy_options);
	result = (char*)palloc0(buf->len + 1);
	memcpy(result, buf->data, buf->len);
	result[buf->len] = '\0';
	release_linebuf(buf);
	return result;
}

static void
read_data(PGconn *conn, char *read_buff, ComputeThreadInfo	*thrinfo)
{
	int	 ret;
	PGresult *res;
	Assert(NULL != conn && NULL != thrinfo);	
	/* deal data then put datat to output queue */
	ret = PQgetCopyData(conn, &read_buff, 0);
	if (ret < 0)			/* done or server/connection error */
	{
		/* End of copy stream */
		if (ret == -1)
		{
			res = PQgetResult(conn);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				ADBLOADER_LOG(LOG_ERROR, "[HASH][thread id : %ld ] Failure while read  end of copy, but PGresult is  PGRES_FATAL_ERROR",
				thrinfo->thread_id);

				check_restart_hash_stream(thrinfo);	
			}
			return;
		}
		/* Failure while reading the copy stream */
		if (ret == -2)
		{
			if (read_buff != NULL)
			{
				PQfreemem(read_buff);
				read_buff = NULL;
			}
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread id : %ld ] Failure while reading the copy stream",
				thrinfo->thread_id);

			check_restart_hash_stream(thrinfo);
		}
	}		
	if (read_buff)
	{
		long			send_flag;
		uint32			hash_result;
		QueueElement	*inner_element;
		char *strtok_r_ptr = NULL;

		/* split buff  : falg,hash*/
		char * buff_tmp = pg_strdup(read_buff);
		char * field = strtok_r(buff_tmp, thrinfo->hash_field->hash_delim, &strtok_r_ptr);
		if (NULL == field)
		{
			/* buffer error */
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] get hash restult error, can't get flag result : %s",
							thrinfo->thread_id, read_buff);
			thrinfo->state = THREAD_FIELD_ERROR;
			/* this error need not to pthread_exit,  improve it*/
			inner_element = mq_poll(thrinfo->inner_queue);
			hash_write_error_message(thrinfo,
									" get hash restult error, can't get flag result",
									read_buff,
									inner_element->lineBuffer->fileline, inner_element->lineBuffer->data, TRUE);
			pfree(inner_element);
			PQfreemem(read_buff);
			if (buff_tmp)
				pfree(buff_tmp);
			pthread_exit(thrinfo);
		}
		else
		{
			send_flag = atol(field);
		}
		field = strtok_r(NULL, thrinfo->hash_field->hash_delim, &strtok_r_ptr);
		if (NULL == field)
		{
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] get hash restult error, can't get hash: %s",
							thrinfo->thread_id, read_buff);
			/* buffer error */
			thrinfo->state = THREAD_FIELD_ERROR;
			/* this error need not to pthread_exit,  improve it*/
			inner_element = mq_poll(thrinfo->inner_queue);
			hash_write_error_message(thrinfo,
									"get hash restult error, can't get hash",
									read_buff,
									inner_element->lineBuffer->fileline, inner_element->lineBuffer->data, TRUE);
			pfree(inner_element);
			PQfreemem(read_buff);
			if (buff_tmp)
				pfree(buff_tmp);
			pthread_exit(thrinfo);
		}
		else
		{
			char *end = field + strlen(field);
			hash_result = strtol(field, &end, 10);
		}
	
		/* read data from inner queue */
		inner_element = mq_poll(thrinfo->inner_queue);
		if (NULL == inner_element)
		{
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] receive hash result : %s, but inner queue is empty",
							thrinfo->thread_id, read_buff);
			/* message confusion */
			thrinfo->state = THREAD_MESSAGE_CONFUSION_ERROR;
			hash_write_error_message(thrinfo,
									"message confusion error", "message confusion error",
									0, NULL, TRUE);
			PQfreemem(read_buff);
			if (buff_tmp)
				pfree(buff_tmp);
			pthread_exit(thrinfo);
		}
		else
		{
			int modulo;
			Assert(inner_element->lineBuffer->lineno == send_flag);
			/* calc which datanode to send */
			modulo = calc_send_datanode(hash_result, thrinfo->hash_field);
			/* put linebuf to outqueue */
			mq_pipe_put(thrinfo->output_queue[modulo], inner_element->lineBuffer);
			ADBLOADER_LOG(LOG_DEBUG,
							"[HASH][thread id : %ld ] line data : %s, hash modulo result : %d",
							thrinfo->thread_id, inner_element->lineBuffer->data, modulo);
			inner_element->lineBuffer = NULL;
			pfree(inner_element);
		}
		PQfreemem(read_buff);
		pfree(buff_tmp);
	}	
}

/* this function only called by read_data() */
static void
check_restart_hash_stream (ComputeThreadInfo	*thrinfo)
{
	QueueElement	*inner_element = NULL;

	Assert(NULL != thrinfo);
	if (RECV_END_FLAG)
	{
		/* if inner queue is not empty */
		if (!mq_empty(thrinfo->inner_queue))
		{
			inner_element = mq_poll(thrinfo->inner_queue);
			/* write down error line */
			hash_write_error_message(thrinfo,
									"error happend, pull one inner_element from inner queue, this data may be error",
									PQerrorMessage(thrinfo->conn), inner_element->lineBuffer->fileline,
									inner_element->lineBuffer->data, FALSE);
			release_linebuf(inner_element->lineBuffer);

			if (!mq_empty(thrinfo->inner_queue))
			{
				thrinfo->state = THREAD_RESTART_COPY_STREAM;
				/* need restart copy */
				restart_hash_stream(thrinfo);
			}
			else
			{
				thrinfo->state = THREAD_DEAL_COMPLETE;
				pthread_exit(thrinfo);
			}
		}
		else
		{
			/* error happend */
			thrinfo->state = THREAD_MESSAGE_CONFUSION_ERROR;
			hash_write_error_message(thrinfo,
									"message confusion , file need to redo",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, TRUE);
			pthread_exit(thrinfo);
		}
	}
	else
	{
		if (!mq_empty(thrinfo->inner_queue))
		{
			inner_element = mq_poll(thrinfo->inner_queue);
			/* write down error line */
			hash_write_error_message(thrinfo,
									"error happend, pull one inner_element from inner queue, this data may be error",
									PQerrorMessage(thrinfo->conn), inner_element->lineBuffer->fileline,
									inner_element->lineBuffer->data, FALSE);

			release_linebuf(inner_element->lineBuffer);
			/* need restart copy */
			thrinfo->state = THREAD_RESTART_COPY_STREAM;
			restart_hash_stream(thrinfo);
		}
		else
		{
			/* error happend */
			thrinfo->state = THREAD_MESSAGE_CONFUSION_ERROR;
			hash_write_error_message(thrinfo,
									"error happend but not know reason, pull one inner_element then restart copy to server",
									NULL, inner_element->lineBuffer->fileline,
									inner_element->lineBuffer->data, TRUE);
			pthread_exit(thrinfo);
		}
	}	
}


static void
restart_hash_stream (ComputeThreadInfo *thrinfo)
{
	char 	 		*copy;
	PGresult 		*res;
	MessageQueue	*queue;

	Assert(NULL != thrinfo->conn);
	if (PQstatus(thrinfo->conn) != CONNECTION_OK)		
		reconnect(thrinfo);		/* reconnect */	

	if (NULL != thrinfo->copy_str)
		copy = thrinfo->copy_str;
	else
		copy = create_copy_string(thrinfo->func_name, thrinfo->hash_field->field_nums,
									thrinfo->hash_field->copy_options);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %ld ] compute hash copy string is : %s ",
				thrinfo->thread_id, copy);

	res = PQexec(thrinfo->conn, copy);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH]PQresultStatus is not PGRES_COPY_BOTH, thread id : %ld, thread exit",
					thrinfo->thread_id);
		thrinfo->state = THREAD_COPY_STATE_ERROR;
		hash_write_error_message(thrinfo,
								"begin copy to server PQresultStatus is not PGRES_COPY_BOTH",
								"begin copy to server PQresultStatus is not PGRES_COPY_BOTH",
								0, NULL, TRUE);
		pthread_exit(thrinfo);
	}
	/* resend inner queue data */
	Assert(thrinfo->inner_queue != NULL);
	queue = (MessageQueue*)palloc0(sizeof(MessageQueue));
	mq_init(queue, THREAD_QUEUE_SIZE, "tmp_queue");
	if (!mq_empty(thrinfo->inner_queue))
	{
		ADBLOADER_LOG(LOG_INFO, "[HASH]thrinfo->send_seq is : %ld, now restart sequence",
					thrinfo->thread_id, thrinfo->send_seq);
		thrinfo->send_seq = 0; /* restart sequence */
	}

	while (!mq_empty(thrinfo->inner_queue))
	{
		LineBuffer	 *hash_field;
		QueueElement *element;
		int 		 send;
		element = mq_poll(thrinfo->inner_queue);
		/* calc hash field again */
		hash_field = package_field(element->lineBuffer, thrinfo);
		if(hash_field == NULL)
		{
			hash_write_error_message(thrinfo,
									"extract hash field error",
									"extract hash field error", element->lineBuffer->fileline,
									element->lineBuffer->data, FALSE);			
			release_linebuf(element->lineBuffer);
			continue;
		}
		/* send data to adbloader server again */
		send = PQputCopyData(thrinfo->conn, hash_field->data, hash_field->len);
		PQflush(thrinfo->conn);
		if(send < 0)
		{
			ADBLOADER_LOG(LOG_DEBUG,
						"[HASH][thread id : %ld ] send copy data error : %s ",
							thrinfo->thread_id, element->lineBuffer->data);

			hash_write_error_message(thrinfo,
									"send hash field to server error",
									PQerrorMessage(thrinfo->conn), element->lineBuffer->fileline,
									element->lineBuffer->data, FALSE);				
			release_linebuf(element->lineBuffer);
		}
		else
		{
			/* put send data to tmp_queue */
			mq_put_element(queue, element);
		}
		release_linebuf(hash_field);
	}
	/* get all data from tmp_queue, then put them to inner_queue */
	while (!mq_empty(queue))
	{
		QueueElement *element;
		element = mq_poll(queue);
		element->lineBuffer->lineno = thrinfo->send_seq;
		thrinfo->send_seq++;
		mq_put_element(thrinfo->inner_queue, element);
	}

	if (RECV_END_FLAG)
	{
		char 				*read_buff = NULL;
		/* send copy end */
		if (PQputCopyEnd(thrinfo->conn, NULL) == 1)
		{
			res = PQgetResult(thrinfo->conn);
			if (PQresultStatus(res) == PGRES_COPY_OUT)
			{
				int result;
		 		while (!mq_empty(thrinfo->inner_queue))
				{
					/* wait server send message */
					read_data(thrinfo->conn, read_buff, thrinfo);						
				}
				result = PQgetCopyData(thrinfo->conn, &read_buff, 0);
				if (result == - 1)
				{
					ADBLOADER_LOG(LOG_INFO,
							"[HASH][thread id : %ld ] file is complete", thrinfo->thread_id);
					thrinfo->state = THREAD_DEAL_COMPLETE;					
				}
			}
			else if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				ADBLOADER_LOG(LOG_ERROR,
						"[HASH][thread id : %ld ] put copy end error, message : %s",
						thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
				thrinfo->state = THREAD_COPY_END_ERROR;

				hash_write_error_message(thrinfo,
										"copy end error, file need to redo",
										PQerrorMessage(thrinfo->conn), 0,
										NULL, TRUE);	
			}
			else if (PQresultStatus(res) == PGRES_COMMAND_OK)
			{
				ADBLOADER_LOG(LOG_INFO,
							"[HASH][thread id : %ld ] file is complete", thrinfo->thread_id);
				thrinfo->state = THREAD_DEAL_COMPLETE;
			}
			PQclear(res);
		
		}
		else
		{
			thrinfo->state = THREAD_COPY_END_ERROR;
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] send copy end to server error, message : %s",
							thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
									thrinfo->state = THREAD_COPY_END_ERROR;
			hash_write_error_message(thrinfo,
									"copy end error, file need to redo",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, TRUE);
		}
		pthread_exit(thrinfo);
	}
	ADBLOADER_LOG(LOG_INFO, "[HASH]restart_hash_stream function complete, now thrinfo->send_seq is : %ld",
					thrinfo->thread_id, thrinfo->send_seq);
	/* destory tmp_queue */
	mq_destory(queue);
}

static int
calc_send_datanode (uint32 hash, HashField *hash_field)
{
	int		modulo;
	Assert(NULL != hash_field);
	modulo = compute_hash_modulo(labs(hash), hash_field->node_nums);
	return modulo;
}

static int
max_value (int * values, int size)
{
	int tmp;
	int flag;
	int result = 0;
	for (flag = 0; flag < size; flag++)
	{
		tmp = values[flag];
		if(tmp > result)
			result = tmp;
	}
	return result;
}

static void
free_buff(char ** buff, int len)
{
	char *tmp;
	int   flag;

	Assert(NULL !=buff);
	for (flag = 0; flag < len; flag++)
	{
		tmp = buff[flag];
		if (NULL != tmp)
		{
			pfree(tmp);
			tmp = NULL;
		}
	}
	pfree(buff);
	buff = NULL;
}

static void
print_struct (HashComputeInfo * hash_computeInfo, HashField * hash_field)
{
	int flag;
	Assert(NULL != hash_computeInfo && NULL != hash_field);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] print HashComputeInfo");
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] tread numbers : %d, connecion info : %s,",
		hash_computeInfo->thread_nums, hash_computeInfo->conninfo);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] funcion name : %s", hash_computeInfo->func_name);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] print HashField");
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] field num: %d ", hash_field->field_nums);
	for (flag = 0; flag < hash_field->field_nums; flag ++)
	{
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] field loc :%d, field type",
				hash_field->field_loc[flag], hash_field->field_type[flag]);
	}
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ]  file delim : %s, hash result delim : %s",
		hash_field->delim, hash_field->hash_delim);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] node num: %d ", hash_field->node_nums);
	for (flag = 0; flag < hash_field->node_nums; flag++)
	{
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] node : %d ", hash_field->node_list[flag]);
	}
	return;
}

static PGconn *
reconnect(ComputeThreadInfo *thrinfo)
{	
	int times = 3;
	Assert(thrinfo->conn != NULL && thrinfo->conninfo != NULL);
	PQfinish(thrinfo->conn);
	while (times > 0)
	{
		thrinfo->conn = PQconnectdb(thrinfo->conninfo);
		/* memory allocation failed */
		if (NULL == thrinfo->conn)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread id : %ld ] memory allocation failed",
				thrinfo->thread_id);
			thrinfo->state = THREAD_MEMORY_ERROR;
			hash_write_error_message(thrinfo,
									"memory allocation failed",
									"memory allocation failed",
									0, NULL, TRUE);
			pthread_exit(thrinfo);
		}
		if (PQstatus(thrinfo->conn) != CONNECTION_OK)
		{
			ADBLOADER_LOG(LOG_WARN, "[HASH][thread id : %ld ] connect error : %s, connect string : %s ",
				thrinfo->thread_id, PQerrorMessage(thrinfo->conn), thrinfo->conninfo);
			PQfinish(thrinfo->conn);
			thrinfo->conn = NULL;
			times--; 
			sleep (5);
			continue;
		}
		else
			break;		
	}
	if (times == 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH][thread id : %ld ] connect error : %s, connect string : %s ",
				thrinfo->thread_id, PQerrorMessage(thrinfo->conn), thrinfo->conninfo);
		thrinfo->conn = NULL;
		thrinfo->state = THREAD_CONNECTION_ERROR;
		hash_write_error_message(thrinfo,
								"connect error",
								PQerrorMessage(thrinfo->conn),
								0, NULL, TRUE);
		pthread_exit(thrinfo);
	}
	return thrinfo->conn;
}

/*
 * compute_modulo
 * This function performs modulo in an optimized way
 * It optimizes modulo of any positive number by
 * 1,2,3,4,7,8,15,16,31,32,63,64 and so on
 * for the rest of the denominators it uses % operator
 * The optimized algos have been taken from
 * http://www-graphics.stanford.edu/~seander/bithacks.html
 */
static int
compute_hash_modulo(unsigned int numerator, unsigned int denominator)
{
	unsigned int d;
	unsigned int m;
	unsigned int s;
	unsigned int mask;
	int k;
	unsigned int q, r;

	if (numerator == 0)
		return 0;

	/* Check if denominator is a power of 2 */
	if ((denominator & (denominator - 1)) == 0)
		return numerator & (denominator - 1);

	/* Check if (denominator+1) is a power of 2 */
	d = denominator + 1;
	if ((d & (d - 1)) == 0)
	{
		/* Which power of 2 is this number */
		s = 0;
		mask = 0x01;
		for (k = 0; k < 32; k++)
		{
			if ((d & mask) == mask)
				break;
			s++;
			mask = mask << 1;
		}

		m = (numerator & xc_mod_m[s]) + ((numerator >> s) & xc_mod_m[s]);

		for (q = 0, r = 0; m > denominator; q++, r++)
			m = (m >> xc_mod_q[s][q]) + (m & xc_mod_r[s][r]);

		m = m == denominator ? 0 : m;

		return m;
	}
	return numerator % denominator;
}

