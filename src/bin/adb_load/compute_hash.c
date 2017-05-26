#include <pthread.h>
#include <unistd.h>

#include "postgres_fe.h"
#include "log_process_fd.h"
#include "log_detail_fd.h"
#include "read_producer.h"
#include "compute_hash.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "log_summary_fd.h"
#include "utility.h"
#include "log_summary.h"
#include "../../include/utils/pg_crc.h"
#include "../../include/utils/pg_crc_tables.h"

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

/*
 * hash_uint32() -- hash a 32-bit value
 */
static uint32 hash_uint32(uint32 k);
//static uint32 hash_any(register const unsigned char *k, register int keylen);
static uint32 get_multi_field_hash_value(char *field_data);

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

#define adb_load_final(a,b,c) \
{ \
  c ^= b; c -= adb_load_rot(b,14); \
  a ^= c; a -= adb_load_rot(c,11); \
  b ^= a; b -= adb_load_rot(a,25); \
  c ^= b; c -= adb_load_rot(b,16); \
  a ^= c; a -= adb_load_rot(c, 4); \
  b ^= a; b -= adb_load_rot(a,14); \
  c ^= b; c -= adb_load_rot(b,24); \
}

#define mix(a,b,c) \
{ \
  a -= c;  a ^= adb_load_rot(c, 4);	c += b; \
  b -= a;  b ^= adb_load_rot(a, 6);	a += c; \
  c -= b;  c ^= adb_load_rot(b, 8);	b += a; \
  a -= c;  a ^= adb_load_rot(c,16);	c += b; \
  b -= a;  b ^= adb_load_rot(a,19);	a += c; \
  c -= b;  c ^= adb_load_rot(b, 4);	b += a; \
}

/* Rotate a uint32 value left by k bits - note multiple evaluation! */
#define adb_load_rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

static int adbLoader_hashThreadCreate (HashComputeInfo *hash_info);
static void adbLoader_ThreadCleanup (void * argp);
static void prepare_hash_field (LineBuffer **element_batch, int size,
                               ComputeThreadInfo *thrinfo, MessageQueue *inner_queue);
static int max_value (int * values, int size);
static int compute_hash_modulo (unsigned int numerator, unsigned int denominator);
static int calc_send_datanode (uint32 hash, HashField *hash_field);
static void restart_hash_stream (ComputeThreadInfo *thrinfo);
static void check_restart_hash_stream (ComputeThreadInfo *thrinfo);
static void free_buff (char ** buff, int len);
static void print_struct (HashComputeInfo * hash_computeInfo, HashField * hash_field);
static void * adbLoader_ThreadMainWrapper (void *argp);
static char * create_copy_string (char *func, int parameter_nums, char *copy_options);
static LineBuffer * get_field_quote (char **fields, char *line, int * loc, char *text_delim, char quotec,
                                     char escapec, int size, ComputeThreadInfo * thrinfo);
static void * hash_threadMain (void *argp);
static LineBuffer * package_field (LineBuffer *lineBuffer, ComputeThreadInfo *thrinfo);
static PGconn * reconnect (ComputeThreadInfo *thrinfo);
static void hash_write_error_message(ComputeThreadInfo *thrinfo,
                                    char * message,
                                    char * hash_error_message,
                                    int line_no,
                                    char *line_data,
                                    bool redo);

static void create_filter_queue_file_fd(int redo_queue_total,
                                        int *redo_queue_index,
                                        char *table_name,
                                        char *filter_queue_file_path);



static void put_copy_end_to_server(ComputeThreadInfo *thrinfo, MessageQueue *inner_queue);
static bool input_queue_can_read(int fd, fd_set *set);
static bool server_can_read(int fd, fd_set *set);
static bool server_can_write(int fd, fd_set *set);
static void get_data_from_server(PGconn *conn, char *read_buff, ComputeThreadInfo *thrinfo);
static bool is_comment_line(char *line_data, char *comment_str);

static const int    THREAD_QUEUE_SIZE = 1200;
static bool         RECV_END_FLAG = FALSE;
static bool         ALL_THREADS_EXIT = FALSE;
static HashThreads  HashThreadsData;
static HashThreads *RunThreads = &HashThreadsData;
static HashThreads  HashThreadsFinishData;
static HashThreads *FinishThreads = &HashThreadsFinishData;

static char        *g_start_cmd = NULL;

FILE **filter_queue_file_fd = NULL;

int
init_hash_compute(HashComputeInfo * hash_info)
{
	int res = 0;

	Assert(hash_info->func_name != NULL);
	Assert(hash_info->conninfo != NULL);
	Assert(hash_info->input_queue != NULL);
	Assert(hash_info->output_queue != NULL);
	Assert(hash_info->hash_field != NULL);

	if (hash_info->redo_queue)
	{
		bool need_redo_queue = false;
		int threads_total = 0;
		int i = 0;

		threads_total = hash_info->datanodes_num * hash_info->threads_num_per_datanode;

		for (i = 0; i < threads_total; i++)
		{
			need_redo_queue = check_need_redo_queue(hash_info->redo_queue_total,
													hash_info->redo_queue_index,
													i);
			if (!need_redo_queue)
			{
				mq_pipe_put(hash_info->output_queue[i], NULL);
			}
		}
	}

	if (hash_info->filter_queue_file)
	{
		create_filter_queue_file_fd(hash_info->redo_queue_total,
									hash_info->redo_queue_index,
									hash_info->table_name,
									hash_info->filter_queue_file_path);
	}

	res = adbLoader_hashThreadCreate(hash_info);
	return res;
}

static void
create_filter_queue_file_fd(int redo_queue_total,
                            int *redo_queue_index,
                            char *table_name,
                            char *filter_queue_file_path)
{
	char filter_queue_path[1024];
	int i = 0;

	filter_queue_file_fd = (FILE **)palloc0(redo_queue_total * sizeof(FILE *));

	for (i = 0 ; i < redo_queue_total; i++)
	{
		sprintf(filter_queue_path, "%s/%s_%d.sql", filter_queue_file_path, table_name, redo_queue_index[i]);

		filter_queue_file_fd[i] = fopen(filter_queue_path, "a+");
		if (filter_queue_file_fd[i] == NULL)
		{
			fprintf(stderr, "could not open log file: %s.\n",  filter_queue_path);
			exit(EXIT_FAILURE);
		}
	}
}

void
fclose_filter_queue_file_fd(int fd_total)
{
	int i = 0;
	for (i = 0; i < fd_total; i++)
	{
		if (fclose(filter_queue_file_fd[i]))
		{
			fprintf(stderr, "could not close filter queue file descriptor.\n");
		}
	}
}

int
check_compute_state(void)
{
	int running = 0;
	pthread_mutex_lock(&RunThreads->mutex);
	running = RunThreads->hs_thread_cur;
	pthread_mutex_unlock(&RunThreads->mutex);
	return running;
}

HashThreads *
get_exit_threads_info(void)
{
	return FinishThreads;
}

int
stop_hash_threads(void)
{
	int flag;
	pthread_mutex_lock(&RunThreads->mutex);
	for (flag = 0; flag < RunThreads->hs_thread_count; flag++)
	{
		ComputeThreadInfo *thrinfo = RunThreads->hs_threads[flag];
		if (thrinfo != NULL)
		{
			thrinfo->exit = true;
			ADBLOADER_LOG(LOG_INFO, "[HASH][thread main ] hash function set thread exit : %lu", (unsigned long)thrinfo->thread_id);
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
hash_write_error_message(ComputeThreadInfo *thrinfo, char * message,
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
			appendLineBufInfoString(error_buffer, "suggest : ");
			appendLineBufInfoString(error_buffer, "must deal this data alone");
			appendLineBufInfoString(error_buffer, "\n");
		}
	}
	appendLineBufInfoString(error_buffer, "-------------------------------------------------------\n");
	write_log_detail_fd(error_buffer->data);
	release_linebuf(error_buffer);
}

void
clean_hash_resource(void)
{
	int flag;
	Assert(RunThreads->hs_thread_cur == 0);

	if (RunThreads->hs_threads != NULL)
	{
		pfree(RunThreads->hs_threads);
		RunThreads->hs_threads = NULL;
	}

	for (flag = 0; flag < FinishThreads->hs_thread_count; flag++)
	{
		ComputeThreadInfo *thrinfo = FinishThreads->hs_threads[flag];
		if (thrinfo != NULL)
		{
			Assert(thrinfo->conn == NULL && thrinfo->inner_queue == NULL);
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
	RECV_END_FLAG = FALSE;
	ALL_THREADS_EXIT = FALSE;
}

void
set_hash_file_start_cmd(char * start_cmd)
{
	Assert(start_cmd != NULL);

	if(g_start_cmd != NULL)
	{
		pfree(g_start_cmd);
		g_start_cmd = NULL;
	}

	g_start_cmd = pg_strdup(start_cmd);

	return;
}

int
adbLoader_hashThreadCreate(HashComputeInfo *hash_info)
{
	int error = 0;
	int i = 0;
	ComputeThreadInfo *thread_info = NULL;

	Assert(hash_info != NULL);
	Assert(hash_info->hash_field != NULL);
	Assert(hash_info->hash_field->field_nums > 0);
	Assert(hash_info->hash_field->datanodes_num > 0);
	Assert(hash_info->hash_field->field_loc != NULL);
	Assert(hash_info->hash_field->field_type != NULL);
	Assert(hash_info->hash_field->node_list != NULL);
	Assert(hash_info->hash_field->text_delim != NULL);
	Assert(hash_info->hash_field->hash_delim != NULL);

	if(pthread_mutex_init(&RunThreads->mutex, NULL) != 0 ||
		pthread_mutex_init(&FinishThreads->mutex, NULL) != 0 )
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] Can not initialize mutex: %s", strerror(errno));
		hash_write_error_message(NULL,
								"Can not initialize mutex, file need to redo",
								g_start_cmd, 0, NULL, TRUE);
		exit(EXIT_FAILURE);
	}

	/*print HashComputeInfo and HashField */
	print_struct(hash_info, hash_info->hash_field);

	RunThreads->hs_thread_count = hash_info->hash_field->hash_threads_num;
	RunThreads->hs_threads = (ComputeThreadInfo **)palloc0(sizeof(ComputeThreadInfo *) * hash_info->hash_field->hash_threads_num);

	FinishThreads->hs_thread_count = hash_info->hash_field->hash_threads_num;
	FinishThreads->hs_threads = (ComputeThreadInfo **)palloc0(sizeof(ComputeThreadInfo *) * hash_info->hash_field->hash_threads_num);

	for (i = 0; i < hash_info->hash_field->hash_threads_num; i++)
	{
		thread_info = (ComputeThreadInfo *)palloc0(sizeof(ComputeThreadInfo));

		/* copy func name */
		thread_info->func_name = pg_strdup(hash_info->func_name);
		if(thread_info->func_name == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] copy function name error");
			return HASH_COMPUTE_ERROR;
		}

		/* copy conninfo */
		thread_info->conninfo = pg_strdup(hash_info->conninfo);
		if(thread_info->conninfo == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] copy conninfo error");
			return HASH_COMPUTE_ERROR;
		}

		thread_info->redo_queue = hash_info->redo_queue;
		thread_info->redo_queue_index = hash_info->redo_queue_index;
		thread_info->redo_queue_total = hash_info->redo_queue_total;
		thread_info->filter_queue_file = hash_info->filter_queue_file;

		thread_info->field_data = get_linebuf();

		thread_info->datanodes_num = hash_info->datanodes_num;
		thread_info->threads_num_per_datanode = hash_info->threads_num_per_datanode;
		thread_info->input_queue = hash_info->input_queue;
		thread_info->output_queue = hash_info->output_queue;
		thread_info->output_queue_num= hash_info->output_queue_num;
		thread_info->hash_field = (HashField *)palloc0(sizeof(HashField));
		if (thread_info->hash_field == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] palloc HashField error");
			return HASH_COMPUTE_ERROR;
		}
		thread_info->hash_field->field_nums = hash_info->hash_field->field_nums;
		thread_info->hash_field->datanodes_num = hash_info->hash_field->datanodes_num;
		thread_info->hash_field->hash_threads_num = hash_info->hash_field->hash_threads_num;
		thread_info->hash_field->field_loc = (int *)palloc0(sizeof(int) * hash_info->hash_field->field_nums);
		thread_info->hash_field->field_type = (Oid *)palloc0(sizeof(Oid) * hash_info->hash_field->field_nums);
		thread_info->hash_field->node_list = (Oid *)palloc0(sizeof(Oid) * hash_info->hash_field->datanodes_num);

		if (thread_info->hash_field->field_loc == NULL  ||
			thread_info->hash_field->field_type == NULL ||
			thread_info->hash_field->node_list == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] copy conninfo error");
			return HASH_COMPUTE_ERROR;
		}

		thread_info->copy_cmd_comment = hash_info->copy_cmd_comment;
		thread_info->copy_cmd_comment_str = pg_strdup(hash_info->copy_cmd_comment_str);

		memcpy(thread_info->hash_field->field_loc, hash_info->hash_field->field_loc, sizeof(int) * hash_info->hash_field->field_nums);
		memcpy(thread_info->hash_field->field_type, hash_info->hash_field->field_type, sizeof(Oid) * hash_info->hash_field->field_nums);
		memcpy(thread_info->hash_field->node_list, hash_info->hash_field->node_list, sizeof(Oid) * hash_info->hash_field->datanodes_num);
		thread_info->hash_field->text_delim = pg_strdup(hash_info->hash_field->text_delim);
		thread_info->hash_field->hash_delim = pg_strdup(hash_info->hash_field->hash_delim);
		if(hash_info->hash_field->copy_options == NULL)
			thread_info->hash_field->copy_options = pg_strdup(hash_info->hash_field->copy_options);
		else
			thread_info->hash_field->copy_options = NULL;

		thread_info->hash_field->quotec = hash_info->hash_field->quotec;
		thread_info->hash_field->has_qoute = hash_info->hash_field->has_qoute;
		thread_info->thr_startroutine = hash_threadMain;
		thread_info->state = THREAD_DEFAULT;
		RunThreads->hs_threads[i] = thread_info;

		if ((error = pthread_create(&thread_info->thread_id, NULL, adbLoader_ThreadMainWrapper, thread_info)) < 0)
		{
			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread main ] create thread error");
			return HASH_COMPUTE_ERROR;
		}
		ADBLOADER_LOG(LOG_INFO, "[HASH][thread main ] create thread : %lu", (unsigned long)thread_info->thread_id);
		RunThreads->hs_thread_cur++;
	}

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
	int flag = 0;
    int loc = 0;

	while (!mq_empty(inner_queue))
	{
		QueueElement * element;
		element = mq_poll(inner_queue);
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
	{
		ALL_THREADS_EXIT = true;
	}

	/* destory inner_queue */
	Assert(inner_queue != NULL);
	mq_destory(inner_queue);
	thrinfo->inner_queue = NULL;

	if (ALL_THREADS_EXIT)
	{
		bool copy_end = false;
		bool happen_error = false;
		ComputeThreadInfo * exit_thread = NULL;

		/* check all other threads state */
		for (flag = 0; flag < FinishThreads->hs_thread_count; flag++)
		{
			exit_thread = FinishThreads->hs_threads[flag];
			if (exit_thread != NULL)
			{
				if (exit_thread->state == THREAD_MEMORY_ERROR ||
					exit_thread->state == THREAD_CONNECTION_ERROR ||
					exit_thread->state == THREAD_SELECT_ERROR ||
					exit_thread->state == THREAD_COPY_STATE_ERROR ||
					exit_thread->state == THREAD_MESSAGE_CONFUSION_ERROR ||
					exit_thread->state == THREAD_FIELD_ERROR ||
					exit_thread->state == THREAD_PGRES_FATAL_ERROR)

					happen_error = true;
			}
		}

		if (happen_error)
			copy_end = false;
		else
			copy_end = true;

		/* check current thread state */
		if (thrinfo->state == THREAD_MEMORY_ERROR ||
			thrinfo->state == THREAD_CONNECTION_ERROR ||
			thrinfo->state == THREAD_SELECT_ERROR ||
			thrinfo->state == THREAD_COPY_STATE_ERROR ||
			thrinfo->state == THREAD_MESSAGE_CONFUSION_ERROR ||
			thrinfo->state == THREAD_FIELD_ERROR ||
			thrinfo->state == THREAD_PGRES_FATAL_ERROR)
			copy_end = false;

		if (RunThreads->hs_thread_count == 1 &&
			((thrinfo->state == THREAD_EXIT_NORMAL) ||
			(thrinfo->state == THREAD_DEAL_COMPLETE) ||
			(thrinfo->state == THREAD_DEFAULT) ||
			(thrinfo->state == THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE)))
			copy_end = true;

		/* if all hash threads exit, put over flag to output queue to notice dispatch */
		if (copy_end)
		{
			for (flag = 0; flag < thrinfo->output_queue_num; flag++)
			{
				mq_pipe_put(thrinfo->output_queue[flag], NULL);
			}
		}
	}

	/* record exit thread */
	pthread_mutex_lock(&FinishThreads->mutex);
	FinishThreads->hs_threads[loc] = thrinfo;
	FinishThreads->hs_thread_cur++;

	if (thrinfo->state == THREAD_DEFAULT || thrinfo->state == THREAD_DEAL_COMPLETE)
	{
		thrinfo->state= THREAD_EXIT_NORMAL;
	}

	pthread_mutex_unlock(&FinishThreads->mutex);
	return;
}

static bool
server_can_write(int fd, fd_set *set)
{
    int res = 0;

    res = FD_ISSET(fd, set);
    return (res > 0 ? true : false);
}

static bool
server_can_read(int fd, fd_set *set)
{
    int res = 0;

    res = FD_ISSET(fd, set);
    return (res > 0 ? true : false);
}

static bool
input_queue_can_read(int fd, fd_set *set)
{
    int res = 0;

    res = FD_ISSET(fd, set);
    return (res > 0 ? true : false);
}

static void
put_copy_end_to_server(ComputeThreadInfo *thrinfo, MessageQueue *inner_queue)
{
	int        end_copy = 0;
	int        end_times = 0;
    PGresult  *res = NULL;
    char      *read_buff = NULL;

	RECV_END_FLAG = true;

	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %lu ] file is complete, before exit thread do something",
		(unsigned long)thrinfo->thread_id);

ENDCOPY:
	end_times++;
	if (end_times > 3)
	{
		ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %lu ] send end copy error", (unsigned long)thrinfo->thread_id);
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
				get_data_from_server(thrinfo->conn, read_buff, thrinfo);
			}
			result = PQgetCopyData(thrinfo->conn, &read_buff, 0);
			if (result == - 1)
			{
				ADBLOADER_LOG(LOG_INFO,
						"[HASH][thread id : %lu ] file is complete", (unsigned long)thrinfo->thread_id);
				PQclear(res);

				if (thrinfo->state == THREAD_HAPPEN_ERROR_CONTINUE)
					thrinfo->state = THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE;

				if (thrinfo->state == THREAD_DEFAULT)
					thrinfo->state = THREAD_EXIT_NORMAL;
			}
		}
		else if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			ADBLOADER_LOG(LOG_ERROR,
					"[HASH][thread id : %lu ] put copy end error, message : %s",
					(unsigned long)thrinfo->thread_id, PQerrorMessage(thrinfo->conn));

			if (thrinfo->state == THREAD_HAPPEN_ERROR_CONTINUE)
				thrinfo->state = THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE;

			if (thrinfo->state == THREAD_DEFAULT)
				thrinfo->state = THREAD_EXIT_NORMAL;

			hash_write_error_message(thrinfo,
									"copy end error, file need to redo",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, TRUE);
			PQclear(res);
		}
		else if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			ADBLOADER_LOG(LOG_INFO,
						"[HASH][thread id : %lu ] file is complete", (unsigned long)thrinfo->thread_id);
			PQclear(res);

			if (thrinfo->state == THREAD_HAPPEN_ERROR_CONTINUE)
				thrinfo->state = THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE;

			if (thrinfo->state == THREAD_DEFAULT)
				thrinfo->state = THREAD_EXIT_NORMAL;

		}
	}
	else if (end_copy == 0)
    {
		goto ENDCOPY;
    }
	else if (end_copy == -1)
    {
        ADBLOADER_LOG(LOG_ERROR,
            "[HASH][thread id : %lu ] send copy end to server error, message : %s",
            (unsigned long)thrinfo->thread_id, PQerrorMessage(thrinfo->conn));

        if (thrinfo->state != THREAD_HAPPEN_ERROR_CONTINUE)
            thrinfo->state = THREAD_COPY_END_ERROR;
    }

	pthread_exit(thrinfo);

    return ;
}


static void *
hash_threadMain(void *argp)
{
	MessageQueuePipe   *input_queue = NULL;
	MessageQueue       *inner_queue = NULL;
	char               *copy = NULL;
	PGresult           *res = NULL;
	LineBuffer         *lineBuffer = NULL;
	char               *read_buff = NULL;
	fd_set              read_fds;
	fd_set              write_fds;
	int                 max_fd = 0;
	int                 select_res = 0;

	ComputeThreadInfo  *thrinfo = (ComputeThreadInfo*) argp;

	input_queue = thrinfo->input_queue;
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

	copy = create_copy_string(thrinfo->func_name,
								thrinfo->hash_field->field_nums,
								thrinfo->hash_field->copy_options);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %lu ] compute hash copy string is : %s ",
				(unsigned long)thrinfo->thread_id, copy);

	res = PQexec(thrinfo->conn, copy);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH]PQresultStatus is not PGRES_COPY_BOTH, thread id : %lu, thread exit",
					(unsigned long)thrinfo->thread_id);
		thrinfo->state = THREAD_COPY_STATE_ERROR;
		hash_write_error_message(thrinfo,
								"PQresultStatus is not PGRES_COPY_BOTH",
								PQerrorMessage(thrinfo->conn),
								0, NULL, TRUE);
		pthread_exit(thrinfo);
	}

	thrinfo->copy_str = copy;
	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %lu ] input queue read fd : %d, input queue write fd : %d, server connection fd : %d",
		(unsigned long)thrinfo->thread_id, input_queue->fd[0],	input_queue->fd[1], thrinfo->conn->sock);

	for(;;)
	{
		if (thrinfo->exit)
		{
			ADBLOADER_LOG(LOG_INFO, "[HASH]hash thread : %lu exit", (unsigned long)thrinfo->thread_id);
			thrinfo->state = THREAD_EXIT_BY_OTHERS;
			pthread_exit(thrinfo);
		}

		read_buff   = NULL;
		lineBuffer  = NULL;

		FD_ZERO(&read_fds);
		FD_SET(thrinfo->conn->sock, &read_fds);
		FD_SET(input_queue->fd[0], &read_fds);
		FD_ZERO(&write_fds);
		FD_SET(thrinfo->conn->sock, &write_fds);

		if (input_queue->fd[0] > thrinfo->conn->sock)
			max_fd = input_queue->fd[0] + 1;
		else
			max_fd = thrinfo->conn->sock + 1;
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %lu ] max fd : %d", (unsigned long)thrinfo->thread_id, max_fd);
		select_res = select(max_fd, &read_fds, &write_fds, NULL, NULL);
        if (select_res < 0)
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				/* exception handling */
				ADBLOADER_LOG(LOG_ERROR,
					"[HASH][thread id : %lu ] select connect server socket return < 0, network may be not well, exit thread",
					(unsigned long)thrinfo->thread_id);

				if (thrinfo->state != THREAD_HAPPEN_ERROR_CONTINUE)
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

        if (server_can_write(thrinfo->conn->sock, &write_fds))
        {
            if (input_queue_can_read(input_queue->fd[0], &read_fds))
            {
                if (mq_full(inner_queue))
                {
                    get_data_from_server(thrinfo->conn, read_buff, thrinfo);
                }

                lineBuffer = mq_pipe_poll(input_queue);
                if (lineBuffer != NULL)
                {
                    prepare_hash_field(&lineBuffer, 1, thrinfo, inner_queue);
                }
                else // threads end flag
                {
                    put_copy_end_to_server(thrinfo, inner_queue);
                }
            }
            else
            {
                ADBLOADER_LOG(LOG_DEBUG,
                    "[HASH][thread id : %lu] input queue can not read now.\n", (unsigned long)thrinfo->thread_id);
            }
        }
        else
        {
            ADBLOADER_LOG(LOG_INFO,
                "[HASH][thread id : %lu] Adb server compute hash relatively slow.\n", (unsigned long)thrinfo->thread_id);
        }

        if (server_can_read(thrinfo->conn->sock, &read_fds))
        {
            get_data_from_server(thrinfo->conn, read_buff, thrinfo);
        }


        if (server_can_write(thrinfo->conn->sock, &write_fds) &&
            !server_can_read(thrinfo->conn->sock, &read_fds) &&
            !input_queue_can_read(input_queue->fd[0], &read_fds))
        {
            FD_ZERO(&read_fds);
            FD_SET(thrinfo->conn->sock, &read_fds);
            FD_SET(input_queue->fd[0], &read_fds);
            select_res = select(max_fd, &read_fds, NULL, NULL, NULL);
            if (select_res > 0)
            {
                continue;
            }
            else
            {
                /* exception handling */
                if (errno != EINTR && errno != EWOULDBLOCK)
                {
                    ADBLOADER_LOG(LOG_ERROR,
                            "[HASH][thread id : %lu ] select connect server socket return < 0, network may be not well, exit thread",
                                (unsigned long)thrinfo->thread_id);
                    thrinfo->state = THREAD_SELECT_ERROR;
                    hash_write_error_message(thrinfo,
                                            "select connect server socket return < 0, network may be not well",
                                            PQerrorMessage(thrinfo->conn),
                                            0, NULL, TRUE);
                    pthread_exit(thrinfo);
                }
            }
        }

        if (!server_can_write(thrinfo->conn->sock, &write_fds) &&
            !server_can_read(thrinfo->conn->sock, &read_fds))
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
                            "[HASH][thread id : %lu ] select connect server socket return < 0, network may be not well, exit thread",
                                (unsigned long)thrinfo->thread_id);
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

    return NULL;
}

static void
prepare_hash_field (LineBuffer **element_batch, int size,
					ComputeThreadInfo * thrinfo,  MessageQueue *inner_queue)
{
	int            i;
	LineBuffer    *lineBuffer = NULL;
	LineBuffer    *hash_field = NULL;

	for (i = 0; i < size; i++)
	{
		lineBuffer = element_batch[i];
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
							"[HASH][thread id : %lu ] send copy data error : %s ",
								(unsigned long)thrinfo->thread_id, lineBuffer->data);

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
				if (element_inner == NULL)
				{
					ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %lu ] palloc0 QueueElement error, may be memory not enough",
								(unsigned long)thrinfo->thread_id);
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
	LineBuffer  *buf = NULL;
	char       **fields = (char **)palloc0(sizeof(char*) * thrinfo->hash_field->field_nums);
	char        *line = (char*)palloc0(lineBuffer->len + 1);

	if (fields == NULL || line == NULL)
	{
		ADBLOADER_LOG(LOG_ERROR,
			"[HASH][thread id : %lu ] distribute function palloc memory error",
			(unsigned long)thrinfo->thread_id);
		return NULL;
	}

	memcpy(line, lineBuffer->data, lineBuffer->len);
	line[lineBuffer->len] = '\0';

	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %lu ] get line : %s", (unsigned long)thrinfo->thread_id, line);

	buf = get_field_quote(fields,
						line,
						thrinfo->hash_field->field_loc,
						thrinfo->hash_field->text_delim,
						thrinfo->hash_field->quotec,
						thrinfo->hash_field->escapec,
						thrinfo->hash_field->field_nums,
						thrinfo);

	ADBLOADER_LOG(LOG_DEBUG,
		"[HASH][thread id : %lu ] get field : %s", (unsigned long)thrinfo->thread_id, buf->data);

	if(buf == NULL)
	{
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id: %lu ] get hash field failed : %s ",
			(unsigned long)thrinfo->thread_id, line);
		pfree(line);
		line = NULL;
		return NULL;
	}

	pfree(line);
	line = NULL;
	free_buff(fields, thrinfo->hash_field->field_nums);

	return buf;
}

static LineBuffer *
get_field_quote (char **fields, char *line, int * loc, char *text_delim, char quotec, char escapec,
							int size, ComputeThreadInfo * thrinfo)
{
	int         max_loc;
	int         split = 0;
	char       *line_end_ptr;
	char       *cur_ptr;
	char       *result;
	char       *output_ptr;
	char        delimc;
	LineBuffer *buf;
	int         tmp_loc;
	int         input_len;

	line_end_ptr = line + strlen(line);
	max_loc = max_value(loc, size);
	cur_ptr = line;
	delimc = text_delim[0];
	result = (char*)palloc0(strlen(line) + 1);
	output_ptr = result;
	for(;;)
	{
		//bool found_delim = false;
		bool saw_quote = false;
		char *start_ptr;
		char *end_ptr;

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
								"[HASH][thread id : %lu] unterminated CSV quoted field ",
								(unsigned long)thrinfo->thread_id);
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
						char nextc = *cur_ptr;
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
		char *field_value;
		int   value_len;
		value_len = strlen(fields[tmp_loc]);
		field_value = (char*)palloc0(value_len + 1);
		memcpy(field_value, fields[tmp_loc], value_len);
		field_value[value_len] = '\0';
		fields[tmp_loc] = field_value;
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
	pfree(result);
	return buf;
}

static char *
create_copy_string(char *func, int parameter_nums, char *copy_options)
{
	LineBuffer *buf = NULL;
	int         flag = 0;
	char       *result = NULL;

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

	if (copy_options != NULL)
		appendLineBufInfo(buf, "%s", copy_options);

	result = (char*)palloc0(buf->len + 1);
	memcpy(result, buf->data, buf->len);
	result[buf->len] = '\0';

	release_linebuf(buf);
	return result;
}

static void
get_data_from_server(PGconn *conn, char *read_buff, ComputeThreadInfo *thrinfo)
{
	int ret = 0;
	PGresult *res = NULL;
	bool need_redo_queue = false;

	Assert(conn != NULL);
	Assert(thrinfo != NULL);

	/* deal data then put data to output queue */
	ret = PQgetCopyData(conn, &read_buff, 0);
	if (ret < 0)   /* done or server/connection error */
	{

		/*adb_load server stop by administrator command*/
		if (ret == -1)
		{
			res = PQgetResult(conn);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				QueueElement* element = NULL;

				ADBLOADER_LOG(LOG_ERROR, "[HASH][thread id : %lu ] Failure while read  end of copy, but PGresult is  PGRES_FATAL_ERROR: %s,PQresultStatus(res):%d\n",
				(unsigned long)thrinfo->thread_id, PQerrorMessage(conn), PQresultStatus(res));

				element = mq_poll(thrinfo->inner_queue);

				if (!thrinfo->copy_cmd_comment ||
					(thrinfo->copy_cmd_comment && !is_comment_line(element->lineBuffer->data,
																	thrinfo->copy_cmd_comment_str)))
				{
					thrinfo->happen_error = true;
					hash_write_error_message(thrinfo,
											"get hash restult error, can't get hash",
											PQerrorMessage(conn),
											0, element->lineBuffer->data, false);

					append_error_info_list(ERRCODE_COMPUTE_HASH, element->lineBuffer->data);

					release_linebuf(element->lineBuffer);
				}

				check_restart_hash_stream(thrinfo);
			}

			return;
		}

		/* Failure while reading the copy stream */
		if (ret == -2)
		{
			QueueElement* element = NULL;

			if (read_buff != NULL)
			{
				PQfreemem(read_buff);
				read_buff = NULL;
			}

			ADBLOADER_LOG(LOG_ERROR, "[HASH][thread id : %lu ] Failure while reading the copy stream",
				(unsigned long)thrinfo->thread_id);

			thrinfo->happen_error = true;
            element = mq_poll(thrinfo->inner_queue);

            hash_write_error_message(thrinfo,
                                    "get hash restult error, can't get hash",
                                    PQerrorMessage(conn),
                                    0, element->lineBuffer->data, false);

			append_error_info_list(ERRCODE_COMPUTE_HASH, element->lineBuffer->data);
            release_linebuf(element->lineBuffer);

			check_restart_hash_stream(thrinfo);
		}
        return;
	}

	if (read_buff != NULL)
	{
		long            send_flag;
		uint32          hash_result;
		QueueElement   *inner_element;
		char           *strtok_r_ptr = NULL;

		/* split buff  : flag,hash*/
		char * buff_tmp = pg_strdup(read_buff);
		char * field = strtok_r(buff_tmp, thrinfo->hash_field->hash_delim, &strtok_r_ptr);
		if (field == NULL)
		{
			/* buffer error */
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %lu ] get hash restult error, can't get flag result : %s",
							(unsigned long)thrinfo->thread_id, read_buff);
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
		if (field == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %lu ] get hash restult error, can't get hash: %s",
							(unsigned long)thrinfo->thread_id, read_buff);
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
		if (inner_element == NULL)
		{
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %lu ] receive hash result : %s, but inner queue is empty",
							(unsigned long)thrinfo->thread_id, read_buff);
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
		else if (!thrinfo->redo_queue)
		{
			/*default hash or user define hash(field num = 1)*/
			if (thrinfo->hash_field->field_nums == 1)
			{
				int modulo_datanode;
				int module_queue;
				uint32 hash_value;
				int output_queue_flag;

				Assert(inner_element->lineBuffer->lineno == send_flag);

				/* calc which datanode to send */
				modulo_datanode = calc_send_datanode(hash_result, thrinfo->hash_field);
				hash_value = hash_uint32(labs(hash_result));
				module_queue = compute_hash_modulo(labs(hash_value), thrinfo->threads_num_per_datanode);
				output_queue_flag = modulo_datanode *thrinfo->threads_num_per_datanode + module_queue;

				/* put linebuf to outqueue */
				mq_pipe_put(thrinfo->output_queue[output_queue_flag], inner_element->lineBuffer);
				ADBLOADER_LOG(LOG_DEBUG,
								"[HASH][thread id : %lu ]TO output_queue num:%s ,hash_result: %u, modulo_datanode:%d, hash_value:%u, module_queue:%d, line data : %s\n",
								(unsigned long)thrinfo->thread_id,
								thrinfo->output_queue[output_queue_flag]->name,
								hash_result,
								modulo_datanode,
								hash_value,
								module_queue,
								inner_element->lineBuffer->data);
				inner_element->lineBuffer = NULL;
				pfree(inner_element);
			}
			else if (thrinfo->hash_field->field_nums > 1) /* user define hash(field num > 1)*/
			{
				int modulo_datanode;
				int module_queue;
				int output_queue_flag;
				uint32 multi_field_hash_value;
				LineBuffer *field_data = NULL;

				Assert(inner_element->lineBuffer->lineno == send_flag);

				/* calc which datanode to send */
				modulo_datanode = calc_send_datanode(hash_result, thrinfo->hash_field);

				field_data = package_field(inner_element->lineBuffer, thrinfo);
				multi_field_hash_value = get_multi_field_hash_value(field_data->data);

				module_queue = compute_hash_modulo(labs(multi_field_hash_value), thrinfo->threads_num_per_datanode);
				output_queue_flag = modulo_datanode *thrinfo->threads_num_per_datanode + module_queue;

				/* put linebuf to outqueue */
				mq_pipe_put(thrinfo->output_queue[output_queue_flag], inner_element->lineBuffer);
				ADBLOADER_LOG(LOG_DEBUG,
						"[HASH][thread id : %lu ]TO output_queue num:%s ,hash_result: %u, modulo_datanode:%d, hash_value:%u, module_queue:%d, line data : %s\n",
						(unsigned long)thrinfo->thread_id,
						thrinfo->output_queue[output_queue_flag]->name,
						hash_result,
						modulo_datanode,
						multi_field_hash_value,
						module_queue,
						inner_element->lineBuffer->data);
				inner_element->lineBuffer = NULL;
				pfree(inner_element);
				pfree(field_data);
				field_data = NULL;
			}
		}
		else if (thrinfo->redo_queue)
		{
			if (thrinfo->hash_field->field_nums == 1)
			{
				int modulo_datanode;
				int module_queue;
				uint32 hash_value;
				int output_queue_flag;

				Assert(inner_element->lineBuffer->lineno == send_flag);

				/* calc which datanode to send */
				modulo_datanode = calc_send_datanode(hash_result, thrinfo->hash_field);
				hash_value = hash_uint32(labs(hash_result));
				module_queue = compute_hash_modulo(labs(hash_value), thrinfo->threads_num_per_datanode);
				output_queue_flag = modulo_datanode *thrinfo->threads_num_per_datanode + module_queue;

				need_redo_queue = check_need_redo_queue(thrinfo->redo_queue_total,
														thrinfo->redo_queue_index,
														output_queue_flag);
				/* just redo queue*/
				if (need_redo_queue && !thrinfo->filter_queue_file)
				{
					mq_pipe_put(thrinfo->output_queue[output_queue_flag], inner_element->lineBuffer);
					ADBLOADER_LOG(LOG_DEBUG,
								"[HASH][thread id : %lu ] line data : %s, TO output_queue num:%s \n",
								(unsigned long)thrinfo->thread_id, inner_element->lineBuffer->data, thrinfo->output_queue[output_queue_flag]->name);
				}

                /* just get filter queue file */
                if (need_redo_queue && thrinfo->filter_queue_file)
                {
                    int index = 0;
                    index = get_filter_queue_file_fd_index(thrinfo->redo_queue_total,
                                                            thrinfo->redo_queue_index,
                                                            output_queue_flag);
                    if (index == -1)
                    {

                    }

                    fwrite(inner_element->lineBuffer->data, strlen(inner_element->lineBuffer->data),
                          1, filter_queue_file_fd[index]);

                }

				inner_element->lineBuffer = NULL;
				pfree(inner_element);
			}
			else if (thrinfo->hash_field->field_nums > 1)
			{
				int modulo_datanode;
				int module_queue;
				int output_queue_flag;
				uint32 multi_field_hash_value;
				LineBuffer *field_data = NULL;

				Assert(inner_element->lineBuffer->lineno == send_flag);

				/* calc which datanode to send */
				modulo_datanode = calc_send_datanode(hash_result, thrinfo->hash_field);

				field_data = package_field(inner_element->lineBuffer, thrinfo);
				multi_field_hash_value = get_multi_field_hash_value(field_data->data);

				module_queue = compute_hash_modulo(labs(multi_field_hash_value), thrinfo->threads_num_per_datanode);
				output_queue_flag = modulo_datanode *thrinfo->threads_num_per_datanode + module_queue;

				need_redo_queue = check_need_redo_queue(thrinfo->redo_queue_total,
														thrinfo->redo_queue_index,
														output_queue_flag);

				if (need_redo_queue)
				{
					/* put linebuf to outqueue */
					mq_pipe_put(thrinfo->output_queue[output_queue_flag], inner_element->lineBuffer);
					ADBLOADER_LOG(LOG_DEBUG,
									"[HASH][thread id : %lu ]TO output_queue num:%s ,hash_result: %u, modulo_datanode:%d, hash_value:%u, module_queue:%d, line data : %s\n",
									(unsigned long)thrinfo->thread_id,
									thrinfo->output_queue[output_queue_flag]->name,
									hash_result,
									modulo_datanode,
									multi_field_hash_value,
									module_queue,
									inner_element->lineBuffer->data);
				}

				inner_element->lineBuffer = NULL;
				pfree(inner_element);
				pfree(field_data);
				field_data = NULL;
			}
		}

		PQfreemem(read_buff);
		pfree(buff_tmp);
	}
}

static uint32
get_multi_field_hash_value(char *field_data)
{
	pg_crc32    crc = 0;

	INIT_CRC32(crc);
	COMP_CRC32(crc, field_data, strlen(field_data));
	FIN_CRC32(crc);

	return (uint32)crc;
}

/* this function only called by read_data() */
static void
check_restart_hash_stream(ComputeThreadInfo *thrinfo)
{
	Assert(thrinfo != NULL);

	if (RECV_END_FLAG)
	{
		/* if inner queue is not empty */
		if (!mq_empty(thrinfo->inner_queue))
		{
			restart_hash_stream(thrinfo);
		}
		else
		{
			if (thrinfo->happen_error)
				thrinfo->state = THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE;
			else
				thrinfo->state = THREAD_DEAL_COMPLETE;
			pthread_exit(thrinfo);
		}
	}
	else
	{
		if (!mq_empty(thrinfo->inner_queue))
		{
			restart_hash_stream(thrinfo);
		}
	}

	return;
}

static void
restart_hash_stream(ComputeThreadInfo *thrinfo)
{
	char *copy = NULL;
	PGresult *res = NULL;
	MessageQueue *queue = NULL;

	Assert(thrinfo->conn != NULL);

	if (PQstatus(thrinfo->conn) != CONNECTION_OK)
		reconnect(thrinfo); /* reconnect */

	if (thrinfo->copy_str != NULL)
		copy = thrinfo->copy_str;
	else
		copy = create_copy_string(thrinfo->func_name, thrinfo->hash_field->field_nums,
									thrinfo->hash_field->copy_options);

	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread id : %lu ] compute hash copy string is : %s ",
				(unsigned long)thrinfo->thread_id, copy);

	res = PQexec(thrinfo->conn, copy);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		ADBLOADER_LOG(LOG_ERROR, "[HASH]PQresultStatus is not PGRES_COPY_BOTH, thread id : %lu, thread exit",
					(unsigned long)thrinfo->thread_id);
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
		ADBLOADER_LOG(LOG_INFO, "[HASH][thread id : %lu ] thrinfo->send_seq is : %ld, now restart sequence",
					(unsigned long)thrinfo->thread_id, thrinfo->send_seq);
		thrinfo->send_seq = 0; /* restart sequence */
	}

	while (!mq_empty(thrinfo->inner_queue))
	{
		LineBuffer *hash_field;
		QueueElement *element;
		int send;
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
						"[HASH][thread id : %lu ] send copy data error : %s ",
							(unsigned long)thrinfo->thread_id, element->lineBuffer->data);

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
		char *read_buff = NULL;
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
					get_data_from_server(thrinfo->conn, read_buff, thrinfo);
				}
				result = PQgetCopyData(thrinfo->conn, &read_buff, 0);
				if (result == - 1)
				{
					ADBLOADER_LOG(LOG_INFO,
							"[HASH][thread id : %lu ] file is complete", (unsigned long)thrinfo->thread_id);

				if (thrinfo->happen_error)
					thrinfo->state = THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE;
				else
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
                if (thrinfo->state != THREAD_HAPPEN_ERROR_CONTINUE)
				    thrinfo->state = THREAD_DEAL_COMPLETE;
			}
			PQclear(res);

		}
		else
		{
			ADBLOADER_LOG(LOG_ERROR,
							"[HASH][thread id : %ld ] send copy end to server error, message : %s",
							thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
									thrinfo->state = THREAD_COPY_END_ERROR;
			hash_write_error_message(thrinfo,
									"copy end error, file need to redo",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, TRUE);
		}

		if (thrinfo->happen_error)
			thrinfo->state = THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE;
		else
			thrinfo->state = THREAD_DEAL_COMPLETE;

		pthread_exit(thrinfo);
	}

	ADBLOADER_LOG(LOG_INFO, "[HASH][thread id : %lu ] restart_hash_stream function complete, now thrinfo->send_seq is : %ld",
					(unsigned long)thrinfo->thread_id, thrinfo->send_seq);
	/* destory tmp_queue */
	mq_destory(queue);

	return;
}

static int
calc_send_datanode (uint32 hash, HashField *hash_field)
{
	int modulo;
	Assert(hash_field != NULL);
	modulo = compute_hash_modulo(labs(hash), hash_field->datanodes_num);
	return modulo;
}

static int
max_value (int * values, int size)
{
	int tmp = 0;
	int flag = 0;
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
free_buff(char **buff, int len)
{
	char *tmp = NULL;
	int   flag = 0;

	Assert(buff != NULL);

	for (flag = 0; flag < len; flag++)
	{
		tmp = buff[flag];
		if (tmp != NULL)
		{
			pfree(tmp);
			tmp = NULL;
		}
	}

	pfree(buff);
	buff = NULL;

	return;
}

static void
print_struct (HashComputeInfo * hash_computeInfo, HashField * hash_field)
{
	int flag;

	Assert(hash_computeInfo != NULL && hash_field != NULL);

	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] print HashComputeInfo");
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] tread numbers : %d, connecion info : %s,",
		hash_computeInfo->datanodes_num, hash_computeInfo->conninfo);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] funcion name : %s", hash_computeInfo->func_name);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] print HashField");
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] field num: %d ", hash_field->field_nums);

	for (flag = 0; flag < hash_field->field_nums; flag ++)
	{
		ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] field loc :%d, field type:%d",
				hash_field->field_loc[flag], hash_field->field_type[flag]);
	}

	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ]  file delim : %s, hash result delim : %s",
		hash_field->text_delim, hash_field->hash_delim);
	ADBLOADER_LOG(LOG_DEBUG, "[HASH][thread main ] node num: %d ", hash_field->datanodes_num);
	for (flag = 0; flag < hash_field->datanodes_num; flag++)
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
		if (thrinfo->conn == NULL)
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

static uint32 hash_uint32(uint32 k)
{
	register uint32 a;
	register uint32 b;
	register uint32 c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
	a += k;

	adb_load_final(a, b, c);

	/* report the result */
	return (c);
}

static bool
is_comment_line(char *line_data, char *comment_str)
{
	char pre_two_char[3] = {0};

	Assert(line_data != NULL);

	pre_two_char[0] = *line_data;
	pre_two_char[1] = *(line_data + 1);
	pre_two_char[2] = '\0';

	if (strcmp(pre_two_char, comment_str) == 0)
		return true;
	else
		return false;
}
