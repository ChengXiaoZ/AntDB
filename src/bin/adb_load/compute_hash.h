#ifndef COMPUTE_HASH_H
#define COMPUTE_HASH_H

#include "libpq-fe.h"

#include "msg_queue.h"
#include "msg_queue_pipe.h"

typedef pthread_t HashThreadID;

typedef struct HashField
{
	int  *field_loc;     /* hash fields locations , begin at 1 */
	Oid  *field_type;    /* hash fields type */	
	int   field_nums;
	char *func_name;
	int   datanodes_num;
	int   hash_threads_num;
	Oid  *node_list;
	char *text_delim;    /* separator */
	char *hash_delim;
	char *copy_options;  /* copy options */
	char  quotec;        /* csv mode use */
	bool  has_qoute;
	char  escapec;
	bool  has_escape;
} HashField;

typedef struct HashComputeInfo
{
	bool                 is_need_create;
	int                  hash_threads_num;
	int                  datanodes_num;
	int                  threads_num_per_datanode;
	char                *func_name;
	char				*table_name;
	char                *conninfo;
	MessageQueuePipe    *input_queue;
	MessageQueuePipe   **output_queue;
	int                  output_queue_num;
    
	int                 *redo_queue_index;
	int                  redo_queue_total;
	bool                 redo_queue;

    bool                 filter_queue_file;
	char				*filter_queue_file_path;

	HashField           *hash_field;
} HashComputeInfo;

typedef enum ThreadWorkState
{
	THREAD_DEFAULT,
	THREAD_MEMORY_ERROR,
	THREAD_CONNECTION_ERROR,
	THREAD_SEND_ERROR,
	THREAD_SELECT_ERROR,
	THREAD_COPY_STATE_ERROR,
	THREAD_COPY_END_ERROR,
	THREAD_GET_COPY_DATA_ERROR,
	THREAD_RESTART_COPY_STREAM,
	THREAD_FIELD_ERROR,
	THREAD_PGRES_FATAL_ERROR,
	THREAD_MESSAGE_CONFUSION_ERROR,
	THREAD_DEAL_COMPLETE,
	THREAD_EXIT_BY_OTHERS,
	THREAD_EXIT_NORMAL,
	THREAD_HAPPEN_ERROR_CONTINUE,
	THREAD_HAPPEN_ERROR_CONTINUE_AND_DEAL_COMPLETE
} ThreadWorkState;

typedef struct ComputeThreadInfo
{
	HashThreadID      thread_id;
	long              send_seq;
	MessageQueuePipe *input_queue;
	MessageQueue     *inner_queue;
	MessageQueuePipe **output_queue;
	int                output_queue_num;
	int                datanodes_num;
	int                threads_num_per_datanode;

	int               *redo_queue_index;
	int                redo_queue_total;
	bool               redo_queue;

    bool               filter_queue_file;

    LineBuffer        *field_data;
	HashField         *hash_field;
	char              *func_name;
	char              *conninfo;
	char              *copy_str;
	PGconn            *conn;
	ThreadWorkState    state;
	bool               exit;
	void              *(* thr_startroutine)(void *); /* thread start function */
} ComputeThreadInfo;

typedef struct HashThreads
{
	int                hs_thread_count;
	int                hs_thread_cur;
	ComputeThreadInfo **hs_threads;
	pthread_mutex_t     mutex;
} HashThreads;

#define HASH_COMPUTE_ERROR     0
#define HASH_COMPUTE_OK        1

#if 0
int init_hash_compute(int datanodes_num,
					int threads_num_per_datanode,
					char * func_name,
					char * conninfo,
					MessageQueuePipe * input_queue,
					MessageQueuePipe ** output_queue,
					int output_queue_num,
					HashField * field,
					bool redo_queue,
					int redo_queue_total,
					int *redo_queue_index,
					bool filter_queue_file,
					char *table_name,
					char *filter_queue_file_path);
#endif

int init_hash_compute(HashComputeInfo * hash_info);

/**
* @brief CheckComputeState
* 
* return  running thread numbers
* @return int  running thread numbers
*/
int CheckComputeState(void);

/* caller don't need to free memory */
HashThreads *GetExitThreadsInfo(void);

int StopHash(void);

/* make sure all threads had exited */
void CleanHashResource(void);


void fclose_filter_queue_file_fd(int fd_total);

void SetHashFileStartCmd(char * start_cmd);
#endif
