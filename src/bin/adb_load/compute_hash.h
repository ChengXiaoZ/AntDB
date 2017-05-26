#ifndef ADB_LOAD_COMPUTE_HASH_H
#define ADB_LOAD_COMPUTE_HASH_H

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

	bool                 copy_cmd_comment;
	char                *copy_cmd_comment_str;

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

	bool                copy_cmd_comment;
	char               *copy_cmd_comment_str;

    LineBuffer        *field_data;
	HashField         *hash_field;
	char              *func_name;
	char              *conninfo;
	char              *copy_str;
	PGconn            *conn;
	ThreadWorkState    state;
	bool               exit;

	bool               happen_error;
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

extern int init_hash_compute(HashComputeInfo * hash_info);

/**
* @brief check_compute_state
*
* return  running thread numbers
* @return int  running thread numbers
*/
extern int check_compute_state(void);

/* caller don't need to free memory */
extern HashThreads *get_exit_threads_info(void);

extern int stop_hash_threads(void);

/* make sure all threads had exited */
extern void clean_hash_resource(void);
extern void fclose_filter_queue_file_fd(int fd_total);
extern void set_hash_file_start_cmd(char * start_cmd);

#endif /* ADB_LOAD_COMPUTE_HASH_H */
