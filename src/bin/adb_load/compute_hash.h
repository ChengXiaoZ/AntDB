#ifndef COMPUTE_HASH_H
#define COMPUTE_HASH_H

#include "libpq-fe.h"

#include "msg_queue.h"
#include "msg_queue_pipe.h"

typedef pthread_t	Hash_ThreadID;

typedef struct Hash_Field
{
	int  *field_loc;     /* hash fields locations , begin at 1 */
	Oid  *field_type;    /* hash fields type */	
	int   field_nums;
	char *func_name;
	int   node_nums;
	Oid  *node_list;
	char *delim;        /* separator */
	char *copy_options; /* copy options */
	char quotec;        /* csv mode use */
	bool has_qoute;
	char escapec;
	bool has_escape;
	char *hash_delim;
} Hash_Field;

typedef struct Hash_ComputeInfo
{
	bool				 is_need_create;
	int					 thread_nums;
	int					 output_queue_size;
	char				*func_name;
	char				*conninfo;
	char				*start_cmd;
	MessageQueuePipe	*input_queue;	
	MessageQueuePipe   **output_queue;
	Hash_Field			*hash_field;
} Hash_ComputeInfo;

typedef enum THREAD_WORK_STATE
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
	THREAD_MESSAGE_CONFUSION_ERROR,
	THREAD_DEAL_COMPLETE,
	THREAD_EXIT_BY_OTHERS,
	THREAD_EXIT_NORMAL
} THREAD_WORK_STATE;

typedef struct Compute_ThreadInfo
{
	Hash_ThreadID 		thread_id;
	long				send_seq;
	int					output_queue_size;
	MessageQueuePipe  	*input_queue;
	MessageQueuePipe  	**output_queue;
	MessageQueue		*inner_queue;
	Hash_Field	  		*hash_field;
	char		  		*func_name;
	char 		  		*conninfo;
	char				*copy_str;
	char				*start_cmd;
	PGconn   	  		*conn;
	THREAD_WORK_STATE	state;
	bool				exit;
	void 				* (* thr_startroutine)(void *); /* thread start function */
} Compute_ThreadInfo;

typedef struct Hash_Threads
{
	int 			   hs_thread_count;
	int				   hs_thread_cur;
	Compute_ThreadInfo **hs_threads;
	pthread_mutex_t	   mutex;
} Hash_Threads;

#define	HASH_COMPUTE_ERROR		0
#define	HASH_COMPUTE_OK			1

int Init_Hash_Compute(int thread_nums, char * func, char * conninfo, MessageQueuePipe * message_queue_in,
			MessageQueuePipe ** message_queue_out, int queue_size, Hash_Field * field, char * start_cmd);

/**
* @brief Check_Compute_state
* 
* return  running thread numbers
* @return int  running thread numbers
*/
int Check_Compute_state(void);

/* caller don't need to free memory */
Hash_Threads *Get_Exit_Threads_Info(void);

int Stop_Hash(void);

/* make sure all threads had exited */
void Clean_Hash_Resource(void);
#endif
