#ifndef DISPATCH_H
#define DISPATCH_H

#include "libpq-fe.h"
#include "msg_queue_pipe.h"

#define DISPATCH_OK    1
#define DISPATCH_ERROR 0

typedef pthread_t DispatchThreadID;

typedef struct DatanodeInfo
{
	Oid    *datanode;
	int     node_nums;
	char  **conninfo;
} DatanodeInfo;

typedef struct DispatchInfo
{
	int                datanodes_num;
	int                threads_num_per_datanode;
	char              *conninfo_agtm;
	MessageQueuePipe **output_queue;
	DatanodeInfo      *datanode_info;
	char              *table_name;
	char              *copy_options;
	bool               process_bar;
	bool               just_check;
} DispatchInfo;

typedef enum DISPATCH_THREAD_WORK_STATE
{
	DISPATCH_THREAD_DEFAULT,
	DISPATCH_THREAD_MEMORY_ERROR,
	DISPATCH_THREAD_CONNECTION_ERROR,
	DISPATCH_THREAD_CONNECTION_DATANODE_ERROR,
	DISPATCH_THREAD_CONNECTION_AGTM_ERROR,
	DISPATCH_THREAD_SEND_ERROR,
	DISPATCH_THREAD_SELECT_ERROR,
	DISPATCH_THREAD_COPY_STATE_ERROR,
	DISPATCH_THREAD_COPY_DATA_ERROR,
	DISPATCH_THREAD_COPY_END_ERROR,
	DISPATCH_GET_BACKEND_FATAL_ERROR,
	DISPATCH_THREAD_FIELD_ERROR,
	DISPATCH_THREAD_MESSAGE_CONFUSION_ERROR,
	DISPATCH_THREAD_KILLED_BY_OTHERTHREAD,
	DISPATCH_THREAD_EXIT_NORMAL
} DispatchThreadWorkState;

typedef enum TABLE_TYPE
{
	TABLE_REPLICATION,
	TABLE_DISTRIBUTE
} TableType;

typedef struct DispatchThreadInfo
{
	DispatchThreadID    thread_id;
	MessageQueuePipe    *output_queue;
	char                *conninfo_datanode;
	char                *conninfo_agtm;
	char                *table_name;
	char                *copy_str; /* copy str */
	char                *copy_options; /* copy with options */
	PGconn              *conn;
	PGconn              *agtm_conn;
	bool                exit;
	bool                need_redo;
	bool                need_rollback;
	bool                just_check;
	int                 send_total;
	void               *(* thr_startroutine)(void *); /* thread start function */
	DispatchThreadWorkState state;
} DispatchThreadInfo;

typedef struct DispatchThreads
{
	int                  send_thread_count;
	int                  send_thread_cur;
	DispatchThreadInfo **send_threads;
	pthread_mutex_t      mutex;
} DispatchThreads;

int init_dispatch_threads (DispatchInfo *dispatch_info, TableType type);
int stop_dispatch (void);
/* make sure all threads had exited */
void CleanDispatchResource (void);
DispatchThreads *get_dispatch_exit_threads (void);
void GetSendCount(int * thread_send_num);
void SetDispatchFileStartCmd(char * start_cmd);

#endif