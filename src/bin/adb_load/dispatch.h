#ifndef DISPATCH_H
#define DISPATCH_H

#include "libpq-fe.h"
#include "msg_queue_pipe.h"

#define DISPATCH_OK	   1
#define DISPATCH_ERROR 0

typedef pthread_t	DispatchThreadID;

typedef struct Datanode_Info
{
	Oid 	*datanode;
	int		node_nums;
	char	**conninfo;
} Datanode_Info;

typedef struct Dispatch_Info
{
	int					 thread_nums;
	char				*conninfo_agtm;
	MessageQueuePipe	**output_queue;
	Datanode_Info		*datanode_info;
	char				*table_name;
	char				*copy_options;
	char				*start_cmd;
	bool				 process_bar;
} Dispatch_Info;

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
	DISPATCH_THREAD_FIELD_ERROR,
	DISPATCH_THREAD_MESSAGE_CONFUSION_ERROR,
	DISPATCH_THREAD_KILLED_BY_OTHERTHREAD,
	DISPATCH_THREAD_EXIT_NORMAL
} DISPATCH_THREAD_WORK_STATE;

typedef enum TABLE_TYPE
{
	TABLE_REPLICATION,
	TABLE_DISTRIBUTE
} TABLE_TYPE;

typedef struct Dispatch_ThreadInfo
{
	DispatchThreadID	thread_id;
	MessageQueuePipe  	*output_queue;
	char 		  		*conninfo_datanode;
	char				*conninfo_agtm;
	char				*table_name;
	char				*copy_str;	/* copy str */
	char				*copy_options; /* copy with options */
	char				*start_cmd;
	PGconn   	  		*conn;
	PGconn				*agtm_conn;
	bool				exit;
	int					send_total;
	void 				* (* thr_startroutine)(void *); /* thread start function */
	DISPATCH_THREAD_WORK_STATE state;
} Dispatch_ThreadInfo;

typedef struct Dispatch_Threads
{
	int 			   	send_thread_count;
	int				   	send_thread_cur;
	Dispatch_ThreadInfo **send_threads;
	pthread_mutex_t	   	mutex;
} Dispatch_Threads;

int Init_Dispatch (Dispatch_Info *dispatch_info, TABLE_TYPE type);
int Stop_Dispath (void);
/* make sure all threads had exited */
void Clean_Dispatch_Resource (void);
Dispatch_Threads *Get_Dispatch_Exit_Threads (void);
void GetSendCount(int * thread_send_num);
#endif