#include "postgres_fe.h"

#include <pthread.h>
#include <unistd.h>

#include "adbloader_log.h"
#include "dispatch.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "read_write_file.h"
#include "utility.h"

static struct enum_name message_name_tab[] =
{
	{DISPATCH_THREAD_DEFAULT, "DISPATCH_THREAD_DEFAULT"},
	{DISPATCH_THREAD_MEMORY_ERROR, "DISPATCH_THREAD_MEMORY_ERROR"},
	{DISPATCH_THREAD_CONNECTION_ERROR, "DISPATCH_THREAD_CONNECTION_ERROR"},
	{DISPATCH_THREAD_CONNECTION_DATANODE_ERROR, "DISPATCH_THREAD_CONNECTION_DATANODE_ERROR"},
	{DISPATCH_THREAD_CONNECTION_AGTM_ERROR, "DISPATCH_THREAD_CONNECTION_AGTM_ERROR"},
	{DISPATCH_THREAD_SEND_ERROR, "DISPATCH_THREAD_SEND_ERROR"},
	{DISPATCH_THREAD_SELECT_ERROR, "DISPATCH_THREAD_SELECT_ERROR"},
	{DISPATCH_THREAD_COPY_STATE_ERROR, "DISPATCH_THREAD_COPY_STATE_ERROR"},
	{DISPATCH_THREAD_COPY_DATA_ERROR, "DISPATCH_THREAD_COPY_DATA_ERROR"},
	{DISPATCH_THREAD_COPY_END_ERROR, "DISPATCH_THREAD_COPY_END_ERROR"},
	{DISPATCH_THREAD_FIELD_ERROR, "DISPATCH_THREAD_FIELD_ERROR"},
	{DISPATCH_THREAD_MESSAGE_CONFUSION_ERROR, "DISPATCH_THREAD_MESSAGE_CONFUSION_ERROR"},
	{DISPATCH_THREAD_KILLED_BY_OTHERTHREAD, "DISPATCH_THREAD_KILLED_BY_OTHERTHREAD"},
	{DISPATCH_THREAD_EXIT_NORMAL, "DISPATCH_THREAD_EXIT_NORMAL"},
	{-1, NULL}
};

DispatchThreads	DispatchThreadsData;
DispatchThreads	*DispatchThreadsRun = &DispatchThreadsData;
DispatchThreads	DispatchThreadsFinishData;
DispatchThreads	*DispatchThreadsFinish = &DispatchThreadsFinishData;
TableType			Table_Type;
static  bool		Is_Deal = false;
static	bool	    process_bar = false;
static char 	  **error_message_name = NULL;
static char		   *g_start_cmd = NULL;
static int 			error_message_max;

static int		  dispatch_threadsCreate(DispatchInfo *dispatch);
static void		* dispatch_threadMain (void *argp);
static void		* dispatch_ThreadMainWrapper (void *argp);
static void		  dispatch_ThreadCleanup (void * argp);
static PGconn	* reconnect (DispatchThreadInfo *thrinfo);
static char		* create_copy_string (char *table_name, char *copy_options);
static void		  deal_after_thread_exit(void);
static void 	  build_communicate_agtm_and_datanode (DispatchThreadInfo *thrinfo);
static void		  dispatch_init_error_nametabs(void);
static char	 	* dispatch_util_message_name (DispatchThreadWorkState state);
static void		  dispatch_write_error_message(DispatchThreadInfo	*thrinfo, char * message,
										char * dispatch_error_message, int line_no, char *line_data, bool redo);

static void
dispatch_init_error_nametabs(void)
{
	int ii;

	if (error_message_name)
		pfree(error_message_name);

	for (ii = 0, error_message_max = 0; message_name_tab[ii].type >= 0; ii++)
	{
		if (error_message_max < message_name_tab[ii].type)
			error_message_max = message_name_tab[ii].type;
	}
	error_message_name = (char **)palloc0(sizeof(char *) * (error_message_max + 1));
	memset(error_message_name, 0, sizeof(char *) * (error_message_max + 1));
	for (ii = 0; message_name_tab[ii].type >= 0; ii++)
	{
		error_message_name[message_name_tab[ii].type] = message_name_tab[ii].name;
	}
}

static char *
dispatch_util_message_name(DispatchThreadWorkState state)
{
	if (error_message_name == NULL)
		dispatch_init_error_nametabs();
	if (state > error_message_max)
		return "UNKNOWN_MESSAGE";
	return error_message_name[state];
}

static void
dispatch_write_error_message(DispatchThreadInfo	*thrinfo, char * message,
			char * dispatch_error_message, int line_no, char *line_data, bool redo)
{
	LineBuffer *error_buffer = NULL;
	if (thrinfo)
		error_buffer = format_error_info(message, DISPATCH, dispatch_error_message,
									line_no, line_data);
	else
		error_buffer = format_error_info(message, DISPATCH, NULL, line_no, line_data);

	if (NULL != thrinfo)
	{
		appendLineBufInfoString(error_buffer, "more infomation : connection :");
		appendLineBufInfoString(error_buffer, thrinfo->conninfo_datanode);
		appendLineBufInfoString(error_buffer, "\n");
	}

	if (redo)
	{
		if (Table_Type == TABLE_DISTRIBUTE)
		{
			appendLineBufInfoString(error_buffer, "\n");
			appendLineBufInfoString(error_buffer, "suggest : ");
			if (thrinfo)				
				appendLineBufInfoString(error_buffer, g_start_cmd);
				
			else if (thrinfo == NULL && dispatch_error_message != NULL)
				appendLineBufInfoString(error_buffer, dispatch_error_message);
			appendLineBufInfoString(error_buffer, "\n");
		}
		else if(Table_Type == TABLE_REPLICATION)
		{
			appendLineBufInfoString(error_buffer, "\n");
			appendLineBufInfoString(error_buffer, "suggest : ");				
			appendLineBufInfoString(error_buffer, "set config file DATANODE_VALID = on, remain this datanode info \n");
			if (thrinfo)				
				appendLineBufInfoString(error_buffer, g_start_cmd);
			else if (thrinfo == NULL && dispatch_error_message != NULL)
				appendLineBufInfoString(error_buffer, dispatch_error_message);
			appendLineBufInfoString(error_buffer, "\n");
		}
	}
	else
	{
		if(line_data)
		{
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

int
InitDispatch(DispatchInfo *dispatch_info, TableType type)
{
	int		res;
	Assert(dispatch_info != NULL);
	if (dispatch_info->process_bar)
		process_bar = true;
		
	res = dispatch_threadsCreate(dispatch_info);
	Table_Type = type;
	return res;
}

int
StopDispatch (void)
{
	int flag;
	pthread_mutex_lock(&DispatchThreadsRun->mutex);
	for (flag = 0; flag < DispatchThreadsRun->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info = DispatchThreadsRun->send_threads[flag];
		if(thread_info != NULL)
			thread_info->exit = true;
	}
	pthread_mutex_unlock(&DispatchThreadsRun->mutex);
	/* wait thread exit */
	for (;;)
	{
		pthread_mutex_lock(&DispatchThreadsRun->mutex);
		if (DispatchThreadsRun->send_thread_cur == 0)
		{
			pthread_mutex_unlock(&DispatchThreadsRun->mutex);
			break;
		}
		/* sleep 5s */
		sleep(5);
	}
	return DISPATCH_OK;
}

static int
dispatch_threadsCreate(DispatchInfo  *dispatch)
{
	int flag;
	DatanodeInfo *datanode_info = dispatch->datanode_info;
	Assert(NULL != dispatch && NULL != dispatch->conninfo_agtm && NULL != dispatch->output_queue
		&& dispatch->thread_nums > 0 && NULL != dispatch->datanode_info);
	if(pthread_mutex_init(&DispatchThreadsRun->mutex, NULL) != 0 ||
		pthread_mutex_init(&DispatchThreadsFinish->mutex, NULL) != 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread main ] Can not initialize dispatch mutex: %s",
						strerror(errno));

		dispatch_write_error_message(NULL, 
									"Can not initialize dispatch mutex ,file need to redo",
									g_start_cmd, 0 , NULL, true);
		exit(1);
	}

	DispatchThreadsRun->send_thread_count = dispatch->thread_nums;
	DispatchThreadsRun->send_threads = (DispatchThreadInfo **)palloc0(sizeof(DispatchThreadInfo *) * dispatch->thread_nums);
	DispatchThreadsFinish->send_thread_count = dispatch->thread_nums;
	DispatchThreadsFinish->send_threads = (DispatchThreadInfo **)palloc0(sizeof(DispatchThreadInfo *) * dispatch->thread_nums);
	for (flag = 0; flag < dispatch->thread_nums; flag++)
	{
		DispatchThreadInfo *thrinfo = (DispatchThreadInfo*)palloc0(sizeof(DispatchThreadInfo ));
		if (NULL == thrinfo)
		{
			ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread main ] Can not malloc memory to DispatchThreadInfo");

			dispatch_write_error_message(NULL, 
										"Can not malloc memory to DispatchThreadInfo ,file need to redo",
										g_start_cmd, 0 , NULL, true);
			exit(1);
		}
		thrinfo->conninfo_datanode = pg_strdup(datanode_info->conninfo[flag]);
		thrinfo->output_queue = dispatch->output_queue[flag];
		thrinfo->table_name = pg_strdup(dispatch->table_name);
		thrinfo->conninfo_agtm = pg_strdup(dispatch->conninfo_agtm);
		if (NULL != dispatch->copy_options)
			thrinfo->copy_options = pg_strdup(dispatch->copy_options);
		else
			thrinfo->copy_options = NULL;
		thrinfo->thr_startroutine = dispatch_threadMain;
		if ((pthread_create(&thrinfo->thread_id, NULL, dispatch_ThreadMainWrapper, thrinfo)) < 0)
		{
			ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread main ] create dispatch thread error");
			dispatch_write_error_message(NULL, 
										"create dispatch thread error",
										g_start_cmd, 0 , NULL, true);
			/* stop start thread */
			StopDispatch();
			return DISPATCH_ERROR;
		}
		DispatchThreadsRun->send_thread_cur++;
		DispatchThreadsRun->send_threads[flag] = thrinfo;
		ADBLOADER_LOG(LOG_INFO, "[DISPATCH][thread main ] create dispatch thread : %ld ",thrinfo->thread_id);
	}
	return DISPATCH_OK;
}

static void *
dispatch_threadMain (void *argp)
{	
	DispatchThreadInfo	*thrinfo = (DispatchThreadInfo*) argp;
	MessageQueuePipe	*output_queue;
	char 				*read_buff = NULL;
	LineBuffer 			*lineBuffer = NULL;
	PGresult 	 		*res;

	bool 				 mq_read = false;
	bool 				 conn_read = false;
	bool 				 conn_write = false;	
	fd_set 				 read_fds;
	fd_set 				 write_fds;
	int 				 max_fd;
	int 				 select_res;

	Assert(NULL != thrinfo->conninfo_agtm && NULL != thrinfo->conninfo_datanode);
	output_queue = thrinfo->output_queue;

	build_communicate_agtm_and_datanode(thrinfo);

	ADBLOADER_LOG(LOG_DEBUG,
		"[DISPATCH][thread id : %ld ] out queue read fd : %d, server connection fd : %d",
		thrinfo->thread_id, output_queue->fd[0], thrinfo->conn->sock);

	for (;;)
	{
		if (thrinfo->exit)
		{
			thrinfo->state = DISPATCH_THREAD_KILLED_BY_OTHERTHREAD;
			dispatch_write_error_message(thrinfo, 
									"thread killed by others ,file need to redo",
									NULL, 0, NULL, true);
			pthread_exit(thrinfo);
		}

		mq_read 	= false;
		conn_read 	= false;
		conn_write	= false;
		read_buff	= NULL;
		lineBuffer 	= NULL;

		FD_ZERO(&read_fds);	
		FD_SET(thrinfo->conn->sock, &read_fds);
		FD_SET(output_queue->fd[0], &read_fds);
		FD_ZERO(&write_fds);
		FD_SET(thrinfo->conn->sock, &write_fds);

		if (output_queue->fd[0] > thrinfo->conn->sock)
			max_fd = output_queue->fd[0] + 1;
		else
			max_fd = thrinfo->conn->sock + 1;

		ADBLOADER_LOG(LOG_DEBUG, "[DISPATCH][thread id : %ld ] dispatch max fd : %d",
			thrinfo->thread_id, max_fd);
		select_res = select(max_fd, &read_fds, &write_fds, NULL, NULL);
		if (select_res > 0)
		{
			if(FD_ISSET(thrinfo->conn->sock, &read_fds))
			{
				conn_read = true;
				res = PQgetResult(thrinfo->conn);
				if (PQresultStatus(res) == PGRES_COPY_IN)
				{
					int result;
					result = PQgetCopyData(thrinfo->conn, &read_buff, 0);
					if (result == -2)
					{
						if (PQputCopyEnd(thrinfo->conn, NULL) == 1)
						{
							res = PQgetResult(thrinfo->conn);
							Assert(PQresultStatus(res) != PGRES_COMMAND_OK);							
							ADBLOADER_LOG(LOG_ERROR,
										"[DISPATCH][thread id : %ld ] copy end error, may be copy data error: %s",
										thrinfo->thread_id, PQerrorMessage(thrinfo->conn));

							dispatch_write_error_message(thrinfo, 
														"thread receive error message form ADB ,file need to redo",
														PQerrorMessage(thrinfo->conn), 0 , NULL, true);
							thrinfo->state = DISPATCH_THREAD_COPY_DATA_ERROR;
						}
					}
				}
				PQfinish(thrinfo->conn);
				thrinfo->conn = NULL;
				pthread_exit(thrinfo);
			}
			else
				conn_read = false;

			if (FD_ISSET(thrinfo->conn->sock,&write_fds) > 0)
			{
				conn_write = true;
				if (FD_ISSET(output_queue->fd[0], &read_fds))
				{
					mq_read = true;
					lineBuffer = mq_pipe_poll(output_queue);
					if (NULL != lineBuffer)
					{
						int send;
						/* send data to ADB*/
						send = PQputCopyData(thrinfo->conn, lineBuffer->data, lineBuffer->len);
						if (process_bar)
							thrinfo->send_total++;

						if (send < 0)
						{
							ADBLOADER_LOG(LOG_ERROR,
							"[DISPATCH][thread id : %ld ] send copy data error : %s ",
								thrinfo->thread_id, lineBuffer->data);
							/* check connection state */
							if (PQstatus(thrinfo->conn) != CONNECTION_OK)
							{
								/* reconnect */
								reconnect(thrinfo);
								send = PQputCopyData(thrinfo->conn, lineBuffer->data, lineBuffer->len);
								if (send < 0)
								{
									ADBLOADER_LOG(LOG_ERROR,
												"[DISPATCH][thread id : %ld ] send copy data error again : %s ",
												thrinfo->thread_id, lineBuffer->data);
									PQfinish(thrinfo->conn);
									thrinfo->conn = NULL;
									thrinfo->state = DISPATCH_THREAD_SEND_ERROR;

									dispatch_write_error_message(thrinfo, 
																"PQputCopyData error ,file need to redo",
																PQerrorMessage(thrinfo->conn), lineBuffer->fileline,
																lineBuffer->data, true);
									pthread_exit(thrinfo);
								}
							}

						}
						/* flush data every time to get error data quickly */
						PQflush(thrinfo->conn);
					}
					else
					{						
						ADBLOADER_LOG(LOG_DEBUG,
							"[DISPATCH][thread id : %ld ] dispatch file is complete, exit thread",
							thrinfo->thread_id);
						if (PQputCopyEnd(thrinfo->conn, NULL) == 1)
						{
							res = PQgetResult(thrinfo->conn);
							if (PQresultStatus(res) == PGRES_COMMAND_OK)
							{
								ADBLOADER_LOG(LOG_INFO,
											"[DISPATCH][thread id : %ld ] copy end ok", thrinfo->thread_id);
								thrinfo->state = DISPATCH_THREAD_EXIT_NORMAL;
							}
							else
							{
								ADBLOADER_LOG(LOG_ERROR,
										"[DISPATCH][thread id : %ld ] send copy end sucess, get some error message : %s",
										thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
								thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
							}
						}
						else
						{
							ADBLOADER_LOG(LOG_ERROR,
											"[DISPATCH][thread id : %ld ] send copy end error", thrinfo->thread_id);
							thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;						
						}
						if (thrinfo->state == DISPATCH_THREAD_COPY_END_ERROR)
						{
							dispatch_write_error_message(thrinfo, 
														"thread send copy end error ,file need to redo",
														PQerrorMessage(thrinfo->conn), 0,
														NULL, true);
						}
						PQfinish(thrinfo->conn);
						thrinfo->conn = NULL;
						pthread_exit(thrinfo);
					}
				}
				else
				{
					mq_read = false;
					ADBLOADER_LOG(LOG_DEBUG,
								 "[DISPATCH][thread id : %ld] output queue can not read now.\n", thrinfo->thread_id);
				}

			}
			else
			{
				conn_write = false;
				ADBLOADER_LOG(LOG_DEBUG,
							 "[DISPATCH][thread id : %ld] Adb cluster processing data is relatively slow.\n", thrinfo->thread_id);
			}


			if (conn_write && !mq_read && !conn_read)
			{
				FD_ZERO(&read_fds);
				FD_SET(thrinfo->conn->sock, &read_fds);
				FD_SET(output_queue->fd[0], &read_fds);
				select_res = select(max_fd, &read_fds, NULL, NULL, NULL);
				if (select_res > 0)
					continue;
				else
				{
					/* exception handling */
					if (errno != EINTR && errno != EWOULDBLOCK)
					{
						ADBLOADER_LOG(LOG_ERROR,
								"[DISPATCH][thread id : %ld ] select connect server socket return < 0, network may be not well, exit thread",
									thrinfo->thread_id);

						thrinfo->state = DISPATCH_THREAD_SELECT_ERROR;

						dispatch_write_error_message(thrinfo, 
													"select connect server socket return < 0, network may be not well ,file need to redo",
													NULL, 0, NULL, true);
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
								"[DISPATCH][thread id : %ld ] select connect server socket return < 0, network may be not well, exit thread",
									thrinfo->thread_id);
						thrinfo->state = DISPATCH_THREAD_SELECT_ERROR;

						dispatch_write_error_message(thrinfo, 
													"select connect server socket return < 0, network may be not well ,file need to redo",
													NULL, 0, NULL, true);
						pthread_exit(thrinfo);
					}
				}
			}			
			
		}
		else
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				/* exception handling */
					ADBLOADER_LOG(LOG_ERROR,
							"[DISPATCH][thread id : %ld ] select return value < 0",
								thrinfo->thread_id);
					thrinfo->state = DISPATCH_THREAD_SELECT_ERROR;

					dispatch_write_error_message(thrinfo, 
												"select connect server socket return < 0, network may be not well ,file need to redo",
												NULL, 0, NULL, true);
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
	return thrinfo;
}

static void 
build_communicate_agtm_and_datanode (DispatchThreadInfo *thrinfo)
{
	char		*agtm_port;
	PGresult 	*res;
	char		*copy = NULL;
	Assert(NULL != thrinfo->conninfo_agtm && NULL != thrinfo->conninfo_datanode);

	thrinfo->conn = PQconnectdb(thrinfo->conninfo_datanode);

	if (NULL == thrinfo->conn)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] memory allocation failed", thrinfo->thread_id);
		thrinfo->state = DISPATCH_THREAD_CONNECTION_DATANODE_ERROR;

		dispatch_write_error_message(thrinfo, 
									"memory allocation failed,file need to redo",
									NULL, 0, NULL, true);
		pthread_exit(thrinfo);
	}

	if (PQstatus(thrinfo->conn) != CONNECTION_OK)
	{
		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH] Connection to database failed: %s, thread id is : %ld",
					PQerrorMessage(thrinfo->conn), thrinfo->thread_id);
		thrinfo->state = DISPATCH_THREAD_CONNECTION_DATANODE_ERROR;

		dispatch_write_error_message(thrinfo, 
									"Connection to database failed,file need to redo",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, true);

		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;
		pthread_exit(thrinfo);
	}

	thrinfo->agtm_conn = PQconnectdb(thrinfo->conninfo_agtm );
	if (NULL == thrinfo->agtm_conn)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] memory allocation failed", thrinfo->thread_id);
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;

		dispatch_write_error_message(thrinfo, 
									"memory allocation failed,file need to redo",
									NULL, 0, NULL, true);
		pthread_exit(thrinfo);
	}

	if (PQstatus((PGconn*)thrinfo->agtm_conn) != CONNECTION_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] connect to agtm error, error message :%s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->agtm_conn));
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;

		dispatch_write_error_message(thrinfo, 
									"connect to agtm error,file need to redo",
									PQerrorMessage((PGconn*)thrinfo->agtm_conn), 0,
									NULL, true);

		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;
		pthread_exit(thrinfo);
	}
	else
		agtm_port = (char*)PQparameterStatus(thrinfo->agtm_conn, "agtm_port");

	Assert(NULL != thrinfo->conn);
	if (pqSendAgtmListenPort(thrinfo->conn, atoi(agtm_port)) < 0)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] could not send agtm port: %s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->conn));		

		dispatch_write_error_message(thrinfo, 
									"could not send agtm port,file need to redo",
									PQerrorMessage((PGconn*)thrinfo->agtm_conn), 0,
									NULL, true);

		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;
		pthread_exit(thrinfo);
	}

	copy = create_copy_string(thrinfo->table_name, thrinfo->copy_options);
	ADBLOADER_LOG(LOG_DEBUG, "[DISPATCH][thread id : %ld ] dispatch copy string is : %s ",
				thrinfo->thread_id, copy);

	res = PQexec(thrinfo->conn, copy);
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread id : %ld ] PQresultStatus is not PGRES_COPY_IN, error message :%s, thread exit",
					thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
		thrinfo->state = DISPATCH_THREAD_COPY_STATE_ERROR;

		dispatch_write_error_message(thrinfo, 
									"PQresultStatus is not PGRES_COPY_IN,file need to redo",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, true);
		pthread_exit(thrinfo);
	}

	thrinfo->copy_str = copy;
}

static void *
dispatch_ThreadMainWrapper (void *argp)
{
	DispatchThreadInfo  *thrinfo = (DispatchThreadInfo*) argp;
	pthread_detach(thrinfo->thread_id);
	pthread_cleanup_push(dispatch_ThreadCleanup, thrinfo);
	thrinfo->thr_startroutine(thrinfo);
	pthread_cleanup_pop(1);

	return thrinfo;
}

static void 
dispatch_ThreadCleanup (void * argp)
{
	int					 flag;
	int					 current_run_thread = 0;
	int					 current_exit_thread = 0;
	DispatchThreadInfo  *thrinfo = (DispatchThreadInfo*) argp;

	pthread_mutex_lock(&DispatchThreadsRun->mutex);
	for (flag = 0; flag < DispatchThreadsRun->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info = DispatchThreadsRun->send_threads[flag];
		if(thread_info == thrinfo)
		{
			DispatchThreadsRun->send_threads[flag] = NULL;
			DispatchThreadsRun->send_thread_cur--;
			current_run_thread = DispatchThreadsRun->send_thread_cur;
			break;
		}
	}
	pthread_mutex_unlock(&DispatchThreadsRun->mutex);

	if (thrinfo->state != DISPATCH_THREAD_EXIT_NORMAL)
	{
		ADBLOADER_LOG(LOG_INFO,
							"[DISPATCH][thread id : %ld ] current exit unnormal", thrinfo->thread_id);
		deal_after_thread_exit();
	}

	/* close socket */
	if (thrinfo->conn)
	{
		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;
	}

	if (thrinfo->agtm_conn)
	{
		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;
	}

	if (thrinfo->conninfo_datanode)
	{
		pfree(thrinfo->conninfo_datanode);
		thrinfo->conninfo_datanode = NULL;
	}

	if (thrinfo->conninfo_agtm)
	{
		pfree(thrinfo->conninfo_agtm);
		thrinfo->conninfo_agtm = NULL;
	}

	if (thrinfo->table_name)
	{
		pfree(thrinfo->table_name);
		thrinfo->table_name = NULL;
	}

	if (thrinfo->copy_str)
	{
		pfree(thrinfo->copy_str);
		thrinfo->copy_str = NULL;
	}

	if (thrinfo->copy_options)
	{
		pfree(thrinfo->copy_options);
		thrinfo->copy_options = NULL;
	}

	ADBLOADER_LOG(LOG_INFO,
	"[DISPATCH][thread id : %ld ] thread exit, total threads is : %d, current thread number: %d, current exit thread number: %d, state :%s",
	thrinfo->thread_id, DispatchThreadsRun->send_thread_count, current_run_thread,
	current_exit_thread, dispatch_util_message_name(thrinfo->state));

	pthread_mutex_lock(&DispatchThreadsFinish->mutex);
	DispatchThreadsFinish->send_threads[flag] = thrinfo;
	DispatchThreadsFinish->send_thread_cur++;
	current_exit_thread = DispatchThreadsFinish->send_thread_cur;
	pthread_mutex_unlock(&DispatchThreadsFinish->mutex);
	return;
}

static PGconn *
reconnect(DispatchThreadInfo *thrinfo)
{	
	int times = 3;
	Assert(thrinfo->conn != NULL && thrinfo->conninfo_datanode != NULL);
	ADBLOADER_LOG(LOG_INFO, "[DISPATCH][thread id : %ld ] connction begin reconnect",
				thrinfo->thread_id);
	PQfinish(thrinfo->conn);
	while (times > 0)
	{
		thrinfo->conn = PQconnectdb(thrinfo->conninfo_datanode);
		/* memory allocation failed */
		if (NULL == thrinfo->conn)
		{
			ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread id : %ld ] memory allocation failed",
				thrinfo->thread_id);

			dispatch_write_error_message(thrinfo, 
										"memory allocation failed,file need to redo",
										NULL, 0,
										NULL, true);
			pthread_exit(thrinfo);
		}
		if (PQstatus(thrinfo->conn) != CONNECTION_OK)
		{
			ADBLOADER_LOG(LOG_WARN, "[DISPATCH][thread id : %ld ] connect error : %s, connect string : %s ",
				thrinfo->thread_id, PQerrorMessage(thrinfo->conn), thrinfo->conninfo_datanode);
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
		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread id : %ld ] connect error : %s, connect string : %s ",
				thrinfo->thread_id, PQerrorMessage(thrinfo->conn), thrinfo->conninfo_datanode);
		thrinfo->conn = NULL;

		dispatch_write_error_message(thrinfo, 
									"connect error,file need to redo",
									NULL, 0,
									NULL, true);
		pthread_exit(thrinfo);
	}
	return thrinfo->conn;
}

static char *
create_copy_string (char *table_name, char *copy_options)
{
	LineBuffer	*buf;
	char		*result;	
	Assert(NULL != table_name);
	buf = get_linebuf();
	appendLineBufInfo(buf, "%s", "COPY  ");
	appendLineBufInfo(buf, "%s", table_name);
	appendLineBufInfo(buf, "%s", " FROM STDIN ");
	if (NULL != copy_options)
		appendLineBufInfo(buf, "%s", copy_options);
	result = (char*)palloc0(buf->len + 1);
	memcpy(result, buf->data, buf->len);
	result[buf->len] = '\0';
	release_linebuf(buf);
	return result;
}

static void
deal_after_thread_exit(void)
{
	if (!Is_Deal)
	{
		int flag;
		if (Table_Type == TABLE_DISTRIBUTE)
		{
			pthread_mutex_lock(&DispatchThreadsRun->mutex);
			for (flag = 0; flag < DispatchThreadsRun->send_thread_count; flag++)
			{
				DispatchThreadInfo *thread_info = DispatchThreadsRun->send_threads[flag];
				if (NULL != thread_info)
				{
					ADBLOADER_LOG(LOG_INFO,
							"[DISPATCH][thread id : %ld ] table type is distribute, current exit unnormal,kill other threads",
							thread_info->thread_id);
					thread_info->exit = true;					
					thread_info->state = DISPATCH_THREAD_KILLED_BY_OTHERTHREAD;
				}
			}
			pthread_mutex_unlock(&DispatchThreadsRun->mutex);
			Is_Deal = true;
		}		
	}
}

void 
CleanDispatchResource(void)
{
	int flag;
	Assert(DispatchThreadsRun->send_thread_cur == 0);
	if (NULL != DispatchThreadsRun->send_threads)
		pfree(DispatchThreadsRun->send_threads);

	for (flag = 0; flag < DispatchThreadsFinish->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info  = DispatchThreadsFinish->send_threads[flag];
		if (NULL != thread_info)
		{
			pfree(thread_info);
			thread_info = NULL;
			DispatchThreadsFinish->send_threads[flag] = NULL;
		}
	}
	pfree(DispatchThreadsFinish->send_threads);	

	DispatchThreadsRun->send_threads = NULL;
	DispatchThreadsRun->send_thread_count = 0;
	DispatchThreadsRun->send_thread_cur = 0;
	pthread_mutex_destroy(&DispatchThreadsRun->mutex);

	DispatchThreadsFinish->send_threads = NULL;
	DispatchThreadsFinish->send_thread_count = 0;
	DispatchThreadsFinish->send_thread_cur = 0;
	pthread_mutex_destroy(&DispatchThreadsFinish->mutex);

	if (process_bar)
		process_bar = false;

	if (g_start_cmd)
		pfree(g_start_cmd);
	g_start_cmd = NULL;

	Is_Deal = false;
}

DispatchThreads *
GetDispatchExitThreads(void)
{
	return DispatchThreadsFinish;
}

void 
GetSendCount(int * thread_send_num)
{
	int flag;

	pthread_mutex_lock(&DispatchThreadsRun->mutex);
	for (flag = 0; flag < DispatchThreadsRun->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info = DispatchThreadsRun->send_threads[flag];
		if (NULL != thread_info)
		{
			thread_send_num[flag] = thread_info->send_total;
		}
	}
	pthread_mutex_unlock(&DispatchThreadsRun->mutex);

	pthread_mutex_lock(&DispatchThreadsFinish->mutex);
	for (flag = 0; flag < DispatchThreadsFinish->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info = DispatchThreadsFinish->send_threads[flag];
		if (NULL != thread_info)
		{
			Assert(thread_send_num[flag] == 0);
			thread_send_num[flag] = thread_info->send_total;
		}
	}
	pthread_mutex_unlock(&DispatchThreadsFinish->mutex);
}

void
SetDispatchFileStartCmd(char * start_cmd)
{
	Assert(NULL != start_cmd);
	if (g_start_cmd)
		pfree(g_start_cmd);
	g_start_cmd = NULL;
	g_start_cmd = pg_strdup(start_cmd);
}

