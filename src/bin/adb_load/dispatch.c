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

DispatchThreads  DispatchThreadsData;
DispatchThreads *DispatchThreadsRun = &DispatchThreadsData;
DispatchThreads  DispatchThreadsFinishData;
DispatchThreads *DispatchThreadsFinish = &DispatchThreadsFinishData;
TableType    Table_Type;

static bool Is_Deal = false;
static bool process_bar = false;
static char **error_message_name = NULL;
static char *g_start_cmd = NULL;
static int error_message_max;
static char *get_linevalue_from_PQerrormsg(char *pqerrormsg);
static bool rollback_in_PQerrormsg(char *PQerrormsg);
static int dispatch_threadsCreate(DispatchInfo *dispatch);
static void *dispatch_threadMain (void *argp);
static void *dispatch_ThreadMainWrapper (void *argp);
static void dispatch_ThreadCleanup (void * argp);
static char *create_copy_string (char *table_name,
								char *copy_options,
								bool copy_cmd_comment,
								char *copy_cmd_comment_str);
//static void deal_after_thread_exit(void);
//static void build_communicate_agtm_and_datanode (DispatchThreadInfo *thrinfo);
static void dispatch_init_error_nametabs(void);
static char *dispatch_util_message_name (DispatchThreadWorkState state);
static void dispatch_write_error_message(DispatchThreadInfo	*thrinfo, char * message,
										char * dispatch_error_message, int line_no, char *line_data, bool redo);

static bool datanode_can_write(int fd, fd_set *set);
static bool datanode_can_read(int fd, fd_set *set);
static bool output_queue_can_read(int fd, fd_set *set);
static void put_data_to_datanode(DispatchThreadInfo *thrinfo, LineBuffer *lineBuffer, MessageQueuePipe *output_queue);
static void put_copy_end_to_datanode(DispatchThreadInfo *thrinfo);
static int  get_data_from_datanode(DispatchThreadInfo *thrinfo);
static void reconnect_agtm_and_datanode(DispatchThreadInfo *thrinfo);
static void connect_agtm_and_datanode(DispatchThreadInfo *thrinfo);
static bool connect_datanode(DispatchThreadInfo *thrinfo);
static bool connect_agtm(DispatchThreadInfo *thrinfo);
static void PQfinish_agtm_and_datanode(DispatchThreadInfo *thrinfo);
static void put_end_to_datanode(DispatchThreadInfo *thrinfo);
static void put_rollback_to_datanode(DispatchThreadInfo *thrinfo);

#define FOR_GET_DATA_FROM_OUTPUT_QUEUE()     \
for (;;)                                     \
{                                            \
    lineBuffer = mq_pipe_poll(output_queue); \
    if (lineBuffer == NULL)                  \
    {                                        \
        thrinfo->need_redo = true;           \
        pthread_exit(thrinfo);               \
    }                                        \
    else                                     \
    {                                        \
        release_linebuf(lineBuffer);         \
        continue;                            \
    }                                        \
}

static void
dispatch_init_error_nametabs(void)
{
	int i = 0;

	if (error_message_name)
		pfree(error_message_name);

	for (i = 0, error_message_max = 0; message_name_tab[i].type >= 0; i++)
	{
		if (error_message_max < message_name_tab[i].type)
			error_message_max = message_name_tab[i].type;
	}
	error_message_name = (char **)palloc0(sizeof(char *) * (error_message_max + 1));
	memset(error_message_name, 0, sizeof(char *) * (error_message_max + 1));
	for (i = 0; message_name_tab[i].type >= 0; i++)
	{
		error_message_name[message_name_tab[i].type] = message_name_tab[i].name;
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

	if (thrinfo != NULL)
	{
		appendLineBufInfoString(error_buffer, "connection infomation :");
		appendLineBufInfoString(error_buffer, thrinfo->conninfo_datanode);
		appendLineBufInfoString(error_buffer, "\n");
	}

	if (redo)
	{
#if 0
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
#endif
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
init_dispatch_threads(DispatchInfo *dispatch_info, TableType type)
{
	int res;

	Assert(dispatch_info != NULL);

	if (dispatch_info->process_bar)
		process_bar = true;

	Table_Type = type;
	res = dispatch_threadsCreate(dispatch_info);

	return res;
}

int
stop_dispatch_threads(void)
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

#if 0
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
#endif

	return DISPATCH_OK;
}

static int
dispatch_threadsCreate(DispatchInfo  *dispatch)
{
	int i = 0;
	int j = 0;
	int dispatch_threads_total = 0;
	DatanodeInfo *datanode_info = dispatch->datanode_info;

	Assert(dispatch != NULL);
	Assert(dispatch->conninfo_agtm != NULL);
	Assert(dispatch->output_queue != NULL);
	Assert(dispatch->datanodes_num > 0);
	Assert(dispatch->datanode_info != NULL);

	if (pthread_mutex_init(&DispatchThreadsRun->mutex, NULL) != 0 ||
		pthread_mutex_init(&DispatchThreadsFinish->mutex, NULL) != 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread main ] Can not initialize dispatch mutex: %s",
						strerror(errno));

		dispatch_write_error_message(NULL,
									"Can not initialize dispatch mutex",
									g_start_cmd, 0 , NULL, true);
		exit(1);
	}

	dispatch_threads_total = dispatch->datanodes_num * dispatch->threads_num_per_datanode;
	DispatchThreadsRun->send_thread_count = dispatch_threads_total;
	DispatchThreadsRun->send_threads = (DispatchThreadInfo **)palloc0(sizeof(DispatchThreadInfo *) * dispatch_threads_total);
	DispatchThreadsFinish->send_thread_count = dispatch_threads_total;
	DispatchThreadsFinish->send_threads = (DispatchThreadInfo **)palloc0(sizeof(DispatchThreadInfo *) * dispatch_threads_total);

	for (i = 0; i < dispatch->datanodes_num; i++)
	{
		for (j = 0; j < dispatch->threads_num_per_datanode; j++)
		{
			DispatchThreadInfo *thrinfo = (DispatchThreadInfo *)palloc0(sizeof(DispatchThreadInfo));
			if (thrinfo == NULL)
			{
				ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread main ] Can not malloc memory to DispatchThreadInfo");

				dispatch_write_error_message(NULL,
											"Can not malloc memory to DispatchThreadInfo",
											g_start_cmd, 0 , NULL, true);
				exit(EXIT_FAILURE);
			}

			thrinfo->conninfo_datanode = pg_strdup(datanode_info->conninfo[i]);
			thrinfo->table_name = pg_strdup(dispatch->table_name);
			thrinfo->conninfo_agtm = pg_strdup(dispatch->conninfo_agtm);
			thrinfo->just_check = dispatch->just_check;
			thrinfo->copy_cmd_comment = dispatch->copy_cmd_comment;
			thrinfo->copy_cmd_comment_str = pg_strdup(dispatch->copy_cmd_comment_str);

			if (dispatch->copy_options != NULL)
			{
				thrinfo->copy_options = pg_strdup(dispatch->copy_options);
			}
			else
			{
				thrinfo->copy_options = NULL;
			}

			thrinfo->thr_startroutine = dispatch_threadMain;

			thrinfo->output_queue = dispatch->output_queue[i * dispatch->threads_num_per_datanode + j];
			if ((pthread_create(&thrinfo->thread_id, NULL, dispatch_ThreadMainWrapper, thrinfo)) < 0)
			{
				ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread main ] create dispatch thread error");
				dispatch_write_error_message(NULL,
											"create dispatch thread error",
											g_start_cmd, 0 , NULL, true);
				/* stop start thread */
				stop_dispatch_threads();

				return DISPATCH_ERROR;
			}

			DispatchThreadsRun->send_thread_cur++;
			DispatchThreadsRun->send_threads[i * dispatch->threads_num_per_datanode + j] = thrinfo;
			ADBLOADER_LOG(LOG_INFO, "[DISPATCH][thread main ] create dispatch thread : %ld ", thrinfo->thread_id);
		}
	}

	return DISPATCH_OK;
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

static bool
datanode_can_write(int fd, fd_set *set)
{
	int res = 0;
	res = FD_ISSET(fd, set);
	return (res > 0 ? true : false);
}

static bool
datanode_can_read(int fd, fd_set *set)
{
	int res = 0;
	res = FD_ISSET(fd, set);
	return (res > 0 ? true : false);
}

static bool
output_queue_can_read(int fd, fd_set *set)
{
	int res = 0;
	res = FD_ISSET(fd, set);
	return (res > 0 ? true : false);
}

static void
put_data_to_datanode(DispatchThreadInfo *thrinfo, LineBuffer *lineBuffer, MessageQueuePipe *output_queue)
{
	int send = 0;

	/* send data to ADB*/
	send = PQputCopyData(thrinfo->conn, lineBuffer->data, lineBuffer->len);

	ADBLOADER_LOG(LOG_DEBUG,
		"[DISPATCH][thread id : %ld ] send data: %s from queue num:%s \n",
		thrinfo->thread_id, lineBuffer->data, output_queue->name);

	if (process_bar)
		thrinfo->send_total++;

	if (send < 0)
	{
		ADBLOADER_LOG(LOG_ERROR,
		"[DISPATCH][thread id : %ld ] send copy data error : %s ",
			thrinfo->thread_id, lineBuffer->data);

		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] send copy data error message: %s ",
					thrinfo->thread_id, PQerrorMessage(thrinfo->conn));

		/* check connection state */
		if (PQstatus(thrinfo->conn) != CONNECTION_OK)
		{
			/* reconnect */
			reconnect_agtm_and_datanode(thrinfo);
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
											"PQputCopyData error",
											PQerrorMessage(thrinfo->conn), lineBuffer->fileline,
											lineBuffer->data, true);
			}
		}
	}

	/* flush data every time to get error data quickly */
	PQflush(thrinfo->conn);
	release_linebuf(lineBuffer);

	return;
}

static void
put_end_to_datanode(DispatchThreadInfo *thrinfo)
{
	PGresult *res = NULL;

	ADBLOADER_LOG(LOG_DEBUG,
		"[DISPATCH][thread id : %ld ] dispatch file is complete, exit thread",
		thrinfo->thread_id);

	if ((thrinfo->need_rollback)?
			(PQputCopyEnd(thrinfo->conn, "adb_load_rollback") == 1):
			(PQputCopyEnd(thrinfo->conn, NULL) == 1))
	{
		res = PQgetResult(thrinfo->conn);
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			ADBLOADER_LOG(LOG_INFO,
						"[DISPATCH][thread id : %ld ] copy end ok", thrinfo->thread_id);
			thrinfo->state = DISPATCH_THREAD_EXIT_NORMAL;
			thrinfo->need_redo = true;
		}
		else
		{
			ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] send copy end sucess, get some error message : %s,PQresultStatus(res):%d \n",
					thrinfo->thread_id, PQerrorMessage(thrinfo->conn), PQresultStatus(res));
			thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
			thrinfo->need_redo = true;
		}
	}
	else
	{
		ADBLOADER_LOG(LOG_ERROR,
						"[DISPATCH][thread id : %ld ] send copy end error", thrinfo->thread_id);
		thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
		thrinfo->need_redo = true;
	}

	if (thrinfo->state == DISPATCH_THREAD_COPY_END_ERROR)
	{
		if (!rollback_in_PQerrormsg(PQerrorMessage(thrinfo->conn)))
		{
			char *linedata = NULL;
			dispatch_write_error_message(thrinfo,
										"thread send copy end error",
										PQerrorMessage(thrinfo->conn), 0,
										NULL, true);

			linedata = get_linevalue_from_PQerrormsg(PQerrorMessage(thrinfo->conn));
			fwrite_adb_load_error_data(linedata);
			pg_free(linedata);
			linedata = NULL;
		}
	}

	PQfinish(thrinfo->conn);
	thrinfo->conn = NULL;

	pthread_exit(thrinfo);
	return;
}

static void
put_rollback_to_datanode(DispatchThreadInfo *thrinfo)
{
	PGresult *res = NULL;

	ADBLOADER_LOG(LOG_DEBUG,
		"[DISPATCH][thread id : %ld ] dispatch file is complete, exit thread",
		thrinfo->thread_id);

	if (PQputCopyEnd(thrinfo->conn, "adb_load_rollback") == 1)
	{
		res = PQgetResult(thrinfo->conn);
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			ADBLOADER_LOG(LOG_INFO,
						"[DISPATCH][thread id : %ld ] copy end ok", thrinfo->thread_id);
			thrinfo->state = DISPATCH_THREAD_EXIT_NORMAL;
			thrinfo->need_redo = true;
		}
		else
		{
			ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] send copy end sucess, get some error message : %s,PQresultStatus(res):%d \n",
					thrinfo->thread_id, PQerrorMessage(thrinfo->conn), PQresultStatus(res));
			thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
			thrinfo->need_redo = true;
		}
	}
	else
	{
		ADBLOADER_LOG(LOG_ERROR,
						"[DISPATCH][thread id : %ld ] send copy end error", thrinfo->thread_id);
		thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
		thrinfo->need_redo = true;
	}

	if (thrinfo->need_rollback)
	{
		if (rollback_in_PQerrormsg(PQerrorMessage(thrinfo->conn)))
		{
			thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
		}
		else
		{
			char *linedata = NULL;
			dispatch_write_error_message(thrinfo,
										"thread send copy end error",
										PQerrorMessage(thrinfo->conn), 0,
										NULL, true);

			linedata = get_linevalue_from_PQerrormsg(PQerrorMessage(thrinfo->conn));
			fwrite_adb_load_error_data(linedata);
			pg_free(linedata);
			linedata = NULL;

			thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
		}
	}
	else // no errored
	{
		if (rollback_in_PQerrormsg(PQerrorMessage(thrinfo->conn)))
		{
			thrinfo->state = DISPATCH_THREAD_EXIT_NORMAL;
		}
		else
		{
			char *linedata = NULL;
			dispatch_write_error_message(thrinfo,
										"thread send copy end error",
										PQerrorMessage(thrinfo->conn), 0,
										NULL, true);

			linedata = get_linevalue_from_PQerrormsg(PQerrorMessage(thrinfo->conn));
			fwrite_adb_load_error_data(linedata);
			pg_free(linedata);
			linedata = NULL;

			thrinfo->state = DISPATCH_THREAD_COPY_END_ERROR;
		}
	}

	PQfinish(thrinfo->conn);
	thrinfo->conn = NULL;

	pthread_exit(thrinfo);
	return;

}

static void
put_copy_end_to_datanode(DispatchThreadInfo *thrinfo)
{

	if (thrinfo->just_check) // for check tool
	{
		put_rollback_to_datanode(thrinfo);
	}
	else  // for send data
	{
		put_end_to_datanode(thrinfo);
	}

	return;
}

static int
get_data_from_datanode(DispatchThreadInfo *thrinfo)
{
	PGresult *res = NULL;
	char     *read_buff = NULL;

	res = PQgetResult(thrinfo->conn);
	if (PQresultStatus(res) == PGRES_COPY_IN)
	{
		int result;
		result = PQgetCopyData(thrinfo->conn, &read_buff, 0);
		if (result == -2)
		{
			char *linedata = NULL;

			if (PQputCopyEnd(thrinfo->conn, NULL) == 1)
			{
				res = PQgetResult(thrinfo->conn);
				Assert(PQresultStatus(res) != PGRES_COMMAND_OK);

				ADBLOADER_LOG(LOG_ERROR,
							"[DISPATCH][thread id : %ld ] copy end error, may be copy data error: %s, PQresultStatus(res):%d\n",
							thrinfo->thread_id, PQerrorMessage(thrinfo->conn), PQresultStatus(res));

				if (PQresultStatus(res) == PGRES_FATAL_ERROR)
				{
					dispatch_write_error_message(thrinfo,
												"datanode backend processing closed the connection unexpectedly, please check datanode running or not.",
												PQerrorMessage(thrinfo->conn), 0 , NULL, true);

					thrinfo->state = DISPATCH_GET_BACKEND_FATAL_ERROR;
					thrinfo->need_rollback = true;
				}else
				{
					dispatch_write_error_message(thrinfo,
												"thread receive error message from ADB",
												PQerrorMessage(thrinfo->conn), 0 , NULL, true);

					thrinfo->state = DISPATCH_THREAD_COPY_DATA_ERROR;
					thrinfo->need_rollback = true;
				}

				linedata = get_linevalue_from_PQerrormsg(PQerrorMessage(thrinfo->conn));
				fwrite_adb_load_error_data(linedata);
				pg_free(linedata);
				linedata = NULL;
				reconnect_agtm_and_datanode(thrinfo);
				return -1;
			}
		}
	}
	else
	{
		reconnect_agtm_and_datanode(thrinfo);
		return -1;
	}

	return 0;
}

static void *
dispatch_threadMain (void *argp)
{
	DispatchThreadInfo  *thrinfo = (DispatchThreadInfo*) argp;
	MessageQueuePipe    *output_queue = NULL;
	LineBuffer          *lineBuffer = NULL;
	int                  max_fd = 0;
	int                  select_res = 0;
	fd_set               read_fds;
	fd_set               write_fds;

	Assert(thrinfo->conninfo_agtm != NULL);
	Assert(thrinfo->conninfo_datanode != NULL);

	output_queue = thrinfo->output_queue;

	//build_communicate_agtm_and_datanode(thrinfo);
    connect_agtm_and_datanode(thrinfo);
	ADBLOADER_LOG(LOG_DEBUG,
		"[DISPATCH][thread id : %ld ] out queue read fd : %d, server connection fd : %d",
		thrinfo->thread_id, output_queue->fd[0], thrinfo->conn->sock);

	for (;;)
	{
		if (thrinfo->exit)
		{
			thrinfo->state = DISPATCH_THREAD_KILLED_BY_OTHERTHREAD;
			/*
			dispatch_write_error_message(thrinfo, "thread killed by others",
									    NULL, 0, NULL, true);
			*/
			thrinfo->need_redo = true;
			pthread_exit(thrinfo);
		}

		lineBuffer = NULL;
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
		if (select_res < 0)
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				/* exception handling */
					ADBLOADER_LOG(LOG_ERROR,
							"[DISPATCH][thread id : %ld ] select return value < 0",
								thrinfo->thread_id);
					thrinfo->state = DISPATCH_THREAD_SELECT_ERROR;

					dispatch_write_error_message(thrinfo,
												"select connect datanode socket return < 0, network may be not well ,file need to redo",
												NULL, 0, NULL, true);
					continue;
			}

			if (PQstatus(thrinfo->conn) != CONNECTION_OK)
			{
				/* reconnect */
				reconnect_agtm_and_datanode(thrinfo);
				continue;
			}
		}

		if (datanode_can_read(thrinfo->conn->sock, &read_fds))
		{
			int result = 0;
			result = get_data_from_datanode(thrinfo);
			if (result == -1)
				continue;
		}

		if (datanode_can_write(thrinfo->conn->sock, &write_fds))
		{
			if (output_queue_can_read(output_queue->fd[0], &read_fds))
			{
				lineBuffer = mq_pipe_poll(output_queue);
				if (lineBuffer != NULL)
				{
					put_data_to_datanode(thrinfo, lineBuffer, output_queue);
				}
				else // threads end flag
				{
					put_copy_end_to_datanode(thrinfo);
				}
			}
			else
			{
				ADBLOADER_LOG(LOG_DEBUG,
								"[DISPATCH][thread id : %ld] output queue can not read now.\n", thrinfo->thread_id);
			}
		}
		else
		{
			ADBLOADER_LOG(LOG_DEBUG,
							"[DISPATCH][thread id : %ld] Adb cluster processing data is relatively slow.\n", thrinfo->thread_id);
		}

		if (datanode_can_write(thrinfo->conn->sock, &write_fds) &&
			!datanode_can_read(thrinfo->conn->sock, &read_fds) &&
			!output_queue_can_read(output_queue->fd[0], &read_fds))
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

				}
			}
		}

		/* now conn can't read and can't write */
		if ( !datanode_can_write(thrinfo->conn->sock, &write_fds) &&
			!datanode_can_read(thrinfo->conn->sock, &read_fds))
		{
			if (PQstatus(thrinfo->conn) != CONNECTION_OK)
			{
				/* reconnect */
				reconnect_agtm_and_datanode(thrinfo);
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
							"[DISPATCH][thread id : %ld ] select connect datanode socket return < 0, network may be not well, exit thread",
								thrinfo->thread_id);
					thrinfo->state = DISPATCH_THREAD_SELECT_ERROR;

					dispatch_write_error_message(thrinfo,
												"select connect datanode socket return < 0, network may be not well ,file need to redo",
												NULL, 0, NULL, true);

					FOR_GET_DATA_FROM_OUTPUT_QUEUE();
				}
			}
		}
	}

	return thrinfo;
}

#if 0
static void
build_communicate_agtm_and_datanode (DispatchThreadInfo *thrinfo)
{
	char     *agtm_port = NULL;
	PGresult *res = NULL;
	char     *copy = NULL;

	Assert(thrinfo->conninfo_agtm != NULL);
	Assert(thrinfo->conninfo_datanode != NULL);

	thrinfo->conn = PQconnectdb(thrinfo->conninfo_datanode);
	if (thrinfo->conn == NULL)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_DATANODE_ERROR;

		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] memory allocation failed", thrinfo->thread_id);
		dispatch_write_error_message(thrinfo,
									"memory allocation failed",
									NULL, 0, NULL, true);
		pthread_exit(thrinfo);
	}

	if (PQstatus(thrinfo->conn) != CONNECTION_OK)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_DATANODE_ERROR;

		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH] Connection to database failed: %s, thread id is : %ld",
					PQerrorMessage(thrinfo->conn), thrinfo->thread_id);
		dispatch_write_error_message(thrinfo,
									"Connection to database failed",
									PQerrorMessage(thrinfo->conn), 0, NULL, true);

		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;

		pthread_exit(thrinfo);
	}

	thrinfo->agtm_conn = PQconnectdb(thrinfo->conninfo_agtm);
	if (thrinfo->agtm_conn == NULL)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;

		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] memory allocation failed", thrinfo->thread_id);
		dispatch_write_error_message(thrinfo,
									"memory allocation failed",
									NULL, 0, NULL, true);

		pthread_exit(thrinfo);
	}

	if (PQstatus((PGconn*)thrinfo->agtm_conn) != CONNECTION_OK)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;

		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] connect to agtm error, error message :%s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->agtm_conn));
		dispatch_write_error_message(thrinfo,
									"connect to agtm error",
									PQerrorMessage((PGconn*)thrinfo->agtm_conn), 0,
									NULL, true);

		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;

		pthread_exit(thrinfo);
	}
	else
		agtm_port = (char*)PQparameterStatus(thrinfo->agtm_conn, "agtm_port");

	Assert(thrinfo->conn != NULL);
	if (pqSendAgtmListenPort(thrinfo->conn, atoi(agtm_port)) < 0)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;

		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] could not send agtm port: %s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->conn));
		dispatch_write_error_message(thrinfo,
									"could not send agtm port",
									PQerrorMessage((PGconn*)thrinfo->agtm_conn), 0,
									NULL, true);

		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;

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
									"PQresultStatus is not PGRES_COPY_IN",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, true);
		pthread_exit(thrinfo);
	}

	thrinfo->copy_str = copy;

	return;
}
#endif

static void
dispatch_ThreadCleanup (void * argp)
{
	int flag;
	int current_run_thread = 0;
	int current_exit_thread = 0;

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

#if 0
	/*exit other dispatch threads, if anyone dispatch thread state is not "DISPATCH_THREAD_EXIT_NORMAL" */
	if (thrinfo->state != DISPATCH_THREAD_EXIT_NORMAL)
	{
		ADBLOADER_LOG(LOG_INFO,
							"[DISPATCH][thread id : %ld ] current exit unnormal", thrinfo->thread_id);
		deal_after_thread_exit();
	}
#endif

	/* close socket */
	if (thrinfo->conn != NULL)
	{
		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;
	}

	if (thrinfo->agtm_conn != NULL)
	{
		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;
	}

	pg_free(thrinfo->conninfo_datanode);
	thrinfo->conninfo_datanode = NULL;

	pg_free(thrinfo->conninfo_agtm);
	thrinfo->conninfo_agtm = NULL;

	pg_free(thrinfo->table_name);
	thrinfo->table_name = NULL;

	pg_free(thrinfo->copy_str);
	thrinfo->copy_str = NULL;

	pg_free(thrinfo->copy_options);
	thrinfo->copy_options = NULL;

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

static void
reconnect_agtm_and_datanode(DispatchThreadInfo *thrinfo)
{
	/* finish agtm and datanode connect */
	PQfinish_agtm_and_datanode(thrinfo);

	/* reconnect agtm and datanode again */
	connect_agtm_and_datanode(thrinfo);

	return;
}

static void
connect_agtm_and_datanode(DispatchThreadInfo *thrinfo)
{
	bool      conn_success = false;
	char     *agtm_port = NULL;
	char     *copy = NULL;
	PGresult *res = NULL;
	int       i = 0;

	Assert(thrinfo->conninfo_agtm != NULL);
	Assert(thrinfo->conninfo_datanode != NULL);

	/* reconnect agtm 3 times, if failed, threads exit */
	for (i = 0; i < 3; i++)
	{
		conn_success = connect_agtm(thrinfo);
		if (conn_success)
		{
			break;
		}
		sleep(1);
	}
	if (!conn_success)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;
		pthread_exit(thrinfo);
	}
	else
	{
		agtm_port = (char*)PQparameterStatus(thrinfo->agtm_conn, "agtm_port");
	}

	/* reconnect datanode 3 times, if failed, threads exit */
	for (i = 0; i < 3; i++)
	{
		conn_success = connect_datanode(thrinfo);
		if (conn_success)
		{
			break;
		}
		sleep(1);
	}
	if (!conn_success)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_DATANODE_ERROR;
		pthread_exit(thrinfo);
	}

	/* send agtm server listen port for datanode */
	if (pqSendAgtmListenPort(thrinfo->conn, atoi(agtm_port)) < 0)
	{
		thrinfo->state = DISPATCH_THREAD_CONNECTION_AGTM_ERROR;

		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] could not send agtm port: %s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->conn));
		dispatch_write_error_message(thrinfo,
									"could not send agtm port",
									PQerrorMessage((PGconn*)thrinfo->agtm_conn), 0,
									NULL, true);

		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;
		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;

		pthread_exit(thrinfo);
	}

	/* exec copy command to datanode */
	copy = create_copy_string(thrinfo->table_name, thrinfo->copy_options,
							  thrinfo->copy_cmd_comment, thrinfo->copy_cmd_comment_str);
	ADBLOADER_LOG(LOG_DEBUG, "[DISPATCH][thread id : %ld ] dispatch copy string is : %s ",
				thrinfo->thread_id, copy);

	res = PQexec(thrinfo->conn, copy);
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		ADBLOADER_LOG(LOG_ERROR, "[DISPATCH][thread id : %ld ] PQresultStatus is not PGRES_COPY_IN, error message :%s, thread exit",
					thrinfo->thread_id, PQerrorMessage(thrinfo->conn));
		thrinfo->state = DISPATCH_THREAD_COPY_STATE_ERROR;

		dispatch_write_error_message(thrinfo,
									"PQresultStatus is not PGRES_COPY_IN",
									PQerrorMessage(thrinfo->conn), 0,
									NULL, true);

		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;
		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;

		pthread_exit(thrinfo);
	}

	thrinfo->copy_str = copy;

	return;
}

static bool
connect_datanode(DispatchThreadInfo *thrinfo)
{
	Assert(thrinfo->conninfo_datanode != NULL);

	thrinfo->conn = PQconnectdb(thrinfo->conninfo_datanode);
	if (thrinfo->conn == NULL)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] memory allocation failed", thrinfo->thread_id);
		dispatch_write_error_message(thrinfo,
									"memory allocation failed",
									NULL, 0, NULL, true);
		return false;
	}

	if (PQstatus((PGconn*)thrinfo->conn) != CONNECTION_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] connect to agtm error, error message :%s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->conn));
		dispatch_write_error_message(thrinfo,
									"connect to agtm error",
									PQerrorMessage((PGconn*)thrinfo->conn), 0,
									NULL, true);

		PQfinish(thrinfo->conn);
		thrinfo->conn = NULL;

		return false;
	}

	return true;
}

static bool
connect_agtm(DispatchThreadInfo *thrinfo)
{
	Assert(thrinfo->conninfo_agtm != NULL);

	thrinfo->agtm_conn = PQconnectdb(thrinfo->conninfo_agtm);
	if (thrinfo->agtm_conn == NULL)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] memory allocation failed", thrinfo->thread_id);
		dispatch_write_error_message(thrinfo,
									"memory allocation failed",
									NULL, 0, NULL, true);
		return false;
	}

	if (PQstatus((PGconn*)thrinfo->agtm_conn) != CONNECTION_OK)
	{
		ADBLOADER_LOG(LOG_ERROR,
					"[DISPATCH][thread id : %ld ] connect to agtm error, error message :%s",
					thrinfo->thread_id, PQerrorMessage((PGconn*)thrinfo->agtm_conn));
		dispatch_write_error_message(thrinfo,
									"connect to agtm error",
									PQerrorMessage((PGconn*)thrinfo->agtm_conn), 0,
									NULL, true);

		PQfinish(thrinfo->agtm_conn);
		thrinfo->agtm_conn = NULL;

		return false;
	}

	return true;
}

static void
PQfinish_agtm_and_datanode(DispatchThreadInfo *thrinfo)
{
	PQfinish(thrinfo->conn);
	PQfinish(thrinfo->agtm_conn);

	return;
}

static char *
create_copy_string (char *table_name,
					char *copy_options,
					bool copy_cmd_comment,
					char *copy_cmd_comment_str)
{
	LineBuffer *buf = NULL;
	char       *result = NULL;

	Assert(table_name != NULL);

	buf = get_linebuf();

	if (copy_cmd_comment)
	{
		appendLineBufInfo(buf, "set COPY_CMD_COMMENT = ON;");
		appendLineBufInfo(buf, "set COPY_CMD_COMMENT_STR = \'%s\';", copy_cmd_comment_str);
	}
	else
	{
		appendLineBufInfo(buf, "set COPY_CMD_COMMENT = OFF;");
	}

	appendLineBufInfo(buf, "%s", "COPY  ");
	appendLineBufInfo(buf, "%s", table_name);
	appendLineBufInfo(buf, "%s", " FROM STDIN ");

	if (copy_options != NULL)
		appendLineBufInfo(buf, "%s", copy_options);

	result = (char*)palloc0(buf->len + 1);
	memcpy(result, buf->data, buf->len);
	result[buf->len] = '\0';

	release_linebuf(buf);
	return result;
}

#if 0
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
				if (thread_info != NULL)
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
#endif

void
clean_dispatch_resource(void)
{
	int flag = 0;

	Assert(DispatchThreadsRun->send_thread_cur == 0);

	if (DispatchThreadsRun->send_threads != NULL)
		pfree(DispatchThreadsRun->send_threads);

	for (flag = 0; flag < DispatchThreadsFinish->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info  = DispatchThreadsFinish->send_threads[flag];
		if (thread_info != NULL)
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

	if (g_start_cmd != NULL)
	{
		pfree(g_start_cmd);
		g_start_cmd = NULL;
	}

	Is_Deal = false;
}

DispatchThreads *
get_dispatch_exit_threads(void)
{
	return DispatchThreadsFinish;
}

void
get_sent_conut(int * thread_send_num)
{
	int flag;

	pthread_mutex_lock(&DispatchThreadsRun->mutex);
	for (flag = 0; flag < DispatchThreadsRun->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info = DispatchThreadsRun->send_threads[flag];
		if (thread_info != NULL)
		{
			thread_send_num[flag] = thread_info->send_total;
		}
	}
	pthread_mutex_unlock(&DispatchThreadsRun->mutex);

	pthread_mutex_lock(&DispatchThreadsFinish->mutex);
	for (flag = 0; flag < DispatchThreadsFinish->send_thread_count; flag++)
	{
		DispatchThreadInfo *thread_info = DispatchThreadsFinish->send_threads[flag];
		if (thread_info != NULL)
		{
			Assert(thread_send_num[flag] == 0);
			thread_send_num[flag] = thread_info->send_total;
		}
	}
	pthread_mutex_unlock(&DispatchThreadsFinish->mutex);
}

void
set_dispatch_file_start_cmd(char * start_cmd)
{
	Assert(start_cmd != NULL);

	if (g_start_cmd != NULL)
	{
		pfree(g_start_cmd);
		g_start_cmd = NULL;
	}

	g_start_cmd = pg_strdup(start_cmd);

	return;
}

static char *
get_linevalue_from_PQerrormsg(char *PQerrormsg)
{
	char *tmp = NULL;

	/*data line token is "VALUE: " in PQerrormsg from datanode */
	const char *str_tok = "VALUE: ";

	tmp = strstr(PQerrormsg, str_tok);
	if (tmp == NULL)
	{
		return NULL;
	}
	else
	{
		return pstrdup(tmp + strlen(str_tok));
	}
}

static bool
rollback_in_PQerrormsg(char *PQerrormsg)
{
	char *tmp = NULL;
	const char *str_tok = "adb_load_rollback";

	tmp = strstr(PQerrormsg, str_tok);
	if (tmp == NULL)
	{
		return false;
	}

	return true;
}


