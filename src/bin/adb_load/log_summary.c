#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

#include "postgres_fe.h"
#include "lib/ilist.h"
#include "log_summary.h"
#include "log_summary_fd.h"
#include "log_process_fd.h"
#include "linebuf.h"

LogSummaryThreadState log_summary_thread_state;
bool g_log_summary_exit = false;

static slist_head  error_info_list;
static GlobleInfo *global_info = NULL;
static int error_threshold = 0;

typedef struct ErrorData
{
	char      *error_code;
	char      *error_desc;
	uint32     error_total;
	int        error_threshold;
	slist_head line_data_list;
} ErrorData;

typedef struct ErrorInfo
{
	ErrorData *error_data;
	slist_node next;
} ErrorInfo;

typedef struct LineDataInfo
{
	char *data;
	slist_node next;
} LineDataInfo;

typedef struct ErrorMsgElem
{
	char *error_code;
	char *line_data;
} ErrorMsgElem;

typedef struct ErrorMsg
{
	ErrorMsgElem *error_msg;
	slist_node next;
}ErrorMsg;

static ErrorMsg *get_error_msg(char *error_code, char *line_data);
static void set_error_threshold(int threshold_value);
static char *get_error_desc(char *error_code);
static ErrorInfo *get_new_error_info(char *error_code, char *line_data);
static void append_error_info_list(char *error_code, char *line_data);
static void write_error_info_list_to_file(void);
static LineBuffer *get_log_buf(ErrorInfo *error_info);
static void free_error_info(ErrorInfo *error_info);
static void pg_free_error_msg(ErrorMsg *msg);
static void init_global_info(void);
static void *build_log_summary_thread_main(void *arg);
static void log_summary_thread_main(void *arg);
static void log_summary_thread_clean(void *arg);

bool
stop_log_summary_thread()
{
	int i = 0;
	g_log_summary_exit = true;

	for (i = 0; i < 10; i++)
	{
		if (log_summary_thread_state == LOGSUMMARY_THREAD_EXIT_NORMAL)
		{
			return true;
		}

		sleep(1);
	}

	return false;
}

int
init_log_summary_thread(LogSummaryThreadInfo *log_summary_info)
{
	init_global_info();
	set_error_threshold(log_summary_info->threshold_value);

	log_summary_info->thr_startroutine = log_summary_thread_main;
	if ((pthread_create(&log_summary_info->thread_id, NULL, build_log_summary_thread_main, log_summary_info)) < 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[LOG_SUMMARY] create log summary thread error");
		return LOG_SUMMARY_RES_ERROR;
	}

	ADBLOADER_LOG(LOG_INFO, "[LOG_SUMMARY]create log summary thread : %ld ", log_summary_info->thread_id);
	return LOG_SUMMARY_RES_OK;
}

static void *
build_log_summary_thread_main(void *arg)
{
	LogSummaryThreadInfo *log_summary_info = (LogSummaryThreadInfo *)arg;

	pthread_detach(log_summary_info->thread_id);
	pthread_cleanup_push(log_summary_thread_clean, log_summary_info);
	log_summary_info->thr_startroutine(log_summary_info);
	pthread_cleanup_pop(1);

	return log_summary_info;
}

static void
log_summary_thread_clean(void *arg)
{
	pthread_mutex_destroy(&global_info->mutex);

	/* reset false */
	g_log_summary_exit = false;

	pg_free(global_info);
	global_info = NULL;

	return;
}

static void
log_summary_thread_main(void *arg)
{
	slist_mutable_iter siter;

	slist_init(&error_info_list);
	log_summary_thread_state = LOGSUMMARY_THREAD_RUNNING;

	for (;;)
	{
		pthread_mutex_lock(&global_info->mutex);

		if (slist_is_empty(&global_info->g_slist) && g_log_summary_exit)
		{
			write_error_info_list_to_file();
			log_summary_thread_state = LOGSUMMARY_THREAD_EXIT_NORMAL;

			pthread_mutex_unlock(&global_info->mutex);
			pthread_exit((void*)0);
		}

		if (!slist_is_empty(&global_info->g_slist))
		{
			slist_foreach_modify(siter, &global_info->g_slist)
			{
				 ErrorMsg *msg = slist_container(ErrorMsg, next, siter.cur);

				 ADBLOADER_LOG(LOG_DEBUG, "[LOG_SUMMARY] get data from global info: error_code:%s, line_data: %s",
							msg->error_msg->error_code, msg->error_msg->line_data);

				 append_error_info_list(msg->error_msg->error_code, msg->error_msg->line_data);

				 pg_free_error_msg(msg);
				 slist_delete_current(&siter);
			}

		}

		pthread_mutex_unlock(&global_info->mutex);

		sleep(1);
	}

	return;
}

void
save_to_log_summary(char *error_code, char *line_data)
{
	ErrorMsg *msg = NULL;

	Assert(error_code != NULL);
	Assert(line_data != NULL);

	pthread_mutex_lock(&global_info->mutex);

	msg = get_error_msg(error_code, line_data);
	slist_push_head(&global_info->g_slist, &msg->next);

	pthread_mutex_unlock(&global_info->mutex);

	return;
}

static ErrorMsg *
get_error_msg(char *error_code, char *line_data)
{
	ErrorMsg * msg = NULL;
	ErrorMsgElem *msgelem = NULL;

	Assert(error_code != NULL);
	Assert(line_data != NULL);

	msg = (ErrorMsg *)palloc0(sizeof(ErrorMsg));
	msgelem = (ErrorMsgElem *)palloc0(sizeof(ErrorMsgElem));

	msg->error_msg = msgelem;
	msgelem->error_code = pg_strdup(error_code);
	msgelem->line_data = pg_strdup(line_data);

	return msg;
}

static void
set_error_threshold(int threshold_value)
{
	error_threshold = threshold_value;
	return;
}

static char *
get_error_desc(char *error_code)
{
	Assert(error_code != NULL);

	if (strcmp(error_code, ERRCODE_COMPUTE_HASH) == 0)
	{
		return "compute hash error";
	}else if (strcmp(error_code, ERRCODE_BAD_COPY_FILE_FORMAT) == 0)
	{
		return "copy file format error";
	}else if (strcmp(error_code, ERRCODE_UNIQUE_VIOLATION) == 0)
	{
		return "unique violation error";
	}else if (strcmp(error_code, ERRCODE_INVALID_TEXT_REPRESENTATION) == 0)
	{
		return "Data type mismatch";
	}
	else if (strcmp(error_code, ERRCODE_FOREIGN_KEY_VIOLATION) == 0)
	{
		return "foreign key violation error";
	}
	else if (strcmp(error_code, ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION) == 0)
	{
		return "integrity constraint violation";
	}
	else if (strcmp(error_code, ERRCODE_NOT_NULL_VIOLATION) == 0)
	{
		return "not null violation error";
	}
	else if (strcmp(error_code, ERRCODE_CHECK_VIOLATION) == 0)
	{
		return "check violation error";
	}
	else if (strcmp(error_code, ERRCODE_EXCLUSION_VIOLATION) == 0)
	{
		return "exclusion violation error";
	}
	else
	{
		return "other error";
	}
}

static ErrorInfo *
get_new_error_info(char *error_code, char *line_data)
{
	ErrorInfo    *error_info = NULL;
	ErrorData    *error_data = NULL;
	LineDataInfo *line_data_info = NULL;

	Assert(error_code != NULL);
	Assert(line_data != NULL);

	error_info = (ErrorInfo *)palloc0(sizeof(ErrorInfo));
	error_data = (ErrorData *)palloc0(sizeof(ErrorData));
	error_info->error_data = error_data;

	error_data->error_code = pg_strdup(error_code);
	error_data->error_desc = pg_strdup(get_error_desc(error_code));

	/* new error_info it's error_total is 1 */
	error_data->error_total = 1;
	error_data->error_threshold = error_threshold;

	line_data_info = (LineDataInfo *)palloc0(sizeof(LineDataInfo));
	line_data_info->data = pg_strdup(line_data);
	slist_init(&error_data->line_data_list);
	slist_push_head(&error_data->line_data_list, &line_data_info->next);

	return error_info;
}

static void
append_error_info_list(char *error_code, char *line_data)
{
	ErrorInfo *error_info = NULL;
	slist_iter iter;
	bool       is_exits = false;

	Assert(error_code != NULL);
	Assert(line_data != NULL);

	if (slist_is_empty(&error_info_list))
	{
		error_info = get_new_error_info(error_code, line_data);
		slist_push_head(&error_info_list, &error_info->next);

		return;
	}

	slist_foreach(iter, &error_info_list)
	{
		ErrorInfo *error_info_local = slist_container(ErrorInfo, next, iter.cur);

		if (strcmp(error_info_local->error_data->error_code, error_code) == 0)
		{
			uint32 total = error_info_local->error_data->error_total;
			uint32 threshold = error_info_local->error_data->error_threshold;
			if (total < threshold)
			{
				LineDataInfo *line_data_info = (LineDataInfo *)palloc0(sizeof(LineDataInfo));
				line_data_info->data = pg_strdup(line_data);
				slist_push_head(&error_info_local->error_data->line_data_list, &line_data_info->next);
			}

			error_info_local->error_data->error_total++;
			is_exits = true;

			break;
		}
	}

	if (!is_exits)
	{
		error_info = get_new_error_info(error_code, line_data);
		slist_push_head(&error_info_list, &error_info->next);
	}

	return;
}

static void
write_error_info_list_to_file()
{
	slist_mutable_iter iter;

	if (slist_is_empty(&error_info_list))
	{
		LineBuffer *log_buf = NULL;
		log_buf = get_linebuf();
		appendLineBufInfoString(log_buf, "success\n");
		write_log_summary_fd(log_buf);

		release_linebuf(log_buf);
		return;
	}

	slist_foreach_modify(iter, &error_info_list)
	{
		LineBuffer *log_buf = NULL;
		ErrorInfo *error_info_local = slist_container(ErrorInfo, next, iter.cur);

		log_buf = get_log_buf(error_info_local);
		write_log_summary_fd(log_buf);

		release_linebuf(log_buf);
		free_error_info(error_info_local);
		slist_delete_current(&iter);
	}

	return;
}

static void
init_global_info()
{
	global_info = (GlobleInfo *)palloc0(sizeof(GlobleInfo));

	if (pthread_mutex_init(&global_info->mutex, NULL) != 0)
	{
		ADBLOADER_LOG(LOG_ERROR, "[LOG_SUMMARY][thread main ] Can not initialize log summary mutex: %s",
						strerror(errno));
	}

	slist_init(&global_info->g_slist);

	return;
}

static LineBuffer *
get_log_buf(ErrorInfo *error_info)
{
	LineBuffer *log_buf = NULL;
	slist_mutable_iter iter;

	Assert(error_info != NULL);

	log_buf = get_linebuf();

	appendLineBufInfoString(log_buf, "Error Desc : ");
	appendLineBufInfo(log_buf, "%s\n", error_info->error_data->error_desc);

	appendLineBufInfoString(log_buf, "Error Code : ");
	appendLineBufInfo(log_buf, "%s\n", error_info->error_data->error_code);

	appendLineBufInfoString(log_buf, "Error Tatol: ");
	appendLineBufInfo(log_buf, "%u\n", error_info->error_data->error_total);

	appendLineBufInfoString(log_buf, "Source Data: ");
	appendLineBufInfo(log_buf, "(threshold %u )\n", error_info->error_data->error_threshold);

	slist_foreach_modify(iter, &error_info->error_data->line_data_list)
	{
		LineDataInfo *line_data =  slist_container(LineDataInfo, next, iter.cur);

		if (strlen(line_data->data) == '\n')
		{
			appendLineBufInfoString(log_buf, line_data->data);
		}
		else
		{
			appendLineBufInfoString(log_buf, line_data->data);
			appendLineBufInfoString(log_buf, "\n");
		}

		pg_free(line_data->data);
		line_data->data = NULL;

		slist_delete_current(&iter);
	}

	appendLineBufInfoString(log_buf, "-------------------------------------------------------\n");

	return log_buf;
}

static void
free_error_info(ErrorInfo *error_info)
{
	pg_free(error_info->error_data->error_code);
	error_info->error_data->error_code = NULL;

	pg_free(error_info->error_data->error_desc);
	error_info->error_data->error_desc = NULL;

	pg_free(error_info->error_data);
	error_info->error_data = NULL;

	return;
}

static void
pg_free_error_msg(ErrorMsg *msg)
{
	pg_free(msg->error_msg->error_code);
	msg->error_msg->error_code = NULL;

	pg_free(msg->error_msg->line_data);
	msg->error_msg->line_data = NULL;

	return;
}

