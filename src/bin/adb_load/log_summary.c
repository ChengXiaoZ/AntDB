#include <stdio.h>
#include <pthread.h>

#include "postgres_fe.h"
#include "lib/ilist.h"
#include "log_summary.h"
#include "log_summary_fd.h"
#include "linebuf.h"

slist_head error_info_list;
static pthread_mutex_t error_info_list_mutex = PTHREAD_MUTEX_INITIALIZER;
static int error_threshold = 0;

static LineBuffer *get_log_buf(ErrorInfo *error_info);
static ErrorInfo *get_new_error_info(char *error_code, char *line_data);
static void free_error_info(ErrorInfo *error_info);
static char *get_error_desc(char *error_code);

void
init_error_info_list()
{
	slist_init(&error_info_list);
	return;
}

void
set_error_threshold(int value)
{
	error_threshold = value;
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
		return "the fields type do not match";
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

	error_info = (ErrorInfo *)palloc0(sizeof(ErrorInfo));
	error_data = (ErrorData *)palloc0(sizeof(ErrorData));
	error_info->error_data = error_data;

	error_data->error_code = pg_strdup(error_code);
	error_data->error_desc = pg_strdup(get_error_desc(error_code));
	error_data->error_total = 1;
	error_data->error_threshold = error_threshold;

	line_data_info = (LineDataInfo *)palloc0(sizeof(LineDataInfo));
	line_data_info->data = pg_strdup(line_data);
	slist_init(&error_data->line_data_list);
	slist_push_head(&error_data->line_data_list, &line_data_info->next);

	return error_info;
}

void
append_error_info_list(char *error_code, char *line_data)
{
	ErrorInfo *error_info = NULL;
	slist_iter iter;
	bool       is_exits = false;

	Assert(error_code != NULL);
	Assert(line_data != NULL);

	pthread_mutex_lock(&error_info_list_mutex);

	if (slist_is_empty(&error_info_list))
	{
		error_info = get_new_error_info(error_code, line_data);
		slist_push_head(&error_info_list, &error_info->next);

		pthread_mutex_unlock(&error_info_list_mutex);
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

	pthread_mutex_unlock(&error_info_list_mutex);

	return;
}

void
write_error_info_list_to_file()
{
	slist_mutable_iter iter;
	LineBuffer *log_buf = NULL;

	init_linebuf(5);

	pthread_mutex_destroy(&error_info_list_mutex);

	if (slist_is_empty(&error_info_list))
	{
		log_buf = get_linebuf();
		appendLineBufInfoString(log_buf, "success");
		write_log_summary_fd(log_buf);

		return;
	}

	slist_foreach_modify(iter, &error_info_list)
	{

		ErrorInfo *error_info_local = slist_container(ErrorInfo, next, iter.cur);

		log_buf = get_log_buf(error_info_local);
		write_log_summary_fd(log_buf);

		free_error_info(error_info_local);
		slist_delete_current(&iter);
	}

	return;
}

static LineBuffer *
get_log_buf(ErrorInfo *error_info)
{
	LineBuffer *log_buf = NULL;
	slist_mutable_iter iter;

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
			appendLineBufInfo(log_buf, "%s", line_data->data);
		else
			appendLineBufInfo(log_buf, "%s\n", line_data->data);

		pg_free(line_data->data);
		line_data->data = NULL;

		slist_delete_current(&iter);
	}

	appendLineBufInfoString(log_buf, "-------------------------\n");
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

