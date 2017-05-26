#ifndef ADB_LOAD_LOG_SUMMARY_H
#define ADB_LOAD_LOG_SUMMARY_H

typedef struct ErrorData
{
	char      *error_code;
	char      *error_desc;
	uint32     error_total;
	uint32     error_threshold;
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

extern slist_head error_info_list;

/* for hash stage error code */
#define ERRCODE_COMPUTE_HASH                "hash_error_code"

/* for send stage error code */
#define ERRCODE_INVALID_TEXT_REPRESENTATION "22P02"
#define ERRCODE_BAD_COPY_FILE_FORMAT        "22P04"
#define ERRCODE_UNIQUE_VIOLATION            "23505"
#define ERRCODE_OTHER                       "other"


extern void init_error_info_list(void);
extern void set_error_threshold(int value);
extern void append_error_info_list(char *error_code, char *line_data);
extern void write_error_info_list_to_file(void);

#endif /* ADB_LOAD_LOG_SUMMARY_H */
