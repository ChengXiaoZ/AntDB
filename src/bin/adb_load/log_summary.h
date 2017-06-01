 #ifndef ADB_LOAD_LOG_SUMMARY_H
#define ADB_LOAD_LOG_SUMMARY_H

typedef pthread_t LogSummaryThreadID;

typedef enum LogSummaryThreadState
{
	LOGSUMMARY_THREAD_EXIT_NORMAL,
	LOGSUMMARY_THREAD_RUNNING
} LogSummaryThreadState;

typedef struct GlobleInfo
{
	pthread_mutex_t mutex;
	slist_head      g_slist;
} GlobleInfo;

typedef struct LogSummaryThreadInfo
{
	LogSummaryThreadID thread_id;
	int                threshold_value;
	void               (*thr_startroutine)(void *);
} LogSummaryThreadInfo;


#define LOG_SUMMARY_RES_OK      0
#define LOG_SUMMARY_RES_ERROR  -1

/* for hash stage error code */
#define ERRCODE_COMPUTE_HASH                "hash09"

/* for send stage error code */
#define ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION "23000"
#define ERRCODE_INVALID_TEXT_REPRESENTATION    "22P02"
#define ERRCODE_BAD_COPY_FILE_FORMAT           "22P04"
#define ERRCODE_UNIQUE_VIOLATION               "23505"
#define ERRCODE_FOREIGN_KEY_VIOLATION          "23503"
#define ERRCODE_NOT_NULL_VIOLATION             "23502"
#define ERRCODE_CHECK_VIOLATION                "23514"
#define ERRCODE_EXCLUSION_VIOLATION            "23P01"
#define ERRCODE_OTHER                          "other"

extern bool g_log_summary_exit;
extern LogSummaryThreadState log_summary_thread_state;

extern int init_log_summary_thread(LogSummaryThreadInfo *log_summary_info);
extern void save_to_log_summary(char *error_code, char *line_data);
extern bool stop_log_summary_thread(void);

#endif /* ADB_LOAD_LOG_SUMMARY_H */
