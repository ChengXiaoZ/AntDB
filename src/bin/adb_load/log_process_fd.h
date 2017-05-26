#ifndef ADB_LOAD_LOG_PROCESS_FD_H
#define ADB_LOAD_LOG_PROCESS_FD_H

typedef enum LOG_LEVEL
{
	LOG_DEBUG,
	LOG_INFO,
	LOG_WARN,
	LOG_ERROR
} LOG_LEVEL;

extern FILE *log_process_fd;
extern void open_log_process_fd(char *logfilepath, LOG_LEVEL log_level);
extern void close_log_process_fd(void);
extern void write_log_process_fd(LOG_LEVEL log_level,
								const char *file,
								const char *fun,
								int line,
								const char *fmt, ...)
	__attribute__((format(PG_PRINTF_ATTRIBUTE, 5, 6)));

#define ADBLOADER_LOG(log_level, fmt, args...) \
	write_log_process_fd(log_level, __FILE__, __FUNCTION__, __LINE__, fmt, ##args)

#endif /* ADB_LOAD_LOG_PROCESS_FD_H */
