#ifndef ADB_LOAD_LOG_SUMMARY_FD_H
#define ADB_LOAD_LOG_SUMMARY_FD_H

#include <pthread.h>
#include "linebuf.h"

extern FILE *log_summary_fd;

extern int open_log_summary_fd(char *log_summary_path);
extern int write_log_summary_fd(LineBuffer *lineBuffer);
extern void close_log_summary_fd(void);

#endif /* ADB_LOAD_LOG_SUMMARY_FD_H */
