#ifndef ADB_LOAD_LOG_DETAIL_FD_H
#define ADB_LOAD_LOG_DETAIL_FD_H

extern FILE *log_detail_fd;

extern void open_log_detail_fd(char *path, char *tablename);
extern void close_log_detail_fd(void);
extern void write_log_detail_fd(char *linedata);
extern void check_log_detail_fd(char *path, char *tablename);

#endif /* ADB_LOAD_LOG_DETAIL_FD_H */
