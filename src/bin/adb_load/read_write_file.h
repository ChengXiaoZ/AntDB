#ifndef ADB_LOADER_READWRITE_FILE
#define ADB_LOADER_READWRITE_FILE

#include <pthread.h>
#include "linebuf.h"

extern FILE *err_data_fd;

/* fopen the error file */
extern int fopen_error_file(char *writeErrorDataPath);

/*fclose the error file */
extern void fclose_error_file(void);

/*error message to write file for some compute hash thread. */
extern int write_error(LineBuffer *lineBuffer);

/*error message to write file for some send data to datanode. */
extern int sent_to_datanode_error(char *error);

#endif
