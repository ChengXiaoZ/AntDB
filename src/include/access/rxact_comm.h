#ifndef RXACT_COMM_H_
#define RXACT_COMM_H_

#include "lib/stringinfo.h"
#include "storage/fd.h"

typedef struct RXactLogData* RXactLog;

extern pgsocket	rxact_listen(void);
extern pgsocket	rxact_connect(void);
extern const char* rxact_get_sock_path(void);

extern void rxact_begin_msg(StringInfo msg, char type);
extern void rxact_reset_msg(StringInfo msg, char type);
extern void rxact_put_short(StringInfo msg, short n);
extern void rxact_put_int(StringInfo msg, int n);
extern void rxact_put_bytes(StringInfo msg, const void *s, int len);
extern void rxact_put_string(StringInfo msg, const char *s);
extern void rxact_put_finsh(StringInfo msg);

extern short rxact_get_short(StringInfo msg);
extern int rxact_get_int(StringInfo msg);
extern char* rxact_get_string(StringInfo msg);
extern void rxact_copy_bytes(StringInfo msg, void *s, int len);
extern void* rxact_get_bytes(StringInfo msg, int len);
extern void rxact_get_msg_end(StringInfo msg);

extern RXactLog rxact_begin_read_log(File fd);
extern void rxact_end_read_log(RXactLog rlog);
extern bool rxact_log_is_eof(RXactLog rlog);
extern void rxact_log_reset(RXactLog rlog);

extern int rxact_log_get_int(RXactLog rlog);
extern short rxact_log_get_short(RXactLog rlog);
extern const char* rxact_log_get_string(RXactLog rlog);
extern void rxact_log_read_bytes(RXactLog rlog, void *p, int n);
extern void rxact_log_seek_bytes(RXactLog rlog, int n);

extern RXactLog rxact_begin_write_log(File fd);
extern void rxact_end_write_log(RXactLog rlog);
extern void rxact_log_write_byte(RXactLog rlog, char c);
extern void rxact_log_write_int(RXactLog rlog, int n);
extern void rxact_log_write_bytes(RXactLog rlog, const void *p, int n);
extern void rxact_log_write_string(RXactLog rlog, const char *str);
extern void rxact_log_simple_write(File fd, const void *p, int n);

extern void rxact_report_log_error(File fd, int elevel);

#endif /* RXACT_COMM_H_ */
