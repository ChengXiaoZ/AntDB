#ifndef RXACT_COMM_H_
#define RXACT_COMM_H_

#include "lib/stringinfo.h"

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

#endif /* RXACT_COMM_H_ */
