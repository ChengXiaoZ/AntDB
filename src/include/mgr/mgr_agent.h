#ifndef MANAGER_AGENT_H
#define MANAGER_AGENT_H

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

typedef struct ManagerAgent ManagerAgent;

extern ManagerAgent* ma_connect(const char *host, unsigned short port);
extern ManagerAgent* ma_connect_hostoid(Oid hostoid);
extern bool ma_isconnected(const ManagerAgent *ma);
extern const char* ma_last_error_msg(const ManagerAgent *ma);
extern void ma_close(ManagerAgent *ma);
extern void ma_put_msg(ManagerAgent *ma, char msg_type, const char *msg, Size msg_len);
extern bool ma_flush(ManagerAgent *ma, bool report_error);
extern char ma_get_message(ManagerAgent *ma, StringInfo buf);
extern char* ma_get_err_info(const StringInfo buf, int field);
extern void ma_clean(void);

extern void ma_beginmessage(StringInfo buf, char msgtype);
#define ma_sendbyte    pq_sendbyte
#define ma_sendbytes   pq_sendbytes
extern void ma_sendstring(StringInfo buf, const char *str);
#define ma_sendint     pq_sendint
#define ma_sendint64   pq_sendint64
extern void ma_endmessage(StringInfo buf, ManagerAgent *ma);

#define ma_getmsgbyte     pq_getmsgbyte
#define ma_getmsgint      pq_getmsgint
#define ma_getmsgint64    pq_getmsgint64
#define ma_getmsgbytes    pq_getmsgbytes
#define ma_copymsgbytes   pq_copymsgbytes
extern const char *ma_getmsgstring(StringInfo msg);
#define ma_getmsgend      pq_getmsgend

#endif /* MANAGER_AGENT_H */
