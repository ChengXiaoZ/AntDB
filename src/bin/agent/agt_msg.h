#ifndef AGENT_MESSAGE_H
#define AGENT_MESSAGE_H

#include "lib/stringinfo.h"

void agt_msg_init(pgsocket fd_client);

/* return first char for message */
int agt_get_msg(StringInfo msg);

int agt_put_msg(char msgtype, const char *s, size_t len);
int agt_flush(void);

void agt_beginmessage(StringInfo buf, char msgtype);
void agt_sendbyte(StringInfo buf, int byt);
void agt_sendbytes(StringInfo buf, const char *data, int datalen);
void agt_sendstring(StringInfo buf, const char *str);
void agt_sendint(StringInfo buf, int i, int b);
void agt_sendint64(StringInfo buf, int64 i);
void agt_endmessage(StringInfo buf);

int	agt_getmsgbyte(StringInfo msg);
unsigned int agt_getmsgint(StringInfo msg, int b);
int64 agt_getmsgint64(StringInfo msg);
const char *agt_getmsgbytes(StringInfo msg, int datalen);
void agt_copymsgbytes(StringInfo msg, char *buf, int datalen);
const char *agt_getmsgstring(StringInfo msg);
void agt_getmsgend(StringInfo msg);

#endif /* AGENT_MESSAGE_H */
