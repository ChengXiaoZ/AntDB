#ifndef AGTM_CLIENT_H
#define AGTM_CLIENT_H

#include "agtm/agtm_protocol.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-fe.h"

typedef struct AGTM_Conn
{
	PGconn 		*pg_Conn;
	AGTM_Result *agtm_Result;
} AGTM_Conn;

#define AGTM_RESULT_COMM_ERROR (-2) /* Communication error */
#define AGTM_RESULT_ERROR      (-1)
#define AGTM_RESULT_OK         (0)

/*
 * initialize connection between AGTM and coordinator or datanode
 */
extern int agtm_Init(void);

/*
 * disconnect between AGTM and coordinator or datanode
 */
extern void agtm_Close(void);

/*
 * reset connection between AGTM and coordinator or datanode
 */
extern void agtm_Reset(void);
extern void agtm_BeginMessage(StringInfo buf, char msgtype);
extern void agtm_SendString(StringInfo buf, const char *str);
extern void agtm_SendInt(StringInfo buf, int i, int b);
extern void agtm_Endmessage(StringInfo buf);
extern void agtm_Flush(void);
extern int agtm_GetListenPort(void);
extern void agtm_SetPort(int listen_port);

extern PGconn* getAgtmConnection(void);
extern PGconn* getAgtmConnectionByDBname(const char *dbname);
extern AGTM_Result* agtm_GetResult(void);
#endif
