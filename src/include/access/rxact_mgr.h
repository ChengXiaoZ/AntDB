#ifndef RXACT_MGR_H
#define RXACT_MGR_H

#include "lib/stringinfo.h"

typedef enum RemoteXactType
{
	RX_PREPARE = 1
	,RX_COMMIT
	,RX_ROLLBACK
}RemoteXactType;

#define RXACT_BUFFER_SIZE 1024
#define SOCKET(port) 		((RxactPort *)(port))->fdsock

typedef struct RxactPort
{
	/* file descriptors */
	int			fdsock;
	/* receive buffer */
	int			RecvLength;
	int			RecvPointer;
	char		RecvBuffer[RXACT_BUFFER_SIZE];
	/* send buffer */
	int			SendPointer;
	char		SendBuffer[RXACT_BUFFER_SIZE];
} RxactPort;

typedef struct RxactPort RxactAgent;
typedef struct RxactPort RxactHandle;

extern int	rxact_listen(void);
extern int	rxact_connect(void);
extern int	rxact_getbyte(RxactPort *port);
extern int	rxact_pollbyte(RxactPort *port);
extern int	rxact_getmessage(RxactPort *port, StringInfo s, int maxlen);
extern int	rxact_getbytes(RxactPort *port, char *s, size_t len);
extern int	rxact_putmessage(RxactPort *port, char msgtype, const char *s, size_t len);
extern int	rxact_putbytes(RxactPort *port, const char *s, size_t len);
extern int	rxact_flush(RxactPort *port);
extern int	rxact_sendfds(RxactPort *port, pgsocket *fds, int count);
extern int	rxact_recvfds(RxactPort *port, pgsocket *fds, int count);
extern int	rxact_sendres(RxactPort *port, int res);
extern int	rxact_recvres(RxactPort *port);
extern int	rxact_sendpids(RxactPort *port, int *pids, int count);
extern int	rxact_recvpids(RxactPort *port, int **pids);
extern const char* rxact_get_sock_path(void);

extern void RemoteXactMgrMain(void) __attribute__((noreturn));

extern RxactHandle *GetRxactManagerHandle(void);
extern void RxactManagerCloseHandle(RxactHandle *handle);

extern void RecordRemoteXact(const char *gid, Oid *node_oids, int count, RemoteXactType type);
extern void RecordRemoteXactSuccess(const char *gid, RemoteXactType type);
extern void RecordRemoteXactFailed(const char *gid, RemoteXactType type);

#endif /* RXACT_MGR_H */
