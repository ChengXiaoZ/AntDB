#ifndef RXACT_MGR_H
#define RXACT_MGR_H

#include "lib/stringinfo.h"

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

extern int RemoteXactMgrMain(void);

extern RxactHandle *GetRxactManagerHandle(void);
extern void RxactManagerCloseHandle(RxactHandle *handle);

extern int RecordRemoteXact(TransactionId xid,
							const char *gid,
							uint8 info,
							StringInfo rbinary);

extern int RecordRemoteXactSuccess(TransactionId xid,
								   const char *gid,
								   uint8 info);

extern int RecordRemoteXactCancel(TransactionId xid,
								  const char *gid,
								  uint8 info);

#endif /* RXACT_MGR_H */
