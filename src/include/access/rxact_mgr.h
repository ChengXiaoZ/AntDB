#ifndef RXACT_MGR_H
#define RXACT_MGR_H

#include "access/xlog.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

typedef enum RemoteXactType
{
	RX_PREPARE = 1
	,RX_COMMIT
	,RX_ROLLBACK
}RemoteXactType;

typedef struct RxactTransactionInfo
{
	char gid[NAMEDATALEN];	/* 2pc id */
	Oid *remote_nodes;		/* all remote nodes, include AGTM */
	bool *remote_success;	/* remote execute success ? */
	int count_nodes;		/* count of remote nodes */
	Oid db_oid;				/* transaction database Oid */
	RemoteXactType type;	/* remote 2pc type */
	bool failed;			/* backend do it failed ? */
}RxactTransactionInfo;

extern void RemoteXactMgrMain(void) __attribute__((noreturn));

extern void RecordRemoteXact(const char *gid, Oid *node_oids, int count, RemoteXactType type);
extern void RecordRemoteXactSuccess(const char *gid, RemoteXactType type);
extern void RecordRemoteXactFailed(const char *gid, RemoteXactType type);
extern void RecordRemoteXactChange(const char *gid, RemoteXactType type);
extern void RemoteXactReloadNode(void);
extern void DisconnectRemoteXact(void);
/* return list of RxactTransactionInfo */
extern List *RxactGetRunningList(void);
extern void FreeRxactTransactionInfo(RxactTransactionInfo *rinfo);
extern void FreeRxactTransactionInfoList(List *list);

/* xlog interfaces */
extern void rxact_redo(XLogRecPtr lsn, XLogRecord *record);
extern void rxact_desc(StringInfo buf, uint8 xl_info, char *rec);
extern void rxact_xlog_startup(void);
extern void rxact_xlog_cleanup(void);
extern void CheckPointRxact(int flags);

#endif /* RXACT_MGR_H */
