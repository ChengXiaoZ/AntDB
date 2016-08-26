#ifndef RXACT_MGR_H
#define RXACT_MGR_H

#include "lib/stringinfo.h"

typedef enum RemoteXactType
{
	RX_PREPARE = 1
	,RX_COMMIT
	,RX_ROLLBACK
}RemoteXactType;

extern void RemoteXactMgrMain(void) __attribute__((noreturn));

extern void RecordRemoteXact(const char *gid, Oid *node_oids, int count, RemoteXactType type);
extern void RecordRemoteXactSuccess(const char *gid, RemoteXactType type);
extern void RecordRemoteXactFailed(const char *gid, RemoteXactType type);
extern void RecordRemoteXactChange(const char *gid, RemoteXactType type);
extern void RemoteXactReloadNode(void);
extern void DisconnectRemoteXact(void);
extern void rlog_desc(const char *file_name) __attribute__((noreturn));;

#endif /* RXACT_MGR_H */
