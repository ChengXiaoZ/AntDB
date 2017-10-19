/*-------------------------------------------------------------------------
 *
 * remote_xact.h
 *	  ADB remote transaction system definitions
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2016 ADB Development Group
 *
 * src/include/access/remote_xact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTE_XACT_H
#define REMOTE_XACT_H

/*----------------------------------------
#define XLOG_XACT_COMMIT			0x00
#define XLOG_XACT_PREPARE			0x10
#define XLOG_XACT_ABORT				0x20
#define XLOG_XACT_COMMIT_PREPARED	0x30
#define XLOG_XACT_ABORT_PREPARED	0x40
#define XLOG_XACT_ASSIGNMENT		0x50
#define XLOG_XACT_COMMIT_COMPACT	0x60
----------------------------------------*/
#define XLOG_RXACT_PREPARE					0x70
#define XLOG_RXACT_PREPARE_SUCCESS			0x80
#define XLOG_RXACT_COMMIT					0x90
#define XLOG_RXACT_COMMIT_PREPARED			0xa0
#define XLOG_RXACT_COMMIT_PREPARED_SUCCESS	0xb0
#define XLOG_RXACT_ABORT					0xc0
#define XLOG_RXACT_ABORT_PREPARED			0xd0
#define XLOG_RXACT_ABORT_PREPARED_SUCCESS	0xe0

#define GIDSIZE 200

typedef struct xl_remote_xact
{
	TransactionId xid;			/* XID of remote xact */
	TimestampTz xact_time;		/* time of remote xact action */
	uint32		xinfo;			/* info flags */
	bool		implicit;		/* is implicit xact */
	bool		missing_ok;		/* OK if gid is not exists?
								   Just for COMMIT/ROLLBACK PREPARED */
   	char		dbname[NAMEDATALEN];	/* MyDatabase name */
	char		gid[GIDSIZE];	/* GID of remote xact */
	int			nnodes;			/* num of involved nodes */
	Oid			nodeIds[1];		/* involved nodes' IDs */
} xl_remote_xact;

#define MinSizeOfRemoteXact offsetof(xl_remote_xact, nodeIds)

extern void RecordRemoteXactCommit(int nnodes, Oid *nodeIds);
extern void RecordRemoteXactAbort(int nnodes, Oid *nodeIds);
extern void RecordRemoteXactPrepare(TransactionId xid,
									TimestampTz prepared_at,
									bool isimplicit,
									const char *gid,
									int nnodes,
									Oid *nodeIds);
extern void RecordRemoteXactCommitPrepared(TransactionId xid,
										   bool isimplicit,
										   bool missing_ok,
										   const char *gid,
										   int nnodes,
										   Oid *nodeIds);
extern void RecordRemoteXactAbortPrepared(TransactionId xid,
										  bool isimplicit,
										  bool missing_ok,
										  const char *gid,
										  int nnodes,
										  Oid *nodeIds);
extern void ReplayRemoteXact(void);
extern void remote_xact_redo(uint8 xl_info, xl_remote_xact *xlrec);
#endif /* REMOTE_XACT_H */

