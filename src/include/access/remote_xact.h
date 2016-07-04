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

#include "access/xact.h"
#include "datatype/timestamp.h"

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
#define XLOG_RXACT_COMMIT_PREPARED			0x80
#define XLOG_RXACT_ABORT_PREPARED			0x90
#define XLOG_RXACT_PREPARE_SUCCESS			0xa0
#define XLOG_RXACT_COMMIT_PREPARED_SUCCESS	0xb0
#define XLOG_RXACT_ABORT_PREPARED_SUCCESS	0xc0

#define GIDSIZE 200
#define IsUnderRemoteXact()	(IS_PGXC_COORDINATOR && !IsConnFromCoord())

typedef char xl_remote_binary;

typedef struct remote_node
{
	Oid			nodeId;
	int			nodePort;
	char		nodeHost[NAMEDATALEN];
} RemoteNode;

typedef struct xl_remote_success
{
	TransactionId 	xid;			/* XID of remote xact */
	char			gid[1];			/* GID of remote xact */
} xl_remote_success;

#define MinSizeOfRemoteSuccess offsetof(xl_remote_success, gid)

typedef struct xl_remote_xact
{
	TransactionId	xid;					/* XID of remote xact */
	int				nnodes;					/* num of involved nodes */
	TimestampTz 	xact_time;				/* time of remote xact action */
	uint8			xinfo;					/* info flags */
	bool			implicit;				/* is implicit xact */
	bool			missing_ok;				/* OK if gid is not exists?
											   Just for COMMIT/ROLLBACK PREPARED */
	char			gid[GIDSIZE];			/* GID of remote xact */
	char			dbname[NAMEDATALEN];	/* MyDatabase name */
	char			user[NAMEDATALEN];		/* User name */
	RemoteNode		rnodes[1];				/* involved nodes' IDs */
} xl_remote_xact;

#define MinSizeOfRemoteXact offsetof(xl_remote_xact, rnodes)

extern void MakeUpRemoteXactBinary(StringInfo buf,
								   uint8 info,
								   TransactionId xid,
								   TimestampTz xact_time,
								   bool isimplicit,
								   bool missing_ok,
								   const char *gid,
								   int nnodes,
								   Oid *nodeIds);
extern xl_remote_xact *DeparseRemoteXactBinary(xl_remote_binary *rbinary,
									int extend, int *offset);
extern void RemoteXactCommit(int nnodes, Oid *nodeIds);
extern void RemoteXactAbort(int nnodes, Oid *nodeIds);
extern void RecordRemoteXactPrepare(TransactionId xid,
									TimestampTz prepared_at,
									bool isimplicit,
									const char *gid,
									int nnodes,
									Oid *nodeIds);
extern void RemoteXactCommitPrepared(TransactionId xid,
									 bool isimplicit,
									 bool missing_ok,
									 const char *gid,
									 int nnodes,
									 Oid *nodeIds);
extern void RemoteXactAbortPrepared(TransactionId xid,
									bool isimplicit,
									bool missing_ok,
									const char *gid,
									int nnodes,
									Oid *nodeIds);
/*---------------------remote xact redo interface--------------------- */
extern void ReplayRemoteXact(void);
extern void rxact_redo_commit_prepared(xl_xact_commit *xlrec,
									   TransactionId xid,
									   XLogRecPtr lsn);
extern void rxact_redo_abort_prepared(xl_xact_abort *xlrec,
									  TransactionId xid);
extern void rxact_redo_prepare(xl_remote_binary *rbinary);
extern void rxact_redo_success(uint8 info,
							   xl_remote_success *xlres);
#endif /* REMOTE_XACT_H */

