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

#define IsUnderRemoteXact()	(IS_PGXC_COORDINATOR && !IsConnFromCoord())

extern void RemoteXactCommit(int nnodes, Oid *nodeIds);
extern void RemoteXactAbort(int nnodes, Oid *nodeIds, bool normal);
extern void StartFinishPreparedRxact(const char *gid,
									 int nnodes,
									 Oid *nodeIds,
									 bool isImplicit,
									 bool isCommit);
extern void EndFinishPreparedRxact(const char *gid,
								   int nnodes,
								   Oid *nodeIds,
								   bool isMissingOK,
								   bool isCommit);
#endif /* REMOTE_XACT_H */

