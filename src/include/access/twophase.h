/*-------------------------------------------------------------------------
 *
 * twophase.h
 *	  Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"

/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData *GlobalTransaction;

/* GUC variable */
extern int	max_prepared_xacts;

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern PGPROC *TwoPhaseGetDummyProc(TransactionId xid);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid);

#ifdef ADB
extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
				TimestampTz prepared_at,
				Oid owner, Oid databaseid,
				int nodecnt, Oid *nodeIds, bool isimplicit);
#else
extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
				TimestampTz prepared_at,
				Oid owner, Oid databaseid);
#endif

extern void EndPrepare(GlobalTransaction gxact);
extern void StartPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);

extern TransactionId PrescanPreparedTransactions(TransactionId **xids_p,
							int *nxids_p);
extern void StandbyRecoverPreparedTransactions(bool overwriteOK);
extern void RecoverPreparedTransactions(void);

extern void RecreateTwoPhaseFile(TransactionId xid, void *content, int len);
extern void RemoveTwoPhaseFile(TransactionId xid, bool giveWarning);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

extern void FinishPreparedTransaction(const char *gid, bool isCommit);
#if defined(ADB) || defined(AGTM)
extern void FinishPreparedTransactionExt(const char *gid,
										 bool isCommit,
#ifdef ADB
										 bool isRemoteInit,
#endif
										 bool isMissingOK);
#endif /* ADB */
#endif   /* TWOPHASE_H */
