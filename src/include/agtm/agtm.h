#ifndef AGTM_H
#define AGTM_H

#include "postgres.h"
#include "miscadmin.h"

#include "agtm/agtm_protocol.h"
#include "catalog/pg_database.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "tcop/dest.h"
#include "utils/snapshot.h"

#ifdef  AGTM_DEBUG
#define CLIENT_AGTM_TIMEOUT 3600
#else
#define CLIENT_AGTM_TIMEOUT 20
#endif

#define IsNormalDatabase()	(MyDatabaseId != InvalidOid &&		\
							 MyDatabaseId != TemplateDbOid)

#define IsUnderAGTM()		((isPGXCCoordinator || isPGXCDataNode) &&	\
							  IsUnderPostmaster &&						\
							  IsNormalDatabase() &&						\
							  IsNormalProcessingMode())

/*
 * get gixd from AGTM
 */
extern TransactionId agtm_GetGlobalTransactionId(bool isSubXact);

/*
 * get Snapshot info from AGTM
 */
extern Snapshot agtm_GetGlobalSnapShot(Snapshot snapshot);

/*
 * get next Sequence from AGTM
 */
extern AGTM_Sequence agtm_GetSeqNextVal(const char *seqname);

/*
 * get current Sequence from AGTM
 */
extern AGTM_Sequence agtm_GetSeqCurrVal(const char *seqname);

/*
 * get last Sequence from AGTM
 */
extern AGTM_Sequence agtm_GetSeqLastVal(const char *seqname);

/*
 * set Sequence current value
 */

extern AGTM_Sequence agtm_SetSeqVal(const char *seqname, AGTM_Sequence nextval);

extern AGTM_Sequence agtm_SetSeqValCalled(const char *seqname, AGTM_Sequence nextval,
									bool iscalled);

/*
 * locks
 */
extern void agtm_XactLockTableWait(TransactionId xid);

extern void agtm_LockTransactionId(TransactionId xid, char lock_type, bool is_lock);

extern void agtm_XactLockReleaseAll(bool no_error);

/*
 * get timestamp from AGTM
 */
extern Timestamp agtm_GetTimestamptz(void);

/*--------------------------------------2 pc API--------------------------------------*/

/*
 * begin transaction on AGTM
 */
extern void agtm_BeginTransaction(void);

extern void agtm_BeginTransaction_ByDBname(const char *dbname);

/*
 * prepare commit transaction on AGTM
 */
extern void agtm_PrepareTransaction(const char *prepared_gid);

extern void agtm_PrepareTransaction_ByDBname(const char *prepared_gid, const char *dbname);

/*
 * commit transcation on AGTM
 */
extern void agtm_CommitTransaction(const char *prepared_gid, bool missing_ok);

extern void agtm_CommitTransaction_ByDBname(const char *prepared_gid, bool missing_ok, const char *dbname);

/*
 * rollback transacton on AGTM
 */
extern void agtm_AbortTransaction(const char *prepared_gid, bool missing_ok);

extern void agtm_AbortTransaction_ByDBname(const char *prepared_gid, bool missing_ok, const char *dbname);

/*
 * create/drop/alter sequence on agtm
 */
extern void agtm_sequence(const char *dmlSeq);

/*
 * create/drop/alter databae on agtm
 */
extern void agtm_Database(const char *dmlDatabase);

/*
 * create/drop/alter tablespace on agtm
 */
extern void agtm_TableSpace(const char *dmlTableSpace);

/*
 * create/drop/alter schema on agtm
 */
extern void agtm_Schema(const char *dmlSchema);

/*
 * create/drop/alter user on agtm
 */
extern void agtm_User(const char *dmlUser);

/* 
 * process command
 */
void ProcessAGtmCommand(StringInfo input_message, CommandDest dest);

#endif
