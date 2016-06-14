#include "postgres.h"

#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_sequence.h"
#include "agtm/agtm_transaction.h"
#include "agtm/agtm_utils.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/elog.h"

void
ProcessAGtmCommand(StringInfo input_message)
{
	AGTM_MessageType mtype;
	mtype = pq_getmsgint(input_message, sizeof (AGTM_MessageType));

	ereport(DEBUG1, 
		(errmsg("[ pid=%d] Process Command mtype = %s (%d).",
		MyProcPid,gtm_util_message_name(mtype), (int)mtype)));
	switch (mtype)
	{
		case AGTM_MSG_GET_GXID:
			ProcessGetGXIDCommand(input_message);
			break;

		case AGTM_MSG_GET_TIMESTAMP:
			ProcessGetTimestamp(input_message);
			break;

		case AGTM_MSG_SNAPSHOT_GET:
			ProcessGetSnapshot(input_message);
			break;

		case AGTM_MSG_SEQUENCE_SET_VAL:
			StartTransactionCommand();
			PG_TRY();
			{
				ProcessSetSeqCommand(input_message);
			} PG_CATCH();
			{
				CommitTransactionCommand();
				PG_RE_THROW();
			} PG_END_TRY();
			CommitTransactionCommand();			
			break;

		case AGTM_MSG_SEQUENCE_GET_NEXT:
			StartTransactionCommand();
			PG_TRY();
			{
				ProcessNextSeqCommand(input_message);
			} PG_CATCH();
			{
				CommitTransactionCommand();
				PG_RE_THROW();
			} PG_END_TRY();
			CommitTransactionCommand();
			break;
			
		case AGTM_MSG_SEQUENCE_GET_CUR:
			ProcessCurSeqCommand(input_message);
			break;

		case AGTM_MSG_SEQUENCE_GET_LAST:
			PorcessLastSeqCommand(input_message);
			break;

		case AGTM_MSG_XACT_LOCK_TABLE_WAIT:
			ProcessXactLockTableWait(input_message);
			break;

		case AGTM_MSG_LOCK_TRANSACTION:
			ProcessLockTransaction(input_message);
			break;

		default:
			ereport(FATAL,
					(EPROTO,
					 errmsg("[ pid=%d] invalid frontend message type %d",
					 MyProcPid, mtype)));
			break;
	}
}
