#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_protocol.h"
#include "agtm/agtm_transaction.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/bitmapset.h"
#include "storage/lock.h"
#include "storage/procarray.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef struct XactLockInfo
{
	TransactionId xid;
	LOCKMODE mode;
	Bitmapset *bms_pq;
}XactLockInfo;

static HTAB *htab_xact_lock = NULL;

static void create_htab_xact_lock(void);
static void inc_xact_lock(XactLockInfo *info, LOCKMODE mode, int pqid);
static bool dec_xact_lock(XactLockInfo *info, LOCKMODE mode, int pqid);

StringInfo ProcessGetGXIDCommand(StringInfo message, StringInfo output)
{
	TransactionId	xid;
	bool			isSubXact;

	isSubXact = pq_getmsgbyte(message);
	pq_getmsgend(message);
	if (IsTransactionState())
	{
		xid = GetCurrentTransactionId();
		elog(LOG, "AGTM return current xid %u.", xid);
	} else
	{
		xid = GetNewTransactionId(isSubXact);
		elog(LOG, "AGTM return new xid %u.", xid);
	}

	/* Respond to the client */
	pq_sendint(output, AGTM_GET_GXID_RESULT, 4);
	pq_sendbytes(output, (char *)&xid, sizeof(xid));

	return output;
}

StringInfo ProcessGetTimestamp(StringInfo message, StringInfo output)
{
	Timestamp timestamp;

	pq_getmsgend(message);
	timestamp = GetCurrentTransactionStartTimestamp();

	/* Respond to the client */
	pq_sendint(output, AGTM_GET_TIMESTAMP_RESULT, 4);
	pq_sendbytes(output, (char *)&timestamp, sizeof(timestamp));

	return output;
}

StringInfo ProcessGetSnapshot(StringInfo message, StringInfo output)
{
	GlobalSnapshot snapshot;

	pq_getmsgend(message);
	snapshot = (GlobalSnapshot)GetTransactionSnapshot();

	/* Respond to the client */
	pq_sendint(output, AGTM_SNAPSHOT_GET_RESULT, 4);

	pq_sendbytes(output, (char *)&snapshot->xmin, sizeof (TransactionId));
	pq_sendbytes(output, (char *)&snapshot->xmax, sizeof (TransactionId));

	pq_sendint(output, snapshot->xcnt, sizeof (int));
	pq_sendbytes(output, (char *)snapshot->xip,
				 sizeof(TransactionId) * snapshot->xcnt);

	pq_sendint(output, snapshot->subxcnt, sizeof (int));
	pq_sendbytes(output, (char *)snapshot->subxip,
				 sizeof(TransactionId) * snapshot->subxcnt);

	pq_sendbytes(output, (char *)&snapshot->suboverflowed, sizeof(snapshot->suboverflowed));
	pq_sendbytes(output, (char *)&snapshot->takenDuringRecovery, sizeof(snapshot->takenDuringRecovery));
	/*pq_sendbytes(output, (char *)&snapshot->copied, sizeof(snapshot->copied));*/
	pq_sendbytes(output, (char *)&snapshot->curcid, sizeof(snapshot->curcid));
	pq_sendbytes(output, (char *)&snapshot->active_count, sizeof(snapshot->active_count));
	pq_sendbytes(output, (char *)&snapshot->regd_count, sizeof(snapshot->regd_count));

	return output;
}

StringInfo ProcessXactLockTableWait(StringInfo message, StringInfo output)
{
	TransactionId xid;
	LOCKTAG		tag;
	for(;;)
	{
		xid = (TransactionId)pq_getmsgint(message, sizeof(TransactionId));
		if(!TransactionIdIsValid(xid))
			break;

		SET_LOCKTAG_TRANSACTION(tag, xid);
		(void) LockAcquire(&tag, ShareLock, false, false);

		LockRelease(&tag, ShareLock, false);

		if (!TransactionIdIsInProgress(xid))
			break;
	}
	while(TransactionIdIsValid(xid))
		xid = (TransactionId)pq_getmsgint(message, sizeof(xid));

	pq_getmsgend(message);

	return NULL;
}

StringInfo ProcessLockTransaction(StringInfo message, StringInfo output)
{
	XactLockInfo *lock_info;
	TransactionId xid;
	LOCKMODE mode;
	bool is_lock;
	bool found;

	xid = (TransactionId)pq_getmsgint(message, sizeof(TransactionId));
	mode = (LOCKMODE)pq_getmsgbyte(message);
	is_lock = (bool)pq_getmsgbyte(message);
	pq_getmsgend(message);

	if(htab_xact_lock == NULL)
		create_htab_xact_lock();

	if(is_lock)
	{
		lock_info = hash_search(htab_xact_lock, &xid, HASH_ENTER, &found);
		if(!found)
		{
			lock_info->xid = xid;
			lock_info->bms_pq = NULL;
			lock_info->mode = NoLock;
		}
		Assert(lock_info->xid == xid);
		inc_xact_lock(lock_info, mode, pq_get_id());
	}else
	{
		lock_info = hash_search(htab_xact_lock, &xid, HASH_FIND, &found);
		if(!found)
			ereport(ERROR, (errmsg("transaction %u not locked", xid)));

		if(dec_xact_lock(lock_info, mode, pq_get_id()) == true)
		{
			bms_free(lock_info->bms_pq);
			hash_search(htab_xact_lock, &xid, HASH_REMOVE, &found);
		}
	}

	return NULL;
}

StringInfo ProcessXactLockReleaseAll(StringInfo message, StringInfo output)
{
	pq_getmsgend(message);
	agtm_AtXactNodeClose(pq_get_id());
	return NULL;
}

void agtm_AtXactNodeClose(int pq_id)
{
	XactLockInfo *info;
	HASH_SEQ_STATUS status;
	if(htab_xact_lock == NULL)
		return;

re_clean_:
	hash_seq_init(&status, htab_xact_lock);
	while((info=hash_seq_search(&status)) != NULL)
	{
		if(dec_xact_lock(info, info->xid, pq_id))
		{
			TransactionId xid = info->xid;
			bms_free(info->bms_pq);
			info->bms_pq = NULL;
			hash_seq_term(&status);
			hash_search(htab_xact_lock, &xid, HASH_REMOVE, NULL);
			goto re_clean_;
		}
	}
}

static void create_htab_xact_lock(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(TransactionId);
	hctl.entrysize = sizeof(XactLockInfo);
	hctl.hcxt = TopMemoryContext;
	htab_xact_lock = hash_create("AGTM TransactionId lock info", 97, &hctl
			, HASH_ELEM | HASH_CONTEXT);
}

static void inc_xact_lock(XactLockInfo *info, LOCKMODE mode, int pqid)
{
	MemoryContext oldcontext;
	AssertArg(info && pqid != INVALID_PQ_ID);

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	info->bms_pq = bms_add_member(info->bms_pq, pqid);
	MemoryContextSwitchTo(oldcontext);
	if(bms_membership(info->bms_pq) == BMS_SINGLETON)
	{
		/* first lock */
		XactLockInfo volatile *tmp = info;
		volatile int tmp_pqid = pqid;
		LOCKTAG tag;
		PG_TRY();
		{
			SET_LOCKTAG_TRANSACTION(tag, info->xid);
			LockAcquire(&tag, mode, false, false);
		}PG_CATCH();
		{
			tmp->bms_pq = bms_del_member(tmp->bms_pq, tmp_pqid);
			PG_RE_THROW();
		}PG_END_TRY();
		info->mode = mode;
	}else if(info->mode != mode)
	{
		info->bms_pq = bms_del_member(info->bms_pq, pqid);
		ereport(ERROR, (errmsg("lock transaction %u use mode %d, but locked use %d"
			, (unsigned)info->xid, (int)mode, (int)(info->mode))));
	}
}

/*
 * return is released
 */
static bool dec_xact_lock(XactLockInfo *info, LOCKMODE mode, int pqid)
{
	AssertArg(info && pqid != INVALID_PQ_ID);
	if(info->mode != mode)
	{
		ereport(ERROR, (errmsg("unlock transaction %u use mode %d, but locked use %d"
			, (unsigned)info->xid, (int)mode, (int)(info->mode))));
	}

	info->bms_pq = bms_del_member(info->bms_pq, pqid);
	if(bms_is_empty(info->bms_pq))
	{
		LOCKTAG tag;
		SET_LOCKTAG_TRANSACTION(tag, info->xid);
		LockRelease(&tag, mode, false);
		return true;
	}
	return false;
}
