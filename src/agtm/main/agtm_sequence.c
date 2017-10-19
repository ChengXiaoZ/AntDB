#include "agtm/agtm_msg.h"
#include "agtm/agtm_sequence.h"
#include "commands/sequence.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/resowner.h"

static void RespondSeqToClient(int64 seq_val, AGTM_ResultType type);
static void GetSeqKey(StringInfo message, char **seq_key);
static int  GetSeqKeyToDatumOid(char *seq_key);

static void
RespondSeqToClient(int64 seq_val, AGTM_ResultType type)
{
	StringInfoData buf;
	
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, type, 4);
	pq_sendbytes(&buf, (char *)&seq_val, sizeof(seq_val));
	pq_endmessage(&buf);
	pq_flush();
}

static void
GetSeqKey(StringInfo message, char **seq_key)
{
	int   seq_key_len = 0;
	seq_key_len = pq_getmsgint(message, sizeof(seq_key_len));
	Assert(seq_key_len > 0);
	*seq_key = (char *)palloc0(seq_key_len + 1);
	memcpy(*seq_key,pq_getmsgbytes(message, seq_key_len),seq_key_len);
	Assert(seq_key && seq_key[0]);
}

static int
GetSeqKeyToDatumOid(char *seq_key)
{
	Datum seq_name_to_oid;
	seq_name_to_oid = DirectFunctionCall1(regclassin, CStringGetDatum(seq_key));
	if(ObjectIdGetDatum(InvalidOid) == seq_name_to_oid)
		ereport(ERROR,
			(errmsg("convert sequence key to oid invalid, sequence key : %s",
				seq_key)));
	return seq_name_to_oid;
}

void
ProcessNextSeqCommand(StringInfo message)
{
	char *seq_key = NULL;
	Datum seq_val_datum;
	int64 seq_val;
	Datum seq_name_to_oid;
	
	GetSeqKey(message,&seq_key);
	pq_getmsgend(message);
	
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForAGTM");
	
	seq_name_to_oid = GetSeqKeyToDatumOid(seq_key);	
	seq_val_datum = DirectFunctionCall1(nextval_oid, seq_name_to_oid);
	seq_val = DatumGetInt64(seq_val_datum);
	
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	CurrentResourceOwner = NULL;
	
	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_SEQUENCE_GET_NEXT_RESULT);

	pfree(seq_key);
}

void
ProcessCurSeqCommand(StringInfo message)
{
	char *seq_key = NULL;
	Datum seq_val_datum;
	int64 seq_val;
	Datum seq_name_to_oid;

	GetSeqKey(message,&seq_key);
	pq_getmsgend(message);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForAGTM");
	
	seq_name_to_oid = GetSeqKeyToDatumOid(seq_key);
		
	/*if nextval function never called in this session and before currval function called,
	 *curral_oid fuction will ereport(error) 
	 */
	seq_val_datum = DirectFunctionCall1(currval_oid, seq_name_to_oid);
	seq_val = DatumGetInt64(seq_val_datum);

	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	CurrentResourceOwner = NULL;
	
	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_MSG_SEQUENCE_GET_CUR_RESULT);

	pfree(seq_key);
}

void
PorcessLastSeqCommand(StringInfo message)
{
	char *seq_key = NULL;
	Datum seq_val_datum;
	int64 seq_val;

	GetSeqKey(message,&seq_key);
	pq_getmsgend(message);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForAGTM");

	/*if nextval function never called in this session and before currval function called,
	 *curral_oid fuction will ereport(error) 
	 */
	seq_val_datum = DirectFunctionCall1(lastval, (Datum)0);
	seq_val = DatumGetInt64(seq_val_datum);
	
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	CurrentResourceOwner = NULL;
	
	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_SEQUENCE_GET_LAST_RESULT);
	
	pfree(seq_key);
}

void
ProcessSetSeqCommand(StringInfo message)
{
	char *seq_key = NULL;
	int64 seq_nextval;
	bool  iscalled;
	int64 seq_val;
	Datum seq_val_datum;
	Datum seq_name_to_oid;

	GetSeqKey(message,&seq_key);
	memcpy(&seq_nextval,pq_getmsgbytes(message, sizeof(seq_nextval)),
		sizeof (seq_nextval));	
	iscalled = pq_getmsgbyte(message);
	pq_getmsgend(message);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForAGTM");
	
	seq_name_to_oid = GetSeqKeyToDatumOid(seq_key);
	seq_val_datum = DirectFunctionCall3(setval3_oid,
		seq_name_to_oid, seq_nextval, iscalled);
	
	seq_val = DatumGetInt64(seq_val_datum);

	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	CurrentResourceOwner = NULL;
	
	/* Respond to the client */
	RespondSeqToClient(seq_val,AGTM_SEQUENCE_SET_VAL_RESULT);	

	pfree(seq_key);
}

