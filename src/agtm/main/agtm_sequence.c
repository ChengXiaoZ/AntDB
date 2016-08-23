#include "postgres.h"

#include "agtm/agtm_msg.h"
#include "agtm/agtm_sequence.h"
#include "catalog/agtm_sequence.h"
#include "commands/sequence.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/resowner.h"

static void RespondSeqToClient(int64 seq_val, AGTM_ResultType type, StringInfo output);

static Datum  GetSeqKeyToDatumOid(char *seq_key);

static Datum prase_to_agtm_sequence_name(StringInfo message);

static	void parse_seqFullName_to_details(StringInfo message, char ** dbName, 
							char ** schemaName, char ** sequenceName);

static void
RespondSeqToClient(int64 seq_val, AGTM_ResultType type, StringInfo output)
{
	pq_sendint(output, type, 4);
	pq_sendbytes(output, (char *)&seq_val, sizeof(seq_val));
}

static Datum
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

StringInfo
ProcessNextSeqCommand(StringInfo message, StringInfo output)
{
	Datum seq_val_datum;
	int64 seq_val;
	Datum seq_name_to_oid;

	seq_name_to_oid= prase_to_agtm_sequence_name(message);
	pq_getmsgend(message);

	seq_val_datum = DirectFunctionCall1(nextval_oid, seq_name_to_oid);
	seq_val = DatumGetInt64(seq_val_datum);

	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_SEQUENCE_GET_NEXT_RESULT, output);

	return output;
}

StringInfo
ProcessCurSeqCommand(StringInfo message, StringInfo output)
{
	Datum seq_val_datum;
	int64 seq_val;
	Datum seq_name_to_oid;

	seq_name_to_oid= prase_to_agtm_sequence_name(message);
	pq_getmsgend(message);

	/*if nextval function never called in this session and before currval function called,
	 *curral_oid fuction will ereport(error) 
	 */
	seq_val_datum = DirectFunctionCall1(currval_oid, seq_name_to_oid);
	seq_val = DatumGetInt64(seq_val_datum);

	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_MSG_SEQUENCE_GET_CUR_RESULT, output);

	return output;
}

StringInfo
PorcessLastSeqCommand(StringInfo message, StringInfo output)
{
	Datum seq_val_datum;
	int64 seq_val;
	char* dbName = NULL;
	char* schemaName = NULL;
	char* sequenceName = NULL;

	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	pq_getmsgend(message);

	/*if nextval function never called in this session and before currval function called,
	 *curral_oid fuction will ereport(error) 
	 */
	seq_val_datum = DirectFunctionCall1(lastval, (Datum)0);
	seq_val = DatumGetInt64(seq_val_datum);

	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_SEQUENCE_GET_LAST_RESULT, output);

	pfree(sequenceName);
	pfree(dbName);
	pfree(schemaName);
	return output;
}

StringInfo
ProcessSetSeqCommand(StringInfo message, StringInfo output)
{
	int64 seq_nextval;
	bool  iscalled;
	int64 seq_val;
	Datum seq_val_datum;
	Datum seq_name_to_oid;

	seq_name_to_oid= prase_to_agtm_sequence_name(message);
	memcpy(&seq_nextval,pq_getmsgbytes(message, sizeof(seq_nextval)),
		sizeof (seq_nextval));	
	iscalled = pq_getmsgbyte(message);
	pq_getmsgend(message);

	seq_val_datum = DirectFunctionCall3(setval3_oid,
		seq_name_to_oid, seq_nextval, iscalled);

	seq_val = DatumGetInt64(seq_val_datum);

	/* Respond to the client */
	RespondSeqToClient(seq_val,AGTM_SEQUENCE_SET_VAL_RESULT, output);

	return output;
}

static	void parse_seqFullName_to_details(StringInfo message, char ** dbName, 
							char ** schemaName, char ** sequenceName)
{
	int	 sequenceSize = 0;
	int  dbNameSize = 0;
	int  schemaSize = 0;

	sequenceSize = pq_getmsgint(message, sizeof(sequenceSize));
	*sequenceName = pnstrdup(pq_getmsgbytes(message, sequenceSize), sequenceSize);
	if(sequenceSize == 0 || *sequenceName == NULL)
		ereport(ERROR,
			(errmsg("sequence name is null")));

	dbNameSize = pq_getmsgint(message, sizeof(dbNameSize));
	*dbName = pnstrdup(pq_getmsgbytes(message, dbNameSize), dbNameSize);
	if(dbNameSize == 0 ||  *dbName == NULL)
		ereport(ERROR,
			(errmsg("sequence database name is null")));

	schemaSize = pq_getmsgint(message, sizeof(schemaSize));
	*schemaName = pnstrdup(pq_getmsgbytes(message, schemaSize), schemaSize);
	if(schemaSize == 0 ||  *schemaName == NULL)
		ereport(ERROR,
			(errmsg("sequence schemaName name is null")));
}

static Datum
prase_to_agtm_sequence_name(StringInfo message)
{
	bool  isExist = FALSE;
	char* dbName = NULL;
	char* schemaName = NULL;
	char* sequenceName = NULL;	
	StringInfoData	buf;
	Oid			lineOid;
	char *	agtmSeqName = NULL;
	int		oid;

	initStringInfo(&buf);
	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	isExist = SequenceIsExist(dbName, schemaName, sequenceName);
	if(!isExist)
		ereport(ERROR,
			(errmsg("%s, %s, %s not exist on agtm !",dbName, schemaName, sequenceName)));
	lineOid = SequenceSystemClassOid(dbName, schemaName, sequenceName);

	appendStringInfo(&buf, "%s", "seq");
	appendStringInfo(&buf, "%u", lineOid);
	agtmSeqName = pnstrdup(buf.data, buf.len);
	pfree(buf.data);

	oid = GetSeqKeyToDatumOid(agtmSeqName);

	pfree(agtmSeqName);
	pfree(sequenceName);
	pfree(dbName);
	pfree(schemaName);
	return oid ;
}

