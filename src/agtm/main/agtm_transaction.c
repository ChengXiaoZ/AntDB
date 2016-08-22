#include "postgres.h"

#include "access/clog.h"
#include "access/hash.h"
#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_protocol.h"
#include "agtm/agtm_transaction.h"
#include "catalog/agtm_sequence.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/snapmgr.h"

static List* parse_string_to_seqOption(StringInfo strOption, RangeVar *var);
static	void parse_seqFullName_to_details(StringInfo message, char ** dbName, 
							char ** schemaName, char ** sequenceName);
static	void free_Sequence_init_list(List * option_list);

typedef enum AgtmNodeTag
{
	T_AgtmInvalid = 0,
	T_AgtmInteger,
	T_AgtmFloat,
	T_AgtmString,
	T_AgtmBitString,
	T_AgtmNull
} AgtmNodeTag;

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
	Snapshot snapshot;

	pq_getmsgend(message);
	snapshot = GetTransactionSnapshot();

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

StringInfo ProcessGetXactStatus(StringInfo message, StringInfo output)
{
	TransactionId	xid;
	XidStatus		xid_status;
	XLogRecPtr		xid_lsn;

	xid = pq_getmsgint(message, sizeof(xid));
	pq_getmsgend(message);

	xid_status = TransactionIdGetStatus(xid, &xid_lsn);

	/* Respond to the client */
	pq_sendint(output, AGTM_GET_XACT_STATUS_RESULT, 4);
	pq_sendbytes(output, (char *)&xid_status, sizeof(XidStatus));
	pq_sendbytes(output, (char *)&xid_lsn, sizeof(XLogRecPtr));

	return output;
}

StringInfo
ProcessSequenceInit(StringInfo message, StringInfo output)
{
	List *option;	
	/* system table agtm_sequence info */
	Oid			lineOid;
	char* dbName = NULL;
	char* schemaName = NULL;
	char* sequenceName = NULL;
	bool  isExist = FALSE;
	RangeVar * rangeVar = NULL;
	CreateSeqStmt * seqStmt = NULL;
	StringInfoData	buf;

	initStringInfo(&buf);
	rangeVar = palloc0(sizeof(RangeVar));

	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	option = parse_string_to_seqOption(message, rangeVar);
	pq_getmsgend(message);	
	/* check info in system table */
	isExist = SequenceIsExist(dbName, schemaName, sequenceName);
	if(isExist)
		ereport(ERROR,
			(errmsg("%s, %s, %s exist!",dbName, schemaName, sequenceName)));

	/* insert database schema sequence into agtm_sequence */
	lineOid = AddAgtmSequence(dbName,schemaName,sequenceName);

	seqStmt = palloc0(sizeof(CreateSeqStmt));
	seqStmt->type = T_CreateSeqStmt;
	rangeVar->type = T_RangeVar;
	rangeVar->catalogname = NULL;
	rangeVar->schemaname = NULL;
	appendStringInfo(&buf, "%s", "seq");
	appendStringInfo(&buf, "%u", lineOid);
	rangeVar->relname = buf.data;
	rangeVar->alias = NULL;

	seqStmt->sequence = rangeVar;
	seqStmt->options = option;
	DefineSequence(seqStmt);

	pfree(rangeVar);
	pfree(seqStmt);
	pfree(buf.data);

	pfree(dbName);
	pfree(schemaName);
	pfree(sequenceName);

	free_Sequence_init_list(option);
	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_INIT_RESULT, 4);
	return output;
}

StringInfo
ProcessSequenceDrop(StringInfo message, StringInfo output)
{
	Oid	  oid;
	char* dbName = NULL;
	bool  isExist = FALSE;
	char* schemaName = NULL;
	char* sequenceName = NULL;	
	DropStmt *drop = NULL;
	RangeVar * rangeVar = NULL;
	StringInfoData	buf;
	List	*rangValList = NULL;
	
	initStringInfo(&buf);
	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	pq_getmsgend(message);
	/* check info in system table */
	isExist = SequenceIsExist(dbName, schemaName, sequenceName);
	if(!isExist)
		ereport(ERROR,
			(errmsg("%s, %s, %s not exist on agtm !",dbName, schemaName, sequenceName)));
	/* delete sequence on agtm */
	oid = DelAgtmSequence(dbName, schemaName, sequenceName);

	drop = palloc0(sizeof(DropStmt));
	rangeVar = palloc0(sizeof(RangeVar));

	drop->type = T_DropStmt;
	drop->removeType = OBJECT_SEQUENCE;
	drop->behavior = DROP_RESTRICT;
	drop->missing_ok = 0;
	drop->concurrent = 0;

	rangeVar->type = T_RangeVar;
	rangeVar->catalogname = NULL;
	rangeVar->schemaname = NULL;
	appendStringInfo(&buf, "%s", "seq");
	appendStringInfo(&buf, "%u", oid);
	rangeVar->relname = buf.data;
	rangeVar->inhOpt = INH_DEFAULT;
	rangeVar->relpersistence = 'p';

	rangValList = lappend(rangValList, makeString(buf.data));
	drop->objects = lappend(drop->objects, (void*)rangValList);

	RemoveRelations((void *)drop);

	pfree(dbName);
	pfree(schemaName);
	pfree(sequenceName);

	pfree(rangeVar);
	pfree(drop);
	pfree(buf.data);

	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_DROP_RESULT, 4);
	return output;
}

static	void free_Sequence_init_list(List * option_list)
{
	ListCell   *option;
	if(option_list == NULL)
		return;

	foreach(option, option_list)
	{
		DefElem    *defel = (DefElem *) lfirst(option);
		if(defel->defnamespace)
		{
			pfree(defel->defnamespace);
		}

		if (defel->defname)
		{
			pfree(defel->defname);
		}

		if (defel->arg)
		{
			switch(nodeTag(defel->arg))
			{
				case T_Integer:
				{	
					pfree(defel->arg);
					break;
				}
				
				case T_Float:
				case T_String:
				{
					char *str = strVal(defel->arg);
					pfree(str);
					pfree(defel->arg);
					break;				
				}
				case T_BitString:
				case T_Null:
					break;
				default:
					break;
			}
		}
		pfree(defel);
	}
	list_free(option_list);
}

static	void parse_seqFullName_to_details(StringInfo message, char ** dbName, 
							char ** schemaName, char ** sequenceName)
{
	int  seqNameSize = 0;
	char * seqFullName = NULL;

	seqNameSize = pq_getmsgint(message, sizeof(seqNameSize));
	seqFullName = pnstrdup(pq_getmsgbytes(message, seqNameSize), seqNameSize);

	*dbName = strtok(seqFullName,".");
	if(dbName == NULL)
		ereport(ERROR,
			(errmsg("sequence database name is null")));
	*dbName = pnstrdup(*dbName, strlen(*dbName));

	*schemaName = strtok(NULL,".");
	if(schemaName == NULL)
		ereport(ERROR,
			(errmsg("sequence schemaName name is null")));
	*schemaName = pnstrdup(*schemaName, strlen(*schemaName));

	*sequenceName = strtok(NULL,".");
	if(sequenceName == NULL)
		ereport(ERROR,
			(errmsg("sequence name is null")));
	*sequenceName = pnstrdup(*sequenceName, strlen(*sequenceName));

	Assert(strtok(NULL,".") == NULL);

	pfree(seqFullName);
}

static List *
parse_string_to_seqOption(StringInfo strOption, RangeVar *var)
{
	int				flag = 0;
	AgtmNodeTag		type;
	int				listSize = 0;
	List			*options = NIL;

	memcpy(&listSize, pq_getmsgbytes(strOption, sizeof(int)), sizeof(listSize));
	for(flag = 0; flag < listSize; flag++)
	{
		int defnamespaceSize;
		int defnameSize;
		DefElem    *defel = palloc0(sizeof(DefElem));
		defel->type = T_DefElem;

		memcpy(&defnamespaceSize, pq_getmsgbytes(strOption, sizeof(int)), sizeof(defnamespaceSize));
		if(defnamespaceSize != 0)
			defel->defnamespace = pnstrdup(pq_getmsgbytes(strOption, defnamespaceSize), defnamespaceSize);

		memcpy(&defnameSize, pq_getmsgbytes(strOption, sizeof(defnameSize)), sizeof(defnameSize));
		if(defnameSize != 0)			
			defel->defname = pnstrdup(pq_getmsgbytes(strOption, defnameSize), defnameSize);

		memcpy(&type, pq_getmsgbytes(strOption, sizeof(type)), sizeof(type));
		switch(type)
		{
			case T_AgtmInteger:
			{
				long val = 0;
				memcpy(&val, pq_getmsgbytes(strOption, sizeof(val)), sizeof(val));
				defel->arg = (Node*)makeInteger(val);
				break;
			}
			case T_AgtmFloat:
			{
				char * str = NULL;
				int floatSize = 0;
				memcpy(&floatSize, pq_getmsgbytes(strOption, sizeof(floatSize)), sizeof(floatSize));
				str = pnstrdup(pq_getmsgbytes(strOption, floatSize), floatSize);
				defel->arg = (Node*)makeFloat((char *)str);
				break;
			}
			case T_AgtmString:
			{
				char * str = NULL;
				int strSize = 0;
				memcpy(&strSize, pq_getmsgbytes(strOption, sizeof(strSize)), sizeof(strSize));
				str = pnstrdup(pq_getmsgbytes(strOption, strSize), strSize);
				defel->arg = (Node*)makeString(str);
				break;
			}
			case T_AgtmBitString:
			{
				ereport(ERROR,
						(errmsg("T_BitString is not support")));
				break;
			}
			case T_AgtmNull:
			{
				Value	   *v = makeNode(Value);
				v->type = T_Null;
				v->val.str = NULL;
				v->val.ival = 0;
				defel->arg = (Node*)v;
				break;
			}
			case T_AgtmInvalid:
			{
				defel->arg = NULL;
				break;
			}
			default:
			{
				pfree(defel);
				ereport(ERROR,
						(errmsg("sequence DefElem type error : %d", nodeTag(defel->arg))));
				break;
			}
		}

		memcpy(&defel->defaction, pq_getmsgbytes(strOption, sizeof(defel->defaction)), sizeof(defel->defaction));
		options = lappend(options,defel);
	}

	memcpy(&var->inhOpt, pq_getmsgbytes(strOption, sizeof(var->inhOpt)), sizeof(var->inhOpt));
	memcpy(&var->relpersistence, pq_getmsgbytes(strOption, sizeof(var->relpersistence)), sizeof(var->relpersistence));
	return options;
}
