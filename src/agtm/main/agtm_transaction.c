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
#include "storage/procarray.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"


static List* parse_string_to_seqOption(StringInfo strOption);

static	void parse_seqFullName_to_details(StringInfo message, char ** dbName, 
							char ** schemaName, char ** sequenceName);

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
	static SnapshotData GlobalAgtmSnapshotData = {
		NULL,
		InvalidTransactionId,
		InvalidTransactionId,
		NULL,
		0,
		0,
		NULL,
		false,
		false,
		false,
		0,
		0,
		0,
#ifdef ADB
		0,
#endif /* ADB */
		};

	pq_getmsgend(message);
	snapshot = GetSnapshotData(&GlobalAgtmSnapshotData);

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

	MemoryContext sequece_Context;
	MemoryContext oldctx = NULL;

	sequece_Context = AllocSetContextCreate(CurrentMemoryContext,
											 "sequence deal",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	oldctx = MemoryContextSwitchTo(sequece_Context);

	initStringInfo(&buf);
	rangeVar = makeNode(RangeVar);

	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	option = parse_string_to_seqOption(message);
	pq_getmsgend(message);
	/* check info in system table */
	isExist = SequenceIsExist(dbName, schemaName, sequenceName);
	if(isExist)
		ereport(ERROR,
			(errmsg("%s, %s, %s exist!",dbName, schemaName, sequenceName)));

	/* insert database schema sequence into agtm_sequence */
	lineOid = AddAgtmSequence(dbName,schemaName,sequenceName);

	seqStmt = makeNode(CreateSeqStmt);
	rangeVar->catalogname = NULL;
	rangeVar->schemaname = NULL;
	appendStringInfo(&buf, "%s", "seq");
	appendStringInfo(&buf, "%u", lineOid);
	rangeVar->relname = buf.data;
	rangeVar->inhOpt = INH_DEFAULT;
	rangeVar->relpersistence = 'p';
	rangeVar->alias = NULL;

	seqStmt->sequence = rangeVar;
	seqStmt->options = option;
	DefineSequence(seqStmt);	

	(void)MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(sequece_Context);

	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_INIT_RESULT, 4);
	return output;
}

StringInfo
ProcessSequenceAlter(StringInfo message, StringInfo output)
{
	List *option;	
	/* system table agtm_sequence info */
	Oid			lineOid;
	char* dbName = NULL;
	char* schemaName = NULL;
	char* sequenceName = NULL;
	bool  isExist = FALSE;

	RangeVar * rangeVar = NULL;
	AlterSeqStmt * seqStmt = NULL;
	StringInfoData	buf;

	MemoryContext sequece_Context;
	MemoryContext oldctx = NULL;

	sequece_Context = AllocSetContextCreate(CurrentMemoryContext,
											 "sequence deal",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	oldctx = MemoryContextSwitchTo(sequece_Context);

	initStringInfo(&buf);
	rangeVar = makeNode(RangeVar);

	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	option = parse_string_to_seqOption(message);
	pq_getmsgend(message);

	/* check info in system table */
	isExist = SequenceIsExist(dbName, schemaName, sequenceName);
	if(!isExist)
		ereport(ERROR,
			(errmsg("%s, %s, %s not exist!",dbName, schemaName, sequenceName)));
	
	lineOid = SequenceSystemClassOid(dbName, schemaName, sequenceName);
	seqStmt = makeNode(AlterSeqStmt);
	rangeVar->catalogname = NULL;
	rangeVar->schemaname = NULL;
	appendStringInfo(&buf, "%s", "seq");
	appendStringInfo(&buf, "%u", lineOid);
	rangeVar->relname = buf.data;
	rangeVar->inhOpt = INH_DEFAULT;
	rangeVar->relpersistence = 'p';
	rangeVar->alias = NULL;

	seqStmt->sequence = rangeVar;
	seqStmt->options = option;

	AlterSequence(seqStmt);

	(void)MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(sequece_Context);

	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_ALTER_RESULT, 4);
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

	MemoryContext sequece_Context;
	MemoryContext oldctx = NULL;

	sequece_Context = AllocSetContextCreate(CurrentMemoryContext,
											 "sequence deal",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	oldctx = MemoryContextSwitchTo(sequece_Context);

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

	drop = makeNode(DropStmt);
	rangeVar = makeNode(RangeVar);

	drop->removeType = OBJECT_SEQUENCE;
	drop->behavior = DROP_RESTRICT;
	drop->missing_ok = 0;
	drop->concurrent = 0;

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

	(void)MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(sequece_Context);

	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_DROP_RESULT, 4);
	return output;
}

StringInfo 
ProcessSequenceDropByDatabase(StringInfo message, StringInfo output)
{
	char * database = NULL;
	int	   dbNameSize = 0;
	List   *list = NULL;

	MemoryContext sequece_Context;
	MemoryContext oldctx = NULL;

	sequece_Context = AllocSetContextCreate(CurrentMemoryContext,
											 "sequence deal",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	oldctx = MemoryContextSwitchTo(sequece_Context);

	dbNameSize = pq_getmsgint(message, sizeof(dbNameSize));
	database = pnstrdup(pq_getmsgbytes(message, dbNameSize), dbNameSize);
	if(dbNameSize == 0 || database == NULL)
		ereport(ERROR,
			(errmsg("sequence database name is null")));

	list = DelAgtmSequenceByDatabse(database);

	if(list != NULL)
	{
		ListCell   *option;
		foreach(option, list)
		{
			DropStmt *drop = NULL;
			RangeVar * rangeVar = NULL;
			List	*rangValList = NULL;
			char    *seq = (char *) lfirst(option);
			drop = makeNode(DropStmt);
			rangeVar = makeNode(RangeVar);

			drop->removeType = OBJECT_SEQUENCE;
			drop->behavior = DROP_RESTRICT;
			drop->missing_ok = 0;
			drop->concurrent = 0;

			rangeVar->catalogname = NULL;
			rangeVar->schemaname = NULL;
			rangeVar->relname = seq;
			rangeVar->inhOpt = INH_DEFAULT;
			rangeVar->relpersistence = 'p';

			rangValList = lappend(rangValList, makeString(seq));
			drop->objects = lappend(drop->objects, (void*)rangValList);

			RemoveRelations((void *)drop);
		}
	}

	(void)MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(sequece_Context);

	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_DROP_BYDB_RESULT, 4);
	return output;
}

StringInfo
ProcessSequenceRename(StringInfo message, StringInfo output)
{
	char* dbName = NULL;
	char* schemaName = NULL;
	char* sequenceName = NULL;
	char* newName = NULL;
	int	  newNameSize;
	SequenceRenameType type;

	MemoryContext sequece_Context;
	MemoryContext oldctx = NULL;

	sequece_Context = AllocSetContextCreate(CurrentMemoryContext,
											 "sequence deal",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	oldctx = MemoryContextSwitchTo(sequece_Context);

	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);

	newNameSize = pq_getmsgint(message, sizeof(newNameSize));
	newName = pnstrdup(pq_getmsgbytes(message, newNameSize), newNameSize);
	if(newNameSize == 0)
		ereport(ERROR,
			(errmsg("sequence new name is null")));

	type = pq_getmsgint(message, sizeof(type));
	pq_getmsgend(message);

	switch(type)
	{
		case T_RENAME_SEQUENCE:
			UpdateSequenceInfo(dbName, schemaName, sequenceName, newName, T_AgtmSeqName);
			break;
		case T_RENAME_SCHEMA:
			UpdateSequenceInfo(dbName, schemaName, sequenceName, newName, T_AgtmSeqSchema);
			break;
		case T_RENAME_DATABASE:
			UpdateSequenceInfo(dbName, schemaName, sequenceName, newName, T_AgtmseqDatabase);
			break;
		default:
			ereport(ERROR,
			(errmsg("sequence rename type error")));
			break;
	}	

	(void)MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(sequece_Context);

	/* Respond to the client */
	pq_sendint(output, AGTM_MSG_SEQUENCE_RENAME_RESULT, 4);
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
	if(dbNameSize == 0 || *dbName == NULL)
		ereport(ERROR,
			(errmsg("sequence database name is null")));

	schemaSize = pq_getmsgint(message, sizeof(schemaSize));
	*schemaName = pnstrdup(pq_getmsgbytes(message, schemaSize), schemaSize);
	if(schemaSize == 0 || *schemaName == NULL)
		ereport(ERROR,
			(errmsg("sequence schemaName name is null")));
}

static List *
parse_string_to_seqOption(StringInfo strOption)
{
	int				flag = 0;
	AgtmNodeTag		type;
	int				listSize = 0;
	List			*options = NIL;

	memcpy(&listSize, pq_getmsgbytes(strOption, sizeof(int)), sizeof(listSize));

	if(listSize == 0)
		return NULL;

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
	return options;
}
