#include "postgres.h"

#include "access/printtup.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_sequence.h"
#include "agtm/agtm_transaction.h"
#include "agtm/agtm_utils.h"
#include "catalog/pg_type.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"

static TupleTableSlot* get_agtm_command_slot(void);

void
ProcessAGtmCommand(StringInfo input_message, CommandDest dest)
{
	DestReceiver *receiver;
	const char *msg_name;
	MemoryContext oldcontext;
	StringInfo output;
	AGTM_MessageType mtype;
	static StringInfoData buf={NULL, 0, 0, 0};

	mtype = pq_getmsgint(input_message, sizeof (AGTM_MessageType));
	msg_name = gtm_util_message_name(mtype);
	set_ps_display(msg_name, true);
	BeginCommand(msg_name, dest);
	ereport(DEBUG1, 
		(errmsg("[ pid=%d] Process Command mtype = %s (%d).",
		MyProcPid, msg_name, (int)mtype)));

	if(buf.data == NULL)
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		initStringInfo(&buf);
		enlargeStringInfo(&buf, VARHDRSZ);
		MemoryContextSwitchTo(oldcontext);
	}
	resetStringInfo(&buf);
	buf.len = VARDATA(buf.data) - buf.data;

	switch (mtype)
	{
		case AGTM_MSG_GET_GXID:
			output = ProcessGetGXIDCommand(input_message, &buf);
			break;

		case AGTM_MSG_GET_TIMESTAMP:
			output = ProcessGetTimestamp(input_message, &buf);
			break;

		case AGTM_MSG_SNAPSHOT_GET:
			output = ProcessGetSnapshot(input_message, &buf);
			break;

		case AGTM_MSG_GET_XACT_STATUS:
			output = ProcessGetXactStatus(input_message, &buf);
			break;
			
		case AGTM_MSG_SEQUENCE_INIT:
			output = ProcessSequenceInit(input_message, &buf);
			break;
			
		case AGTM_MSG_SEQUENCE_ALTER:
			output = ProcessSequenceAlter(input_message, &buf);
			break;
			
		case AGTM_MSG_SEQUENCE_DROP:
			output = ProcessSequenceDrop(input_message, &buf);
			break;
			
		case AGTM_MSG_SEQUENCE_DROP_BYDB:
			output = ProcessSequenceDropByDatabase(input_message, &buf);
			break;
			
		case AGTM_MSG_SEQUENCE_RENAME:
			output = ProcessSequenceRename(input_message, &buf);
			break;
			
		case AGTM_MSG_SEQUENCE_SET_VAL:
			PG_TRY();
			{
				output = ProcessSetSeqCommand(input_message, &buf);
			} PG_CATCH();
			{
			/*	CommitTransactionCommand();*/
				PG_RE_THROW();
			} PG_END_TRY();
			break;

		case AGTM_MSG_SEQUENCE_GET_NEXT:
			PG_TRY();
			{
				output = ProcessNextSeqCommand(input_message, &buf);
			} PG_CATCH();
			{
			/*	CommitTransactionCommand();*/
				PG_RE_THROW();
			} PG_END_TRY();
			break;

		case AGTM_MSG_SEQUENCE_GET_CUR:
			output = ProcessCurSeqCommand(input_message, &buf);
			break;

		case AGTM_MSG_SEQUENCE_GET_LAST:
			output = PorcessLastSeqCommand(input_message, &buf);
			break;

		default:
			ereport(FATAL,
					(EPROTO,
					 errmsg("[ pid=%d] invalid frontend message type %d",
					 MyProcPid, mtype)));
			break;
	}

	if(output != NULL)
	{
		Portal portal;
		TupleTableSlot *slot;
		static int16 format = 1;

		Assert(output->len >= VARHDRSZ);
		SET_VARSIZE(output->data, output->len);

		oldcontext = MemoryContextSwitchTo(MessageContext);

		slot = ExecClearTuple(get_agtm_command_slot());
		slot->tts_values[0] = PointerGetDatum(output->data);
		slot->tts_isnull[0] = false;
		ExecStoreVirtualTuple(slot);

		receiver = CreateDestReceiver(dest);
		portal = NULL;
		if(dest == DestRemote)
		{
			portal = CreatePortal("", true, true);
			/* Don't display the portal in pg_cursors */
			portal->visible = false;
			PortalDefineQuery(portal, NULL, "", msg_name, NIL, NULL);
			PortalStart(portal, NULL, 0, InvalidSnapshot);
			portal->tupDesc = slot->tts_tupleDescriptor;
			PortalSetResultFormat(portal, 1, &format);
			SetRemoteDestReceiverParams(receiver, portal);
		}
		(*receiver->rStartup)(receiver, CMD_UTILITY, slot->tts_tupleDescriptor);
		(*receiver->receiveSlot)(slot, receiver);
		(*receiver->rShutdown)(receiver);
		(*receiver->rDestroy)(receiver);

		if(portal)
			PortalDrop(portal, false);
	}

	EndCommand(msg_name, dest);
	resetStringInfo(&buf);
}

static TupleTableSlot* get_agtm_command_slot(void)
{
	MemoryContext oldcontext;
	static TupleTableSlot *slot = NULL;
	static TupleDesc desc = NULL;

	if(desc == NULL)
	{
		TupleDesc volatile temp = NULL;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		PG_TRY();
		{
			temp = CreateTemplateTupleDesc(1, false);
			TupleDescInitEntry((TupleDesc)temp, 1, "result", BYTEAOID, -1, 0);
		}PG_CATCH();
		{
			if(temp)
				FreeTupleDesc((TupleDesc)temp);
			PG_RE_THROW();
		}PG_END_TRY();
		desc = (TupleDesc)temp;
		MemoryContextSwitchTo(oldcontext);
	}
	if(slot == NULL)
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		slot = MakeSingleTupleTableSlot(desc);
		ExecSetSlotDescriptor(slot, desc);
		MemoryContextSwitchTo(oldcontext);
	}
	return slot;
}
