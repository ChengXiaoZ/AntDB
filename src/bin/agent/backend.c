#include "agent.h"

#include <signal.h>

#include "agt_msg.h"
#include "agt_utility.h"
#include "lib/stringinfo.h"
#include "mgr/mgr_msg_type.h"
#include "utils/memutils.h"

void agent_backend(pgsocket fd)
{
	sigjmp_buf	local_sigjmp_buf;
	StringInfoData input_message;
	int msg_type;

	pqsignal(SIGCHLD, SIG_DFL);
	PG_exception_stack = NULL;
	MemoryContextReset(TopMemoryContext);
	MemoryContextSwitchTo(TopMemoryContext);
	agt_msg_init(fd);
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										 "ErrorContext",
										 ALLOCSET_SMALL_MINSIZE,
										 ALLOCSET_SMALL_INITSIZE,
										 ALLOCSET_SMALL_MAXSIZE);
	initStringInfo(&input_message);
	MemoryContextSwitchTo(MessageContext);

	PG_exception_stack = &local_sigjmp_buf;
	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Forget any pending QueryCancel */
		QueryCancelPending = false;		/* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(MessageContext);
		FlushErrorState();
	}
	MemoryContextSwitchTo(MessageContext);
	/* send read for query */
	agt_put_msg(AGT_MSG_IDLE, NULL, 0);

	for(;;)
	{
		MemoryContextResetAndDeleteChildren(MessageContext);
		msg_type = agt_get_msg(&input_message);
		switch(msg_type)
		{
		case AGT_MSG_COMMAND:
			do_agent_command(&input_message);
			break;
		case AGT_MSG_EXIT:
			exit(EXIT_SUCCESS);
			break;
		default:
			ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION)
				, errmsg("invalid message type")));
			break;
		}
		agt_endmessage(&input_message);
		agt_put_msg(AGT_MSG_IDLE, NULL, 0);
	}

	exit(EXIT_FAILURE);
}