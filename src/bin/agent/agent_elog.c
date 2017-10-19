
#include "agent.h"

#include "agt_msg.h"
#include "utils/memutils.h"

/* We provide a small stack of ErrorData records for re-entrant cases */
#define ERRORDATA_STACK_SIZE  5

static ErrorData errordata[ERRORDATA_STACK_SIZE];

static int	errordata_stack_depth = -1; /* index of topmost active frame */

static int	recursion_depth = 0;	/* to detect actual recursion */

/* Global variables */
ErrorContextCallback *error_context_stack = NULL;

sigjmp_buf *PG_exception_stack = NULL;

/*
 * Hook for intercepting messages before they are sent to the server log.
 * Note that the hook will not get called for messages that are suppressed
 * by log_min_messages.  Also note that logging hooks implemented in preload
 * libraries will miss any log messages that are generated before the
 * library is loaded.
 */
emit_log_hook_type emit_log_hook = NULL;

/* Macro for checking errordata_stack_depth is reasonable */
#define CHECK_STACK_DEPTH() \
	do { \
		if (errordata_stack_depth < 0) \
		{ \
			errordata_stack_depth = -1; \
			ereport(ERROR, (errmsg_internal("errstart was not called"))); \
		} \
	} while (0)

#define proc_exit exit

/*
 * This macro handles expansion of a format string and associated parameters;
 * it's common code for errmsg(), errdetail(), etc.  Must be called inside
 * a routine that is declared like "const char *fmt, ..." and has an edata
 * pointer set up.  The message is assigned to edata->targetfield, or
 * appended to it if appendval is true.  The message is subject to translation
 * if translateit is true.
 *
 * Note: we pstrdup the buffer rather than just transferring its storage
 * to the edata field because the buffer might be considerably larger than
 * really necessary.
 */
#define EVALUATE_MESSAGE(domain, targetfield, appendval, translateit)	\
	{ \
		char		   *fmtbuf; \
		StringInfoData	buf; \
		/* Internationalize the error format string */ \
		if ((translateit) && !in_error_recursion_trouble()) \
			fmt = dgettext((domain), fmt);				  \
		/* Expand %m in format string */ \
		fmtbuf = expand_fmt_string(fmt, edata); \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) { \
			appendStringInfoString(&buf, edata->targetfield); \
			appendStringInfoChar(&buf, '\n'); \
		} \
		/* Generate actual output --- have to use appendStringInfoVA */ \
		for (;;) \
		{ \
			va_list		args; \
			bool		success; \
			va_start(args, fmt); \
			success = appendStringInfoVA(&buf, fmtbuf, args); \
			va_end(args); \
			if (success) \
				break; \
			enlargeStringInfo(&buf, buf.maxlen); \
		} \
		/* Done with expanded fmt */ \
		pfree(fmtbuf); \
		/* Save the completed message into the stack item */ \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}

#define err_sendstring agt_sendstring
static char *expand_fmt_string(const char *fmt, ErrorData *edata);
static const char *useful_strerror(int errnum);
static const char *get_errno_symbol(int errnum);
static void agent_send_error_data(ErrorData *edata);
static const char *error_severity(int elevel);

/*
 * in_error_recursion_trouble --- are we at risk of infinite error recursion?
 *
 * This function exists to provide common control of various fallback steps
 * that we take if we think we are facing infinite error recursion.  See the
 * callers for details.
 */
bool
in_error_recursion_trouble(void)
{
	/* Pull the plug if recurse more than once */
	return (recursion_depth > 2);
}

bool errstart(int elevel, const char *filename, int lineno,
		 const char *funcname, const char *domain)
{
	ErrorData  *edata;
	bool		output_to_server;
	bool		output_to_client;
	int			i;

	/*
	 * Check some cases in which we want to promote an error into a more
	 * severe error.  None of this logic applies for non-error messages.
	 */
	if (elevel >= ERROR)
	{
		/*
		 * If the error level is ERROR or more, errfinish is not going to
		 * return to caller; therefore, if there is any stacked error already
		 * in progress it will be lost.  This is more or less okay, except we
		 * do not want to have a FATAL or PANIC error downgraded because the
		 * reporting process was interrupted by a lower-grade error.  So check
		 * the stack and make sure we panic if panic is warranted.
		 */
		for (i = 0; i <= errordata_stack_depth; i++)
			elevel = Max(elevel, errordata[i].elevel);
	}

	if(elevel < WARNING)
		return false;

	output_to_server = output_to_client = true;

	/*
	 * We need to do some actual work.  Make sure that memory context
	 * initialization has finished, else we can't do anything useful.
	 */
	if (ErrorContext == NULL)
	{
		/* Ooops, hard crash time; very little we can do safely here */
		write_stderr("error occurred at %s:%d before error message processing is available\n",
					 filename ? filename : "(unknown file)", lineno);
		exit(2);
	}

	/*
	 * Okay, crank up a stack entry to store the info in.
	 */

	if (recursion_depth++ > 0 && elevel >= ERROR)
	{
		/*
		 * Ooops, error during error processing.  Clear ErrorContext as
		 * discussed at top of file.  We will not return to the original
		 * error's reporter or handler, so we don't need it.
		 */
		MemoryContextReset(ErrorContext);
	}
	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.  We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.
		 */
		errordata_stack_depth = -1;		/* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	/* Initialize data for this error frame */
	edata = &errordata[errordata_stack_depth];
	MemSet(edata, 0, sizeof(ErrorData));
	edata->elevel = elevel;
	edata->output_to_server = output_to_server;
	edata->output_to_client = output_to_client;
	if (filename)
	{
		const char *slash;

		/* keep only base name, useful especially for vpath builds */
		slash = strrchr(filename, '/');
		if (slash)
			filename = slash + 1;
	}
	edata->filename = filename;
	edata->lineno = lineno;
	edata->funcname = funcname;
	/* the default text domain is the backend's */
	edata->domain = domain ? domain : PG_TEXTDOMAIN("postgres");
	/* initialize context_domain the same way (see set_errcontext_domain()) */
	edata->context_domain = edata->domain;
	/* Select default errcode based on elevel */
	if (elevel >= ERROR)
		edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
	else if (elevel == WARNING)
		edata->sqlerrcode = ERRCODE_WARNING;
	else
		edata->sqlerrcode = ERRCODE_SUCCESSFUL_COMPLETION;
	/* errno is saved here so that error parameter eval can't change it */
	edata->saved_errno = errno;

	recursion_depth--;
	return true;
}

/*
 * errfinish --- end an error-reporting cycle
 *
 * Produce the appropriate error report(s) and pop the error stack.
 *
 * If elevel is ERROR or worse, control does not return to the caller.
 * See elog.h for the error level definitions.
 */
void
errfinish(int dummy,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	int			elevel = edata->elevel;

	MemoryContext oldcontext;
	ErrorContextCallback *econtext;

	recursion_depth++;
	CHECK_STACK_DEPTH();

	/*
	 * Do processing in ErrorContext, which we hope has enough reserved space
	 * to report an error.
	 */
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	/*
	 * Call any context callback functions.  Errors occurring in callback
	 * functions will be treated as recursive errors --- this ensures we will
	 * avoid infinite recursion (see errstart).
	 */
	for (econtext = error_context_stack;
		 econtext != NULL;
		 econtext = econtext->previous)
		(*econtext->callback) (econtext->arg);

	/*
	 * If ERROR (not more nor less) we pass it off to the current handler.
	 * Printing it and popping the stack is the responsibility of the handler.
	 */
	if (elevel == ERROR)
	{
		recursion_depth--;
		PG_RE_THROW();
	}

	/* Emit the message to the right places */
	EmitErrorReport();

	/* Now free up subsidiary data attached to stack entry, and release it */
	if (edata->message)
		pfree(edata->message);
	if (edata->detail)
		pfree(edata->detail);
	if (edata->detail_log)
		pfree(edata->detail_log);
	if (edata->hint)
		pfree(edata->hint);
	if (edata->context)
		pfree(edata->context);
	if (edata->schema_name)
		pfree(edata->schema_name);
	if (edata->table_name)
		pfree(edata->table_name);
	if (edata->column_name)
		pfree(edata->column_name);
	if (edata->datatype_name)
		pfree(edata->datatype_name);
	if (edata->constraint_name)
		pfree(edata->constraint_name);
	if (edata->internalquery)
		pfree(edata->internalquery);

	errordata_stack_depth--;

	/* Exit error-handling context */
	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/*
	 * Perform error recovery action as specified by elevel.
	 */
	if (elevel == FATAL)
	{
		/*
		 * fflush here is just to improve the odds that we get to see the
		 * error message, in case things are so hosed that proc_exit crashes.
		 * Any other code you might be tempted to add here should probably be
		 * in an on_proc_exit or on_shmem_exit callback instead.
		 */
		fflush(stdout);
		fflush(stderr);
		agt_flush();

		/*
		 * Do normal process-exit cleanup, then return exit code 1 to indicate
		 * FATAL termination.  The postmaster may or may not consider this
		 * worthy of panic, depending on which subprocess returns it.
		 */
		proc_exit(1);
	}

	if (elevel >= PANIC)
	{
		/*
		 * Serious crash time. Postmaster will observe SIGABRT process exit
		 * status and kill the other backends too.
		 *
		 * XXX: what if we are *in* the postmaster?  abort() won't kill our
		 * children...
		 */
		fflush(stdout);
		fflush(stderr);
		agt_flush();
		abort();
	}

	/*
	 * But check for cancel/die interrupt first --- this is so that the user
	 * can stop a query emitting tons of notice or warning messages, even if
	 * it's in a loop that otherwise fails to check for interrupts.
	 */
	/*CHECK_FOR_INTERRUPTS();*/
}

/*
 * Actual output of the top-of-stack error message
 *
 * In the ereport(ERROR) case this is called from PostgresMain (or not at all,
 * if the error is caught by somebody).  For all other severity levels this
 * is called by errfinish.
 */
void
EmitErrorReport(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);
	/*
	 * Call hook before sending message to log.  The hook function is allowed
	 * to turn off edata->output_to_server, so we must recheck that afterward.
	 * Making any other change in the content of edata is not considered
	 * supported.
	 *
	 * Note: the reason why the hook can only turn off output_to_server, and
	 * not turn it on, is that it'd be unreliable: we will never get here at
	 * all if errstart() deems the message uninteresting.  A hook that could
	 * make decisions in that direction would have to hook into errstart(),
	 * where it would have much less information available.  emit_log_hook is
	 * intended for custom log filtering and custom log message transmission
	 * mechanisms.
	 */
	if (edata->output_to_server && emit_log_hook)
		(*emit_log_hook) (edata);

	agent_send_error_data(edata);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
}

/*
 * FlushErrorState --- flush the error state after error recovery
 *
 * This should be called by an error handler after it's done processing
 * the error; or as soon as it's done CopyErrorData, if it intends to
 * do stuff that is likely to provoke another error.  You are not "out" of
 * the error subsystem until you have done this.
 */
void
FlushErrorState(void)
{
	/*
	 * Reset stack to empty.  The only case where it would be more than one
	 * deep is if we serviced an error that interrupted construction of
	 * another message.  We assume control escaped out of that message
	 * construction and won't ever go back.
	 */
	errordata_stack_depth = -1;
	recursion_depth = 0;
	/* Delete all data in ErrorContext */
	MemoryContextResetAndDeleteChildren(ErrorContext);
}

static void agent_send_error_data(ErrorData *edata)
{
	StringInfoData msgbuf;
	/* New style with separate fields */
	char		tbuf[12];
	int			ssval;
	int			i;

	/* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
	agt_beginmessage(&msgbuf, (edata->elevel < ERROR) ? 'N' : 'E');

	agt_sendbyte(&msgbuf, PG_DIAG_SEVERITY);
	err_sendstring(&msgbuf, error_severity(edata->elevel));

	/* unpack MAKE_SQLSTATE code */
	ssval = edata->sqlerrcode;
	for (i = 0; i < 5; i++)
	{
		tbuf[i] = PGUNSIXBIT(ssval);
		ssval >>= 6;
	}
	tbuf[i] = '\0';

	agt_sendbyte(&msgbuf, PG_DIAG_SQLSTATE);
	err_sendstring(&msgbuf, tbuf);

	/* M field is required per protocol, so always send something */
	agt_sendbyte(&msgbuf, PG_DIAG_MESSAGE_PRIMARY);
	if (edata->message)
		err_sendstring(&msgbuf, edata->message);
	else
		err_sendstring(&msgbuf, _("missing error text"));

	if (edata->detail)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_MESSAGE_DETAIL);
		err_sendstring(&msgbuf, edata->detail);
	}

	/* detail_log is intentionally not used here */

	if (edata->hint)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_MESSAGE_HINT);
		err_sendstring(&msgbuf, edata->hint);
	}

	if (edata->context)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_CONTEXT);
		err_sendstring(&msgbuf, edata->context);
	}

	if (edata->schema_name)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_SCHEMA_NAME);
		err_sendstring(&msgbuf, edata->schema_name);
	}

	if (edata->table_name)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_TABLE_NAME);
		err_sendstring(&msgbuf, edata->table_name);
	}

	if (edata->column_name)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_COLUMN_NAME);
		err_sendstring(&msgbuf, edata->column_name);
	}

	if (edata->datatype_name)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_DATATYPE_NAME);
		err_sendstring(&msgbuf, edata->datatype_name);
	}

	if (edata->constraint_name)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_CONSTRAINT_NAME);
		err_sendstring(&msgbuf, edata->constraint_name);
	}

	if (edata->cursorpos > 0)
	{
		snprintf(tbuf, sizeof(tbuf), "%d", edata->cursorpos);
		agt_sendbyte(&msgbuf, PG_DIAG_STATEMENT_POSITION);
		err_sendstring(&msgbuf, tbuf);
	}

	if (edata->internalpos > 0)
	{
		snprintf(tbuf, sizeof(tbuf), "%d", edata->internalpos);
		agt_sendbyte(&msgbuf, PG_DIAG_INTERNAL_POSITION);
		err_sendstring(&msgbuf, tbuf);
	}

	if (edata->internalquery)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_INTERNAL_QUERY);
		err_sendstring(&msgbuf, edata->internalquery);
	}

	if (edata->filename)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_SOURCE_FILE);
		err_sendstring(&msgbuf, edata->filename);
	}

	if (edata->lineno > 0)
	{
		snprintf(tbuf, sizeof(tbuf), "%d", edata->lineno);
		agt_sendbyte(&msgbuf, PG_DIAG_SOURCE_LINE);
		err_sendstring(&msgbuf, tbuf);
	}

	if (edata->funcname)
	{
		agt_sendbyte(&msgbuf, PG_DIAG_SOURCE_FUNCTION);
		err_sendstring(&msgbuf, edata->funcname);
	}

	agt_sendbyte(&msgbuf, '\0');		/* terminator */
	agt_endmessage(&msgbuf);
}

/*
 * elog_start --- startup for old-style API
 *
 * All that we do here is stash the hidden filename/lineno/funcname
 * arguments into a stack entry, along with the current value of errno.
 *
 * We need this to be separate from elog_finish because there's no other
 * C89-compliant way to deal with inserting extra arguments into the elog
 * call.  (When using C99's __VA_ARGS__, we could possibly merge this with
 * elog_finish, but there doesn't seem to be a good way to save errno before
 * evaluating the format arguments if we do that.)
 */
void
elog_start(const char *filename, int lineno, const char *funcname)
{
	ErrorData  *edata;

	/* Make sure that memory context initialization has finished */
	if (ErrorContext == NULL)
	{
		/* Ooops, hard crash time; very little we can do safely here */
		write_stderr("error occurred at %s:%d before error message processing is available\n",
					 filename ? filename : "(unknown file)", lineno);
		exit(2);
	}

	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.  We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.  Note that the message is intentionally not localized,
		 * else failure to convert it to client encoding could cause further
		 * recursion.
		 */
		errordata_stack_depth = -1;		/* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	edata = &errordata[errordata_stack_depth];
	if (filename)
	{
		const char *slash;

		/* keep only base name, useful especially for vpath builds */
		slash = strrchr(filename, '/');
		if (slash)
			filename = slash + 1;
	}
	edata->filename = filename;
	edata->lineno = lineno;
	edata->funcname = funcname;
	/* errno is saved now so that error parameter eval can't change it */
	edata->saved_errno = errno;
}

/*
 * elog_finish --- finish up for old-style API
 */
void
elog_finish(int elevel, const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	CHECK_STACK_DEPTH();

	/*
	 * Do errstart() to see if we actually want to report the message.
	 */
	errordata_stack_depth--;
	errno = edata->saved_errno;
	if (!errstart(elevel, edata->filename, edata->lineno, edata->funcname, NULL))
		return;					/* nothing to do */

	/*
	 * Format error message just like errmsg_internal().
	 */
	recursion_depth++;
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(edata->domain, message, false, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/*
	 * And let errfinish() finish up.
	 */
	errfinish(0);
}

/*
 * errcode --- add SQLSTATE error code to the current error
 *
 * The code is expected to be represented as per MAKE_SQLSTATE().
 */
int
errcode(int sqlerrcode)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->sqlerrcode = sqlerrcode;

	return 0;					/* return value does not matter */
}

/*
 * errmsg --- add a primary error message text to the current error
 *
 * In addition to the usual %-escapes recognized by printf, "%m" in
 * fmt is replaced by the error message for the caller's value of errno.
 *
 * Note: no newline is needed at the end of the fmt string, since
 * ereport will provide one for the output methods that need it.
 */
int
errmsg(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(edata->domain, message, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}

/*
 * errmsg_internal --- add a primary error message text to the current error
 *
 * This is exactly like errmsg() except that strings passed to errmsg_internal
 * are not translated, and are customarily left out of the
 * internationalization message dictionary.  This should be used for "can't
 * happen" cases that are probably not worth spending translation effort on.
 * We also use this for certain cases where we *must* not try to translate
 * the message because the translation would fail and result in infinite
 * error recursion.
 */
int
errmsg_internal(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(edata->domain, message, false, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}

/*
 * errdetail --- add a detail error message text to the current error
 */
int
errdetail(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(edata->domain, detail, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
void
write_stderr(const char *fmt,...)
{
	va_list		ap;

#ifdef WIN32
	char		errbuf[2048];	/* Arbitrary size? */
#endif

	fmt = _(fmt);

	va_start(ap, fmt);
#ifndef WIN32
	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, fmt, ap);
	fflush(stderr);
#else
	vsnprintf(errbuf, sizeof(errbuf), fmt, ap);

	/*
	 * On Win32, we print to stderr if running on a console, or write to
	 * eventlog if running as a service
	 */
	if (pgwin32_is_service())	/* Running as a service */
	{
		write_eventlog(ERROR, errbuf, strlen(errbuf));
	}
	else
	{
		/* Not running as service, write to stderr */
		write_console(errbuf, strlen(errbuf));
		fflush(stderr);
	}
#endif
	va_end(ap);
}

/*
 * pg_re_throw --- out-of-line implementation of PG_RE_THROW() macro
 */
void
pg_re_throw(void)
{
	/* If possible, throw the error to the next outer setjmp handler */
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	else
	{
		/*
		 * If we get here, elog(ERROR) was thrown inside a PG_TRY block, which
		 * we have now exited only to discover that there is no outer setjmp
		 * handler to pass the error to.  Had the error been thrown outside
		 * the block to begin with, we'd have promoted the error to FATAL, so
		 * the correct behavior is to make it FATAL now; that is, emit it and
		 * then call proc_exit.
		 */
		ErrorData  *edata = &errordata[errordata_stack_depth];

		Assert(errordata_stack_depth >= 0);
		Assert(edata->elevel == ERROR);
		edata->elevel = FATAL;

		/*
		 * At least in principle, the increase in severity could have changed
		 * where-to-output decisions, so recalculate.  This should stay in
		 * sync with errstart(), which see for comments.
		 */
		/*if (IsPostmasterEnvironment)
			edata->output_to_server = is_log_level_output(FATAL,
														  log_min_messages);
		else
			edata->output_to_server = (FATAL >= log_min_messages);
		if (whereToSendOutput == DestRemote)
		{
			if (ClientAuthInProgress)
				edata->output_to_client = true;
			else
				edata->output_to_client = (FATAL >= client_min_messages);
		}*/
		edata->output_to_server = true;

		/*
		 * We can use errfinish() for the rest, but we don't want it to call
		 * any error context routines a second time.  Since we know we are
		 * about to exit, it should be OK to just clear the context stack.
		 */
		error_context_stack = NULL;

		errfinish(0);
	}

	/* Doesn't return ... */
	ExceptionalCondition("pg_re_throw tried to return", "FailedAssertion",
						 __FILE__, __LINE__);
}

/*
 * expand_fmt_string --- process special format codes in a format string
 *
 * We must replace %m with the appropriate strerror string, since vsnprintf
 * won't know what to do with it.
 *
 * The result is a palloc'd string.
 */
static char *
expand_fmt_string(const char *fmt, ErrorData *edata)
{
	StringInfoData buf;
	const char *cp;

	initStringInfo(&buf);

	for (cp = fmt; *cp; cp++)
	{
		if (cp[0] == '%' && cp[1] != '\0')
		{
			cp++;
			if (*cp == 'm')
			{
				/*
				 * Replace %m by system error string.  If there are any %'s in
				 * the string, we'd better double them so that vsnprintf won't
				 * misinterpret.
				 */
				const char *cp2;

				cp2 = useful_strerror(edata->saved_errno);
				for (; *cp2; cp2++)
				{
					if (*cp2 == '%')
						appendStringInfoCharMacro(&buf, '%');
					appendStringInfoCharMacro(&buf, *cp2);
				}
			}
			else
			{
				/* copy % and next char --- this avoids trouble with %%m */
				appendStringInfoCharMacro(&buf, '%');
				appendStringInfoCharMacro(&buf, *cp);
			}
		}
		else
			appendStringInfoCharMacro(&buf, *cp);
	}

	return buf.data;
}

/*
 * A slightly cleaned-up version of strerror()
 */
static const char *
useful_strerror(int errnum)
{
	/* this buffer is only used if strerror() and get_errno_symbol() fail */
	static char errorstr_buf[48];
	const char *str;

#ifdef WIN32
	/* Winsock error code range, per WinError.h */
	if (errnum >= 10000 && errnum <= 11999)
		return pgwin32_socket_strerror(errnum);
#endif
	str = strerror(errnum);

	/*
	 * Some strerror()s return an empty string for out-of-range errno.  This
	 * is ANSI C spec compliant, but not exactly useful.  Also, we may get
	 * back strings of question marks if libc cannot transcode the message to
	 * the codeset specified by LC_CTYPE.  If we get nothing useful, first try
	 * get_errno_symbol(), and if that fails, print the numeric errno.
	 */
	if (str == NULL || *str == '\0' || *str == '?')
		str = get_errno_symbol(errnum);

	if (str == NULL)
	{
		snprintf(errorstr_buf, sizeof(errorstr_buf),
		/*------
		  translator: This string will be truncated at 47
		  characters expanded. */
				 _("operating system error %d"), errnum);
		str = errorstr_buf;
	}

	return str;
}

/*
 * errcode_for_file_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of disk file access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_file_access(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (edata->saved_errno)
	{
			/* Permission-denied failures */
		case EPERM:				/* Not super-user */
		case EACCES:			/* Permission denied */
#ifdef EROFS
		case EROFS:				/* Read only file system */
#endif
			edata->sqlerrcode = ERRCODE_INSUFFICIENT_PRIVILEGE;
			break;

			/* File not found */
		case ENOENT:			/* No such file or directory */
			edata->sqlerrcode = ERRCODE_UNDEFINED_FILE;
			break;

			/* Duplicate file */
		case EEXIST:			/* File exists */
			edata->sqlerrcode = ERRCODE_DUPLICATE_FILE;
			break;

			/* Wrong object type or state */
		case ENOTDIR:			/* Not a directory */
		case EISDIR:			/* Is a directory */
#if defined(ENOTEMPTY) && (ENOTEMPTY != EEXIST) /* same code on AIX */
		case ENOTEMPTY: /* Directory not empty */
#endif
			edata->sqlerrcode = ERRCODE_WRONG_OBJECT_TYPE;
			break;

			/* Insufficient resources */
		case ENOSPC:			/* No space left on device */
			edata->sqlerrcode = ERRCODE_DISK_FULL;
			break;

		case ENFILE:			/* File table overflow */
		case EMFILE:			/* Too many open files */
			edata->sqlerrcode = ERRCODE_INSUFFICIENT_RESOURCES;
			break;

			/* Hardware failure */
		case EIO:				/* I/O error */
			edata->sqlerrcode = ERRCODE_IO_ERROR;
			break;

			/* All else is classified as internal errors */
		default:
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			break;
	}

	return 0;					/* return value does not matter */
}

/*
 * errcode_for_socket_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of socket access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_socket_access(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (edata->saved_errno)
	{
			/* Loss of connection */
		case EPIPE:
#ifdef ECONNRESET
		case ECONNRESET:
#endif
			edata->sqlerrcode = ERRCODE_CONNECTION_FAILURE;
			break;

			/* All else is classified as internal errors */
		default:
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			break;
	}

	return 0;					/* return value does not matter */
}

/*
 * Returns a symbol (e.g. "ENOENT") for an errno code.
 * Returns NULL if the code is unrecognized.
 */
static const char *
get_errno_symbol(int errnum)
{
	switch (errnum)
	{
		case E2BIG:
			return "E2BIG";
		case EACCES:
			return "EACCES";
#ifdef EADDRINUSE
		case EADDRINUSE:
			return "EADDRINUSE";
#endif
#ifdef EADDRNOTAVAIL
		case EADDRNOTAVAIL:
			return "EADDRNOTAVAIL";
#endif
		case EAFNOSUPPORT:
			return "EAFNOSUPPORT";
#ifdef EAGAIN
		case EAGAIN:
			return "EAGAIN";
#endif
#ifdef EALREADY
		case EALREADY:
			return "EALREADY";
#endif
		case EBADF:
			return "EBADF";
#ifdef EBADMSG
		case EBADMSG:
			return "EBADMSG";
#endif
		case EBUSY:
			return "EBUSY";
		case ECHILD:
			return "ECHILD";
#ifdef ECONNABORTED
		case ECONNABORTED:
			return "ECONNABORTED";
#endif
		case ECONNREFUSED:
			return "ECONNREFUSED";
#ifdef ECONNRESET
		case ECONNRESET:
			return "ECONNRESET";
#endif
		case EDEADLK:
			return "EDEADLK";
		case EDOM:
			return "EDOM";
		case EEXIST:
			return "EEXIST";
		case EFAULT:
			return "EFAULT";
		case EFBIG:
			return "EFBIG";
#ifdef EHOSTUNREACH
		case EHOSTUNREACH:
			return "EHOSTUNREACH";
#endif
		case EIDRM:
			return "EIDRM";
		case EINPROGRESS:
			return "EINPROGRESS";
		case EINTR:
			return "EINTR";
		case EINVAL:
			return "EINVAL";
		case EIO:
			return "EIO";
#ifdef EISCONN
		case EISCONN:
			return "EISCONN";
#endif
		case EISDIR:
			return "EISDIR";
#ifdef ELOOP
		case ELOOP:
			return "ELOOP";
#endif
		case EMFILE:
			return "EMFILE";
		case EMLINK:
			return "EMLINK";
		case EMSGSIZE:
			return "EMSGSIZE";
		case ENAMETOOLONG:
			return "ENAMETOOLONG";
		case ENFILE:
			return "ENFILE";
		case ENOBUFS:
			return "ENOBUFS";
		case ENODEV:
			return "ENODEV";
		case ENOENT:
			return "ENOENT";
		case ENOEXEC:
			return "ENOEXEC";
		case ENOMEM:
			return "ENOMEM";
		case ENOSPC:
			return "ENOSPC";
		case ENOSYS:
			return "ENOSYS";
#ifdef ENOTCONN
		case ENOTCONN:
			return "ENOTCONN";
#endif
		case ENOTDIR:
			return "ENOTDIR";
#if defined(ENOTEMPTY) && (ENOTEMPTY != EEXIST) /* same code on AIX */
		case ENOTEMPTY:
			return "ENOTEMPTY";
#endif
#ifdef ENOTSOCK
		case ENOTSOCK:
			return "ENOTSOCK";
#endif
#ifdef ENOTSUP
		case ENOTSUP:
			return "ENOTSUP";
#endif
		case ENOTTY:
			return "ENOTTY";
		case ENXIO:
			return "ENXIO";
#if defined(EOPNOTSUPP) && (!defined(ENOTSUP) || (EOPNOTSUPP != ENOTSUP))
		case EOPNOTSUPP:
			return "EOPNOTSUPP";
#endif
#ifdef EOVERFLOW
		case EOVERFLOW:
			return "EOVERFLOW";
#endif
		case EPERM:
			return "EPERM";
		case EPIPE:
			return "EPIPE";
		case EPROTONOSUPPORT:
			return "EPROTONOSUPPORT";
		case ERANGE:
			return "ERANGE";
#ifdef EROFS
		case EROFS:
			return "EROFS";
#endif
		case ESRCH:
			return "ESRCH";
#ifdef ETIMEDOUT
		case ETIMEDOUT:
			return "ETIMEDOUT";
#endif
#ifdef ETXTBSY
		case ETXTBSY:
			return "ETXTBSY";
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		case EWOULDBLOCK:
			return "EWOULDBLOCK";
#endif
		case EXDEV:
			return "EXDEV";
	}

	return NULL;
}

/*
 * error_severity --- get localized string representing elevel
 */
static const char *error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = _("DEBUG");
			break;
		case LOG:
		case COMMERROR:
			prefix = _("LOG");
			break;
		case INFO:
			prefix = _("INFO");
			break;
		case NOTICE:
			prefix = _("NOTICE");
			break;
		case WARNING:
			prefix = _("WARNING");
			break;
		case ERROR:
			prefix = _("ERROR");
			break;
		case FATAL:
			prefix = _("FATAL");
			break;
		case PANIC:
			prefix = _("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}
