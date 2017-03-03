#include "postgres.h"

#include <sys/stat.h>
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "libpq/libpq-be.h"	/* FrontendProtocol */
#include "libpq/pqcomm.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "commands/copyfunc.h"

typedef struct CopyFuncState
{
	ExprContext	   *expr_context;
	ExprState	   *expr_state;
	FILE		   *file_input;
	FILE		   *file_output;
	char		  **fields;
	const char	   *null_str;
	uint64			processed;
	StringInfoData	buf_input;
	StringInfoData	buf_output;
	FmgrInfo		out_func;
	int				count_param;
	bool			input_eof;
	bool			csv_mode;
	char			delimiter;
	char			quote;
}CopyFuncState;

static CopyFuncState* make_copy_state(Node *expr, const char *query_str);
static void end_copy_state(CopyFuncState *state, CopyFuncStmt *stmt);
static Node* parse_param_hook(ParseState *pstate, ParamRef *pref);
static Node* coerce_param_hook(ParseState *pstate, Param *param,
								Oid targetTypeId, int32 targetTypeMod, int location);
static void SendStartCopy(CopyFuncState *state, CopyFuncStmt *stmt);
static void SendStopCopy(CopyFuncState *state);
static void ParseOptions(CopyFuncState *state, List *options);
static void DoCopyBoth(CopyFuncState *state);

typedef bool (*get_line_func)(CopyFuncState *state);
typedef void (*put_line_func)(CopyFuncState *state);
static bool get_copy_line(CopyFuncState *state);
static bool get_copy_line_file(CopyFuncState *state);
static void put_copy_line(CopyFuncState *state);
static void put_copy_line_file(CopyFuncState *state);
static void parse_line(CopyFuncState *state);

uint64 DoCopyFunction(CopyFuncStmt *stmt, const char *queryString)
{
	CopyFuncState *state;
	uint64 processed;
	bool is_from_pipe = (stmt->fromname == NULL);
	bool is_to_pipe = (stmt->toname == NULL);

	if((!is_from_pipe || !is_to_pipe) && !superuser())
	{
		if (stmt->fromname || stmt->toname)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file")));
	}

	state = make_copy_state(stmt->func, queryString);
	ParseOptions(state, stmt->options);
	initStringInfo(&(state->buf_input));
	initStringInfo(&(state->buf_output));

	SendStartCopy(state, stmt);
	DoCopyBoth(state);
	SendStopCopy(state);
	processed = state->processed;
	end_copy_state(state, stmt);

	return processed;
}

static CopyFuncState* make_copy_state(Node *expr, const char *query_str)
{
	CopyFuncState *state;
	ParseState *pstate;
	Node *new_expr;
	ParamListInfo pli;
	Oid oid;
	int i;
	bool is_varlean;

	state = palloc0(sizeof(*state));

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = query_str;
	pstate->p_ref_hook_state = state;
	pstate->p_paramref_hook = parse_param_hook;
	pstate->p_coerce_param_hook = coerce_param_hook;
	new_expr = transformExpr(pstate, expr, EXPR_KIND_VALUES);
	/*if(exprType(new_expr) != INT4OID)
	{
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("expression result type is not int4"),
			errposition(exprLocation(new_expr))));
	}*/
	state->expr_state = ExecInitExpr((Expr*)new_expr, NULL);
	getTypeOutputInfo(exprType(new_expr), &oid, &is_varlean);
	fmgr_info(oid, &(state->out_func));

	state->expr_context = CreateStandaloneExprContext();
	state->expr_context->ecxt_param_list_info = pli =
		palloc0(sizeof(ParamListInfoData) + 
			(state->count_param - 1) * sizeof(ParamExternData));
	pli->numParams = state->count_param;
	for(i=0;i<pli->numParams;++i)
	{
		ParamExternData *prm = &(pli->params[i]);
		prm->pflags = PARAM_FLAG_CONST;
		prm->ptype = UNKNOWNOID;
	}
	state->fields = (char**)palloc0(state->count_param * sizeof(char*));

	return state;
}

static void end_copy_state(CopyFuncState *state, CopyFuncStmt *stmt)
{
	if(state->expr_context)
	{
		if(state->expr_context->ecxt_param_list_info)
		{
			pfree(state->expr_context->ecxt_param_list_info);
			state->expr_context->ecxt_param_list_info = NULL;
		}
		FreeExprContext(state->expr_context, true);
	}
	if(state->file_input)
	{
		if(stmt->is_from_program)
			ClosePipeStream(state->file_input);
		else
			FreeFile(state->file_input);
	}
	if(state->file_output)
	{
		if(stmt->is_to_program)
			ClosePipeStream(state->file_output);
		else
			FreeFile(state->file_output);
	}
	if(state->fields)
		pfree(state->fields);
	if(state->buf_input.data)
		pfree(state->buf_input.data);
	if(state->buf_output.data)
		pfree(state->buf_output.data);
	pfree(state);
}

static Node* parse_param_hook(ParseState *pstate, ParamRef *pref)
{
	Param *param;
	CopyFuncState *state = pstate->p_ref_hook_state;

	if(pref->number <= 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("invalid param id %d", pref->number),
			errposition(pref->location)));
	}

	if(state->count_param < pref->number)
		state->count_param = pref->number;

	param = makeNode(Param);
	param->paramkind = PARAM_EXTERN;
	param->paramid = pref->number;
	param->paramtype = UNKNOWNOID;
	param->paramtypmod = -1;
	param->location = pref->location;

	return (Node*)param;
}

static Node* coerce_param_hook(ParseState *pstate, Param *param,
									   Oid targetTypeId, int32 targetTypeMod,
											  int location)
{
	/* make an input function call */

	FuncExpr *expr;
	List *args;
	int16 typlen;
	bool typbyval;
	char typalign;
	char typdelim;
	Oid typioparam;
	Oid func;

	get_type_io_data(targetTypeId, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &func);
	args = list_make3(param
			, makeConst(OIDOID, -1, InvalidOid, sizeof(Oid), ObjectIdGetDatum(typioparam), false, true)
			, makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(-1), false, true));
	expr = makeFuncExpr(func, targetTypeId, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CAST);
	expr->location = location;

	return (Node*)expr;
}

static void SendStartCopy(CopyFuncState *state, CopyFuncStmt *stmt)
{
	StringInfoData buf;
	PG_TRY();
	{
		if(stmt->fromname == NULL)
		{
			state->file_input = NULL;
		}else
		{
			if(stmt->is_from_program)
			{
				state->file_input = OpenPipeStream(stmt->fromname, "r");
				if(state->file_input == NULL)
				{
					ereport(ERROR,
						(errmsg("could not execute command \"%s\": %m",
										stmt->fromname)));
				}
			}else
			{
				if (!is_absolute_path(stmt->fromname))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_NAME),
							errmsg("relative path not allowed for COPY to file")));

				state->file_input = AllocateFile(stmt->fromname, "r");
				if(state->file_input == NULL)
				{
					ereport(ERROR,(errcode_for_file_access(),
						errmsg("could not open file \"%s\" for writing: %m"
							, stmt->fromname)));
				}
			}
		}
		state->input_eof = false;
		if(stmt->toname == NULL)
		{
			state->file_output = NULL;
		}else
		{
			if(stmt->is_to_program)
			{
				state->file_output = OpenPipeStream(stmt->toname, "w");
				if(state->file_output == NULL)
				{
					ereport(ERROR,
						(errmsg("could not execute command \"%s\": %m",
										stmt->toname)));
				}
			}else
			{
				int oumask;
				if (!is_absolute_path(stmt->toname))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_NAME),
							errmsg("relative path not allowed for COPY to file")));

				oumask = umask(S_IWGRP | S_IWOTH);
				state->file_output = AllocateFile(stmt->toname, "wa");
				umask(oumask);
				if(state->file_output == NULL)
				{
					ereport(ERROR,(errcode_for_file_access(),
						errmsg("could not open file \"%s\" for writing: %m"
							, stmt->fromname)));
				}
			}
		}

		if(state->file_input == NULL && state->file_output == NULL)
		{
			/* copy both */
			pq_beginmessage(&buf, 'W');
		}else if(state->file_input == NULL)
		{
			/* copy in */
			pq_beginmessage(&buf, 'G');
		}else if(state->file_output == NULL)
		{
			/* copy out */
			pq_beginmessage(&buf, 'H');
		}else
		{
			/* copy file to file */
			buf.data = NULL;
		}
		if(buf.data)
		{
			if(PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
			{
				ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
					, errmsg("client version too old")));
			}
			pq_sendbyte(&buf, 0);	/* text format */
			pq_sendint(&buf, 0, 2);	/* 0 column */
			pq_endmessage(&buf);
			pq_flush();
		}
		if(state->file_output != NULL)
			pq_startcopyout();
	}PG_CATCH();
	{
		if(state->file_input)
		{
			if(stmt->is_from_program)
				ClosePipeStream(state->file_input);
			else
				FreeFile(state->file_input);
		}
		if(state->file_output)
		{
			if(stmt->is_to_program)
				ClosePipeStream(state->file_output);
			else
				FreeFile(state->file_output);
		}
		PG_RE_THROW();
	}PG_END_TRY();
}

static void SendStopCopy(CopyFuncState *state)
{
	if(state->file_output == NULL)
	{
		Assert(state->buf_output.len == 0);
		pq_putemptymessage('c');
	}else
	{
		put_copy_line_file(state);
	}
}

static void ParseOptions(CopyFuncState *state, List *options)
{
	ListCell *lc;
	DefElem *def;
	char *str;
	bool format_specified = false;

	foreach(lc, options)
	{
		def = lfirst(lc);
		if(strcmp(def->defname, "format") == 0)
		{
			if (format_specified)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			str = defGetString(def);
			format_specified = true;
			if (strcmp(str, "text") == 0)
				 /* default format */ ;
			else if (strcmp(str, "csv") == 0)
				state->csv_mode = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("COPY format \"%s\" not recognized", str)));
		}else if(strcmp(def->defname, "delimiter") == 0)
		{
			if (state->delimiter)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] == '\0' || str[1] != '\0')
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("delimiter must be one character")));
			state->delimiter = str[0];
		}else if(strcmp(def->defname, "null") == 0)
		{
			if (state->null_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			state->null_str = defGetString(def);
		}else if(strcmp(def->defname, "quote") == 0)
		{
			if (state->quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] == '\0' || str[1] != '\0')
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("quote must be one character")));
			state->quote = str[0];
		}else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
							def->defname)));
		}
	}

	if(state->null_str == NULL)
		state->null_str = "null";
	if(state->quote == '\0')
		state->quote = '"';
	if(state->delimiter == '\0')
		state->delimiter = ',';
}

static void DoCopyBoth(CopyFuncState *state)
{
	MemoryContext mem_context;
	MemoryContext old_context;
	ParamExternData *prm;
	ParamListInfo pli;
	Datum datum;
	get_line_func get_func;
	put_line_func put_func;
	Size i;
	ExprDoneCond done;
	bool is_null;

	mem_context = AllocSetContextCreate(CurrentMemoryContext,
										   "copy func",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);
	old_context = MemoryContextSwitchTo(mem_context);
	pli = state->expr_context->ecxt_param_list_info;
	state->processed = 0;
	if(state->file_input == NULL)
		get_func = get_copy_line;
	else
		get_func = get_copy_line_file;
	if(state->file_output == NULL)
		put_func = put_copy_line;
	else
		put_func = put_copy_line_file;

	for(;;)
	{
		CHECK_FOR_INTERRUPTS();
		MemoryContextReset(CurrentMemoryContext);
		ResetExprContext(state->expr_context);
		if((*get_func)(state) == false)
			break;
		parse_line(state);

		for(i=0;i<pli->numParams;++i)
		{
			prm = &(pli->params[i]);
			if(state->fields[i])
			{
				prm->value = CStringGetDatum(state->fields[i]);
				prm->isnull = false;
			}else
			{
				prm->value = (Datum)0;
				prm->isnull = true;
			}
		}

re_exec_:
		datum = ExecEvalExpr(state->expr_state, state->expr_context, &is_null, &done);

		if(done != ExprEndResult)
		{
			resetStringInfo(&(state->buf_output));
			appendStringInfo(&(state->buf_output), UINT64_FORMAT, state->processed);
			appendStringInfoChar(&(state->buf_output), state->delimiter);
			appendStringInfoString(&(state->buf_output),
				is_null ? state->null_str : OutputFunctionCall(&(state->out_func), datum));
			(*put_func)(state);
		}
		if(done == ExprMultipleResult)
		{
			goto re_exec_;
		}

		++(state->processed);
	}
	resetStringInfo(&(state->buf_output));

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(mem_context);
}

static bool get_copy_line(CopyFuncState *state)
{
	StringInfo buf;
	int mtype;
	if(state->input_eof)
		return false;

	buf = &(state->buf_input);

read_again_:
	resetStringInfo(buf);
	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();
	mtype = pq_getbyte();
	if (mtype == EOF)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	if (pq_getmessage(buf, 0))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	RESUME_CANCEL_INTERRUPTS();
	switch (mtype)
	{
		case 'd':		/* CopyData */
			break;
		case 'c':		/* CopyDone */
			/* COPY IN correctly terminated by frontend */
			state->input_eof = true;
			return false;
		case 'f':		/* CopyFail */
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("COPY from stdin failed: %s",
					   pq_getmsgstring(buf))));
			break;
		case 'H':		/* Flush */
		case 'S':		/* Sync */

			/*
			 * Ignore Flush/Sync for the convenience of client
			 * libraries (such as libpq) that may send those
			 * without noticing that the command they just
			 * sent was COPY.
			 */
			goto read_again_;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected message type 0x%02X during COPY from stdin",
							mtype)));
			break;
	}
	return true;
}

static bool get_copy_line_file(CopyFuncState *state)
{
	char *str;
	StringInfo buf;
	int space;

	buf = &(state->buf_input);
	resetStringInfo(buf);
	if(state->input_eof)
		return false;

	for(;;)
	{
		space = buf->maxlen - buf->len;
		Assert(space > 0);
		str = fgets(buf->data + buf->len, space, state->file_input);
		if(ferror(state->file_input))
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("could not read from COPY file: %m")));
		}else if(str == NULL)
		{
			state->input_eof = true;
			return false;
		}

		Assert(str != NULL);
		buf->len += strlen(str);
		if(feof(state->file_input))
			return buf->len == 0 ? false:true;

		if(buf->len == buf->maxlen)
		{
			if(buf->data[buf->len-1] != '\0')
			{
				enlargeStringInfo(buf, buf->maxlen + 1024);
				continue;
			}
		}
		break;
	}
	while(buf->len > 0 && buf->data[buf->len-1])
	{
		str = &(buf->data[buf->len-1]);
		if(*str == '\r' || *str == '\n')
		{
			*str = '\0';
			--(buf->len);
		}else
		{
			break;
		}
	}
	return true;
}

static void put_copy_line(CopyFuncState *state)
{
	StringInfo buf = &(state->buf_output);
	pq_putmessage('d', buf->data, buf->len);
	pq_flush();
	resetStringInfo(buf);
}

static void put_copy_line_file(CopyFuncState *state)
{
	StringInfo buf;
	int nwrite;

	buf = &(state->buf_output);
#ifdef WIN32
	appendStringInfoString(buf, "\r\n");
#else
	appendStringInfoChar(buf, '\n');
#endif

	nwrite = fwrite(buf->data, 1, buf->len, state->file_output);
	if(nwrite != buf->len)
	{
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("could not write to COPY file: %m")));
	}
}

static void parse_line(CopyFuncState *state)
{

	char 		*line  = NULL;
	char	    *line_end_ptr;
	char	    *cur_ptr;
	char		*result;
	char	    *output_ptr;
	char		delimc;
	char		quotec;
	int			split = 0;
	int			input_len;
	int			tmp_loc;
	
	Size count = state->expr_context->ecxt_param_list_info->numParams;
	Assert(NULL != state->buf_input.data);	
	line = (char*)palloc0(state->buf_input.len + 1);
	memcpy(line, state->buf_input.data, state->buf_input.len);
	line[state->buf_input.len] = '\0';

	delimc = state->delimiter;
	quotec = state->quote;
	line_end_ptr = line + strlen(line);
	cur_ptr = line;
	result = (char*)palloc0(strlen(line) + 1);
	output_ptr = result;
	for(;;)
	{
		bool		found_delim = false;
		bool		saw_quote = false;
		char	   *start_ptr;
		char	   *end_ptr;

		if (split + 1 > count)
			break;
		start_ptr = cur_ptr;
		state->fields[split] = output_ptr;
		for (;;)
		{
			char c;
			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (c == delimc)
				{
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}
			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					ereport(ERROR,(errmsg("unterminated CSV quoted field")));
				c = *cur_ptr++;				
				if (c == quotec)
					break;
				/* Add c to output string */
				*output_ptr++ = c;
			}
		}

endfield:
		*output_ptr++ = '\0';
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == 0)
		{
			state->fields[split] = "";
		}
		split++;
	}
	for (tmp_loc = 0; tmp_loc < count; tmp_loc++)
	{
		char		*field_value;
		int			value_len;
		value_len = strlen(state->fields[tmp_loc]);
		field_value	= (char*)palloc0(value_len + 1);
		memcpy(field_value, state->fields[tmp_loc], value_len);
		field_value[value_len] = '\0';
		state->fields[tmp_loc] = field_value;
	}
	pfree(result);
	pfree(line);
}
