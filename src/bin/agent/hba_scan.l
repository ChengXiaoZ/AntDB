%{

#include "postgres.h"

#include <ctype.h>
#include <unistd.h>

#include "hba_scan.h"

#undef fprintf
#define fprintf(file, fmt, msg) hba_flex_fatal(msg)
#define YYSTYPE hba_YYSTYPE
#define YY_USER_ACTION							\
	do{											\
		yylval->start = yytext - yylval->line;	\
		yylval->length = yyleng;				\
	}while(0);

typedef enum HBA_SCAN_TOKEN
{
	HBA_CONF_LOCAL = 300,
	HBA_CONF_HOST,
	HBA_CONF_HOSTSSL,
	HBA_CONF_HOSTNOSSL,
	HBA_CONF_ALL,
	HBA_CONF_INTEGER,
	HBA_CONF_IPV4,
	HBA_CONF_IPV6,
	HBA_CONF_ID,
	HBA_CONF_STRING,
	HBA_CONF_UNQUOTED_STRING,

	HBA_CONF_COMMENT,
	HBA_CONF_EOL
}HBA_SCAN_TOKEN;

typedef struct hba_YYSTYPE
{
	char *line;
	int   start;
	int   length;
	int   ival;		/* return integer value when HBA_CONF_INTEGER */
}hba_YYSTYPE;

static unsigned int hba_flex_file_no;
static const char *hba_flex_file_name;

static int hba_flex_fatal(const char *msg);
static int process_integer(const char *token, YYSTYPE *lval);
static char *hba_lex_get_line(FILE *fp, Size *slen);
static HbaInfo* hba_parse_line(yyscan_t scanner, char *line, Size slen, YY_BUFFER_STATE tmp_state);
static void process_hba_type(int token, YYSTYPE *yysval, HbaInfo *info);
static char* hba_lex_string(yyscan_t scanner, YYSTYPE *yysval);
static char* hba_lex_db_user(yyscan_t scanner, YYSTYPE *yysval);

%}

%option reentrant
%option bison-bridge
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option warn
%option prefix="hba_yy"

DIGIT			[0-9]
HEXDIGIT		[0-9a-fA-F]

UNIT_LETTER		[a-zA-Z]

INTEGER			{DIGIT}+

LETTER			[A-Za-z_\200-\377]
LETTER_OR_DIGIT [A-Za-z_0-9\200-\377]

ID				{LETTER}{LETTER_OR_DIGIT}*

IP4PART		[01]?[0-9]{1,2}|2([0-4][0-9])|25[0-5]
IP4			{IP4PART}\.{IP4PART}\.{IP4PART}\.{IP4PART}

IP6PART		{HEXDIGIT}{1,4}
IP6RC		{IP6PART}:
IP6LC		:{IP6PART}
IP6FULL		{IP6RC}{7,7}{IP6PART}

UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/\*\$@%])*
STRING			\'([^'\\\n#"]|\\.|\'\')*\'
%%

\n					hba_flex_file_no++; return HBA_CONF_EOL;
[ \t\r]+			/* eat whitespace */
#.*					return HBA_CONF_COMMENT;/* .* matches anything until newline */

{IP4}				return HBA_CONF_IPV4;

{IP6RC}{IP6LC}{1,6}		return HBA_CONF_IPV6;	/* xxxx::xxxx[:xxxx[...]] */
{IP6RC}{2}{IP6LC}{1,5}	return HBA_CONF_IPV6;	/* xxxx:xxxx::xxxx[:xxxx[...]] */
{IP6RC}{3}{IP6LC}{1,4}	return HBA_CONF_IPV6;	/* xxxx:xxxx:xxxx::xxxx[:xxxx[...]] */
{IP6RC}{4}{IP6LC}{1,3}	return HBA_CONF_IPV6;
{IP6RC}{5}{IP6LC}{1,2}	return HBA_CONF_IPV6;
{IP6RC}{6}{IP6LC}		return HBA_CONF_IPV6;

{IP6RC}{1,7}:		return HBA_CONF_IPV6;		/* xxxx:[xxxx:[...]]: */
:{IP6LC}{1,7}		return HBA_CONF_IPV6;		/* ::xxxx[:xxxx[:xxxx]] */
{IP6FULL}			return HBA_CONF_IPV6;

{INTEGER}			return process_integer(yytext, yylval);

[aA][lL][lL]							return HBA_CONF_ALL;
[hH][oO][sS][tT]						return HBA_CONF_HOST;
[hH][oO][sS][tT][sS][sS][lL]			return HBA_CONF_HOSTSSL;
[hH][oO][sS][tT][nN][oO][sS][sS][lL]	return HBA_CONF_HOSTNOSSL;
[lL][oO][cC][aA][lL]					return HBA_CONF_LOCAL;

{ID}				return HBA_CONF_ID;

{STRING}			return HBA_CONF_STRING;
{UNQUOTED_STRING}	return HBA_CONF_UNQUOTED_STRING;

.					return yytext[0];
<<EOF>>				return HBA_CONF_EOL;
%%

HbaInfo* parse_hba_file(const char *filename)
{
	yyscan_t scanner;
	HbaInfo *result,
			*cur_info,
			**prev_info;
	char	*line;
	volatile FILE *fp;
	YY_BUFFER_STATE tmp_state;
	Size slen;
	char tmp_buf[2];

	AssertArg(filename);
	fp = fopen(filename, "r");
	if(fp == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("Can not open file \"%s\" for read:%m", filename)));
	}

	result = NULL;
	prev_info = &result;
	PG_TRY();
	{
		if(hba_yylex_init(&scanner) != 0)
			ereport(ERROR, (errmsg("conf_yylex_init() failed:%m")));
		tmp_buf[0] = tmp_buf[1] = '\0';
		tmp_state = hba_yy_scan_buffer(tmp_buf, 2, scanner);
		hba_flex_file_name = filename;
		hba_flex_file_no = 1;

		while((line = hba_lex_get_line((FILE*)fp, &slen)) != NULL)
		{
			cur_info = hba_parse_line(scanner, line, slen+2, tmp_state);
			Assert(cur_info);
			*prev_info = cur_info;
			prev_info = &(cur_info->next);
		}
	}PG_CATCH();
	{
		fclose((FILE*)fp);
		PG_RE_THROW();
	}PG_END_TRY();

	fclose((FILE*)fp);
	hba_yy_delete_buffer(tmp_state, scanner);
	hba_yylex_destroy(scanner);
	return result;
}

#define REPORT_HBA_LEX_ERROR(sval)	\
	do{							\
		ereport(ERROR, (errmsg("syntax error on file \"%s\" line %d column %d"	\
				, hba_flex_file_name, hba_flex_file_no, (sval)->start)));		\
	}while(0)
static HbaInfo* hba_parse_line(yyscan_t scanner, char *line, Size slen, YY_BUFFER_STATE tmp_state)
{
	HbaInfo *info;
	YYSTYPE yysval;
	YY_BUFFER_STATE yy_buffer_state;
	int token;
	AssertArg(scanner && line && slen > 1);

	info = palloc0(sizeof(*info));
	info->line = line;
	yy_buffer_state = hba_yy_scan_buffer(line, slen, scanner);

	yysval.line = line;

	/* parse type */
	token = yylex(&yysval, scanner);
	if(token == HBA_CONF_COMMENT)
	{
		/* comment line */
		info->type = HBA_TYPE_EMPTY;
		token = yylex(&yysval, scanner);
		goto wait_eol_;
	}else if(token == HBA_CONF_EOL)
	{
		/* empty line */
		info->type = HBA_TYPE_EMPTY;
		goto wait_eol_;
	}else
	{
		process_hba_type(token, &yysval, info);
	}

	/* parse database */
	info->database = hba_lex_db_user(scanner, &yysval);
	info->db_loc = yysval.start;
	info->db_len = yysval.length;

	/* parse user */
	info->user = hba_lex_db_user(scanner, &yysval);
	info->user_loc = yysval.start;
	info->user_len = yysval.length;

	if(info->type != HBA_TYPE_LOCAL)
	{
		/* parse address when type is not local */
		token = yylex(&yysval, scanner);

		/* need ipv4 or ipv6 address */
		if(token == HBA_CONF_IPV4)
			info->addr_is_ipv6 = false;
		else if(token == HBA_CONF_IPV6)
			info->addr_is_ipv6 = true;
		else
			REPORT_HBA_LEX_ERROR(&yysval);
		info->addr = pnstrdup(yysval.line + yysval.start, yysval.length);
		info->addr_loc = yysval.start;
		info->addr_len = yysval.length;

		/* need '/' */
		token = yylex(&yysval, scanner);
		if(token != '/')
			REPORT_HBA_LEX_ERROR(&yysval);

		/* need address mark */
		token = yylex(&yysval, scanner);
		if(token != HBA_CONF_INTEGER)
			REPORT_HBA_LEX_ERROR(&yysval);
		if(info->addr_is_ipv6)
		{
			if(yysval.ival > 128)
				REPORT_HBA_LEX_ERROR(&yysval);
		}else /* if(info->addr_is_ipv6 == false) */
		{
			if(yysval.ival > 32)
				REPORT_HBA_LEX_ERROR(&yysval);
		}
		info->addr_mark = yysval.ival;
		info->mark_loc = yysval.start;
		info->mark_len = yysval.length;
	}

	/* parse auth method */
	info->auth_method = hba_lex_string(scanner, &yysval);
	info->method_loc = yysval.start;
	info->method_len = yysval.length;

	/* options */
	token = yylex(&yysval, scanner);
	if(token == HBA_CONF_COMMENT)
	{
		token = yylex(&yysval, scanner);
		goto wait_eol_;
	}else if(token == HBA_CONF_EOL)
	{
		goto wait_eol_;
	}else if(token != HBA_CONF_IPV4
		&& token != HBA_CONF_IPV6
		&& token != HBA_CONF_INTEGER)
	{
		/*if(info->opt_loc == 0)*/
			info->opt_loc = yysval.start;
	}else
	{
		REPORT_HBA_LEX_ERROR(&yysval);
	}

	token = yylex(&yysval, scanner);
	if(token != '=')
		REPORT_HBA_LEX_ERROR(&yysval);

	token = yylex(&yysval, scanner);
	if(token == HBA_CONF_COMMENT
		|| token == HBA_CONF_EOL)
	{
		REPORT_HBA_LEX_ERROR(&yysval);
	}

	info->opt_len = (yysval.start - info->opt_loc) + yysval.length;
	info->options = pnstrdup(yysval.line, info->opt_len);

	/* end of line */
	token = yylex(&yysval, scanner);
	if(token == HBA_CONF_COMMENT)
		token = yylex(&yysval, scanner);

wait_eol_:
	if(token != HBA_CONF_EOL)
	{
		REPORT_HBA_LEX_ERROR(&yysval);
	}

	hba_yy_switch_to_buffer(tmp_state, scanner);
	hba_yy_delete_buffer(yy_buffer_state, scanner);
	return info;
}

static void process_hba_type(int token, YYSTYPE *yysval, HbaInfo *info)
{
	AssertArg(yysval && info);
	switch(token)
	{
	case HBA_CONF_LOCAL:
		info->type = HBA_TYPE_LOCAL;
		break;
	case HBA_CONF_HOST:
		info->type = HBA_TYPE_HOST;
		break;
	case HBA_CONF_HOSTSSL:
		info->type = HBA_TYPE_HOSTSSL;
		break;
	case HBA_CONF_HOSTNOSSL:
		info->type = HBA_TYPE_HOSTNOSSL;
		break;
	default:
		REPORT_HBA_LEX_ERROR(yysval);
	}
	info->type_loc = yysval->start;
	info->type_len = yysval->length;
}

static char* hba_lex_string(yyscan_t scanner, YYSTYPE *yysval)
{
	char *result;
	int token;
	AssertArg(scanner && yysval && yysval->line);

	token = yylex(yysval, scanner);
	switch(token)
	{
	case HBA_CONF_INTEGER:
	case HBA_CONF_COMMENT:
	case HBA_CONF_EOL:
		REPORT_HBA_LEX_ERROR(yysval);
		return NULL;	/* keep compiler quiet */
	default:
		break;
	}
	result = pnstrdup(yysval->line + yysval->start, yysval->length);
	return result;
}

static char* hba_lex_db_user(yyscan_t scanner, YYSTYPE *yysval)
{
	char *result;
	int token;
	AssertArg(scanner && yysval && yysval->line);

	token = yylex(yysval, scanner);
	switch(token)
	{
	case HBA_CONF_COMMENT:
	case HBA_CONF_EOL:
		REPORT_HBA_LEX_ERROR(yysval);
		return NULL;	/* keep compiler quiet */
	default:
		break;
	}
	result = pnstrdup(yysval->line + yysval->start, yysval->length);
	return result;
}

static int process_integer(const char *token, YYSTYPE *lval)
{
	long		val;
	char	   *endptr;

	errno = 0;
	val = strtol(token, &endptr, 10);
	if (*endptr != '\0' || errno == ERANGE
#ifdef HAVE_LONG_INT_64
		/* if long > 32 bits, check for overflow of int4 */
		|| val != (long) ((int32) val)
#endif
		|| val < 0)
	{
		/* integer too large, treat it as a float */
		ereport(ERROR, (errmsg("hba file \"%s\" line %d column %d number \"%s\" out of range"
			, hba_flex_file_name, hba_flex_file_no, lval->start, token)));
	}
	lval->ival = (int)val;
	return HBA_CONF_INTEGER;
}

#define BUFSIZE 128
static char *hba_lex_get_line(FILE *fp, Size *slen)
{
	char		tmpbuf[BUFSIZE];
	char	   *buf;
	Size		buflen = BUFSIZE;
	Size		used = 0;

	buf = (char *) palloc(BUFSIZE);
	buf[0] = '\0';

	while (fgets(tmpbuf, BUFSIZE-1, fp) != NULL)
	{
		Size		thislen = strlen(tmpbuf);

		/* Append tmpbuf to whatever we had already */
		memcpy(buf + used, tmpbuf, thislen + 1);
		used += thislen;

		/* Done if we collected a newline */
		if (thislen > 0 && tmpbuf[thislen - 1] == '\n')
			break;

		/* Else, enlarge buf to ensure we can append next bufferload */
		buflen += BUFSIZE;
		buf = (char *) repalloc(buf, buflen);
	}

	if (used > 0)
	{
		*slen = used;
		buf[used+1] = buf[used+2] = YY_END_OF_BUFFER_CHAR;
		return buf;
	}

	/* Reached EOF */
	pfree(buf);
	return NULL;
}

static int hba_flex_fatal(const char *msg)
{
	ereport(ERROR,
		(errmsg("%s at file \"%s\" line %u", msg
			, hba_flex_file_name, hba_flex_file_no)));
	return 0;	/* keep compiler quiet */
}

void* hba_yyalloc(yy_size_t bytes, yyscan_t yyscanner)
{
	return palloc(bytes);
}

void* hba_yyrealloc(void *ptr, yy_size_t bytes, yyscan_t yyscanner)
{
	if (ptr)
		return repalloc(ptr, bytes);
	else
		return palloc(bytes);
}

void hba_yyfree(void *ptr, yyscan_t yyscanner)
{
	if (ptr)
		pfree(ptr);
}
