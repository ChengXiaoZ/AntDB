
%{

#include "postgres.h"

#include <ctype.h>
#include <unistd.h>

#include "conf_scan.h"

#undef fprintf
#define fprintf(file, fmt, msg) conf_flex_fatal(msg)
#define YYSTYPE conf_YYSTYPE
#define YY_USER_ACTION							\
	do{											\
		yylval->start = yytext - yylval->line;	\
		yylval->length = yyleng;				\
	}while(0);

typedef enum CONF_SCAN_TOKEN
{
	SCAN_CONF_ID = 1,
	SCAN_CONF_STRING,
	SCAN_CONF_INTEGER,
	SCAN_CONF_REAL,
	SCAN_CONF_EQUALS,
	SCAN_CONF_UNQUOTED_STRING,
	SCAN_CONF_QUALIFIED_ID,
	SCAN_CONF_COMMENT,
	SCAN_CONF_EOL = 99,
	SCAN_CONF_ERROR
}CONF_SCAN_TOKEN;

typedef struct conf_YYSTYPE
{
	char *line;
	int   start;
	int   length;
}conf_YYSTYPE;

static unsigned int conf_flex_file_no;
static const char *conf_flex_file_name;

static int conf_flex_fatal(const char *msg);

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
%option prefix="conf_yy"


SIGN			("-"|"+")
DIGIT			[0-9]
HEXDIGIT		[0-9a-fA-F]

UNIT_LETTER		[a-zA-Z]

INTEGER			{SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*

EXPONENT		[Ee]{SIGN}?{DIGIT}+
REAL			{SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?

LETTER			[A-Za-z_\200-\377]
LETTER_OR_DIGIT [A-Za-z_0-9\200-\377]

ID				{LETTER}{LETTER_OR_DIGIT}*
QUALIFIED_ID	{ID}"."{ID}

UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
STRING			\'([^'\\\n]|\\.|\'\')*\'

%%

\n					conf_flex_file_no++; return SCAN_CONF_EOL;
[ \t\r]+			/* eat whitespace */
#.*					return SCAN_CONF_COMMENT;/* .* matches anything until newline */

{ID}				return SCAN_CONF_ID;
{QUALIFIED_ID}		return SCAN_CONF_QUALIFIED_ID;
{STRING}			return SCAN_CONF_STRING;
{UNQUOTED_STRING}	return SCAN_CONF_UNQUOTED_STRING;
{INTEGER}			return SCAN_CONF_INTEGER;
{REAL}				return SCAN_CONF_REAL;
=					return SCAN_CONF_EQUALS;

.					return SCAN_CONF_ERROR;

%%

static ConfInfo * parse_conf_file_internal(yyscan_t scanner, const char *filename, YY_BUFFER_STATE tmp_state, bool ignore_open);
static ConfInfo * parse_conf_line(yyscan_t scanner, char *line, Size slen, YY_BUFFER_STATE tmp_state);
static char* conf_lex_get_line(FILE *fp, Size *slen);
static char* conf_get_yystype_value(const conf_YYSTYPE *yystype);

ConfInfo * parse_conf_file(const char *filename)
{
	yyscan_t scanner;
	ConfInfo *result;
	YY_BUFFER_STATE tmp_state;
	char tmp_buf[2];

	if(conf_yylex_init(&scanner) != 0)
		ereport(ERROR, (errmsg("conf_yylex_init() failed:%m")));
	tmp_buf[0] = tmp_buf[1] = '\0';
	tmp_state = conf_yy_scan_buffer(tmp_buf, 2, scanner);

	conf_flex_file_no = 1;
	conf_flex_file_name = NULL;
	result = parse_conf_file_internal(scanner, filename, tmp_state, false);
	conf_yy_delete_buffer(tmp_state, scanner);
	conf_yylex_destroy(scanner);
	return result;
}

static ConfInfo * parse_conf_file_internal(yyscan_t scanner, const char *filename, YY_BUFFER_STATE tmp_state, bool ignore_open)
{
	const char *save_conf_flex_file_name;
	volatile FILE *fp;
	ConfInfo *result
			,**prev_info
			,*cur_info;
	char *line;
	Size slen;
	int save_conf_file_no;

	fp = fopen(filename, "r");
	if(fp == NULL)
	{
		if(ignore_open)
			return NULL;
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("Can not open file \"%s\" for read:%m", filename)));
	}

	result = NULL;
	prev_info = &result;
	save_conf_flex_file_name = conf_flex_file_name;
	save_conf_file_no = conf_flex_file_no;
	conf_flex_file_no = 1;

	PG_TRY();
	{
		conf_flex_file_name = pstrdup(filename);
		while((line = conf_lex_get_line((FILE*)fp, &slen)) != NULL)
		{
			cur_info = parse_conf_line(scanner, line, slen+2, tmp_state);
			cur_info->filename = (char*)conf_flex_file_name;
			*prev_info = cur_info;
			prev_info = &(cur_info->next);
		}
	}PG_CATCH();
	{
		fclose((FILE*)fp);
		PG_RE_THROW();
	}PG_END_TRY();

	fclose((FILE*)fp);
	if(result == NULL)
		pfree((char*)conf_flex_file_name);
	conf_flex_file_name = save_conf_flex_file_name;
	conf_flex_file_no = save_conf_file_no;
	return result;
}

#define REPORT_LEX_ERROR()	\
	do{						\
		ereport(ERROR, (errmsg("syntax error on file \"%s\" line %d column %d"	\
				, conf_flex_file_name, conf_flex_file_no, yystype.start)));		\
	}while(0)
static ConfInfo * parse_conf_line(yyscan_t scanner, char *line, Size slen, YY_BUFFER_STATE tmp_state)
{
	ConfInfo *info;
	conf_YYSTYPE yystype;
	YY_BUFFER_STATE yy_buffer_state;
	CONF_SCAN_TOKEN token;
	AssertArg(scanner && line && slen > 1);

	info = palloc0(sizeof(*info));
	info->line = line;
	yy_buffer_state = conf_yy_scan_buffer(line, slen, scanner);

	/*memset(&yystype, 0, sizeof(yystype));*/
	yystype.line = line;
	token = yylex(&yystype, scanner);

	/* first token on line is option name */
	if(token == SCAN_CONF_COMMENT)
	{
		/* comment line */
		token = yylex(&yystype, scanner);
		goto wait_eol_;
	}else if(token == SCAN_CONF_EOL)
	{
		/* empty line */
		goto wait_eol_;
	}else if(token != SCAN_CONF_ID
		&& token != SCAN_CONF_QUALIFIED_ID)
	{
		REPORT_LEX_ERROR();
	}
	info->name = conf_get_yystype_value(&yystype);
	info->name_loc = yystype.start;
	info->name_len = yystype.length;

	/* next we have an optional equal sign; discard if present */
	token = yylex(&yystype, scanner);
	if(token == SCAN_CONF_EQUALS)
		token = yylex(&yystype, scanner);

	/* now we must have the option value */
	if (token != SCAN_CONF_ID &&
		token != SCAN_CONF_STRING &&
		token != SCAN_CONF_INTEGER &&
		token != SCAN_CONF_REAL &&
		token != SCAN_CONF_UNQUOTED_STRING)
	{
		REPORT_LEX_ERROR();
	}
	info->value = conf_get_yystype_value(&yystype);
	info->value_loc = yystype.start;
	info->value_len = yystype.length;

	/* now we'd like an end of line, or possibly EOF */
	token = yylex(&yystype, scanner);
	if(token == SCAN_CONF_COMMENT)
		token = yylex(&yystype, scanner);
	if(token != SCAN_CONF_EOL)
		REPORT_LEX_ERROR();

wait_eol_:
	if(token != SCAN_CONF_EOL)
	{
		REPORT_LEX_ERROR();
	}

	if(info->name != NULL
		&& (pg_strcasecmp(info->name, "include_dir") == 0
		|| pg_strcasecmp(info->name, "include_if_exists") == 0
		|| pg_strcasecmp(info->name, "include") == 0))
	{
		ereport(ERROR, (errmsg("configure file \"%s\" not support \"%s\" yet!"
			, conf_flex_file_name, info->name)));
	}

	conf_yy_switch_to_buffer(tmp_state, scanner);
	conf_yy_delete_buffer(yy_buffer_state, scanner);
	return info;
}

static char* conf_get_yystype_value(const conf_YYSTYPE *yystype)
{
	char *val;
	AssertArg(yystype && yystype->line);
	val = palloc(yystype->length+1);
	memcpy(val, yystype->line + yystype->start, yystype->length);
	val[yystype->length] = '\0';
	return val;
}

#define BUFSIZE 128
static char *conf_lex_get_line(FILE *fp, Size *slen)
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

static int conf_flex_fatal(const char *msg)
{
	ereport(ERROR,
		(errmsg("%s at file \"%s\" line %u", msg, conf_flex_file_name, conf_flex_file_no)));
	return 0;	/* keep compiler quiet */
}

void *
conf_yyalloc(yy_size_t bytes, yyscan_t yyscanner)
{
	return palloc(bytes);
}

void *
conf_yyrealloc(void *ptr, yy_size_t bytes, yyscan_t yyscanner)
{
	if (ptr)
		return repalloc(ptr, bytes);
	else
		return palloc(bytes);
}

void
conf_yyfree(void *ptr, yyscan_t yyscanner)
{
	if (ptr)
		pfree(ptr);
}
