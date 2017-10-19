/*
  This code implements one part of functonality of
  free available library PL/Vision. Please look www.quest.com

  Original author: Steven Feuerstein, 1996 - 2002
  PostgreSQL implementation author: Pavel Stehule, 2006

  This module is under BSD Licence

  History:
    1.0. first public version 13. March 2006
*/
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "string.h"
#include "stdlib.h"
#include "utils/pg_locale.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "oraschema.h"

static int ora_instr_mb(text *txt, text *pattern, int start, int nth);
static text *ora_substr(Datum str, int start, int len);
static bool is_kind(char c, int kind);
static text *ora_concat2(text *str1, text *str2);
static text *ora_concat3(text *str1, text *str2, text *str3);

#define ora_substr_text(str, start, len) \
	ora_substr(PointerGetDatum((str)), (start), (len))

static const char* char_names[] = {
	"NULL","SOH","STX","ETX","EOT","ENQ","ACK","DEL",
	"BS",  "HT", "NL", "VT", "NP", "CR", "SO", "SI",
	"DLE", "DC1","DC2","DC3","DC4","NAK","SYN","ETB",
	"CAN", "EM","SUB","ESC","FS","GS","RS","US","SP"
};

#define NON_EMPTY_CHECK(str) \
if (VARSIZE_ANY_EXHDR(str) == 0) \
	ereport(ERROR, \
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
			 errmsg("invalid parameter"), \
		 errdetail("Not allowed empty string.")));

#define PARAMETER_ERROR(detail) \
	ereport(ERROR, \
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
		 errmsg("invalid parameter"), \
		 errdetail(detail)));

#ifndef _pg_mblen
#define _pg_mblen	pg_mblen
#endif

typedef enum
{
	POSITION,
	FIRST,
	LAST
}  position_mode;

#if PG_VERSION_NUM < 80400
text *
cstring_to_text_with_len(const char *c, int n)
{
	text *result;
	result = palloc(n + VARHDRSZ);
	SET_VARSIZE(result, n + VARHDRSZ);
	memcpy(VARDATA(result), c, n);

	return result;
}
#endif

/*
 * Make substring, can handle negative start
 */
int
ora_mb_strlen(text *str, char **sizes, int **positions)
{
	int r_len;
	int cur_size = 0;
	int sz;
	char *p;
	int cur = 0;

	p = VARDATA_ANY(str);
	r_len = VARSIZE_ANY_EXHDR(str);

	if (NULL != sizes)
		*sizes = palloc(r_len * sizeof(char));
	if (NULL != positions)
		*positions = palloc(r_len * sizeof(int));

	while (cur < r_len)
	{
		sz = _pg_mblen(p);
		if (sizes)
			(*sizes)[cur_size] = sz;
		if (positions)
			(*positions)[cur_size] = cur;
		cur += sz;
		p += sz;
		cur_size += 1;
	}

	return cur_size;
}

int
ora_mb_strlen1(text *str)
{
	int r_len;
	int c;
	char *p;

	r_len = VARSIZE_ANY_EXHDR(str);

	if (pg_database_encoding_max_length() == 1)
		return r_len;

	p = VARDATA_ANY(str);
	c = 0;
	while (r_len > 0)
	{
		int sz;

		sz = _pg_mblen(p);
		p += sz;
		r_len -= sz;
		c += 1;
	}

	return c;
}

/*
 * len < 0 means "length is not specified".
 */
static text *
ora_substr(Datum str, int start, int len)
{
	if (start == 0)
		start = 1;	/* 0 is interpreted as 1 */
	else if (start < 0)
	{
		text   *t;
		int32	n;

		t = DatumGetTextPP(str);
		n = pg_mbstrlen_with_len(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
		start = n + start + 1;
		if (start <= 0)
			return cstring_to_text("");
		str = PointerGetDatum(t);	/* save detoasted text */
	}

	if (len < 0)
		return DatumGetTextP(DirectFunctionCall2(text_substr_no_len,
			str, Int32GetDatum(start)));
	else
		return DatumGetTextP(DirectFunctionCall3(text_substr,
			str, Int32GetDatum(start), Int32GetDatum(len)));
}

/* simply search algorhitm - can be better */
static int
ora_instr_mb(text *txt, text *pattern, int start, int nth)
{
	int			c_len_txt, c_len_pat;
	int			b_len_pat;
	int		   *pos_txt;
	const char *str_txt, *str_pat;
	int			beg, end, i, dx;

	str_txt = VARDATA_ANY(txt);
	c_len_txt = ora_mb_strlen(txt, NULL, &pos_txt);
	str_pat = VARDATA_ANY(pattern);
	b_len_pat = VARSIZE_ANY_EXHDR(pattern);
	c_len_pat = pg_mbstrlen_with_len(str_pat, b_len_pat);

	if (start > 0)
	{
		dx = 1;
		beg = start - 1;
		end = c_len_txt - c_len_pat + 1;
		if (beg >= end)
			return 0;	/* out of range */
	}
	else
	{
		dx = -1;
		beg = Min(c_len_txt + start, c_len_txt - c_len_pat);
		end = -1;
		if (beg <= end)
			return 0;	/* out of range */
	}

	for (i = beg; i != end; i += dx)
	{
		if (memcmp(str_txt + pos_txt[i], str_pat, b_len_pat) == 0)
		{
			if (--nth == 0)
				return i + 1;
		}
	}

	return 0;
}

int
ora_instr(text *txt, text *pattern, int start, int nth)
{
	int			len_txt, len_pat;
	const char *str_txt, *str_pat;
	int			beg, end, i, dx;

	if (nth <= 0)
		PARAMETER_ERROR("Four parameter isn't positive.");

	/* Forward for multibyte strings */
	if (pg_database_encoding_max_length() > 1)
		return ora_instr_mb(txt, pattern, start, nth);

	str_txt = VARDATA_ANY(txt);
	len_txt = VARSIZE_ANY_EXHDR(txt);
	str_pat = VARDATA_ANY(pattern);
	len_pat = VARSIZE_ANY_EXHDR(pattern);

	if (start > 0)
	{
		dx = 1;
		beg = start - 1;
		end = len_txt - len_pat + 1;
		if (beg >= end)
			return 0;	/* out of range */
	}
	else
	{
		dx = -1;
		beg = Min(len_txt + start, len_txt - len_pat);
		end = -1;
		if (beg <= end)
			return 0;	/* out of range */
	}

	for (i = beg; i != end; i += dx)
	{
		if (memcmp(str_txt + i, str_pat, len_pat) == 0)
		{
			if (--nth == 0)
				return i + 1;
		}
	}

	return 0;
}

/****************************************************************
 * oracle.instr
 *
 * Syntax:
 *   FUNCTION oracle.instr (string_in VARCHAR, pattern VARCHAR)
 *   FUNCTION oracle.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER)
 *   FUNCTION oracle.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER, nth INTEGER)
 *            RETURN INT;
 *
 * Purpouse:
 *   Search pattern in string.
 *
 ****************************************************************/
Datum
orastr_instr2 (PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);

	PG_RETURN_INT32(ora_instr(arg1, arg2, 1, 1));
}

Datum
orastr_instr3 (PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);
	int arg3 = PG_GETARG_INT32(2);

	PG_RETURN_INT32(ora_instr(arg1, arg2, arg3, 1));
}

Datum
orastr_instr4 (PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);
	int arg3 = PG_GETARG_INT32(2);
	int arg4 = PG_GETARG_INT32(3);

	PG_RETURN_INT32(ora_instr(arg1, arg2, arg3, arg4));
}

/****************************************************************
 * oracle.normalize
 *
 * Syntax:
 *   FUNCTION oracle.normalize (string_in IN VARCHAR)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Normalize string - replace white chars by space, replace
 * spaces by space
 *
 ****************************************************************/
Datum
orastr_normalize(PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	text *result;
	char *aux, *aux_cur;
	int i, l;
	char c, *cur;
	bool write_spc = false;
	bool ignore_stsp = true;
	bool mb_encode;
	int sz;

	mb_encode = pg_database_encoding_max_length() > 1;

	l = VARSIZE_ANY_EXHDR(str);
	aux_cur = aux = palloc(l);

	write_spc = false;
	cur = VARDATA_ANY(str);

	for (i = 0; i < l; i++)
	{
		switch ((c = *cur))
		{
			case '\t':
			case '\n':
			case '\r':
			case ' ':
				write_spc = ignore_stsp ? false : true;
				break;
			default:
				/* ignore all other unvisible chars */

				if (mb_encode)
				{
					sz = _pg_mblen(cur);
					if (sz > 1 || (sz == 1 && c > 32))
					{
						int j;

						if (write_spc)
						{
							*aux_cur++ = ' ';
							write_spc = false;

						}
						for (j = 0; j < sz; j++)
						{
							*aux_cur++ = *cur++;
						}
						ignore_stsp = false;
						i += sz - 1;
					}
					continue;

				}
				else
					if (c > 32)
					{
						if (write_spc)
						{
							*aux_cur++ = ' ';
							write_spc = false;
						}
						*aux_cur++ = c;
						ignore_stsp = false;
						continue;
					}

		}
		cur += 1;
	}

	l = aux_cur - aux;
	result = palloc(l+VARHDRSZ);
	SET_VARSIZE(result, l + VARHDRSZ);
	memcpy(VARDATA(result), aux, l);

	PG_RETURN_TEXT_P(result);
}

/****************************************************************
 * oracle.is_prefix
 *
 * Syntax:
 *   FUNCTION oracle.is_prefix (string_in IN VARCHAR,
 *                    prefix_in VARCHAR,
 *                    case_sensitive BOOL := true)
 *      RETURN bool;
 *   FUNCTION oracle.is_prefix (num_in IN NUMERIC,
 *                    prefix_in NUMERIC) RETURN bool;
 *   FUNCTION oracle.is_prefix (int_in IN INT,
 *                    prefix_in INT)  RETURN bool;
 *
 * Purpouse:
 *   Returns true, if prefix_in is prefix of string_in
 *
 ****************************************************************/
Datum
orastr_is_prefix_text (PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	text *prefix = PG_GETARG_TEXT_PP(1);
	bool case_sens = PG_GETARG_BOOL(2);
	bool mb_encode;

	int str_len = VARSIZE_ANY_EXHDR(str);
	int pref_len = VARSIZE_ANY_EXHDR(prefix);

	int i;
	char *ap, *bp;


	mb_encode = pg_database_encoding_max_length() > 1;

	if (mb_encode && !case_sens)
	{
		str = (text*)DatumGetPointer(DirectFunctionCall1(lower, PointerGetDatum(str)));
		prefix = (text*)DatumGetPointer(DirectFunctionCall1(lower, PointerGetDatum(prefix)));
	}

	ap = VARDATA_ANY(str);
	bp = VARDATA_ANY(prefix);

	for (i = 0; i < pref_len; i++)
	{
		if (i >= str_len)
			break;
		if (case_sens || mb_encode)
		{
			if (*ap++ != *bp++)
				break;
		}
		else if (!mb_encode)
		{
			if (pg_toupper((unsigned char) *ap++) != pg_toupper((unsigned char) *bp++))
				break;
		}
	}

	PG_RETURN_BOOL(i == pref_len);
}

Datum
orastr_is_prefix_int (PG_FUNCTION_ARGS)
{
	int n = PG_GETARG_INT32(0);
	int prefix = PG_GETARG_INT32(1);
	bool result = false;

	do
	{
		if (n == prefix)
		{
			result = true;
			break;
		}
		n = n / 10;

	} while (n >= prefix);

	PG_RETURN_BOOL(result);
}

Datum
orastr_is_prefix_int64 (PG_FUNCTION_ARGS)
{
	int64 n = PG_GETARG_INT64(0);
	int64 prefix = PG_GETARG_INT64(1);
	bool result = false;

	do
	{
		if (n == prefix)
		{
			result = true;
			break;
		}
		n = n / 10;

	} while (n >= prefix);

	PG_RETURN_BOOL(result);
}


/****************************************************************
 * oracle.rvrs
 *
 * Syntax:
 *   FUNCTION oracle.rvrs (string_in IN VARCHAR,
 *					  start_in IN INTEGER := 1,
 *					  end_in IN INTEGER := NULL)
 *  	RETURN VARCHAR2;
 *
 * Purpouse:
 *   Reverse string or part of string
 *
 ****************************************************************/
Datum
orastr_rvrs(PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	int start = PG_GETARG_INT32(1);
	int end = PG_GETARG_INT32(2);
	int len;
	int i;
	int new_len;
	text *result;
	char *data;
	char *sizes = NULL;
	int *positions = NULL;
	bool mb_encode;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	mb_encode = pg_database_encoding_max_length() > 1;

	if (!mb_encode)
		len = VARSIZE_ANY_EXHDR(str);
	else
		len = ora_mb_strlen(str, &sizes, &positions);



	start = PG_ARGISNULL(1) ? 1 : start;
	end = PG_ARGISNULL(2) ? (start < 0 ? -len : len) : end;

	if ((start > end && start > 0) || (start < end && start < 0))
		PARAMETER_ERROR("Second parameter is bigger than third.");

	if (start < 0)
	{
		end = len + start + 1;
		start = end;
	}

	new_len = end - start + 1;

	if (mb_encode)
	{
		int max_size;
		int cur_size;
		char *p;
		int j;
		int fz_size;

		fz_size = VARSIZE_ANY_EXHDR(str);

		if ((max_size = (new_len*pg_database_encoding_max_length())) > fz_size)
			result = palloc(fz_size + VARHDRSZ);
		else
			result = palloc(max_size + VARHDRSZ);
		data = (char*) VARDATA(result);

		cur_size = 0;
		p = VARDATA_ANY(str);
		for (i = end - 1; i>= start - 1; i--)
		{
			for (j=0; j<sizes[i]; j++)
				*data++ = *(p+positions[i]+j);
			cur_size += sizes[i];
		}
		SET_VARSIZE(result, cur_size + VARHDRSZ);

	}
	else
	{
		char *p = VARDATA_ANY(str);
		result = palloc(new_len + VARHDRSZ);
		data = (char*) VARDATA(result);
		SET_VARSIZE(result, new_len + VARHDRSZ);

		for (i = end - 1; i >= start - 1; i--)
			*data++ = p[i];
	}

	PG_RETURN_TEXT_P(result);
}

/****************************************************************
 * oracle.left
 *
 * Syntax:
 *   FUNCTION oracle.left (string_in IN VARCHAR,
 *							num_in INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns firs num_in charaters. You can use negative num_in
 *   left('abcde', -2) -> abc
 *
 ****************************************************************/
Datum
orastr_left (PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_P(0);
	int n = PG_GETARG_INT32(1);
	if (n < 0)
		n = ora_mb_strlen1(str) + n;
	n = n < 0 ? 0 : n;

	PG_RETURN_TEXT_P(ora_substr_text(str, 1, n));
}

/****************************************************************
 * oracle.right
 *
 * Syntax:
 *   FUNCTION oracle.right (string_in IN VARCHAR,
 *							num_in INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns last (right) num_in characters.
 *
 ****************************************************************/
Datum
orastr_right (PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_P(0);
	int n = PG_GETARG_INT32(1);
	if (n < 0)
		n = ora_mb_strlen1(str) + n;
	n = (n < 0) ? 0 : n;

	PG_RETURN_TEXT_P(ora_substr_text(str, -n, -1));
}

/****************************************************************
 * oracle.nth
 *
 * Syntax:
 *   FUNCTION oracle.nth (string_in IN VARCHAR,
 * 					 nth_in IN INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to return the Nth character in a string.
 *
 ****************************************************************/
Datum
orachr_nth (PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), 1));
}

/****************************************************************
 * oracle.first
 *
 * Syntax:
 *   FUNCTION oracle.first (string_in IN VARCHAR,
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to return the first character in a string.
 *
 ****************************************************************/
Datum
orachr_first (PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), 1, 1));
}

/****************************************************************
 * oracle.last
 *
 * Syntax:
 *   FUNCTION oracle.last (string_in IN VARCHAR,
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to return the last character in a string.
 *
 ****************************************************************/
Datum
orachr_last (PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), -1, 1));
}

/****************************************************************
 * oracle.is_blank, oracle.is_digit, ...
 *
 * Syntax:
 *   FUNCTION oracle.is_kind (string_in IN VARCHAR,
 *      kind INT)
 *          RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to see if a character is blank, ...
 *   1 blank, 2 digit, 3 quote, 4 other, 5 letter
 *
 ****************************************************************/
static bool
is_kind(char c, int kind)
{
	switch (kind)
	{
		case 1:
			return c == ' ';
		case 2:
			return '0' <= c && c <= '9';
		case 3:
			return c == '\'';
		case 4:
			return
				(32 <= c && c <= 47) ||
				(58 <= c && c <= 64) ||
				(91 <= c && c <= 96) || (123 <= c && c <= 126);
		case 5:
			return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
		default:
			PARAMETER_ERROR("Second parametr isn't in enum {1,2,3,4,5}");
			return false;
	}
}

Datum
orachr_is_kind_i (PG_FUNCTION_ARGS)
{
	int32 c = PG_GETARG_INT32(0);
	int32 k = PG_GETARG_INT32(1);

	PG_RETURN_INT32(is_kind((char)c,k));
}

Datum
orachr_is_kind_a (PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	int32 k = PG_GETARG_INT32(1);
	char c;

	NON_EMPTY_CHECK(str);
	if (pg_database_encoding_max_length() > 1)
	{
		if (_pg_mblen(VARDATA_ANY(str)) > 1)
			PG_RETURN_INT32( (k == 5) );
	}

	c = *VARDATA_ANY(str);
	PG_RETURN_INT32(is_kind(c,k));
}

/****************************************************************
 * oracle.char_name
 *
 * Syntax:
 *   FUNCTION oracle.char_name (letter_in IN VARCHAR)
 *   	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns the name of the character to ascii code as a VARCHAR.
 *
 ****************************************************************/
Datum
orachr_char_name(PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	text *result;
	unsigned char c;

	NON_EMPTY_CHECK(str);
	c = (unsigned char)*(VARDATA_ANY(str));

	if (c >= lengthof(char_names))
		result = ora_substr_text(str, 1, 1);
	else
		result = cstring_to_text(char_names[c]);

	PG_RETURN_TEXT_P(result);
}

/****************************************************************
 * substr
 *
 * Syntax:
 *   FUNCTION substr (string, start_position, [length])
 *   	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns len chars from start_in position, compatible with Oracle
 *
 ****************************************************************/
Datum
orastr_substr3(PG_FUNCTION_ARGS)
{
	int32	len = PG_GETARG_INT32(2);
	if (len < 0)
		PG_RETURN_NULL();
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), len));
}

Datum
orastr_substr2(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), -1));
}

static text*
ora_concat2(text *str1, text *str2)
{
	int l1;
	int l2;
	text *result;

	l1 = VARSIZE_ANY_EXHDR(str1);
	l2 = VARSIZE_ANY_EXHDR(str2);

	result = palloc(l1+l2+VARHDRSZ);
	memcpy(VARDATA(result), VARDATA_ANY(str1), l1);
	memcpy(VARDATA(result) + l1, VARDATA_ANY(str2), l2);
	SET_VARSIZE(result, l1 + l2 + VARHDRSZ);

	return result;
}

static text*
ora_concat3(text *str1, text *str2, text *str3)
{
	int l1;
	int l2;
	int l3;
	text *result;

	l1 = VARSIZE_ANY_EXHDR(str1);
	l2 = VARSIZE_ANY_EXHDR(str2);
	l3 = VARSIZE_ANY_EXHDR(str3);

	result = palloc(l1+l2+l3+VARHDRSZ);
	memcpy(VARDATA(result), VARDATA_ANY(str1), l1);
	memcpy(VARDATA(result) + l1, VARDATA_ANY(str2), l2);
	memcpy(VARDATA(result) + l1+l2, VARDATA_ANY(str3), l3);
	SET_VARSIZE(result, l1 + l2 + l3 + VARHDRSZ);

	return result;
}

/****************************************************************
 * oracle.swap
 *
 * Syntax:
 *    FUNCTION swap
 *      (string_in IN VARCHAR2,
 *       replace_in IN VARCHAR2,
 *       start_in IN INTEGER := 1,
 *       oldlen_in IN INTEGER := NULL)
 *  RETURN VARCHAR2
 *
 * Purpouse:
 *   Replace a substring in a string with a specified string.
 *
 ****************************************************************/
Datum
orastr_swap(PG_FUNCTION_ARGS)
{
	text *string_in;
	text *replace_in;
	int start_in = 1;
	int oldlen_in;
	int v_len;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		string_in = PG_GETARG_TEXT_P(0);

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	else
		replace_in = PG_GETARG_TEXT_P(1);

	if (!PG_ARGISNULL(2))
		start_in = PG_GETARG_INT32(2);

	if (PG_ARGISNULL(3))
		oldlen_in = ora_mb_strlen1(replace_in);
	else
		oldlen_in = PG_GETARG_INT32(3);

	v_len =  ora_mb_strlen1(string_in);

	start_in = start_in > 0 ? start_in : v_len + start_in + 1;

	if (start_in == 0 || start_in > v_len)
		PG_RETURN_TEXT_P(TextPCopy(string_in));
	else if (start_in == 1)
		PG_RETURN_TEXT_P(ora_concat2(
			replace_in, ora_substr_text(string_in, oldlen_in+1, -1)));
	else
		PG_RETURN_TEXT_P(ora_concat3(
			ora_substr_text(string_in, 1, start_in - 1),
			replace_in,
			ora_substr_text(string_in, start_in + oldlen_in, -1)));
}

/****************************************************************
 * oracle.betwn
 *
 * Find the Substring Between Start and End Locations
 *
 * Syntax:
 *     FUNCTION oracle.betwn (string_in IN VARCHAR2,
 *       start_in IN INTEGER,
 *       end_in IN INTEGER,
 *       inclusive IN BOOLEAN := TRUE)
 *      RETURN VARCHAR2;
 *
 *     FUNCTION oracle.betwn (string_in IN VARCHAR2,
 *       start_in IN VARCHAR2,
 *       end_in IN VARCHAR2 := NULL,
 *       startnth_in IN INTEGER := 1,
 *       endnth_in IN INTEGER := 1,
 *       inclusive IN BOOLEAN := TRUE,
 *       gotoend IN BOOLEAN := FALSE)
 *      RETURN VARCHAR2;
 *
 * Purpouse:
 *   Call this function to extract a sub-string from a string. This
 * function is overloaded. You can either provide the start and end
 * locations or you can provide start and end substrings.
 *
 ****************************************************************/
Datum
orastr_betwn_i(PG_FUNCTION_ARGS)
{
	text *string_in = PG_GETARG_TEXT_P(0);
	int start_in = PG_GETARG_INT32(1);
	int end_in = PG_GETARG_INT32(2);
	bool inclusive = PG_GETARG_BOOL(3);

	if ((start_in < 0 && end_in > 0) ||
		(start_in > 0 && end_in < 0) ||
		(start_in > end_in))
		PARAMETER_ERROR("Wrong positions.");

	if (start_in < 0)
	{
		int v_len =  ora_mb_strlen1(string_in);
		start_in = v_len + start_in + 1;
		end_in = v_len + start_in + 1;
	}

	if (!inclusive)
	{
		start_in += 1;
		end_in -= 1;

		if (start_in > end_in)
			PG_RETURN_TEXT_P(cstring_to_text(""));
	}

	PG_RETURN_TEXT_P(ora_substr_text(string_in,
									 start_in,
									 end_in - start_in + 1));
}

Datum
orastr_betwn_c(PG_FUNCTION_ARGS)
{
	text *string_in;
	text *start_in;
	text *end_in;
	int startnth_in;
	int endnth_in;
	bool inclusive;
	bool gotoend;

	int v_start;
	int v_end;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4) ||
		PG_ARGISNULL(5) || PG_ARGISNULL(6))
		PG_RETURN_NULL();


	string_in = PG_GETARG_TEXT_P(0);
	start_in = PG_GETARG_TEXT_P(1);
	end_in = PG_ARGISNULL(2) ? start_in : PG_GETARG_TEXT_P(2);
	startnth_in = PG_GETARG_INT32(3);
	endnth_in = PG_GETARG_INT32(4);
	inclusive = PG_GETARG_BOOL(5);
	gotoend = PG_GETARG_BOOL(6);

	if (startnth_in == 0)
	{
		v_start = 1;
		v_end = ora_instr(string_in, end_in, 1, endnth_in);
	}
	else
	{
		v_start = ora_instr(string_in, start_in, 1, startnth_in);
		v_end = ora_instr(string_in, end_in, v_start + 1, endnth_in);
	}

	if (v_start == 0)
		PG_RETURN_NULL();

	if (!inclusive)
	{
		if (startnth_in > 0)
			v_start += ora_mb_strlen1(start_in);

		v_end -= 1;
	}
	else
		v_end += (ora_mb_strlen1(end_in) - 1);

	if (((v_start > v_end) && (v_end > 0)) ||
		(v_end <= 0 && !gotoend))
		PG_RETURN_NULL();

	if (v_end <= 0)
		v_end = ora_mb_strlen1(string_in);

	PG_RETURN_TEXT_P(ora_substr_text(string_in,
									 v_start,
									 v_end - v_start + 1));
}

Datum
orastr_bpcharlen(PG_FUNCTION_ARGS)
{
    BpChar     *arg = PG_GETARG_BPCHAR_PP(0);
    int         len;

    /* byte-length of the argument (trailing spaces not ignored) */
    len = VARSIZE_ANY_EXHDR(arg);

    /* in multibyte encoding, convert to number of characters */
    if (pg_database_encoding_max_length() != 1)
        len = pg_mbstrlen_with_len(VARDATA_ANY(arg), len);

    PG_RETURN_INT32(len);
}

/*                              ABCDEFGHIJKLMNOPQRSTUVWXYZ */
static const char* sound_map = "01230120022455012623010202";

#define SM_IDX(c)   (pg_toupper(c) - 'A')
#define SD_LEN      4

Datum
orastr_soundex(PG_FUNCTION_ARGS)
{
	char *str = text_to_cstring((const text *)PG_GETARG_TEXT_P(0));
	char cursmv, presmv = '0';
	int cnt = 0;
	char outstr[SD_LEN + 1] = {0};

	/* try to get first alphanumeric character. */
	while (str[0] && !isalpha(str[0]))
		str++;

	/* return null if no alphanumeric character. */
	if (!str[0])
		PG_RETURN_NULL();

	/* push the first uppercase letter to outstr */
	outstr[cnt++] = pg_toupper(*str++);

	while (str[0] && cnt < SD_LEN)
	{
		if (isalpha(*str))
		{
			cursmv = sound_map[SM_IDX(str[0])];
			if (cursmv != '0' && cursmv != presmv)
			{
				outstr[cnt++] = cursmv;
				presmv = cursmv;
			}
		}
		str++;
	}

	while (cnt < SD_LEN)
		outstr[cnt++] = '0';
	outstr[cnt] = '\0';

	PG_RETURN_TEXT_P(cstring_to_text(outstr));
}

#define PG_GETARG_NAME_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_NAME(_n) : NULL)

Datum
orastr_convert2(PG_FUNCTION_ARGS)
{
	return orastr_convert(fcinfo);
}

/*
 * CONVERT(char, dest_char_set[, source_char_set ])
 */
Datum
orastr_convert(PG_FUNCTION_ARGS)
{
	text	*src = PG_GETARG_TEXT_P_COPY(0);
	char	*dest_encoding_name = NameStr(*PG_GETARG_NAME(1));
	int		dest_encoding = pg_char_to_encoding(dest_encoding_name);
	Name	src_encoding_Name = PG_GETARG_NAME_IF_EXISTS(2);
	int		src_encoding;
	const char *src_str;
	char	*dest_str;
	int		len;

	if (dest_encoding < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid destination encoding name \"%s\"",
						dest_encoding_name)));

	if (src_encoding_Name)
	{
		src_encoding = pg_char_to_encoding(NameStr(*src_encoding_Name));
	} else
	{
		src_encoding = GetDatabaseEncoding();
	}

	/* make sure that source string is valid */
	len = VARSIZE_ANY_EXHDR(src);
	src_str = VARDATA_ANY(src);
	pg_verify_mbstr_len(src_encoding, src_str, len, false);

	dest_str = (char *) pg_do_encoding_conversion(
				(unsigned char *) src_str, len, src_encoding, dest_encoding);
	if (dest_str != src_str)
		len = strlen(dest_str);

	/* free memory if allocated by the toaster */
	PG_FREE_IF_COPY(src, 0);

	PG_RETURN_TEXT_P(cstring_to_text_with_len((const char *)dest_str, len));
}

/*
 * TRANSLATE(expr, from_string, to_string)
 */
Datum
orastr_translate(PG_FUNCTION_ARGS)
{
	int			source_len,
				from_len,
				to_len;

	source_len = VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_PP(0));
	from_len = VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_PP(1));
	to_len = VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_PP(2));

	if (source_len <= 0 || from_len <= 0 || to_len <= 0)
		PG_RETURN_NULL();

	return translate(fcinfo);
}

/*
 * NLS_CHARSET_ID(string)
 */
Datum
orastr_nls_charset_id(PG_FUNCTION_ARGS)
{
	Name		s = PG_GETARG_NAME(0);
	int			r;

	r = pg_char_to_encoding(NameStr(*s));
	if (r < 0)
		PG_RETURN_NULL();
	PG_RETURN_INT32(r);
}

/*
 * NLS_CHARSET_NAME(number)
 */
Datum
orastr_nls_charset_name(PG_FUNCTION_ARGS)
{
	return PG_encoding_to_char(fcinfo);
}
