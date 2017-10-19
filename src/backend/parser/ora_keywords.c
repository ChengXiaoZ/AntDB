#include "postgres.h"

#include "nodes/parsenodes.h"
#include "parser/ora_gramparse.h"
#include "parser/keywords.h"
/* we don't need YYSTYPE */
#define YYSTYPE_IS_DECLARED
#include "ora_gram.h"

#define PG_KEYWORD(a,b,c) {a,b,c},

const ScanKeyword OraScanKeywords[]={
#include "parser/ora_kwlist.h"
};

const int OraNumScanKeywords = lengthof(OraScanKeywords);
