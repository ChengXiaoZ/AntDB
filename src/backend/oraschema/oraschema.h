#ifndef _ORA_SCHEMA_H_
#define _ORA_SCHEMA_H_

/*
 * Use internal
 *
 * Location:
 *		src/backend/oraschema/oraschema.h
 */

#include "postgres.h"
#include "catalog/catversion.h"
#include "nodes/pg_list.h"
#include <sys/time.h>
#include "utils/datetime.h"
#include "utils/datum.h"

#define TextPCopy(t) \
	DatumGetTextP(datumCopy(PointerGetDatum(t), false, -1))

#define PG_GETARG_IF_EXISTS(n, type, defval) \
	((PG_NARGS() > (n) && !PG_ARGISNULL(n)) ? PG_GETARG_##type(n) : (defval))

extern int ora_instr(text *txt, text *pattern, int start, int nth);
extern int ora_mb_strlen(text *str, char **sizes, int **positions);
extern int ora_mb_strlen1(text *str);

extern int ora_seq_search(const char *name, char * array[], int max);

#if PG_VERSION_NUM >= 80400
extern Oid equality_oper_funcid(Oid argtype);
#endif

#endif /* _ORA_SCHEMA_H_ */