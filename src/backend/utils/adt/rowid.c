/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2015-2017 AntDB Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"

typedef struct OraRowID
{
	int32			node_id;
	BlockNumber		block;
	OffsetNumber	offset;
}OraRowID;

static int rowid_compare(const OraRowID *l, const OraRowID *r);

Datum rowid_in(PG_FUNCTION_ARGS)
{
	OraRowID *rowid;
	const char *str;
	StaticAssertStmt(offsetof(OraRowID, offset)+sizeof(OffsetNumber) == 10, "please change pg_type");

	str = PG_GETARG_CSTRING(0);
	Assert(str);
	if(strlen(str) != (8+8+4))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
			, errmsg("invalid argument string length")));

	rowid = palloc(sizeof(*rowid));
	b64_decode(str, 8, (char*)&(rowid->node_id));
	b64_decode(str+8, 8, (char*)&(rowid->block));
	b64_decode(str+16, 4, (char*)&(rowid->offset));

	PG_RETURN_POINTER(rowid);
}

Datum rowid_out(PG_FUNCTION_ARGS)
{
	OraRowID *rowid = (OraRowID*)PG_GETARG_POINTER(0);
	char *output = palloc(8+8+4+1);
	b64_encode((char*)&(rowid->node_id), sizeof(rowid->node_id), output);
	b64_encode((char*)&(rowid->block), sizeof(rowid->block), output+8);
	b64_encode((char*)&(rowid->offset), sizeof(rowid->offset), output+16);
	output[8+8+4] = '\0';
	PG_RETURN_CSTRING(output);
}

Datum rowid_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
	OraRowID *rowid = palloc(sizeof(OraRowID));

	rowid->node_id = pq_getmsgint(buf, sizeof(rowid->node_id));
	rowid->block = pq_getmsgint(buf, sizeof(rowid->block));
	rowid->offset = pq_getmsgint(buf, sizeof(rowid->offset));

	PG_RETURN_POINTER(rowid);
}

Datum rowid_send(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	OraRowID *rowid = (OraRowID*)PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);
	pq_sendint(&buf, rowid->node_id, sizeof(rowid->node_id));
	pq_sendint(&buf, rowid->block, sizeof(rowid->block));
	pq_sendint(&buf, rowid->offset, sizeof(rowid->offset));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum rowid_eq(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) == 0);
}

Datum rowid_ne(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) != 0);
}

Datum rowid_lt(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) < 0);
}

Datum rowid_le(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) <= 0);
}

Datum rowid_gt(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) > 0);
}

Datum rowid_ge(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) >= 0);
}

Datum rowid_larger(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	OraRowID *result = palloc(sizeof(OraRowID));
	memcpy(result, rowid_compare(l, r) > 0 ? l:r, sizeof(OraRowID));
	PG_RETURN_POINTER(result);
}

Datum rowid_smaller(PG_FUNCTION_ARGS)
{
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	OraRowID *result = palloc(sizeof(OraRowID));
	memcpy(result, rowid_compare(l, r) < 0 ? l:r, sizeof(OraRowID));
	PG_RETURN_POINTER(result);
}

Datum rowid_make(uint32 node_id, ItemPointer const tid)
{
	OraRowID *rowid;
	AssertArg(tid);
	rowid = palloc(sizeof(*rowid));
	rowid->node_id = node_id;
	rowid->block = ItemPointerGetBlockNumber(tid);
	rowid->offset = ItemPointerGetOffsetNumber(tid);
	return PointerGetDatum(rowid);
}

static int rowid_compare(const OraRowID *l, const OraRowID *r)
{
	AssertArg(l && r);
	if(l->node_id < r->node_id)
		return -1;
	else if(l->node_id > r->node_id)
		return 1;
	else if(l->block < r->block)
		return -1;
	else if(l->block > r->block)
		return 1;
	else if(l->offset < r->offset)
		return -1;
	else if(l->offset > r->offset)
		return 1;
	return 0;
}
