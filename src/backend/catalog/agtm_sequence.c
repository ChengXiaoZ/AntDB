#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/agtm_sequence.h"
#include "catalog/indexing.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

Oid AddAgtmSequence(const char* database,
				const char* schema, const char* sequence)
{
	Relation	adbSequence;
	HeapTuple	htup;
	Datum		values[Natts_agtm_sequence];
	bool		nulls[Natts_agtm_sequence];
	NameData    nameDatabase;
	NameData    nameSchema;
	NameData    nameSequence;
	Oid			oid;

	if(database == NULL || schema == NULL || sequence == NULL)
		elog(ERROR, "AddAgtmSequence database schema sequence must no null");

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

	namestrcpy(&nameDatabase, database);
	values[Anum_agtm_sequence_database - 1] = NameGetDatum(&nameDatabase);

	namestrcpy(&nameSchema, schema);
	values[Anum_agtm_sequence_schema - 1] = NameGetDatum(&nameSchema);

	namestrcpy(&nameSequence, sequence);
	values[Anum_agtm_sequence_sequence - 1] = NameGetDatum(&nameSequence);

	adbSequence = heap_open(AgtmSequenceRelationId, RowExclusiveLock);
	htup = heap_form_tuple(RelationGetDescr(adbSequence), values, nulls);

	oid = simple_heap_insert(adbSequence, htup);

	/* add index */
	CatalogUpdateIndexes(adbSequence,htup);

	heap_close(adbSequence, RowExclusiveLock);

	return oid;
}

Oid DelAgtmSequence(const char* database,
				const char* schema, const char* sequence)
{
	Relation	adbSequence;
	HeapTuple	htup;
	NameData    nameDatabase;
	NameData    nameSchema;
	NameData    nameSequence;
	Oid			oid;

	if(database == NULL || schema == NULL || sequence == NULL)
		elog(ERROR, "DelAgtmSequence database schema sequence must no null");

	namestrcpy(&nameDatabase, database);
	namestrcpy(&nameSchema, schema);
	namestrcpy(&nameSequence, sequence);

	adbSequence = heap_open(AgtmSequenceRelationId, RowExclusiveLock);

	htup = SearchSysCache3(AGTMSEQUENCEFIELDS, NameGetDatum(&nameDatabase),
		NameGetDatum(&nameSchema), NameGetDatum(&nameSequence));

	if (!HeapTupleIsValid(htup)) /* should not happen */
		elog(ERROR, "cache lookup failed for relation agtm_sequence, database :%s,schema :%s,sequence :%s",
			database, schema, sequence);

	oid = HeapTupleGetOid(htup);

	simple_heap_delete(adbSequence, &htup->t_self);

	ReleaseSysCache(htup);

	heap_close(adbSequence, RowExclusiveLock);

	return oid;
}

bool SequenceIsExist(const char* database,
				const char* schema, const char* sequence)
{
	Relation	adbSequence;
	HeapTuple	htup;
	NameData    nameDatabase;
	NameData    nameSchema;
	NameData    nameSequence;
	bool		isExist = FALSE;

	if(database == NULL || schema == NULL || sequence == NULL)
		elog(ERROR, "SequenceIsExist database schema sequence must no null");

	namestrcpy(&nameDatabase, database);
	namestrcpy(&nameSchema, schema);
	namestrcpy(&nameSequence, sequence);

	adbSequence = heap_open(AgtmSequenceRelationId, RowExclusiveLock);

	htup = SearchSysCache3(AGTMSEQUENCEFIELDS, NameGetDatum(&nameDatabase),
		NameGetDatum(&nameSchema), NameGetDatum(&nameSequence));

	if (HeapTupleIsValid(htup))
	{
		isExist = TRUE;
		ReleaseSysCache(htup);
	}
	else
		isExist = FALSE;

	heap_close(adbSequence, RowExclusiveLock);

	return isExist;
}


