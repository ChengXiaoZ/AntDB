#ifdef ADB
#include "postgres.h"
#include "access/gist_private.h"
#include "access/hash.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "catalog/namespace.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/tqual.h"
#include "pgxc/slot.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "catalog/pgxc_node.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "utils/lsyscache.h"

#define SELECT_HASH_TABLE "select nspname, relname from pg_class pg, pgxc_class xc , pg_namespace pgn where pg.oid=xc.pcrelid and pclocatortype = 'H' and pg.relnamespace = pgn.oid;"

typedef struct pgstattuple_slot
{
	uint64		tuple_count;
	uint64		tuple_len;
	uint64		dead_tuple_count;
	uint64		dead_tuple_len;
} pgstattuple_slot;

typedef struct pgstattuple_type
{
	uint64		table_len;
	uint64		tuple_count;
	uint64		tuple_len;
	uint64		dead_tuple_count;
	uint64		dead_tuple_len;
	uint64		free_space;
} pgstattuple_type;

typedef struct schema_table
{
	NameData	schema;
	NameData	table;
} schema_table;

pgstattuple_slot pgstattuple_slots[SLOTSIZE];


PG_FUNCTION_INFO_V1(pgstatslot);
PG_FUNCTION_INFO_V1(pg_hashtables);
PG_FUNCTION_INFO_V1(pgstatdatabaseslot);
PG_FUNCTION_INFO_V1(pgvalueslot);

extern Datum pgstatslot(PG_FUNCTION_ARGS);
extern Datum pg_hashtables(PG_FUNCTION_ARGS);
extern Datum pgstatdatabaseslot(PG_FUNCTION_ARGS);
extern Datum pgvalueslot(PG_FUNCTION_ARGS);

#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
static void pgstat_heap_slot(Relation rel, FunctionCallInfo fcinfo, pgstattuple_type *pstat, BlockNumber* pallnblocks);
static void build_pgstatslot_type(TupleDesc tupdesc, FunctionCallInfo fcinfo, pgstattuple_type *stat,BlockNumber nblocks);
static void init_pgstattuple_slots(void);
#endif

/* --------------------------------------------------------
 * pgstatdatabaseslot()
 *
 * Get the slot distribution of current database's data.
 *
 * Usage: SELECT pgstatdatabaseslot();
 *
 * --------------------------------------------------------
 */

Datum
pgstatdatabaseslot(PG_FUNCTION_ARGS)
{
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
	Relation	rel;
	TupleDesc	tupdesc;
	int nodeindex, status;

	char* 		pcrelname;
	char* 		pschema;
	int 		i;
	int 		ret;

	List *tablename_list = NIL;
	ListCell*	lc;
	schema_table* pschema_table;
	Oid np_oid, rel_oid ;

	BlockNumber	allnblocks;
	pgstattuple_type 	stat = {0};
	allnblocks = 0;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pgstattuple functions"))));

	if(IS_PGXC_COORDINATOR)
		elog(ERROR, "this command cann't be executed on coordinator.");


	/*1.check slot is valid.*/
	SlotGetInfo(0, &nodeindex, &status);
	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	/*set pgstattuple_slots*/
	init_pgstattuple_slots();



	/*2.gather all hash tables*/
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	ret = SPI_execute(SELECT_HASH_TABLE, false, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("clean slot error %s result is %d", SELECT_HASH_TABLE, ret)));


	for (i = 0; i < SPI_processed; i++)
	{
		pschema = SPI_getvalue(SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 1);
		pcrelname = SPI_getvalue(SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 2);

		pschema_table = palloc(sizeof(schema_table));
		namestrcpy(&pschema_table->schema, pschema);
		namestrcpy(&pschema_table->table, pcrelname);

		tablename_list = lappend(tablename_list,pschema_table);

	}


	/*3.scan*/
	allnblocks = 0;
	foreach (lc, tablename_list)
	{
		pschema_table = (schema_table *)lfirst(lc);


		np_oid = LookupExplicitNamespace(pschema_table->schema.data, true);
		rel_oid = get_relname_relid(pschema_table->table.data, np_oid);
		rel = heap_open(rel_oid, AccessShareLock);
		if (LOCATOR_TYPE_HASH != rel->dn_locatorType)
			elog(ERROR, "only execute on hash table.");

		pgstat_heap_slot(rel, fcinfo, &stat, &allnblocks);
	}


	/*4.handle resut*/
	build_pgstatslot_type(tupdesc, fcinfo, &stat,allnblocks);

	PG_RETURN_DATUM((Datum) 0);

#else
	elog(ERROR, "This function isn't supported in nonexpansion version.");
#endif
}


/* --------------------------------------------------------
 * pg_hashtables()
 *
 * Get all hash table name.
 *
 * Usage: SELECT pg_hashtables();
 *
 * --------------------------------------------------------
 */

Datum
pg_hashtables(PG_FUNCTION_ARGS)
{
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate*	tupstore;
	MemoryContext 		per_query_ctx;
	MemoryContext 		oldcontext;
	Datum				values[2];
	bool				nulls[2];
	int					ret;
	char* 				pcrelname;
	char* 				pschema;
	int 				i;
	int 				ci;
	List *tablename_list = NIL;
	ListCell *lc;
	schema_table* pschema_table;


	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	ret = SPI_execute(SELECT_HASH_TABLE, false, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("clean slot error %s result is %d", SELECT_HASH_TABLE, ret)));


	for (i = 0; i < SPI_processed; i++)
	{
		pschema = SPI_getvalue(SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 1);
		pcrelname = SPI_getvalue(SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, 2);

		pschema_table = malloc(sizeof(schema_table));
		namestrcpy(&pschema_table->schema, pschema);
		namestrcpy(&pschema_table->table, pcrelname);
		tablename_list = lappend(tablename_list,pschema_table);
	}

	foreach (lc, tablename_list)
	{
		pschema_table = (schema_table *)lfirst(lc);
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		ci = 0;
		values[ci++] = CStringGetTextDatum(pschema_table->schema.data);
		values[ci++] = CStringGetTextDatum(pschema_table->table.data);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	SPI_freetuptable(SPI_tuptable);
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
#else
	elog(ERROR, "This function isn't supported in nonexpansion version.");
#endif
}
static void init_pgstattuple_slots(void)
{
	int i;
	for(i=0; i<SLOTSIZE; i++)
	{
		pgstattuple_slots[i].tuple_count 	= 0;
		pgstattuple_slots[i].tuple_len 		= 0;
		pgstattuple_slots[i].dead_tuple_count = 0;
		pgstattuple_slots[i].dead_tuple_len = 0;
	}
}

/* --------------------------------------------------------
 * pgstatslot()
 *
 * Get the slot distribution of table's data.
 *
 * Usage: SELECT pgstatslot('tablename');
 *
 * --------------------------------------------------------
 */

Datum
pgstatslot(PG_FUNCTION_ARGS)
{
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
	text* 		relname = PG_GETARG_TEXT_P(0);
	RangeVar*	relrv;
	Relation	rel;
	int nodeindex, status;
	BlockNumber	allnblocks;
	pgstattuple_type 	stat = {0};
	TupleDesc	tupdesc;
	allnblocks = 0;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pgstattuple functions"))));

	if(IS_PGXC_COORDINATOR)
		elog(ERROR, "this command cann't be executed on coordinator.");


	/*1.check slot is valid.*/
	SlotGetInfo(0, &nodeindex, &status);
	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	/*set pgstattuple_slots*/
	init_pgstattuple_slots();



	/*2.scan table*/
	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	if (LOCATOR_TYPE_HASH != rel->dn_locatorType)
		elog(ERROR, "only execute on hash table.");

	pgstat_heap_slot(rel, fcinfo, &stat, &allnblocks);


	/*3.handle resut*/
	build_pgstatslot_type(tupdesc, fcinfo, &stat,allnblocks);

	PG_RETURN_DATUM((Datum) 0);
#else
	elog(ERROR, "This function isn't supported in nonexpansion version.");
#endif
}


static void
pgstat_heap_slot(Relation rel, FunctionCallInfo fcinfo, pgstattuple_type *pstat, BlockNumber* pallnblocks)
{
	HeapScanDesc 		scan;
	HeapTuple			tuple;
	BlockNumber 		nblocks;
	BlockNumber 		block = 0;
	BlockNumber 		tupblock;
	Buffer				buffer;
	int					slotid;

	ReturnSetInfo*		rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));



	/* Disable syncscan because we assume we scan from block zero upwards */
	scan = heap_beginscan_strat(rel, SnapshotAny, 0, NULL, true, false);

	nblocks = scan->rs_nblocks; /* # blocks to be scanned */

	/* scan the relation */
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		/* must hold a buffer lock to call HeapTupleSatisfiesVisibility */
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		slotid = GetHeapTupleSlotId(scan->rs_rd, tuple);

		if (HeapTupleSatisfiesVisibility(tuple, SnapshotNow, scan->rs_cbuf))
		{
			pstat->tuple_len += tuple->t_len;
			pstat->tuple_count++;
			pgstattuple_slots[slotid].tuple_count++;
			pgstattuple_slots[slotid].tuple_len += tuple->t_len;
		}
		else
		{
			pstat->dead_tuple_len += tuple->t_len;
			pstat->dead_tuple_count++;
			pgstattuple_slots[slotid].dead_tuple_count++;
			pgstattuple_slots[slotid].dead_tuple_len += tuple->t_len;
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		/*
		 * To avoid physically reading the table twice, try to do the
		 * free-space scan in parallel with the heap scan.  However,
		 * heap_getnext may find no tuples on a given page, so we cannot
		 * simply examine the pages returned by the heap scan.
		 */
		tupblock = BlockIdGetBlockNumber(&tuple->t_self.ip_blkid);

		while (block <= tupblock)
		{
			CHECK_FOR_INTERRUPTS();

			buffer = ReadBufferExtended(rel, MAIN_FORKNUM, block,
										RBM_NORMAL, scan->rs_strategy);
			LockBuffer(buffer, BUFFER_LOCK_SHARE);
			pstat->free_space += PageGetHeapFreeSpace((Page) BufferGetPage(buffer));
			UnlockReleaseBuffer(buffer);
			block++;
		}
	}

	while (block < nblocks)
	{
		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, block,
									RBM_NORMAL, scan->rs_strategy);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		pstat->free_space += PageGetHeapFreeSpace((Page) BufferGetPage(buffer));
		UnlockReleaseBuffer(buffer);
		block++;
	}

	heap_endscan(scan);
	relation_close(rel, AccessShareLock);

	(*pallnblocks) = (*pallnblocks) + nblocks;

	return;
}

void build_pgstatslot_type(
	TupleDesc tupdesc,
	FunctionCallInfo fcinfo,
	pgstattuple_type *stat,
	BlockNumber nblocks)
{
#define NCOLUMNS_SLOT	12
#define NCHARS_SLOT		32
	ReturnSetInfo*		rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate*	tupstore;
	MemoryContext 		per_query_ctx;
	MemoryContext 		oldcontext;
	int 				row_id;
	int 				column_index = 0;

	double				tuple_percent;
	double				dead_tuple_percent;
	double				free_percent;

	int					nodeindex, status;
	char* 				slot_located;

	Datum				values[NCOLUMNS_SLOT];
	bool				nulls[NCOLUMNS_SLOT];

	stat->table_len = (uint64) nblocks *BLCKSZ;
	if (stat->table_len == 0)
	{
		tuple_percent = 0.0;
		dead_tuple_percent = 0.0;
		free_percent = 0.0;
	}
	else
	{
		tuple_percent = 100.0 * stat->tuple_len / stat->table_len;
		dead_tuple_percent = 100.0 * stat->dead_tuple_len / stat->table_len;
		free_percent = 100.0 * stat->free_space / stat->table_len;
	}

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);


	column_index = 0;
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));
	values[column_index++] = CStringGetTextDatum(PGXCNodeName);
	values[column_index++] = Int32GetDatum(SLOTSIZE);
	values[column_index++] = CStringGetTextDatum("");
	values[column_index++] = Int64GetDatum(stat->table_len);
	values[column_index++] = Int64GetDatum(stat->tuple_count);
	values[column_index++] = Int64GetDatum(stat->tuple_len);
	values[column_index++] = Float8GetDatum(tuple_percent);
	values[column_index++] = Int64GetDatum(stat->dead_tuple_count);
	values[column_index++] = Int64GetDatum(stat->dead_tuple_len);
	values[column_index++] = Float8GetDatum(dead_tuple_percent);
	values[column_index++] = Int64GetDatumFast(stat->free_space);
	values[column_index++] = Float8GetDatumFast(free_percent);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	for(row_id=0; row_id<SLOTSIZE; row_id++)
	{
		if((pgstattuple_slots[row_id].tuple_count==0)
			&&(pgstattuple_slots[row_id].dead_tuple_count==0))
			continue;

		if (stat->table_len == 0)
		{
			tuple_percent = 0.0;
			dead_tuple_percent = 0.0;
		}
		else
		{
			tuple_percent = 100.0 * pgstattuple_slots[row_id].tuple_len / stat->table_len;
			dead_tuple_percent = 100.0 * pgstattuple_slots[row_id].dead_tuple_len / stat->table_len;
		}

		SlotGetInfo(row_id, &nodeindex, &status);
		slot_located = get_pgxc_nodename(PGXCNodeGetNodeOid(nodeindex, PGXC_NODE_DATANODE));

		column_index = 0;
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		values[column_index++] = CStringGetTextDatum(PGXCNodeName);
		values[column_index++] = Int32GetDatum(row_id);
		values[column_index++] = CStringGetTextDatum(slot_located);
		values[column_index++] = Int64GetDatum(0);
		values[column_index++] = Int64GetDatum(pgstattuple_slots[row_id].tuple_count);
		values[column_index++] = Int64GetDatum(pgstattuple_slots[row_id].tuple_len);
		values[column_index++] = Float8GetDatum(tuple_percent);
		values[column_index++] = Int64GetDatum(pgstattuple_slots[row_id].dead_tuple_count);
		values[column_index++] = Int64GetDatum(pgstattuple_slots[row_id].dead_tuple_len);
		values[column_index++] = Float8GetDatum(dead_tuple_percent);
		values[column_index++] = Int64GetDatumFast(0);
		values[column_index++] = Float8GetDatumFast(0);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
}



/* --------------------------------------------------------
 * pgvalueslot()
 *
 * Get the slot of value.
 *
 * Usage: SELECT pgvalueslot('tablename', 'value');
 *
 * --------------------------------------------------------
 */
Datum
pgvalueslot(PG_FUNCTION_ARGS)
{
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
	text	   *tablename 	= PG_GETARG_TEXT_P(0);
	text*		textvalue	= PG_GETARG_TEXT_P(1);
	Datum 		dvalue;
	Relation	rel;
	RangeVar   *relrv;
	int			slotid;
	char	   *strvalue;
	int			attrnum;
	char		locatorType;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pgstattuple functions"))));

	strvalue = text_to_cstring(textvalue);

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
	rel = relation_openrv(relrv, NoLock);

	if(IS_PGXC_REAL_DATANODE)
	{
		attrnum = rel->dn_partAttrNum;
		locatorType = rel->dn_locatorType;
	}
	else if(IS_PGXC_COORDINATOR)
	{
		attrnum = rel->rd_locator_info->partAttrNum;
		locatorType = rel->rd_locator_info->locatorType;
	}
	else
		elog(ERROR, "this command cann't be executed on restore mode.");

	if (LOCATOR_TYPE_HASH != locatorType)
		elog(ERROR, "this command can only be executed on hash table.");
	dvalue = BuildFieldFromCStrings(TupleDescGetAttInMetadata(rel->rd_att), strvalue, attrnum);
	slotid = GetValueSlotId(rel, dvalue, attrnum);

	relation_close(rel, NoLock);

	PG_RETURN_INT32(slotid);
#else
	elog(ERROR, "This function isn't supported in nonexpansion version.");
#endif
}

#endif
