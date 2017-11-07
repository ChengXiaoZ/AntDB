/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/locator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#define LOCATOR_TYPE_REPLICATED 'R'
#define LOCATOR_TYPE_HASH 'H'
#define LOCATOR_TYPE_RANGE 'G'
#define LOCATOR_TYPE_RROBIN 'N'
#define LOCATOR_TYPE_CUSTOM 'C'
#define LOCATOR_TYPE_MODULO 'M'
#define LOCATOR_TYPE_NONE 'O'
#define LOCATOR_TYPE_DISTRIBUTED 'D'	/* for distributed table without specific
										 * scheme, e.g. result of JOIN of
										 * replicated and distributed table */
#ifdef ADB
#define LOCATOR_TYPE_USER_DEFINED 'U'
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
#define LOCATOR_TYPE_META 'A'
#define LOCATOR_TYPE_INVALID 'X'
#endif
#endif

/* Maximum number of preferred Datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsLocatorNone(x) (x == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) (x == LOCATOR_TYPE_REPLICATED)
#ifdef ADB
#define IsLocatorColumnDistributed(x) ((x) == LOCATOR_TYPE_HASH || \
									   (x) == LOCATOR_TYPE_RROBIN || \
									   (x) == LOCATOR_TYPE_MODULO || \
									   (x) == LOCATOR_TYPE_DISTRIBUTED || \
									   (x) == LOCATOR_TYPE_USER_DEFINED)
#else
#define IsLocatorColumnDistributed(x) (x == LOCATOR_TYPE_HASH || \
									   x == LOCATOR_TYPE_RROBIN || \
									   x == LOCATOR_TYPE_MODULO || \
									   x == LOCATOR_TYPE_DISTRIBUTED)
#endif
#define IsLocatorDistributedByValue(x) (x == LOCATOR_TYPE_HASH || \
										x == LOCATOR_TYPE_MODULO || \
										x == LOCATOR_TYPE_RANGE)
#ifdef ADB
#define IsLocatorDistributedByUserDefined(x) (x == LOCATOR_TYPE_USER_DEFINED)
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
#define IsLocatorMeta(x) (x == LOCATOR_TYPE_META)
#endif
#endif

#include "nodes/primnodes.h"
#include "utils/relcache.h"

/*
 * How relation is accessed in the query
 */
typedef enum
{
	RELATION_ACCESS_READ,				/* SELECT */
	RELATION_ACCESS_READ_FOR_UPDATE,	/* SELECT FOR UPDATE */
	RELATION_ACCESS_UPDATE,				/* UPDATE OR DELETE */
	RELATION_ACCESS_INSERT				/* INSERT */
} RelationAccessType;

typedef struct
{
	Oid			relid;			/* OID of relation */
	char		locatorType;	/* locator type, see above */
	AttrNumber	partAttrNum;	/* Distribution column attribute */
	List	   *nodeList;		/* Node indices where data is located */
	ListCell   *roundRobinNode;	/* Index of the next node to use */
#ifdef ADB
	Oid			funcid;
	List	   *funcAttrNums;
#endif
} RelationLocInfo;

#define IsRelationReplicated(rel_loc)			IsLocatorReplicated((rel_loc)->locatorType)
#define IsRelationColumnDistributed(rel_loc) 	IsLocatorColumnDistributed((rel_loc)->locatorType)
#define IsRelationDistributedByValue(rel_loc)	IsLocatorDistributedByValue((rel_loc)->locatorType)
#ifdef ADB
#define IsRelationDistributedByUserDefined(rel_loc) IsLocatorDistributedByUserDefined((rel_loc)->locatorType)
#endif

/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 * Note on dist_vars:
 * dist_vars is a list of Var nodes indicating the columns by which the
 * relations (result of query) are distributed. The result of equi-joins between
 * distributed relations, can be considered to be distributed by distribution
 * columns of either of relation. Hence a list. dist_vars is ignored in case of
 * distribution types other than HASH or MODULO.
 */
typedef struct
{
	NodeTag		type;
	List		*primarynodelist;	/* Primary node list indexes */
	List		*nodeList;			/* Node list indexes */
	char		baselocatortype;	/* Locator type, see above */
#ifdef ADB
	Oid			en_funcid;
	List		*en_expr;
#else
	Expr		*en_expr;			/* Expression to evaluate at execution time
									 * if planner can not determine execution
									 * nodes */
#endif
	Oid		en_relid;				/* Relation to determine execution nodes */
	RelationAccessType accesstype;	/* Access type to determine execution
									 * nodes */
	List	*en_dist_vars;				/* See above for details */
} ExecNodes;

#define IsExecNodesReplicated(en)			IsLocatorReplicated((en)->baselocatortype)
#define IsExecNodesColumnDistributed(en) 	IsLocatorColumnDistributed((en)->baselocatortype)
#define IsExecNodesDistributedByValue(en)	IsLocatorDistributedByValue((en)->baselocatortype)
#ifdef ADB
#define IsExecNodesDistributedByUserDefined(en)	IsLocatorDistributedByUserDefined((en)->baselocatortype)
#endif

/* Extern variables related to locations */
extern Oid primary_data_node;
extern Oid preferred_data_node[MAX_PREFERRED_NODES];
extern int num_preferred_data_nodes;

/* Function for RelationLocInfo building and management */
extern void RelationBuildLocator(Relation rel);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *srcInfo);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);
extern char *GetRelationDistribColumn(RelationLocInfo *locInfo);
#ifdef ADB
extern List *GetRelationDistribColumnList(RelationLocInfo *locInfo);
extern Oid GetRelationDistribFunc(Oid relid);
#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
extern void RelationBuildLocatorOnDN(Relation rel);
#endif
#endif
extern char GetLocatorType(Oid relid);
extern List *GetPreferredReplicationNode(List *relNodes);
extern bool IsTableDistOnPrimary(RelationLocInfo *locInfo);
extern bool IsLocatorInfoEqual(RelationLocInfo *locInfo1,
							   RelationLocInfo *locInfo2);
extern int GetRoundRobinNode(Oid relid);
extern bool IsTypeDistributable(Oid colType);
extern bool IsDistribColumn(Oid relid, AttrNumber attNum);

#ifdef ADB
extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info,
								   int nelems,
								   Datum* valueForDistCol,
								   bool* isValueNull,
								   Oid* typeOfValueForDistCol,
								   RelationAccessType accessType);
#else
extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info,
								   Datum valueForDistCol,
								   bool isValueNull,
								   Oid typeOfValueForDistCol,
								   RelationAccessType accessType);
#endif
extern ExecNodes *GetRelationNodesByQuals(Oid reloid,
										  Index varno,
										  Node *quals,
										  RelationAccessType relaccess);

#ifdef ADB
extern ExecNodes *GetRelationNodesByMultQuals(RelationLocInfo *rel_loc_info,
											  Oid reloid,
											  Index varno,
											  Node *quals,
											  RelationAccessType relaccess);
extern void CoerceUserDefinedFuncArgs(Oid funcid,
									  int nargs,
									  Datum *values,
									  bool *nulls,
									  Oid *types);

#if (!defined ADBMGRD) && (!defined AGTM) && (defined ENABLE_EXPANSION)
extern Datum locator_compute_hash(Oid type, Datum value, char locator);
extern int locator_compute_modulo(unsigned int numerator, unsigned int denominator);
#endif
#endif

/* Global locator data */
extern void FreeExecNodes(ExecNodes **exec_nodes);
extern List *GetAllDataNodes(void);
extern List *GetAllCoordNodes(void);

#endif   /* LOCATOR_H */
