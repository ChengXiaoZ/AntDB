#ifndef BEGIN_NODE
#	define BEGIN_NODE(t)
#endif
#ifndef END_NODE
#	define END_NODE(t)
#endif
#ifndef BEGIN_STRUCT
#	define BEGIN_STRUCT(t)
#endif
#ifndef END_STRUCT
#	define END_STRUCT(t)
#endif

#ifndef NODE_BASE2
#	define NODE_BASE2(t,m) NODE_BASE(t)
#endif
#ifndef NODE_SAME
#	define NODE_SAME(t1,t2) \
		BEGIN_NODE(t1)		\
			NODE_BASE(t2)	\
		END_NODE(t1)
#endif
#ifndef NODE_ARG_
#	define NODE_ARG_ node
#endif

#ifndef NODE_BASE
#	define NODE_BASE(b)
#endif
#ifndef NODE_NODE
#	define NODE_NODE(t,m)
#endif
#ifndef NODE_NODE_MEB
#	define NODE_NODE_MEB(t,m)
#endif
#ifndef NODE_NODE_ARRAY
#	define NODE_NODE_ARRAY(t,m,l)
#endif
#ifndef NODE_BITMAPSET
#	define NODE_BITMAPSET(t,m)
#endif
#ifndef NODE_BITMAPSET_ARRAY
#	define NODE_BITMAPSET_ARRAY(t,m,l)
#endif
#ifndef NODE_RELIDS
#	define NODE_RELIDS(t,m) NODE_BITMAPSET(Bitmapset,m)
#endif
#ifndef NODE_RELIDS_ARRAY
#	define NODE_RELIDS_ARRAY(t,m,l) NODE_BITMAPSET_ARRAY(Bitmapset,m,l)
#endif
#ifndef NODE_LOCATION
#	define NODE_LOCATION(t,m) NODE_SCALAR(t,m)
#endif
#ifndef NODE_SCALAR
#	define NODE_SCALAR(t,m)
#endif
#ifndef NODE_OID
#	define NODE_OID(t,m) NODE_SCALAR(Oid, m)
#endif
#ifndef NODE_SCALAR_POINT
#	define NODE_SCALAR_POINT(t,m,l)
#endif
#ifndef NODE_OTHER_POINT
#	define NODE_OTHER_POINT(t,m)
#endif
#ifndef NODE_STRING
#	define NODE_STRING(m)
#endif
#ifndef NODE_STRUCT
#	define NODE_STRUCT(t,m)
#endif
#ifndef NODE_STRUCT_ARRAY
#	define NODE_STRUCT_ARRAY(t,m,l)
#endif
#ifndef NODE_STRUCT_LIST
#	define NODE_STRUCT_LIST(t,m)
#endif
#ifndef NODE_STRUCT_MEB
#	define NODE_STRUCT_MEB(t,m)
#endif
#ifndef NODE_ENUM
#	define NODE_ENUM(t,m)
#endif
#ifndef NODE_DATUM
#	define NODE_DATUM(t, m, o, n)
#endif

#ifndef NO_NODE_PlannedStmt
BEGIN_NODE(PlannedStmt)
	NODE_ENUM(CmdType,commandType)
	NODE_SCALAR(uint32,queryId)
	NODE_SCALAR(bool,hasReturning)
	NODE_SCALAR(bool,hasModifyingCTE)
	NODE_SCALAR(bool,canSetTag)
	NODE_SCALAR(bool,transientPlan)
	NODE_NODE(Plan,planTree)
	NODE_NODE(List,rtable)
	NODE_NODE(List,resultRelations)
	NODE_NODE(Node,utilityStmt)
	NODE_NODE(List,subplans)
	NODE_BITMAPSET(Bitmapset,rewindPlanIDs)
	NODE_NODE(List,rowMarks)
	NODE_NODE(List,relationOids)
	NODE_NODE(List,invalItems)
	NODE_SCALAR(int,nParamExec)
END_NODE(PlannedStmt)
#endif /* NO_NODE_PlannedStmt */

#ifndef NO_NODE_Plan
BEGIN_NODE(Plan)
	NODE_SCALAR(Cost,startup_cost)
	NODE_SCALAR(Cost,total_cost)
	NODE_SCALAR(double,plan_rows)
	NODE_SCALAR(int,plan_width)
	NODE_NODE(List,targetlist)
	NODE_NODE(List,qual)
	NODE_NODE(Plan,lefttree)
	NODE_NODE(Plan,righttree)
	NODE_NODE(List,initPlan)
	NODE_BITMAPSET(Bitmapset,extParam)
	NODE_BITMAPSET(Bitmapset,allParam)
END_NODE(Plan)
#endif /* NO_NODE_Plan */

#ifndef NO_NODE_Result
BEGIN_NODE(Result)
	NODE_BASE2(Plan,plan)
	NODE_NODE(Node,resconstantqual)
END_NODE(Result)
#endif /* NO_NODE_Result */

#ifndef NO_NODE_ModifyTable
BEGIN_NODE(ModifyTable)
	NODE_BASE2(Plan,plan)
	NODE_ENUM(CmdType,operation)
	NODE_SCALAR(bool,canSetTag)
	NODE_NODE(List,resultRelations)
	NODE_SCALAR(int,resultRelIndex)
	NODE_NODE(List,plans)
	NODE_NODE(List,returningLists)
	NODE_NODE(List,fdwPrivLists)
	NODE_NODE(List,rowMarks)
	NODE_SCALAR(int,epqParam)
#ifdef PGXC	
	NODE_NODE(List,remote_plans)
#endif	
END_NODE(ModifyTable)
#endif /* NO_NODE_ModifyTable */

#ifndef NO_NODE_Append
BEGIN_NODE(Append)
	NODE_BASE2(Plan,plan)
	NODE_NODE(List,appendplans)
END_NODE(Append)
#endif /* NO_NODE_Append */

#ifndef NO_NODE_MergeAppend
BEGIN_NODE(MergeAppend)
	NODE_BASE2(Plan,plan)
	NODE_NODE(List,mergeplans)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_NODE(MergeAppend)
#endif /* NO_NODE_MergeAppend */

#ifndef NO_NODE_RecursiveUnion
BEGIN_NODE(RecursiveUnion)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(int,wtParam)
	/* Remaining fields are zero/null in UNION ALL case */
	NODE_SCALAR(int,numCols)		/* number of columns to check for
								 * duplicate-ness */
	NODE_SCALAR_POINT(AttrNumber,dupColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,dupOperators,NODE_ARG_->numCols)
	NODE_SCALAR(long,numGroups)
END_NODE(RecursiveUnion)
#endif /* NO_NODE_RecursiveUnion */

#ifndef NO_NODE_BitmapAnd
BEGIN_NODE(BitmapAnd)
	NODE_BASE2(Plan,plan)
	NODE_NODE(List,bitmapplans)
END_NODE(BitmapAnd)
#endif /* NO_NODE_BitmapAnd */

#ifndef NO_NODE_BitmapOr
BEGIN_NODE(BitmapOr)
	NODE_BASE2(Plan,plan)
	NODE_NODE(List,bitmapplans)
END_NODE(BitmapOr)
#endif /* NO_NODE_BitmapOr */

#ifndef NO_NODE_Scan
BEGIN_NODE(Scan)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(Index,scanrelid)
END_NODE(Scan)
#endif /* NO_NODE_Scan */

NODE_SAME(SeqScan,Scan)

#ifndef NO_NODE_IndexScan
BEGIN_NODE(IndexScan)
	NODE_BASE2(Scan,scan)
	NODE_SCALAR(Oid,indexid)
	NODE_NODE(List,indexqual)
	NODE_NODE(List,indexqualorig)
	NODE_NODE(List,indexorderby)
	NODE_NODE(List,indexorderbyorig)
	NODE_ENUM(ScanDirection,indexorderdir)
END_NODE(IndexScan)
#endif /* NO_NODE_IndexScan */

#ifndef NO_NODE_IndexOnlyScan
BEGIN_NODE(IndexOnlyScan)
	NODE_BASE2(Scan,scan)
	NODE_SCALAR(Oid,indexid)
	NODE_NODE(List,indexqual)
	NODE_NODE(List,indexorderby)
	NODE_NODE(List,indextlist)
	NODE_ENUM(ScanDirection,indexorderdir)
END_NODE(IndexOnlyScan)
#endif /* NO_NODE_IndexOnlyScan */

#ifndef NO_NODE_BitmapIndexScan
BEGIN_NODE(BitmapIndexScan)
	NODE_BASE2(Scan,scan)
	NODE_SCALAR(Oid,indexid)
	NODE_NODE(List,indexqual)
	NODE_NODE(List,indexqualorig)
END_NODE(BitmapIndexScan)
#endif /* NO_NODE_BitmapIndexScan */

#ifndef NO_NODE_BitmapHeapScan
BEGIN_NODE(BitmapHeapScan)
	NODE_BASE2(Scan,scan)
	NODE_NODE(List,bitmapqualorig)
END_NODE(BitmapHeapScan)
#endif /* NO_NODE_BitmapHeapScan */

#ifndef NO_NODE_TidScan
BEGIN_NODE(TidScan)
	NODE_BASE2(Scan,scan)
	NODE_NODE(List,tidquals)
END_NODE(TidScan)
#endif /* NO_NODE_TidScan */

#ifndef NO_NODE_SubqueryScan
BEGIN_NODE(SubqueryScan)
	NODE_BASE2(Scan,scan)
	NODE_NODE(Plan,subplan)
END_NODE(SubqueryScan)
#endif /* NO_NODE_SubqueryScan */

#ifndef NO_NODE_FunctionScan
BEGIN_NODE(FunctionScan)
	NODE_BASE2(Scan,scan)
	NODE_NODE(Node,funcexpr)
	NODE_NODE(List,funccolnames)
	NODE_NODE(List,funccoltypes)
	NODE_NODE(List,funccoltypmods)
	NODE_NODE(List,funccolcollations)
END_NODE(FunctionScan)
#endif /* NO_NODE_FunctionScan */

#ifndef NO_NODE_ValuesScan
BEGIN_NODE(ValuesScan)
	NODE_BASE2(Scan,scan)
	NODE_NODE(List,values_lists)
END_NODE(ValuesScan)
#endif /* NO_NODE_ValuesScan */

#ifndef NO_NODE_CteScan
BEGIN_NODE(CteScan)
	NODE_BASE2(Scan,scan)
	NODE_SCALAR(int,ctePlanId)
	NODE_SCALAR(int,cteParam)
END_NODE(CteScan)
#endif /* NO_NODE_CteScan */

#ifndef NO_NODE_WorkTableScan
BEGIN_NODE(WorkTableScan)
	NODE_BASE2(Scan,scan)
	NODE_SCALAR(int,wtParam)
END_NODE(WorkTableScan)
#endif /* NO_NODE_WorkTableScan */

#ifndef NO_NODE_ForeignScan
BEGIN_NODE(ForeignScan)
	NODE_BASE2(Scan,scan)
	NODE_NODE(List,fdw_exprs)
	NODE_NODE(List,fdw_private)
	NODE_SCALAR(bool,fsSystemCol)
END_NODE(ForeignScan)
#endif /* NO_NODE_ForeignScan */

#ifndef NO_NODE_Join
BEGIN_NODE(Join)
	NODE_BASE2(Plan,plan)
	NODE_ENUM(JoinType,jointype)
	NODE_NODE(List,joinqual)
END_NODE(Join)
#endif /* NO_NODE_Join */

#ifndef NO_NODE_NestLoop
BEGIN_NODE(NestLoop)
	NODE_BASE2(Join,join)
	NODE_NODE(List,nestParams)
END_NODE(NestLoop)
#endif /* NO_NODE_NestLoop */

#ifndef NO_NODE_NestLoopParam
BEGIN_NODE(NestLoopParam)
	NODE_SCALAR(int,paramno)
	NODE_NODE(Var,paramval)
END_NODE(NestLoopParam)
#endif /* NO_NODE_NestLoopParam */

#ifndef NO_NODE_MergeJoin
BEGIN_NODE(MergeJoin)
	NODE_BASE2(Join,join)
	NODE_NODE(List,mergeclauses)
	NODE_SCALAR_POINT(Oid,mergeFamilies,list_length(NODE_ARG_->mergeclauses))
	NODE_SCALAR_POINT(Oid,mergeCollations,list_length(NODE_ARG_->mergeclauses))
	NODE_SCALAR_POINT(int,mergeStrategies,list_length(NODE_ARG_->mergeclauses))
	NODE_SCALAR_POINT(bool,mergeNullsFirst,list_length(NODE_ARG_->mergeclauses))
END_NODE(MergeJoin)
#endif /* NO_NODE_MergeJoin */

#ifndef NO_NODE_HashJoin
BEGIN_NODE(HashJoin)
	NODE_BASE2(Join,join)
	NODE_NODE(List,hashclauses)
END_NODE(HashJoin)
#endif /* NO_NODE_HashJoin */

#ifndef NO_NODE_Material
BEGIN_NODE(Material)
	NODE_BASE2(Plan,plan)
END_NODE(Material)
#endif /* NO_NODE_Material */

#ifndef NO_NODE_Sort
BEGIN_NODE(Sort)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
#ifdef PGXC
	NODE_SCALAR(bool,srt_start_merge)
#endif /* PGXC */
END_NODE(Sort)
#endif /* NO_NODE_Sort */

#ifndef NO_NODE_Group
BEGIN_NODE(Group)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,grpColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,grpOperators,NODE_ARG_->numCols)
END_NODE(Group)
#endif /* NO_NODE_Group */

#ifndef NO_NODE_Agg
BEGIN_NODE(Agg)
	NODE_BASE2(Plan,plan)
	NODE_ENUM(AggStrategy,aggstrategy)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,grpColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,grpOperators,NODE_ARG_->numCols)
	NODE_SCALAR(long,numGroups)
#ifdef PGXC
	NODE_SCALAR(bool,skip_trans)
#endif /* PGXC */
END_NODE(Agg)
#endif /* NO_NODE_Agg */

#ifndef NO_NODE_WindowAgg
BEGIN_NODE(WindowAgg)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(Index,winref)
	NODE_SCALAR(int,partNumCols)
	NODE_SCALAR_POINT(AttrNumber,partColIdx,NODE_ARG_->partNumCols)
	NODE_SCALAR_POINT(Oid,partOperators,NODE_ARG_->partNumCols)
	NODE_SCALAR(int,ordNumCols)
	NODE_SCALAR_POINT(AttrNumber,ordColIdx,NODE_ARG_->ordNumCols)
	NODE_SCALAR_POINT(Oid,ordOperators,NODE_ARG_->ordNumCols)
	NODE_SCALAR(int,frameOptions)
	NODE_NODE(Node,startOffset)
	NODE_NODE(Node,endOffset)
END_NODE(WindowAgg)
#endif /* NO_NODE_WindowAgg */

#ifndef NO_NODE_Unique
BEGIN_NODE(Unique)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,uniqColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,uniqOperators,NODE_ARG_->numCols)
END_NODE(Unique)
#endif /* NO_NODE_Unique */

#ifndef NO_NODE_Hash
BEGIN_NODE(Hash)
	NODE_BASE2(Plan,plan)
	NODE_SCALAR(Oid,skewTable)
	NODE_SCALAR(AttrNumber,skewColumn)
	NODE_SCALAR(bool,skewInherit)
	NODE_OID(type,skewColType)
	NODE_SCALAR(int32,skewColTypmod)
END_NODE(Hash)
#endif /* NO_NODE_Hash */

#ifndef NO_NODE_SetOp
BEGIN_NODE(SetOp)
	NODE_BASE2(Plan,plan)
	NODE_ENUM(SetOpCmd,cmd)
	NODE_ENUM(SetOpStrategy,strategy)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,dupColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,dupOperators,NODE_ARG_->numCols)
	NODE_SCALAR(AttrNumber,flagColIdx)
	NODE_SCALAR(int,firstFlag)
	NODE_SCALAR(long,numGroups)
END_NODE(SetOp)
#endif /* NO_NODE_SetOp */

#ifndef NO_NODE_LockRows
BEGIN_NODE(LockRows)
	NODE_BASE2(Plan,plan)
	NODE_NODE(List,rowMarks)
	NODE_SCALAR(int,epqParam)
END_NODE(LockRows)
#endif /* NO_NODE_LockRows */

#ifndef NO_NODE_Limit
BEGIN_NODE(Limit)
	NODE_BASE2(Plan,plan)
	NODE_NODE(Node,limitOffset)
	NODE_NODE(Node,limitCount)
END_NODE(Limit)
#endif /* NO_NODE_Limit */

#ifndef NO_NODE_PlanRowMark
BEGIN_NODE(PlanRowMark)
	NODE_SCALAR(Index,rti)
	NODE_SCALAR(Index,prti)
	NODE_SCALAR(Index,rowmarkId)
	NODE_ENUM(RowMarkType,markType)
	NODE_SCALAR(bool,noWait)
	NODE_SCALAR(bool,isParent)
END_NODE(PlanRowMark)
#endif /* NO_NODE_PlanRowMark */

#ifndef NO_NODE_PlanInvalItem
BEGIN_NODE(PlanInvalItem)
	NODE_SCALAR(int,cacheId)
	NODE_SCALAR(uint32,hashValue)
END_NODE(PlanInvalItem)
#endif /* NO_NODE_PlanInvalItem */

#ifdef PGXC
#ifndef NO_NODE_ExecNodes
BEGIN_NODE(ExecNodes)
	NODE_NODE(List,primarynodelist)
	NODE_NODE(List,nodeList)
	NODE_SCALAR(char,baselocatortype)
#ifdef ADB
	NODE_OID(proc,en_funcid)
	NODE_NODE(List, en_expr)
#else
	NODE_NODE(Expr, en_expr)
#endif
	NODE_SCALAR(Oid,en_relid)
	NODE_ENUM(RelationAccessType,accesstype)
	NODE_NODE(List,en_dist_vars)
END_NODE(ExecNodes)
#endif /* NO_NODE_ExecNodes */

#ifndef NO_NODE_SimpleSort
BEGIN_NODE(SimpleSort)
	NODE_SCALAR(int,numCols)
	NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(Oid,sortCollations,NODE_ARG_->numCols)
	NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_NODE(SimpleSort)
#endif /* NO_NODE_SimpleSort */

#ifndef NO_NODE_RemoteQuery
BEGIN_NODE(RemoteQuery)
	NODE_BASE2(Scan, scan)
	NODE_ENUM(ExecDirectType,exec_direct_type)
	NODE_STRING(sql_statement)
	NODE_NODE(ExecNodes,exec_nodes)
	NODE_ENUM(CombineType,combine_type)
	NODE_SCALAR(bool,read_only)
	NODE_SCALAR(bool,force_autocommit)
	NODE_STRING(statement)
	NODE_STRING(cursor)
	NODE_SCALAR(int,rq_num_params)
	NODE_SCALAR_POINT(Oid,rq_param_types,NODE_ARG_->rq_num_params)
	NODE_SCALAR(bool,rq_params_internal)
	NODE_ENUM(RemoteQueryExecType,exec_type)
	NODE_SCALAR(bool,is_temp)
	NODE_SCALAR(bool,rq_finalise_aggs)
	NODE_SCALAR(bool,rq_sortgroup_colno)
	NODE_NODE(Query, remote_query)
	NODE_NODE(List,base_tlist)
	NODE_NODE(List,coord_var_tlist)
	NODE_NODE(List,query_var_tlist)
	NODE_SCALAR(bool,has_row_marks)
	NODE_SCALAR(bool,rq_save_command_id)
	NODE_SCALAR(bool,rq_use_pk_for_rep_change)
	NODE_SCALAR(int,rq_max_param_num)
END_NODE(RemoteQuery)
#endif /* NO_NODE_RemoteQuery */

/*#ifndef NO_STRUCT_PGXCNodeHandle
BEGIN_STRUCT(PGXCNodeHandle)
	NODE_SCALAR(Oid,nodeoid)
	NODE_SCALAR(int,sock)
	NODE_SCALAR(char,transaction_status)
	NODE_ENUM(DNConnectionState,state)
	struct RemoteQueryState *combiner;
#ifdef DN_CONNECTION_DEBUG
	NODE_SCALAR(bool,have_row_desc)
#endif
	NODE_OTHER_POINT(FILE,file_data)
	NODE_STRING(error)
	NODE_STRING(outBuffer)
	NODE_SCALAR(size_t,outSize)
	NODE_SCALAR(size_t,outEnd)
	NODE_STRING(inBuffer)
	NODE_SCALAR(size_t,inSize)
	NODE_SCALAR(size_t,inStart)
	NODE_SCALAR(size_t,inEnd)
	NODE_SCALAR(size_t,inCursor)
	NODE_ENUM(RESP_ROLLBACK,ck_resp_rollback)
END_STRUCT(PGXCNodeHandle)
#endif *//* NO_STRUCT_PGXCNodeHandle */

#ifndef NO_NODE_AlterNodeStmt
BEGIN_NODE(AlterNodeStmt)
	NODE_STRING(node_name)
	NODE_NODE(List,options)
END_NODE(AlterNodeStmt)
#endif /* NO_NODE_AlterNodeStmt */

#ifndef NO_NODE_CreateNodeStmt
BEGIN_NODE(CreateNodeStmt)
	NODE_STRING(node_name)
	NODE_NODE(List,options)
END_NODE(CreateNodeStmt)
#endif /* NO_NODE_CreateNodeStmt */

#ifndef NO_NODE_DropNodeStmt
BEGIN_NODE(DropNodeStmt)
	NODE_STRING(node_name)
END_NODE(DropNodeStmt)
#endif /* NO_NODE_DropNodeStmt */

#ifndef NO_NODE_CreateGroupStmt
BEGIN_NODE(CreateGroupStmt)
	NODE_STRING(group_name)
	NODE_NODE(List,nodes)
END_NODE(CreateGroupStmt)
#endif /* NO_NODE_CreateGroupStmt */

#ifndef NO_NODE_DropGroupStmt
BEGIN_NODE(DropGroupStmt)
	NODE_STRING(group_name)
END_NODE(DropGroupStmt)
#endif /* NO_NODE_DropGroupStmt */

#endif /* PGXC */

#ifndef NO_NODE_Alias
BEGIN_NODE(Alias)
	NODE_STRING(aliasname)
	NODE_NODE(List,colnames)
END_NODE(Alias)
#endif /* NO_NODE_Alias */

#ifndef NO_NODE_RangeVar
BEGIN_NODE(RangeVar)
	NODE_STRING(catalogname)
	NODE_STRING(schemaname)
	NODE_STRING(relname)
	NODE_ENUM(InhOption,inhOpt)
	NODE_SCALAR(char,relpersistence)
	NODE_NODE(Alias,alias)
	NODE_LOCATION(int,location)
END_NODE(RangeVar)
#endif /* NO_NODE_RangeVar */

#ifndef NO_NODE_IntoClause
BEGIN_NODE(IntoClause)
	NODE_NODE(RangeVar,rel)
	NODE_NODE(List,colNames)
	NODE_NODE(List,options)
	NODE_ENUM(OnCommitAction,onCommit)
	NODE_STRING(tableSpaceName)
	NODE_NODE(Node,viewQuery)
	NODE_SCALAR(bool,skipData)
#ifdef PGXC
	NODE_NODE(DistributeBy,distributeby)
	NODE_NODE(PGXCSubCluster,subcluster)
#endif
END_NODE(IntoClause)
#endif /* NO_NODE_IntoClause */

#ifndef NO_NODE_Expr
BEGIN_NODE(Expr)
END_NODE(Expr)
#endif /* NO_NODE_Expr */

#ifndef NO_NODE_Var
BEGIN_NODE(Var)
	NODE_SCALAR(Index,varno)
	NODE_SCALAR(AttrNumber,varattno)
	NODE_OID(type,vartype)
	NODE_SCALAR(int32,vartypmod)
	NODE_OID(collation,varcollid)
	NODE_SCALAR(Index,varlevelsup)
	NODE_SCALAR(Index,varnoold)
	NODE_SCALAR(AttrNumber,varoattno)
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(Var)
#endif /* NO_NODE_Var */

#ifndef NO_NODE_Const
BEGIN_NODE(Const)
	NODE_OID(type,consttype)		/* pg_type NODE_SCALAR(OID,of) the constant's datatype */
	NODE_SCALAR(int32,consttypmod)	/* typmod value, if any */
	NODE_OID(collation,constcollid)	/* OID of collation, or InvalidOid if none */
	NODE_SCALAR(int,constlen)		/* typlen of the constant's datatype */
	NODE_DATUM(Datum,constvalue,NODE_ARG_->consttype, NODE_ARG_->constisnull)		/* the constant's value */
	NODE_SCALAR(bool,constisnull)	/* whether the constant is null (if true,
								 * constvalue is undefined) */
	NODE_SCALAR(bool,constbyval)		/* whether this datatype is passed by value.
								 * If true, then all the information is stored
								 * in the Datum. If false, then the Datum
								 * contains a pointer to the information. */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(Const)
#endif /* NO_NODE_Const */

#ifndef NO_NODE_Param
BEGIN_NODE(Param)
	NODE_ENUM(ParamKind,paramkind)		/* kind of parameter. See above */
	NODE_SCALAR(int,paramid)		/* numeric ID for parameter */
	NODE_OID(type,paramtype)		/* pg_type OID of parameter's datatype */
	NODE_SCALAR(int32,paramtypmod)	/* typmod value, if known */
	NODE_OID(collation,paramcollid)	/* OID of collation, or InvalidOid if none */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(Param)
#endif /* NO_NODE_Param */

#ifndef NO_NODE_Aggref
BEGIN_NODE(Aggref)
	NODE_OID(proc,aggfnoid)		/* pg_proc Oid of the aggregate */
	NODE_OID(type,aggtype)		/* type Oid of result of the aggregate */
	NODE_OID(collation,aggcollid)		/* OID of collation of result */
	NODE_OID(collation,inputcollid)	/* OID of collation that function should use */
#ifdef PGXC
	NODE_OID(type,aggtrantype)	/* type Oid of transition results */
	NODE_SCALAR(bool,agghas_collectfn)	/* is collection function available */
#endif /* PGXC */
	NODE_NODE(List,args)			/* arguments and sort expressions */
	NODE_NODE(List,aggorder)		/* ORDER BY (list of SortGroupClause) */
	NODE_NODE(List,aggdistinct)	/* DISTINCT (list of SortGroupClause) */
	NODE_SCALAR(bool,aggstar)		/* TRUE if argument list was really '*' */
	NODE_SCALAR(Index,agglevelsup)	/* > 0 if agg belongs to outer query */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(Aggref)
#endif /* NO_NODE_Aggref */

#ifndef NO_NODE_WindowFunc
BEGIN_NODE(WindowFunc)
	NODE_OID(proc,winfnoid)		/* pg_proc Oid of the function */
	NODE_OID(type,wintype)		/* type Oid of result of the window function */
	NODE_OID(collation,wincollid)		/* OID of collation of result */
	NODE_OID(collation,inputcollid)	/* OID of collation that function should use */
	NODE_NODE(List,args)			/* arguments to the window function */
	NODE_SCALAR(Index,winref)			/* index of associated WindowClause */
	NODE_SCALAR(bool,winstar)		/* TRUE if argument list was really '*' */
	NODE_SCALAR(bool,winagg)			/* is function a simple aggregate? */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(WindowFunc)
#endif /* NO_NODE_WindowFunc */

#ifndef NO_NODE_ArrayRef
BEGIN_NODE(ArrayRef)
	NODE_OID(type,refarraytype)	/* type of the array proper */
	NODE_OID(type,refelemtype)	/* type of the array elements */
	NODE_SCALAR(int32,reftypmod)		/* typmod of the array (and elements too) */
	NODE_OID(collation,refcollid)		/* OID of collation, or InvalidOid if none */
	NODE_NODE(List,refupperindexpr)/* expressions that evaluate to upper array
								 * indexes */
	NODE_NODE(List,reflowerindexpr)/* expressions that evaluate to lower array
								 * indexes */
	NODE_NODE(Expr,refexpr)		/* the expression that evaluates to an array
								 * value */
	NODE_NODE(Expr,refassgnexpr)	/* expression for the source value, or NULL if
								 * fetch */
END_NODE(ArrayRef)
#endif /* NO_NODE_ArrayRef */

#ifndef NO_NODE_FuncExpr
BEGIN_NODE(FuncExpr)
	NODE_OID(proc,funcid)			/* PG_PROC OID of the function */
	NODE_OID(type,funcresulttype) /* PG_TYPE OID of result value */
	NODE_SCALAR(bool,funcretset)		/* true if function returns set */
	NODE_SCALAR(bool,funcvariadic)	/* true if variadic arguments have been
								 * combined into an array last argument */
	NODE_ENUM(CoercionForm,funcformat)	/* how to display this function call */
	NODE_OID(collation,funccollid)		/* OID of collation of result */
	NODE_OID(collation,inputcollid)	/* OID of collation that function should use */
	NODE_NODE(List,args)			/* arguments to the function */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(FuncExpr)
#endif /* NO_NODE_FuncExpr */

#ifndef NO_NODE_NamedArgExpr
BEGIN_NODE(NamedArgExpr)
	NODE_NODE(Expr,arg)			/* the argument expression */
	NODE_STRING(name)			/* the name */
	NODE_SCALAR(int,argnumber)		/* argument's number in positional notation */
	NODE_LOCATION(int,location)		/* argument name location, or -1 if unknown */
END_NODE(NamedArgExpr)
#endif /* NO_NODE_NamedArgExpr */

#ifndef NO_NODE_OpExpr
BEGIN_NODE(OpExpr)
	NODE_OID(operator,opno)			/* PG_OPERATOR OID of the operator */
	NODE_OID(proc,opfuncid)		/* PG_PROC OID of underlying function */
	NODE_OID(type,opresulttype)	/* PG_TYPE OID of result value */
	NODE_SCALAR(bool,opretset)		/* true if operator returns set */
	NODE_OID(collation,opcollid)		/* OID of collation of result */
	NODE_OID(collation,inputcollid)	/* OID of collation that operator should use */
	NODE_NODE(List,args)			/* arguments to the operator (1 or 2) */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(OpExpr)
#endif /* NO_NODE_OpExpr */

NODE_SAME(DistinctExpr,OpExpr)

NODE_SAME(NullIfExpr,OpExpr)

#ifndef NO_NODE_ScalarArrayOpExpr
BEGIN_NODE(ScalarArrayOpExpr)
	NODE_OID(operator,opno)			/* PG_OPERATOR OID of the operator */
	NODE_OID(proc,opfuncid)		/* PG_PROC OID of underlying function */
	NODE_SCALAR(bool,useOr)			/* true for ANY, false for ALL */
	NODE_OID(collation,inputcollid)	/* OID of collation that operator should use */
	NODE_NODE(List,args)			/* the scalar and array operands */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ScalarArrayOpExpr)
#endif /* NO_NODE_ScalarArrayOpExpr */

#ifndef NO_NODE_BoolExpr
BEGIN_NODE(BoolExpr)
	NODE_ENUM(BoolExprType,boolop)
	NODE_NODE(List,args)			/* arguments to this expression */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(BoolExpr)
#endif /* NO_NODE_BoolExpr */

#ifndef NO_NODE_SubLink
BEGIN_NODE(SubLink)
	NODE_ENUM(SubLinkType,subLinkType)	/* see above */
	NODE_NODE(Node,testexpr)		/* outer-query test for ALL/ANY/ROWCOMPARE */
	NODE_NODE(List,operName)		/* originally specified operator name */
	NODE_NODE(Node,subselect)		/* subselect as Query* or parsetree */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(SubLink)
#endif /* NO_NODE_SubLink */

#ifndef NO_NODE_SubPlan
BEGIN_NODE(SubPlan)
	/* Fields copied from original SubLink: */
	NODE_ENUM(SubLinkType,subLinkType)	/* see above */
	/* The combining operators, transformed to an executable expression: */
	NODE_NODE(Node,testexpr)		/* OpExpr or RowCompareExpr expression tree */
	NODE_NODE(List,paramIds)		/* IDs of Params embedded in the above */
	/* Identification of the Plan tree to use: */
	NODE_SCALAR(int,plan_id)		/* Index (from 1) in PlannedStmt.subplans */
	/* Identification of the SubPlan for EXPLAIN and debugging purposes: */
	NODE_STRING(plan_name)		/* A name assigned during planning */
	/* Extra data useful for determining subplan's output type: */
	NODE_OID(type,firstColType)	/* Type of first column of subplan result */
	NODE_SCALAR(int32,firstColTypmod) /* Typmod of first column of subplan result */
	NODE_OID(collation,firstColCollation)		/* Collation of first column of
										 * subplan result */
	/* Information about execution strategy: */
	NODE_SCALAR(bool,useHashTable)	/* TRUE to store subselect output in a hash
								 * table (implies we are doing "IN") */
	NODE_SCALAR(bool,unknownEqFalse) /* TRUE if it's okay to return FALSE when the
								 * spec result is UNKNOWN; this allows much
								 * simpler handling of null values */
	/* Information for passing params into and out of the subselect: */
	/* setParam and parParam are lists of integers (param IDs) */
	NODE_NODE(List,setParam)		/* initplan subqueries have to set these
								 * Params for parent plan */
	NODE_NODE(List,parParam)		/* indices of input Params from parent plan */
	NODE_NODE(List,args)			/* exprs to pass as parParam values */
	/* Estimated execution costs: */
	NODE_SCALAR(Cost,startup_cost)	/* one-time setup cost */
	NODE_SCALAR(Cost,per_call_cost)	/* cost for each subplan evaluation */
END_NODE(SubPlan)
#endif /* NO_NODE_SubPlan */

#ifndef NO_NODE_AlternativeSubPlan
BEGIN_NODE(AlternativeSubPlan)
	NODE_NODE(List,subplans)		/* SubPlan(s) with equivalent results */
END_NODE(AlternativeSubPlan)
#endif /* NO_NODE_AlternativeSubPlan */

#ifndef NO_NODE_FieldSelect
BEGIN_NODE(FieldSelect)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_SCALAR(AttrNumber,fieldnum)		/* attribute number of field to extract */
	NODE_OID(type,resulttype)		/* type of the field (result type of this
								 * node) */
	NODE_SCALAR(int32,resulttypmod)	/* output typmod (usually -1) */
	NODE_OID(collation,resultcollid)	/* OID of collation of the field */
END_NODE(FieldSelect)
#endif /* NO_NODE_FieldSelect */

#ifndef NO_NODE_FieldStore
BEGIN_NODE(FieldStore)
	NODE_NODE(Expr,arg)			/* input tuple value */
	NODE_NODE(List,newvals)		/* new value(s) for field(s) */
	NODE_NODE(List,fieldnums)		/* integer list of field attnums */
	NODE_OID(type,resulttype)		/* type of result (same as type of arg) */
	/* Like RowExpr, we deliberately omit a typmod and collation here */
END_NODE(FieldStore)
#endif /* NO_NODE_FieldStore */

#ifndef NO_NODE_RelabelType
BEGIN_NODE(RelabelType)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_OID(type,resulttype)		/* output type of coercion expression */
	NODE_SCALAR(int32,resulttypmod)	/* output typmod (usually -1) */
	NODE_OID(collation,resultcollid)	/* OID of collation, or InvalidOid if none */
	NODE_ENUM(CoercionForm,relabelformat) /* how to display this node */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(RelabelType)
#endif /* NO_NODE_RelabelType */

#ifndef NO_NODE_CoerceViaIO
BEGIN_NODE(CoerceViaIO)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_OID(type,resulttype)		/* output type of coercion */
	/* output typmod is not stored, but is presumed -1 */
	NODE_OID(collation,resultcollid)	/* OID of collation, or InvalidOid if none */
	NODE_ENUM(CoercionForm,coerceformat)	/* how to display this node */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CoerceViaIO)
#endif /* NO_NODE_CoerceViaIO */

#ifndef NO_NODE_ArrayCoerceExpr
BEGIN_NODE(ArrayCoerceExpr)
	NODE_NODE(Expr,arg)			/* input expression (yields an array) */
	NODE_OID(collation,elemfuncid)		/* OID of element coercion function, or 0 */
	NODE_OID(type,resulttype)		/* output type of coercion (an array type) */
	NODE_SCALAR(int32,resulttypmod)	/* output typmod (also element typmod) */
	NODE_OID(collation,resultcollid)	/* OID of collation, or InvalidOid if none */
	NODE_SCALAR(bool,isExplicit)		/* conversion semantics flag to pass to func */
	NODE_ENUM(CoercionForm,coerceformat)	/* how to display this node */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ArrayCoerceExpr)
#endif /* NO_NODE_ArrayCoerceExpr */

#ifndef NO_NODE_ConvertRowtypeExpr
BEGIN_NODE(ConvertRowtypeExpr)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_OID(type,resulttype)		/* output type (always a composite type) */
	/* Like RowExpr, we deliberately omit a typmod and collation here */
	NODE_ENUM(CoercionForm,convertformat) /* how to display this node */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ConvertRowtypeExpr)
#endif /* NO_NODE_ConvertRowtypeExpr */

#ifndef NO_NODE_CollateExpr
BEGIN_NODE(CollateExpr)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_OID(collation,collOid)		/* collation's OID */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CollateExpr)
#endif /* NO_NODE_CollateExpr */

#ifndef NO_NODE_CaseExpr
BEGIN_NODE(CaseExpr)
	NODE_OID(type,casetype)		/* type of expression result */
	NODE_OID(collation,casecollid)		/* OID of collation, or InvalidOid if none */
	NODE_NODE(Expr,arg)			/* implicit equality comparison argument */
	NODE_NODE(List,args)			/* the arguments (list of WHEN clauses) */
	NODE_NODE(Expr,defresult)		/* the default result (ELSE clause) */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
#ifdef ADB
	NODE_SCALAR(bool,isdecode)
#endif /* ADB */
END_NODE(CaseExpr)
#endif /* NO_NODE_CaseExpr */

#ifndef NO_NODE_CaseWhen
BEGIN_NODE(CaseWhen)
	NODE_NODE(Expr,expr)			/* condition expression */
	NODE_NODE(Expr,result)			/* substitution result */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CaseWhen)
#endif /* NO_NODE_CaseWhen */

#ifndef NO_NODE_CaseTestExpr
BEGIN_NODE(CaseTestExpr)
	NODE_OID(type,typeId)			/* type for substituted value */
	NODE_SCALAR(int32,typeMod)		/* typemod for substituted value */
	NODE_OID(collation,collation)		/* collation for the substituted value */
END_NODE(CaseTestExpr)
#endif /* NO_NODE_CaseTestExpr */

#ifndef NO_NODE_ArrayExpr
BEGIN_NODE(ArrayExpr)
	NODE_OID(type,array_typeid)	/* type of expression result */
	NODE_OID(collation,array_collid)	/* OID of collation, or InvalidOid if none */
	NODE_OID(type,element_typeid) /* common type of array elements */
	NODE_NODE(List,elements)		/* the array elements or sub-arrays */
	NODE_SCALAR(bool,multidims)		/* true if elements are sub-arrays */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ArrayExpr)
#endif /* NO_NODE_ArrayExpr */

#ifndef NO_NODE_RowExpr
BEGIN_NODE(RowExpr)
	NODE_NODE(List,args)			/* the fields */
	NODE_OID(type,row_typeid)		/* RECORDOID or a composite type's ID */
	NODE_ENUM(CoercionForm,row_format)	/* how to display this node */
	NODE_NODE(List,colnames)		/* list of String, or NIL */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(RowExpr)
#endif /* NO_NODE_RowExpr */

#ifndef NO_NODE_RowCompareExpr
BEGIN_NODE(RowCompareExpr)
	NODE_ENUM(RowCompareType,rctype)		/* LT LE GE or GT, never EQ or NE */
	NODE_NODE(List,opnos)			/* OID list of pairwise comparison ops */
	NODE_NODE(List,opfamilies)		/* OID list of containing operator families */
	NODE_NODE(List,inputcollids)	/* OID list of collations for comparisons */
	NODE_NODE(List,largs)			/* the left-hand input arguments */
	NODE_NODE(List,rargs)			/* the right-hand input arguments */
END_NODE(RowCompareExpr)
#endif /* NO_NODE_RowCompareExpr */

#ifndef NO_NODE_CoalesceExpr
BEGIN_NODE(CoalesceExpr)
	NODE_OID(type,coalescetype)	/* type of expression result */
	NODE_OID(collation,coalescecollid) /* OID of collation, or InvalidOid if none */
	NODE_NODE(List,args)			/* the arguments */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CoalesceExpr)
#endif /* NO_NODE_CoalesceExpr */

#ifndef NO_NODE_MinMaxExpr
BEGIN_NODE(MinMaxExpr)
	NODE_OID(type,minmaxtype)		/* common type of arguments and result */
	NODE_OID(collation,minmaxcollid)	/* OID of collation of result */
	NODE_OID(collation,inputcollid)	/* OID of collation that function should use */
	NODE_ENUM(MinMaxOp,op)				/* function to execute */
	NODE_NODE(List,args)			/* the arguments */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(MinMaxExpr)
#endif /* NO_NODE_MinMaxExpr */

#ifndef NO_NODE_XmlExpr
BEGIN_NODE(XmlExpr)
	NODE_ENUM(XmlExprOp,op)				/* xml function ID */
	NODE_STRING(name)			/* name in xml(NAME foo ...) syntaxes */
	NODE_NODE(List,named_args)		/* non-XML expressions for xml_attributes */
	NODE_NODE(List,arg_names)		/* parallel list of Value strings */
	NODE_NODE(List,args)			/* list of expressions */
	NODE_ENUM(XmlOptionType,xmloption)	/* DOCUMENT or CONTENT */
	NODE_OID(type,type)			/* target type/typmod for XMLSERIALIZE */
	NODE_SCALAR(int32,typmod)
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(XmlExpr)
#endif /* NO_NODE_XmlExpr */

#ifndef NO_NODE_NullTest
BEGIN_NODE(NullTest)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_ENUM(NullTestType,nulltesttype)	/* IS NULL, IS NOT NULL */
	NODE_SCALAR(bool,argisrow)		/* T if input is of a composite type */
END_NODE(NullTest)
#endif /* NO_NODE_NullTest */

#ifndef NO_NODE_BooleanTest
BEGIN_NODE(BooleanTest)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_ENUM(BoolTestType,booltesttype)	/* test type */
END_NODE(BooleanTest)
#endif /* NO_NODE_BooleanTest */

#ifndef NO_NODE_CoerceToDomain
BEGIN_NODE(CoerceToDomain)
	NODE_NODE(Expr,arg)			/* input expression */
	NODE_OID(type,resulttype)		/* domain type ID (result type) */
	NODE_SCALAR(int32,resulttypmod)	/* output typmod (currently always -1) */
	NODE_OID(collation,resultcollid)	/* OID of collation, or InvalidOid if none */
	NODE_ENUM(CoercionForm,coercionformat)	/* how to display this node */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CoerceToDomain)
#endif /* NO_NODE_CoerceToDomain */

#ifndef NO_NODE_CoerceToDomainValue
BEGIN_NODE(CoerceToDomainValue)
	NODE_OID(type,typeId)			/* type for substituted value */
	NODE_SCALAR(int32,typeMod)		/* typemod for substituted value */
	NODE_OID(collation,collation)		/* collation for the substituted value */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CoerceToDomainValue)
#endif /* NO_NODE_CoerceToDomainValue */

#ifndef NO_NODE_SetToDefault
BEGIN_NODE(SetToDefault)
	NODE_OID(type,typeId)			/* type for substituted value */
	NODE_SCALAR(int32,typeMod)		/* typemod for substituted value */
	NODE_OID(collation,collation)		/* collation for the substituted value */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(SetToDefault)
#endif /* NO_NODE_SetToDefault */

#ifndef NO_NODE_CurrentOfExpr
BEGIN_NODE(CurrentOfExpr)
	NODE_SCALAR(Index,cvarno)			/* RT index of target relation */
	NODE_STRING(cursor_name)	/* name of referenced cursor, or NULL */
	NODE_SCALAR(int,cursor_param)	/* refcursor parameter number, or 0 */
END_NODE(CurrentOfExpr)
#endif /* NO_NODE_CurrentOfExpr */

#ifndef NO_NODE_TargetEntry
BEGIN_NODE(TargetEntry)
	NODE_NODE(Expr,expr)			/* expression to evaluate */
	NODE_SCALAR(AttrNumber,resno)			/* attribute number (see notes above) */
	NODE_STRING(resname)		/* name of the column (could be NULL) */
	NODE_SCALAR(Index,ressortgroupref)/* nonzero if referenced by a sort/group
								 * clause */
	NODE_SCALAR(Oid,resorigtbl)		/* OID of column's source table */
	NODE_SCALAR(AttrNumber,resorigcol)		/* column's number in source table */
	NODE_SCALAR(bool,resjunk)		/* set to true to eliminate the attribute from
								 * final target list */
END_NODE(TargetEntry)
#endif /* NO_NODE_TargetEntry */

#ifndef NO_NODE_RangeTblRef
BEGIN_NODE(RangeTblRef)
	NODE_SCALAR(int,rtindex)
END_NODE(RangeTblRef)
#endif /* NO_NODE_RangeTblRef */

#ifndef NO_NODE_JoinExpr
BEGIN_NODE(JoinExpr)
	NODE_ENUM(JoinType,jointype)		/* type of join */
	NODE_SCALAR(bool,isNatural)		/* Natural join? Will need to shape table */
	NODE_NODE(Node,larg)			/* left subtree */
	NODE_NODE(Node,rarg)			/* right subtree */
	NODE_NODE(List,usingClause)	/* USING clause, if any (list of String) */
	NODE_NODE(Node,quals)			/* qualifiers on join, if any */
	NODE_NODE(Alias,alias)			/* user-written alias clause, if any */
	NODE_SCALAR(int,rtindex)		/* RT index assigned for join, or 0 */
END_NODE(JoinExpr)
#endif /* NO_NODE_JoinExpr */

#ifndef NO_NODE_FromExpr
BEGIN_NODE(FromExpr)
	NODE_NODE(List,fromlist)		/* List of join subtrees */
	NODE_NODE(Node,quals)			/* qualifiers on join, if any */
END_NODE(FromExpr)
#endif /* NO_NODE_FromExpr */

#ifdef PGXC

#ifndef NO_NODE_DistributeBy
BEGIN_NODE(DistributeBy)
	NODE_ENUM(DistributionType,disttype)		/* Distribution type */
	NODE_STRING(colname)		/* Distribution column name */
#ifdef ADB
	NODE_NODE(List, funcname)
	NODE_NODE(List, funcargs)
#endif
END_NODE(DistributeBy)
#endif /* NO_NODE_DistributeBy */

#ifndef NO_NODE_PGXCSubCluster
BEGIN_NODE(PGXCSubCluster)
	NODE_ENUM(PGXCSubClusterType,clustertype)	/* Subcluster type */
	NODE_NODE(List,members)		/* List of nodes or groups */
END_NODE(PGXCSubCluster)
#endif /* NO_NODE_PGXCSubCluster */
#endif

#ifndef NO_NODE_PlannerGlobal
BEGIN_NODE(PlannerGlobal)
	NODE_STRUCT(ParamListInfoData,boundParams)	/* Param values provided to planner() */
	NODE_NODE(List,subplans)		/* Plans for SubPlan nodes */
	NODE_NODE(List,subroots)		/* PlannerInfos for SubPlan nodes */
	NODE_BITMAPSET(Bitmapset,rewindPlanIDs)	/* indices of subplans that require REWIND */
	NODE_NODE(List,finalrtable)	/* "flat" rangetable for executor */
	NODE_NODE(List,finalrowmarks)	/* "flat" list of PlanRowMarks */
	NODE_NODE(List,resultRelations)	/* "flat" list of integer RT indexes */
	NODE_NODE(List,relationOids)	/* OIDs of relations the plan depends on */
	NODE_NODE(List,invalItems)		/* other dependencies, as PlanInvalItems */
	NODE_SCALAR(int,nParamExec)		/* number of PARAM_EXEC Params used */
	NODE_SCALAR(Index,lastPHId)		/* highest PlaceHolderVar ID assigned */
	NODE_SCALAR(Index,lastRowMarkId)	/* highest PlanRowMark ID assigned */
	NODE_SCALAR(bool,transientPlan)	/* redo plan when TransactionXmin changes? */
END_NODE(PlannerGlobal)
#endif /* NO_NODE_PlannerGlobal */

#ifndef NO_NODE_PlannerInfo
BEGIN_NODE(PlannerInfo)
	NODE_NODE(Query,parse)			/* the Query being planned */
	NODE_NODE(PlannerGlobal,glob)		/* global info for current planner run */
	NODE_SCALAR(Index,query_level)	/* 1 at the outermost Query */
	NODE_NODE(PlannerInfo,parent_root)	/* NULL at outermost Query */
	NODE_NODE(List,plan_params)	/* list of PlannerParamItems, see below */
	NODE_SCALAR(int,simple_rel_array_size)	/* allocated size of array */
	NODE_NODE_ARRAY(RelOptInfo,simple_rel_array,NODE_ARG_->simple_rel_array_size)		/* All 1-rel RelOptInfos */
	NODE_NODE_ARRAY(RangeTblEntry,simple_rte_array,NODE_ARG_->simple_rel_array_size)	/* rangetable as an array */
	NODE_RELIDS(Relids,all_baserels)
	NODE_NODE(List,join_rel_list)	/* list of join-relation RelOptInfos */
	NODE_OTHER_POINT(HTAB, join_rel_hash) /* optional hashtable for join relations */
	/*TODO NODE_NODE_ARRAY(List, join_rel_level, 0)*/ /* lists of join-relation RelOptInfos */
	NODE_SCALAR(int,join_cur_level) /* index of list being extended */
	NODE_NODE(List,init_plans)		/* init SubPlans for query */
	NODE_NODE(List,cte_plan_ids)	/* per-CTE-item list of subplan IDs */
	NODE_NODE(List,eq_classes)		/* list of active EquivalenceClasses */
	NODE_NODE(List,canon_pathkeys) /* list of "canonical" PathKeys */
	NODE_NODE(List,left_join_clauses)		/* list of RestrictInfos for
										 * mergejoinable outer join clauses
										 * w/nonnullable var on left */
	NODE_NODE(List,right_join_clauses)		/* list of RestrictInfos for
										 * mergejoinable outer join clauses
										 * w/nonnullable var on right */
	NODE_NODE(List,full_join_clauses)		/* list of RestrictInfos for
										 * mergejoinable full join clauses */
	NODE_NODE(List,join_info_list) /* list of SpecialJoinInfos */
	NODE_NODE(List,lateral_info_list)		/* list of LateralJoinInfos */
	NODE_NODE(List,append_rel_list)	/* list of AppendRelInfos */
	NODE_NODE(List,rowMarks)		/* list of PlanRowMarks */
	NODE_NODE(List,placeholder_list)		/* list of PlaceHolderInfos */
	NODE_NODE(List,query_pathkeys) /* desired pathkeys for query_planner(), and
								 * actual pathkeys after planning */
	NODE_NODE(List,group_pathkeys) /* groupClause pathkeys, if any */
	NODE_NODE(List,window_pathkeys)	/* pathkeys of bottom window, if any */
	NODE_NODE(List,distinct_pathkeys)		/* distinctClause pathkeys, if any */
	NODE_NODE(List,sort_pathkeys)	/* sortClause pathkeys, if any */
	NODE_NODE(List,minmax_aggs)	/* List of MinMaxAggInfos */
	NODE_NODE(List,initial_rels)	/* RelOptInfos we are now trying to join */
	NODE_OTHER_POINT(MemoryContext,planner_cxt)	/* context holding PlannerInfo */
	NODE_SCALAR(double,total_table_pages)		/* # of pages in all tables of query */
	NODE_SCALAR(double,tuple_fraction) /* tuple_fraction passed to query_planner */
	NODE_SCALAR(double,limit_tuples)	/* limit_tuples passed to query_planner */
	NODE_SCALAR(bool,hasInheritedTarget)		/* true if parse->resultRelation is an
										 * inheritance child rel */
	NODE_SCALAR(bool,hasJoinRTEs)	/* true if any RTEs are RTE_JOIN kind */
	NODE_SCALAR(bool,hasLateralRTEs) /* true if any RTEs are marked LATERAL */
	NODE_SCALAR(bool,hasHavingQual)	/* true if havingQual was non-null */
	NODE_SCALAR(bool,hasPseudoConstantQuals) /* true if any RestrictInfo has
										 * pseudoconstant = true */
	NODE_SCALAR(bool,hasRecursion)	/* true if planning a recursive WITH item */
#ifdef PGXC
	NODE_SCALAR(int,rs_alias_index) /* used to build the alias reference */
	NODE_NODE(List,xc_rowMarks)		/* list of PlanRowMarks of type ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE */
#endif
	NODE_SCALAR(int,wt_param_id)	/* PARAM_EXEC ID for the work table */
	NODE_NODE(Plan,non_recursive_plan)	/* plan for non-recursive term */
	NODE_RELIDS(Relids,curOuterRels)	/* outer rels above current node */
	NODE_NODE(List,curOuterParams) /* not-yet-assigned NestLoopParams */
	NODE_OTHER_POINT(void,join_search_private)
	NODE_RELIDS(Relids,nullable_baserels)
END_NODE(PlannerInfo)
#endif /* NO_NODE_PlannerInfo */

#ifndef NO_NODE_RelOptInfo
BEGIN_NODE(RelOptInfo)
	NODE_ENUM(RelOptKind,reloptkind)
	/* all relations included in this RelOptInfo */
	NODE_RELIDS(Relids,relids)			/* set of base relids (rangetable indexes) */
	/* size estimates generated by planner */
	NODE_SCALAR(double,rows)			/* estimated number of result tuples */
	NODE_SCALAR(int,width)			/* estimated avg width of result tuples */
	/* per-relation planner control flags */
	NODE_SCALAR(bool,consider_startup)		/* keep cheap-startup-cost paths? */
	NODE_SCALAR(bool,consider_param_startup) /* ditto, for parameterized paths? */
	/* materialization information */
	NODE_NODE(List,reltargetlist)	/* Vars to be output by scan of relation */
	NODE_NODE(List,pathlist)		/* Path structures */
	NODE_NODE(List,ppilist)		/* ParamPathInfos used in pathlist */
	NODE_NODE(Path,cheapest_startup_path)
	NODE_NODE(Path,cheapest_total_path)
	NODE_NODE(Path,cheapest_unique_path)
	NODE_NODE(List,cheapest_parameterized_paths)
	/* information about a base rel (not set for join rels!) */
	NODE_SCALAR(Index,relid)
	NODE_SCALAR(Oid,reltablespace)	/* containing tablespace */
	NODE_ENUM(RTEKind,rtekind)		/* RELATION, SUBQUERY, or FUNCTION */
	NODE_SCALAR(AttrNumber,min_attr)		/* smallest attrno of rel (often <0) */
	NODE_SCALAR(AttrNumber,max_attr)		/* largest attrno of rel */
	NODE_RELIDS_ARRAY(Relids, attr_needed, (NODE_ARG_->max_attr-NODE_ARG_->min_attr))	/* array indexed [min_attr .. max_attr] */
	NODE_SCALAR_POINT(int32, attr_widths,(NODE_ARG_->max_attr-NODE_ARG_->min_attr))	/* array indexed [min_attr .. max_attr] */
	NODE_NODE(List,lateral_vars)	/* LATERAL Vars and PHVs referenced by rel */
	NODE_RELIDS(Relids,lateral_relids) /* minimum parameterization of rel */
	NODE_RELIDS(Relids,lateral_referencers)	/* rels that reference me laterally */
	NODE_NODE(List,indexlist)		/* list of IndexOptInfo */
	NODE_SCALAR(BlockNumber,pages)			/* size estimates derived from pg_class */
	NODE_SCALAR(double,tuples)
	NODE_SCALAR(double,allvisfrac)
	/* use "struct Plan" to avoid including plannodes.h here */
	NODE_NODE(Plan,subplan)		/* if subquery */
	NODE_NODE(PlannerInfo,subroot)		/* if subquery */
	NODE_NODE(List,subplan_params) /* if subquery */
	/* use "struct FdwRoutine" to avoid including fdwapi.h here */
	NODE_OTHER_POINT(FdwRoutine,fdwroutine)		/* if foreign table */
	NODE_OTHER_POINT(void,fdw_private)	/* if foreign table */
	/* used by various scans and joins: */
	NODE_NODE(List,baserestrictinfo)		/* RestrictInfo structures (if base
										 * rel) */
	NODE_STRUCT_MEB(QualCost,baserestrictcost)		/* cost of evaluating the above */
	NODE_NODE(List,joininfo)		/* RestrictInfo structures for join clauses
								 * involving this rel */
	NODE_SCALAR(bool,has_eclass_joins)		/* T means joininfo is incomplete */
END_NODE(RelOptInfo)
#endif /* NO_NODE_RelOptInfo */

#ifndef NO_NODE_IndexOptInfo
BEGIN_NODE(IndexOptInfo)
	NODE_SCALAR(Oid,indexoid)		/* OID of the index relation */
	NODE_SCALAR(Oid,reltablespace)	/* tablespace of index (not table) */
	NODE_NODE(RelOptInfo,rel)			/* back-link to index's table */
	/* index-size statistics (from pg_class and elsewhere) */
	NODE_SCALAR(BlockNumber,pages)			/* number of disk pages in index */
	NODE_SCALAR(double,tuples)			/* number of index tuples in index */
	NODE_SCALAR(int,tree_height)	/* index tree height, or -1 if unknown */
	/* index descriptor information */
	NODE_SCALAR(int,ncolumns)		/* number of columns in index */
	NODE_SCALAR_POINT(int,indexkeys,NODE_ARG_->ncolumns)		/* column numbers of index's keys, or 0 */
	NODE_SCALAR_POINT(Oid,indexcollations,NODE_ARG_->ncolumns)	/* OIDs of collations of index columns */
	NODE_SCALAR_POINT(Oid,opfamily,NODE_ARG_->ncolumns)		/* OIDs of operator families for columns */
	NODE_SCALAR_POINT(Oid,opcintype,NODE_ARG_->ncolumns)		/* OIDs of opclass declared input data types */
	NODE_SCALAR_POINT(Oid,sortopfamily,NODE_ARG_->ncolumns)	/* OIDs of btree opfamilies, if orderable */
	NODE_SCALAR_POINT(bool,reverse_sort,NODE_ARG_->ncolumns)	/* is sort order descending? */
	NODE_SCALAR_POINT(bool,nulls_first,NODE_ARG_->ncolumns)	/* do NULLs come first in the sort order? */
	NODE_SCALAR(Oid,relam)			/* OID of the access method (in pg_am) */
	NODE_SCALAR(RegProcedure,amcostestimate)	/* OID of the access method's cost fcn */
	NODE_NODE(List,indexprs)		/* expressions for non-simple index columns */
	NODE_NODE(List,indpred)		/* predicate if a partial index, else NIL */
	NODE_NODE(List,indextlist)		/* targetlist representing index columns */
	NODE_SCALAR(bool,predOK)			/* true if predicate matches query */
	NODE_SCALAR(bool,unique)			/* true if a unique index */
	NODE_SCALAR(bool,immediate)		/* is uniqueness enforced immediately? */
	NODE_SCALAR(bool,hypothetical)	/* true if index doesn't really exist */
	NODE_SCALAR(bool,canreturn)		/* can index return IndexTuples? */
	NODE_SCALAR(bool,amcanorderbyop) /* does AM support order by operator result? */
	NODE_SCALAR(bool,amoptionalkey)	/* can query omit key for the first column? */
	NODE_SCALAR(bool,amsearcharray)	/* can AM handle ScalarArrayOpExpr quals? */
	NODE_SCALAR(bool,amsearchnulls)	/* can AM search for NULL/NOT NULL entries? */
	NODE_SCALAR(bool,amhasgettuple)	/* does AM have amgettuple interface? */
	NODE_SCALAR(bool,amhasgetbitmap) /* does AM have amgetbitmap interface? */
END_NODE(IndexOptInfo)
#endif /* NO_NODE_IndexOptInfo */

#ifndef NO_NODE_EquivalenceClass
BEGIN_NODE(EquivalenceClass)
	NODE_NODE(List,ec_opfamilies)	/* btree operator family OIDs */
	NODE_OID(collation,ec_collation)	/* collation, if datatypes are collatable */
	NODE_NODE(List,ec_members)		/* list of EquivalenceMembers */
	NODE_NODE(List,ec_sources)		/* list of generating RestrictInfos */
	NODE_NODE(List,ec_derives)		/* list of derived RestrictInfos */
	NODE_RELIDS(Relids,ec_relids)		/* all relids appearing in ec_members */
	NODE_SCALAR(bool,ec_has_const)	/* any pseudoconstants in ec_members? */
	NODE_SCALAR(bool,ec_has_volatile)	/* the (sole) member is a volatile expr */
	NODE_SCALAR(bool,ec_below_outer_join)	/* equivalence applies below an OJ */
	NODE_SCALAR(bool,ec_broken)		/* failed to generate needed clauses? */
	NODE_SCALAR(Index,ec_sortref)		/* originating sortclause label, or 0 */
	NODE_NODE(EquivalenceClass,ec_merged) /* set if merged into another EC */
END_NODE(EquivalenceClass)
#endif /* NO_NODE_EquivalenceClass */

#ifndef NO_NODE_EquivalenceMember
BEGIN_NODE(EquivalenceMember)
	NODE_NODE(Expr,em_expr)		/* the expression represented */
	NODE_RELIDS(Relids,em_relids)		/* all relids appearing in em_expr */
	NODE_RELIDS(Relids,em_nullable_relids)		/* nullable by lower outer joins */
	NODE_SCALAR(bool,em_is_const)	/* expression is pseudoconstant? */
	NODE_SCALAR(bool,em_is_child)	/* derived version for a child relation? */
	NODE_OID(collation,em_datatype)	/* the "nominal type" used by the opfamily */
END_NODE(EquivalenceMember)
#endif /* NO_NODE_EquivalenceMember */

#ifndef NO_NODE_PathKey
BEGIN_NODE(PathKey)
	NODE_NODE(EquivalenceClass,pk_eclass)	/* the value that is ordered */
	NODE_SCALAR(Oid,pk_opfamily)	/* btree opfamily defining the ordering */
	NODE_SCALAR(int,pk_strategy)	/* sort direction (ASC or DESC) */
	NODE_SCALAR(bool,pk_nulls_first) /* do NULLs come before normal values? */
END_NODE(PathKey)
#endif /* NO_NODE_PathKey */

#ifndef NO_NODE_ParamPathInfo
BEGIN_NODE(ParamPathInfo)
	NODE_RELIDS(Relids,ppi_req_outer)	/* rels supplying parameters used by path */
	NODE_SCALAR(double,ppi_rows)		/* estimated number of result tuples */
	NODE_NODE(List,ppi_clauses)	/* join clauses available from outer rels */
END_NODE(ParamPathInfo)
#endif /* NO_NODE_ParamPathInfo */

#ifndef NO_NODE_Path
BEGIN_NODE(Path)
	NODE_NODE(RelOptInfo, parent)			/* the relation this path can build */
	NODE_NODE(ParamPathInfo,param_info)	/* parameterization info, or NULL if none */
	/* estimated size/costs for path (see costsize.c for more info) */
	NODE_SCALAR(double,rows)			/* estimated number of result tuples */
	NODE_SCALAR(Cost,startup_cost)	/* cost expended before fetching any tuples */
	NODE_SCALAR(Cost,total_cost)		/* total cost (assuming all tuples fetched) */
	NODE_NODE(List,pathkeys)		/* sort ordering of path's output */
	/* pathkeys is a List of PathKey nodes; see above */
END_NODE(Path)
#endif /* NO_NODE_Path */

#ifndef NO_NODE_IndexPath
BEGIN_NODE(IndexPath)
	NODE_NODE(IndexOptInfo,indexinfo)
	NODE_NODE(List,indexclauses)
	NODE_NODE(List,indexquals)
	NODE_NODE(List,indexqualcols)
	NODE_NODE(List,indexorderbys)
	NODE_NODE(List,indexorderbycols)
	NODE_ENUM(ScanDirection,indexscandir)
	NODE_SCALAR(Cost,indextotalcost)
	NODE_SCALAR(Selectivity,indexselectivity)
END_NODE(IndexPath)
#endif /* NO_NODE_IndexPath */

#ifndef NO_NODE_BitmapHeapPath
BEGIN_NODE(BitmapHeapPath)
	NODE_NODE(Path,bitmapqual)		/* IndexPath, BitmapAndPath, BitmapOrPath */
END_NODE(BitmapHeapPath)
#endif /* NO_NODE_BitmapHeapPath */

#ifndef NO_NODE_BitmapAndPath
BEGIN_NODE(BitmapAndPath)
	NODE_NODE(List,bitmapquals)	/* IndexPaths and BitmapOrPaths */
	NODE_SCALAR(Selectivity,bitmapselectivity)
END_NODE(BitmapAndPath)
#endif /* NO_NODE_BitmapAndPath */

#ifndef NO_NODE_BitmapOrPath
BEGIN_NODE(BitmapOrPath)
	NODE_NODE(List,bitmapquals)	/* IndexPaths and BitmapAndPaths */
	NODE_SCALAR(Selectivity,bitmapselectivity)
END_NODE(BitmapOrPath)
#endif /* NO_NODE_BitmapOrPath */

#ifndef NO_NODE_TidPath
BEGIN_NODE(TidPath)
	NODE_NODE(List,tidquals)		/* qual(s) involving CTID = something */
END_NODE(TidPath)
#endif /* NO_NODE_TidPath */

#ifndef NO_NODE_ForeignPath
BEGIN_NODE(ForeignPath)
	NODE_NODE(List,fdw_private)
END_NODE(ForeignPath)
#endif /* NO_NODE_ForeignPath */

#ifndef NO_NODE_AppendPath
BEGIN_NODE(AppendPath)
	NODE_NODE(List,subpaths)		/* list of component Paths */
END_NODE(AppendPath)
#endif /* NO_NODE_AppendPath */

#ifndef NO_NODE_MergeAppendPath
BEGIN_NODE(MergeAppendPath)
	NODE_NODE(List,subpaths)		/* list of component Paths */
	NODE_SCALAR(double,limit_tuples)	/* hard limit on output tuples, or -1 */
END_NODE(MergeAppendPath)
#endif /* NO_NODE_MergeAppendPath */

#ifndef NO_NODE_ResultPath
BEGIN_NODE(ResultPath)
	NODE_NODE(List,quals)
END_NODE(ResultPath)
#endif /* NO_NODE_ResultPath */

#ifndef NO_NODE_MaterialPath
BEGIN_NODE(MaterialPath)
	NODE_NODE(Path,subpath)
END_NODE(MaterialPath)
#endif /* NO_NODE_MaterialPath */

#ifndef NO_NODE_UniquePath
BEGIN_NODE(UniquePath)
	NODE_NODE(Path,subpath)
	NODE_ENUM(UniquePathMethod,umethod)
	NODE_NODE(List,in_operators)	/* equality operators of the IN clause */
	NODE_NODE(List,uniq_exprs)		/* expressions to be made unique */
END_NODE(UniquePath)
#endif /* NO_NODE_UniquePath */

#ifndef NO_NODE_JoinPath
BEGIN_NODE(JoinPath)
	NODE_ENUM(JoinType,jointype)
	NODE_NODE(Path,outerjoinpath)	/* path for the outer side of the join */
	NODE_NODE(Path,innerjoinpath)	/* path for the inner side of the join */
	NODE_NODE(List,joinrestrictinfo)		/* RestrictInfos to apply to join */
END_NODE(JoinPath)
#endif /* NO_NODE_JoinPath */

NODE_SAME(NestPath,JoinPath)

#ifndef NO_NODE_MergePath
BEGIN_NODE(MergePath)
	NODE_BASE2(JoinPath,jpath)
	NODE_NODE(List,path_mergeclauses)		/* join clauses to be used for merge */
	NODE_NODE(List,outersortkeys)	/* keys for explicit sort, if any */
	NODE_NODE(List,innersortkeys)	/* keys for explicit sort, if any */
	NODE_SCALAR(bool,materialize_inner)		/* add Materialize to inner? */
END_NODE(MergePath)
#endif /* NO_NODE_MergePath */

#ifndef NO_NODE_HashPath
BEGIN_NODE(HashPath)
	NODE_BASE2(JoinPath,jpath)
	NODE_NODE(List,path_hashclauses)		/* join clauses used for hashing */
	NODE_SCALAR(int,num_batches)	/* number of batches expected */
END_NODE(HashPath)
#endif /* NO_NODE_HashPath */

#ifdef PGXC
#ifndef NO_NODE_RemoteQueryPath
BEGIN_NODE(RemoteQueryPath)
	NODE_NODE(ExecNodes,rqpath_en)
	NODE_NODE(RemoteQueryPath,leftpath)
	NODE_NODE(RemoteQueryPath,rightpath)
	NODE_ENUM(JoinType,jointype)
	NODE_NODE(List,join_restrictlist)	/* restrict list corresponding to JOINs,
												 * only considered if rest of
												 * the JOIN information is
												 * available
												 */
	NODE_SCALAR(bool,rqhas_unshippable_qual) /* TRUE if there is at least
													 * one qual which can not be
													 * shipped to the datanodes
													 */
	NODE_SCALAR(bool,rqhas_temp_rel)			/* TRUE if one of the base relations
													 * involved in this path is a temporary
													 * table.
													 */
	NODE_SCALAR(bool,rqhas_unshippable_tlist)/* TRUE if there is at least one
													 * targetlist entry which is
													 * not completely shippable.
													 */
END_NODE(RemoteQueryPath)
#endif /* NO_NODE_RemoteQueryPath */
#endif /* PGXC */

#ifndef NO_NODE_RestrictInfo
BEGIN_NODE(RestrictInfo)
	NODE_NODE(Expr,clause)			/* the represented clause of WHERE or JOIN */
	NODE_SCALAR(bool,is_pushed_down) /* TRUE if clause was pushed down in level */
	NODE_SCALAR(bool,outerjoin_delayed)		/* TRUE if delayed by lower outer join */
	NODE_SCALAR(bool,can_join)		/* see comment above */
	NODE_SCALAR(bool,pseudoconstant) /* see comment above */
	/* The set of relids (varnos) actually referenced in the clause: */
	NODE_RELIDS(Relids,clause_relids)
	/* The set of relids required to evaluate the clause: */
	NODE_RELIDS(Relids,required_relids)
	/* If an outer-join clause, the outer-side relations, else NULL: */
	NODE_RELIDS(Relids,outer_relids)
	/* The relids used in the clause that are nullable by lower outer joins: */
	NODE_RELIDS(Relids,nullable_relids)
	/* These fields are set for any binary opclause: */
	NODE_RELIDS(Relids,left_relids)	/* relids in left side of clause */
	NODE_RELIDS(Relids,right_relids)	/* relids in right side of clause */
	/* This field is NULL unless clause is an OR clause: */
	NODE_NODE(Expr,orclause)		/* modified clause with RestrictInfos */
	/* This field is NULL unless clause is potentially redundant: */
	NODE_NODE(EquivalenceClass,parent_ec)	/* generating EquivalenceClass */
	/* cache space for cost and selectivity */
	NODE_STRUCT_MEB(QualCost,eval_cost)		/* eval cost of clause; -1 if not yet set */
	NODE_SCALAR(Selectivity,norm_selec)		/* selectivity for "normal" (JOIN_INNER)
								 * semantics; -1 if not yet set; >1 means a
								 * redundant clause */
	NODE_SCALAR(Selectivity,outer_selec)	/* selectivity for outer join semantics; -1 if
								 * not yet set */
	/* valid if clause is mergejoinable, else NIL */
	NODE_NODE(List,mergeopfamilies)	/* opfamilies containing clause operator */
	/* cache space for mergeclause processing; NULL if not yet set */
	NODE_NODE(EquivalenceClass,left_ec)	/* EquivalenceClass containing lefthand */
	NODE_NODE(EquivalenceClass,right_ec) /* EquivalenceClass containing righthand */
	NODE_NODE(EquivalenceMember,left_em) /* EquivalenceMember for lefthand */
	NODE_NODE(EquivalenceMember,right_em)	/* EquivalenceMember for righthand */
	NODE_STRUCT_LIST(MergeScanSelCache,scansel_cache)	/* list of MergeScanSelCache structs */
	/* transient workspace for use while considering a specific join path */
	NODE_SCALAR(bool,outer_is_left)	/* T = outer var on left, F = on right */
	/* valid if clause is hashjoinable, else InvalidOid: */
	NODE_SCALAR(Oid,hashjoinoperator)		/* copy of clause operator */
	/* cache space for hashclause processing; -1 if not yet set */
	NODE_SCALAR(Selectivity,left_bucketsize)	/* avg bucketsize of left side */
	NODE_SCALAR(Selectivity,right_bucketsize)		/* avg bucketsize of right side */
END_NODE(RestrictInfo)
#endif /* NO_NODE_RestrictInfo */

#ifndef NO_STRUCT_MergeScanSelCache
BEGIN_STRUCT(MergeScanSelCache)
	/* Ordering details (cache lookup key) */
	NODE_SCALAR(Oid,opfamily)		/* btree opfamily defining the ordering */
	NODE_OID(collation,collation)		/* collation for the ordering */
	NODE_SCALAR(int,strategy)		/* sort direction (ASC or DESC) */
	NODE_SCALAR(bool,nulls_first)	/* do NULLs come before normal values? */
	/* Results */
	NODE_SCALAR(Selectivity,leftstartsel)	/* first-join fraction for clause left side */
	NODE_SCALAR(Selectivity,leftendsel)		/* last-join fraction for clause left side */
	NODE_SCALAR(Selectivity,rightstartsel)	/* first-join fraction for clause right side */
	NODE_SCALAR(Selectivity,rightendsel)	/* last-join fraction for clause right side */
END_STRUCT(MergeScanSelCache)
#endif /* NO_STRUCT_MergeScanSelCache */

#ifndef NO_NODE_PlaceHolderVar
BEGIN_NODE(PlaceHolderVar)
	NODE_NODE(Expr,phexpr)			/* the represented expression */
	NODE_RELIDS(Relids,phrels)			/* base relids syntactically within expr src */
	NODE_SCALAR(Index,phid)			/* ID for PHV (unique within planner run) */
	NODE_SCALAR(Index,phlevelsup)		/* > 0 if PHV belongs to outer query */
END_NODE(PlaceHolderVar)
#endif /* NO_NODE_PlaceHolderVar */

#ifndef NO_NODE_SpecialJoinInfo
BEGIN_NODE(SpecialJoinInfo)
	NODE_RELIDS(Relids,min_lefthand)	/* base relids in minimum LHS for join */
	NODE_RELIDS(Relids,min_righthand)	/* base relids in minimum RHS for join */
	NODE_RELIDS(Relids,syn_lefthand)	/* base relids syntactically within LHS */
	NODE_RELIDS(Relids,syn_righthand)	/* base relids syntactically within RHS */
	NODE_ENUM(JoinType,jointype)		/* always INNER, LEFT, FULL, SEMI, or ANTI */
	NODE_SCALAR(bool,lhs_strict)		/* joinclause is strict for some LHS rel */
	NODE_SCALAR(bool,delay_upper_joins)		/* can't commute with upper RHS */
	NODE_NODE(List,join_quals)		/* join quals, in implicit-AND list format */
END_NODE(SpecialJoinInfo)
#endif /* NO_NODE_SpecialJoinInfo */

#ifndef NO_NODE_LateralJoinInfo
BEGIN_NODE(LateralJoinInfo)
	NODE_RELIDS(Relids,lateral_lhs)	/* rels needed to compute a lateral value */
	NODE_RELIDS(Relids,lateral_rhs)	/* rel where lateral value is needed */
END_NODE(LateralJoinInfo)
#endif /* NO_NODE_LateralJoinInfo */

#ifndef NO_NODE_AppendRelInfo
BEGIN_NODE(AppendRelInfo)
	NODE_SCALAR(Index,parent_relid)	/* RT index of append parent rel */
	NODE_SCALAR(Index,child_relid)	/* RT index of append child rel */
	NODE_OID(type,parent_reltype) /* OID of parent's composite type */
	NODE_OID(type,child_reltype)	/* OID of child's composite type */
	NODE_NODE(List,translated_vars)	/* Expressions in the child's Vars */
	NODE_SCALAR(Oid,parent_reloid)	/* OID of parent relation */
END_NODE(AppendRelInfo)
#endif /* NO_NODE_AppendRelInfo */

#ifndef NO_NODE_PlaceHolderInfo
BEGIN_NODE(PlaceHolderInfo)
	NODE_SCALAR(Index,phid)			/* ID for PH (unique within planner run) */
	NODE_NODE(PlaceHolderVar,ph_var)		/* copy of PlaceHolderVar tree */
	NODE_RELIDS(Relids,ph_eval_at)		/* lowest level we can evaluate value at */
	NODE_RELIDS(Relids,ph_lateral)		/* relids of contained lateral refs, if any */
	NODE_RELIDS(Relids,ph_needed)		/* highest level the value is needed at */
	NODE_SCALAR(int32,ph_width)		/* estimated attribute width */
END_NODE(PlaceHolderInfo)
#endif /* NO_NODE_PlaceHolderInfo */

#ifndef NO_NODE_MinMaxAggInfo
BEGIN_NODE(MinMaxAggInfo)
	NODE_OID(proc,aggfnoid)		/* pg_proc Oid of the aggregate */
	NODE_SCALAR(Oid,aggsortop)		/* Oid of its sort operator */
	NODE_NODE(Expr,target)			/* expression we are aggregating on */
	NODE_NODE(PlannerInfo,subroot)		/* modified "root" for planning the subquery */
	NODE_NODE(Path,path)			/* access path for subquery */
	NODE_SCALAR(Cost,pathcost)		/* estimated cost to fetch first row */
	NODE_NODE(Param,param)			/* param for subplan's output */
END_NODE(MinMaxAggInfo)
#endif /* NO_NODE_MinMaxAggInfo */

#ifndef NO_NODE_PlannerParamItem
BEGIN_NODE(PlannerParamItem)
	NODE_NODE(Node,item)			/* the Var, PlaceHolderVar, or Aggref */
	NODE_SCALAR(int,paramId)		/* its assigned PARAM_EXEC slot number */
END_NODE(PlannerParamItem)
#endif /* NO_NODE_PlannerParamItem */

/*#ifndef NO_STRUCT_SemiAntiJoinFactors
BEGIN_STRUCT(SemiAntiJoinFactors)
	NODE_SCALAR(Selectivity,outer_match_frac)
	NODE_SCALAR(Selectivity,match_count)
END_STRUCT(SemiAntiJoinFactors)
#endif*/ /* NO_STRUCT_SemiAntiJoinFactors */

/*#ifndef NO_STRUCT_JoinCostWorkspace
BEGIN_STRUCT(JoinCostWorkspace)
	NODE_SCALAR(Cost,startup_cost)
	NODE_SCALAR(Cost,total_cost)
	NODE_SCALAR(Cost,run_cost)
	NODE_SCALAR(Cost,inner_run_cost)
	NODE_SCALAR(Cost,inner_rescan_run_cost)
	NODE_SCALAR(double,outer_rows)
	NODE_SCALAR(double,inner_rows)
	NODE_SCALAR(double,outer_skip_rows)
	NODE_SCALAR(double,inner_skip_rows)
	NODE_SCALAR(int,numbuckets)
	NODE_SCALAR(int,numbatches)
END_STRUCT(JoinCostWorkspace)
#endif*/ /* NO_STRUCT_JoinCostWorkspace */

#ifndef NO_STRUCT_QualCost
BEGIN_STRUCT(QualCost)
	NODE_SCALAR(Cost,startup)		/* one-time cost */
	NODE_SCALAR(Cost,per_tuple)		/* per-evaluation cost */
END_STRUCT(QualCost)
#endif /* NO_STRUCT_QualCost */

#ifndef NO_STRUCT_ParamExternData
BEGIN_STRUCT(ParamExternData)
	NODE_DATUM(Datum,value,NODE_ARG_->ptype, NODE_ARG_->isnull)			/* parameter value */
	NODE_SCALAR(bool,isnull)			/* is it NULL? */
	NODE_SCALAR(uint16,pflags)			/* flag bits, see above */
	NODE_OID(type,ptype)			/* parameter's datatype, or 0 */
END_STRUCT(ParamExternData)
#endif /* NO_STRUCT_ParamExternData */

#ifndef NO_STRUCT_ParamListInfoData
BEGIN_STRUCT(ParamListInfoData)
	NODE_OTHER_POINT(ParamFetchHook,paramFetch)	/* parameter fetch hook */
	NODE_OTHER_POINT(void,paramFetchArg)
	NODE_OTHER_POINT(ParserSetupHook,parserSetup)	/* parser setup hook */
	NODE_OTHER_POINT(void,parserSetupArg)
	NODE_SCALAR(int,numParams)		/* number of ParamExternDatas following */
	NODE_STRUCT_ARRAY(ParamExternData,params, NODE_ARG_->numParams)	/* VARIABLE LENGTH ARRAY */
END_STRUCT(ParamListInfoData)
#endif /* NO_STRUCT_ParamListInfoData */

#ifndef NO_NODE_Query
BEGIN_NODE(Query)
	NODE_ENUM(CmdType,commandType)	
	NODE_ENUM(QuerySource,querySource)	
	NODE_SCALAR(uint32,queryId)		
	NODE_SCALAR(bool,canSetTag)		
	NODE_NODE(Node,utilityStmt)	
								
	NODE_SCALAR(int,resultRelation) 
								
	NODE_SCALAR(bool,hasAggs)		
	NODE_SCALAR(bool,hasWindowFuncs) 
	NODE_SCALAR(bool,hasSubLinks)	
	NODE_SCALAR(bool,hasDistinctOn)	
	NODE_SCALAR(bool,hasRecursive)	
	NODE_SCALAR(bool,hasModifyingCTE)
	NODE_SCALAR(bool,hasForUpdate)	
	NODE_NODE(List,cteList)		
	NODE_NODE(List,rtable)			
	NODE_NODE(FromExpr,jointree)		
	NODE_NODE(List,targetList)		
	NODE_NODE(List,returningList)	
	NODE_NODE(List,groupClause)	
	NODE_NODE(Node,havingQual)		
	NODE_NODE(List,windowClause)	
	NODE_NODE(List,distinctClause) 
	NODE_NODE(List,sortClause)		
	NODE_NODE(Node,limitOffset)	
	NODE_NODE(Node,limitCount)		
	NODE_NODE(List,rowMarks)		
	NODE_NODE(Node,setOperations)	
	NODE_NODE(List,constraintDeps) 
#ifdef PGXC
	NODE_STRING(sql_statement)
	NODE_SCALAR(bool,is_local)
	NODE_SCALAR(bool,has_to_save_cmd_id)
#endif
END_NODE(Query)
#endif /* NO_NODE_Query */

#ifndef NO_NODE_TypeName
BEGIN_NODE(TypeName)
	NODE_NODE(List,names)			/* qualified name (list of Value strings) */
	NODE_OID(type,typeOid)		/* type identified by OID */
	NODE_SCALAR(bool,setof)			/* is a set? */
	NODE_SCALAR(bool,pct_type)		/* %TYPE specified? */
	NODE_NODE(List,typmods)		/* type modifier expression(s) */
	NODE_SCALAR(int32,typemod)		/* prespecified type modifier */
	NODE_NODE(List,arrayBounds)	/* array bounds */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(TypeName)
#endif /* NO_NODE_TypeName */

#ifndef NO_NODE_ColumnRef
BEGIN_NODE(ColumnRef)
	NODE_NODE(List,fields)			/* field names (Value strings) or A_Star */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ColumnRef)
#endif /* NO_NODE_ColumnRef */

#ifdef ADB
#ifndef NO_NODE_ColumnRefJoin
BEGIN_NODE(ColumnRefJoin)
	NODE_LOCATION(int,location)	/* location for "(+)" */
	NODE_NODE(ColumnRef,column)
END_NODE(ColumnRefJoin)
#endif /* NO_NODE_ColumnRefJoin */
#endif /* ADB */

#ifndef NO_NODE_ParamRef
BEGIN_NODE(ParamRef)
	NODE_SCALAR(int,number)			/* the number of the parameter */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ParamRef)
#endif /* NO_NODE_ParamRef */
/*
 * A_Expr - infix, prefix, and postfix expressions
 */


#ifndef NO_NODE_A_Expr
BEGIN_NODE(A_Expr)
	NODE_ENUM(A_Expr_Kind,kind)			/* see above */
	NODE_NODE(List,name)			/* possibly-qualified name of operator */
	NODE_NODE(Node,lexpr)			/* left argument, or NULL if none */
	NODE_NODE(Node,rexpr)			/* right argument, or NULL if none */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(A_Expr)
#endif /* NO_NODE_A_Expr */

#ifndef NO_NODE_A_Const
BEGIN_NODE(A_Const)
	NODE_NODE_MEB(Value,val)			/* value (includes type info, see value.h) */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(A_Const)
#endif /* NO_NODE_A_Const */

#ifndef NO_NODE_TypeCast
BEGIN_NODE(TypeCast)
	NODE_NODE(Node,arg)			/* the expression being casted */
	NODE_NODE(TypeName,typeName)		/* the target type */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(TypeCast)
#endif /* NO_NODE_TypeCast */
/*
 * CollateClause - a COLLATE expression
 */
#ifndef NO_NODE_CollateClause
BEGIN_NODE(CollateClause)
	NODE_NODE(Node,arg)			/* input expression */
	NODE_NODE(List,collname)		/* possibly-qualified collation name */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(CollateClause)
#endif /* NO_NODE_CollateClause */
/*
 * FuncCall - a function or aggregate invocation
 *
 * agg_order (if not NIL) indicates we saw 'foo(... ORDER BY ...)'.
 * agg_star indicates we saw a 'foo(*)' construct, while agg_distinct
 * indicates we saw 'foo(DISTINCT ...)'.  In any of these cases, the
 * construct *must* be an aggregate call.  Otherwise, it might be either an
 * aggregate or some other kind of function.  However, if OVER is present
 * it had better be an aggregate or window function.
 */
#ifndef NO_NODE_FuncCall
BEGIN_NODE(FuncCall)
	NODE_NODE(List,funcname)		/* qualified name of function */
	NODE_NODE(List,args)			/* the arguments (list of exprs) */
	NODE_NODE(List,agg_order)		/* ORDER BY (list of SortBy) */
	NODE_SCALAR(bool,agg_star)		/* argument was really '*' */
	NODE_SCALAR(bool,agg_distinct)	/* arguments were labeled DISTINCT */
	NODE_SCALAR(bool,func_variadic)	/* last argument was labeled VARIADIC */
	NODE_NODE(WindowDef,over)		/* OVER clause, if any */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(FuncCall)
#endif /* NO_NODE_FuncCall */
/*
 * A_Star - '*' representing all columns of a table or compound field
 *
 * This can appear within ColumnRef.fields, A_Indirection.indirection, and
 * ResTarget.indirection lists.
 */
#ifndef NO_NODE_A_Star
BEGIN_NODE(A_Star)
END_NODE(A_Star)
#endif /* NO_NODE_A_Star */
/*
 * A_Indices - array subscript or slice bounds ([lidx:uidx] or [uidx])
 */
#ifndef NO_NODE_A_Indices
BEGIN_NODE(A_Indices)
	NODE_NODE(Node,lidx)			/* NULL if it's a single subscript */
	NODE_NODE(Node,uidx)
END_NODE(A_Indices)
#endif /* NO_NODE_A_Indices */
/*
 * A_Indirection - select a field and/or array element from an expression
 *
 * The indirection list can contain A_Indices nodes (representing
 * subscripting), string Value nodes (representing field selection --- the
 * string value is the name of the field to select), and A_Star nodes
 * (representing selection of all fields of a composite type).
 * For example, a complex selection operation like
 *				(foo).field1[42][7].field2
 * would be represented with a single A_Indirection node having a 4-element
 * indirection list.
 *
 * Currently, A_Star must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 */
#ifndef NO_NODE_A_Indirection
BEGIN_NODE(A_Indirection)
	NODE_NODE(Node,arg)			/* the thing being selected from */
	NODE_NODE(List,indirection)	/* subscripts and/or field names and/or * */
END_NODE(A_Indirection)
#endif /* NO_NODE_A_Indirection */
/*
 * A_ArrayExpr - an ARRAY[] construct
 */
#ifndef NO_NODE_A_ArrayExpr
BEGIN_NODE(A_ArrayExpr)
	NODE_NODE(List,elements)		/* array element expressions */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(A_ArrayExpr)
#endif /* NO_NODE_A_ArrayExpr */
/*
 * ResTarget -
 *	  result target (used in target list of pre-transformed parse trees)
 *
 * In a SELECT target list, 'name' is the column label from an
 * 'AS ColumnLabel' clause, or NULL if there was none, and 'val' is the
 * value expression itself.  The 'indirection' field is not used.
 *
 * INSERT uses ResTarget in its target-column-names list.  Here, 'name' is
 * the name of the destination column, 'indirection' stores any subscripts
 * attached to the destination, and 'val' is not used.
 *
 * In an UPDATE target list, 'name' is the name of the destination column,
 * 'indirection' stores any subscripts attached to the destination, and
 * 'val' is the expression to assign.
 *
 * See A_Indirection for more info about what can appear in 'indirection'.
 */
#ifndef NO_NODE_ResTarget
BEGIN_NODE(ResTarget)
	NODE_STRING(name)			/* column name or NULL */
	NODE_NODE(List,indirection)	/* subscripts, field names, and '*', or NIL */
	NODE_NODE(Node,val)			/* the value expression to compute or assign */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(ResTarget)
#endif /* NO_NODE_ResTarget */
/*
 * SortBy - for ORDER BY clause
 */
#ifndef NO_NODE_SortBy
BEGIN_NODE(SortBy)
	NODE_NODE(Node,node)			/* expression to sort on */
	NODE_ENUM(SortByDir,sortby_dir)		/* ASC/DESC/USING/default */
	NODE_ENUM(SortByNulls,sortby_nulls)	/* NULLS FIRST/LAST */
	NODE_NODE(List,useOp)			/* name of op to use, if SORTBY_USING */
	NODE_LOCATION(int,location)		/* operator location, or -1 if none/unknown */
END_NODE(SortBy)
#endif /* NO_NODE_SortBy */
/*
 * WindowDef - raw representation of WINDOW and OVER clauses
 *
 * For entries in a WINDOW list, "name" is the window name being defined.
 * For OVER clauses, we use "name" for the "OVER window" syntax, or "refname"
 * for the "OVER (window)" syntax, which is subtly different --- the latter
 * implies overriding the window frame clause.
 */
#ifndef NO_NODE_WindowDef
BEGIN_NODE(WindowDef)
	NODE_STRING(name)			/* window's own name */
	NODE_STRING(refname)		/* referenced window name, if any */
	NODE_NODE(List,partitionClause)	/* PARTITION BY expression list */
	NODE_NODE(List,orderClause)	/* ORDER BY (list of SortBy) */
	NODE_SCALAR(int,frameOptions)	/* frame_clause options, see below */
	NODE_NODE(Node,startOffset)	/* expression for starting bound, if any */
	NODE_NODE(Node,endOffset)		/* expression for ending bound, if any */
	NODE_LOCATION(int,location)		/* parse location, or -1 if none/unknown */
END_NODE(WindowDef)
#endif /* NO_NODE_WindowDef */

/*
 * RangeSubselect - subquery appearing in a FROM clause
 */
#ifndef NO_NODE_RangeSubselect
BEGIN_NODE(RangeSubselect)
	NODE_SCALAR(bool,lateral)		/* does it have LATERAL prefix? */
	NODE_NODE(Node,subquery)		/* the untransformed sub-select clause */
	NODE_NODE(Alias,alias)			/* table alias & optional column aliases */
END_NODE(RangeSubselect)
#endif /* NO_NODE_RangeSubselect */
/*
 * RangeFunction - function call appearing in a FROM clause
 */
#ifndef NO_NODE_RangeFunction
BEGIN_NODE(RangeFunction)
	NODE_SCALAR(bool,lateral)		/* does it have LATERAL prefix? */
	NODE_NODE(Node,funccallnode)	/* untransformed function call tree */
	NODE_NODE(Alias,alias)			/* table alias & optional column aliases */
	NODE_NODE(List,coldeflist)		/* list of ColumnDef nodes to describe result
								 * of function returning RECORD */
END_NODE(RangeFunction)
#endif /* NO_NODE_RangeFunction */
/*
 * ColumnDef - column definition (used in various creates)
 *
 * If the column has a default value, we may have the value expression
 * in either "raw" form (an untransformed parse tree) or "cooked" form
 * (a post-parse-analysis, executable expression tree), depending on
 * how this ColumnDef node was created (by parsing, or by inheritance
 * from an existing relation).  We should never have both in the same node!
 *
 * Similarly, we may have a COLLATE specification in either raw form
 * (represented as a CollateClause with arg==NULL) or cooked form
 * (the collation's OID).
 *
 * The constraints list may contain a CONSTR_DEFAULT item in a raw
 * parsetree produced by gram.y, but transformCreateStmt will remove
 * the item and set raw_default instead.  CONSTR_DEFAULT items
 * should not appear in any subsequent processing.
 */
#ifndef NO_NODE_ColumnDef
BEGIN_NODE(ColumnDef)
	NODE_STRING(colname)		/* name of column */
	NODE_NODE(TypeName,typeName)		/* type of column */
	NODE_SCALAR(int,inhcount)		/* number of times column is inherited */
	NODE_SCALAR(bool,is_local)		/* column has local (non-inherited) def'n */
	NODE_SCALAR(bool,is_not_null)	/* NOT NULL constraint specified? */
	NODE_SCALAR(bool,is_from_type)	/* column definition came from table type */
	NODE_SCALAR(char,storage)		/* attstorage setting, or 0 for default */
	NODE_NODE(Node,raw_default)	/* default value (untransformed parse tree) */
	NODE_NODE(Node,cooked_default) /* default value (transformed expr tree) */
	NODE_NODE(CollateClause,collClause)	/* untransformed COLLATE spec, if any */
	NODE_OID(collation,collOid)		/* collation OID (InvalidOid if not set) */
	NODE_NODE(List,constraints)	/* other constraints on column */
	NODE_NODE(List,fdwoptions)		/* per-column FDW options */
END_NODE(ColumnDef)
#endif /* NO_NODE_ColumnDef */
/*
 * TableLikeClause - CREATE TABLE ( ... LIKE ... ) clause
 */
#ifndef NO_NODE_TableLikeClause
BEGIN_NODE(TableLikeClause)
	NODE_NODE(RangeVar,relation)
	NODE_SCALAR(bits32,options)		/* OR of TableLikeOption flags */
END_NODE(TableLikeClause)
#endif /* NO_NODE_TableLikeClause */

/*
 * IndexElem - index parameters (used in CREATE INDEX)
 *
 * For a plain index attribute, 'name' is the name of the table column to
 * index, and 'expr' is NULL.  For an index expression, 'name' is NULL and
 * 'expr' is the expression tree.
 */
#ifndef NO_NODE_IndexElem
BEGIN_NODE(IndexElem)
	NODE_STRING(name)			/* name of attribute to index, or NULL */
	NODE_NODE(Node,expr)			/* expression to index, or NULL */
	NODE_STRING(indexcolname)	/* name for index column; NULL = default */
	NODE_NODE(List,collation)		/* name of collation; NIL = default */
	NODE_NODE(List,opclass)		/* name of desired opclass; NIL = default */
	NODE_ENUM(SortByDir,ordering)		/* ASC/DESC/default */
	NODE_ENUM(SortByNulls,nulls_ordering) /* FIRST/LAST/default */
END_NODE(IndexElem)
#endif /* NO_NODE_IndexElem */
/*
 * DefElem - a generic "name = value" option definition
 *
 * In some contexts the name can be qualified.  Also, certain SQL commands
 * allow a SET/ADD/DROP action to be attached to option settings, so it's
 * convenient to carry a field for that too.  (Note: currently, it is our
 * practice that the grammar allows namespace and action only in statements
 * where they are relevant; C code can just ignore those fields in other
 * statements.)
 */

#ifndef NO_NODE_DefElem
BEGIN_NODE(DefElem)
	NODE_STRING(defnamespace)	/* NULL if unqualified name */
	NODE_STRING(defname)
	NODE_NODE(Node,arg)			/* a (Value *) or a (TypeName *) */
	NODE_ENUM(DefElemAction,defaction)	/* unspecified action, or SET/ADD/DROP */
END_NODE(DefElem)
#endif /* NO_NODE_DefElem */
/*
 * LockingClause - raw representation of FOR [NO KEY] UPDATE/[KEY] SHARE
 *		options
 *
 * Note: lockedRels == NIL means "all relations in query".  Otherwise it
 * is a list of RangeVar nodes.  (We use RangeVar mainly because it carries
 * a location field --- currently, parse analysis insists on unqualified
 * names in LockingClause.)
 */


#ifndef NO_NODE_LockingClause
BEGIN_NODE(LockingClause)
	NODE_NODE(List,lockedRels)		/* FOR [KEY] UPDATE/SHARE relations */
	NODE_ENUM(LockClauseStrength,strength)
	NODE_SCALAR(bool,noWait)			/* NOWAIT option */
END_NODE(LockingClause)
#endif /* NO_NODE_LockingClause */
/*
 * XMLSERIALIZE (in raw parse tree only)
 */
#ifndef NO_NODE_XmlSerialize
BEGIN_NODE(XmlSerialize)
	NODE_ENUM(XmlOptionType,xmloption)	/* DOCUMENT or CONTENT */
	NODE_NODE(Node,expr)
	NODE_NODE(TypeName,typeName)
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(XmlSerialize)
#endif /* NO_NODE_XmlSerialize */

/****************************************************************************
 *	Nodes for a Query tree
 ****************************************************************************/

/*--------------------
 * RangeTblEntry -
 *	  A range table is a List of RangeTblEntry nodes.
 *
 *	  A range table entry may represent a plain relation, a sub-select in
 *	  FROM, or the result of a JOIN clause.  (Only explicit JOIN syntax
 *	  produces an RTE, not the implicit join resulting from multiple FROM
 *	  items.  This is because we only need the RTE to deal with SQL features
 *	  like outer joins and join-output-column aliasing.)  Other special
 *	  RTE types also exist, as indicated by RTEKind.
 *
 *	  Note that we consider RTE_RELATION to cover anything that has a pg_class
 *	  entry.  relkind distinguishes the sub-cases.
 *
 *	  alias is an Alias node representing the AS alias-clause attached to the
 *	  FROM expression, or NULL if no clause.
 *
 *	  eref is the table reference name and column reference names (either
 *	  real or aliases).  Note that system columns (OID etc) are not included
 *	  in the column list.
 *	  eref->aliasname is required to be present, and should generally be used
 *	  to identify the RTE for error messages etc.
 *
 *	  In RELATION RTEs, the colnames in both alias and eref are indexed by
 *	  physical attribute number; this means there must be colname entries for
 *	  dropped columns.  When building an RTE we insert empty strings ("") for
 *	  dropped columns.  Note however that a stored rule may have nonempty
 *	  colnames for columns dropped since the rule was created (and for that
 *	  matter the colnames might be out of date due to column renamings).
 *	  The same comments apply to FUNCTION RTEs when the function's return type
 *	  is a named composite type.
 *
 *	  In JOIN RTEs, the colnames in both alias and eref are one-to-one with
 *	  joinaliasvars entries.  A JOIN RTE will omit columns of its inputs when
 *	  those columns are known to be dropped at parse time.  Again, however,
 *	  a stored rule might contain entries for columns dropped since the rule
 *	  was created.  (This is only possible for columns not actually referenced
 *	  in the rule.)  When loading a stored rule, we replace the joinaliasvars
 *	  items for any such columns with null pointers.  (We can't simply delete
 *	  them from the joinaliasvars list, because that would affect the attnums
 *	  of Vars referencing the rest of the list.)
 *
 *	  inh is TRUE for relation references that should be expanded to include
 *	  inheritance children, if the rel has any.  This *must* be FALSE for
 *	  RTEs other than RTE_RELATION entries.
 *
 *	  inFromCl marks those range variables that are listed in the FROM clause.
 *	  It's false for RTEs that are added to a query behind the scenes, such
 *	  as the NEW and OLD variables for a rule, or the subqueries of a UNION.
 *	  This flag is not used anymore during parsing, since the parser now uses
 *	  a separate "namespace" data structure to control visibility, but it is
 *	  needed by ruleutils.c to determine whether RTEs should be shown in
 *	  decompiled queries.
 *
 *	  requiredPerms and checkAsUser specify run-time access permissions
 *	  checks to be performed at query startup.  The user must have *all*
 *	  of the permissions that are OR'd together in requiredPerms (zero
 *	  indicates no permissions checking).  If checkAsUser is not zero,
 *	  then do the permissions checks using the access rights of that user,
 *	  not the current effective user ID.  (This allows rules to act as
 *	  setuid gateways.)  Permissions checks only apply to RELATION RTEs.
 *
 *	  For SELECT/INSERT/UPDATE permissions, if the user doesn't have
 *	  table-wide permissions then it is sufficient to have the permissions
 *	  on all columns identified in selectedCols (for SELECT) and/or
 *	  modifiedCols (for INSERT/UPDATE; we can tell which from the query type).
 *	  selectedCols and modifiedCols are bitmapsets, which cannot have negative
 *	  integer members, so we subtract FirstLowInvalidHeapAttributeNumber from
 *	  column numbers before storing them in these fields.  A whole-row Var
 *	  reference is represented by setting the bit for InvalidAttrNumber.
 *--------------------
 */

#ifndef NO_NODE_RangeTblEntry
BEGIN_NODE(RangeTblEntry)
	NODE_ENUM(RTEKind,rtekind)		/* see above */
#ifdef PGXC
	NODE_STRING(relname)
#endif
	NODE_SCALAR(Oid,relid)			/* OID of the relation */
	NODE_SCALAR(char,relkind)		/* relation kind (see pg_class.relkind) */
	NODE_NODE(Query,subquery);		/* the sub-query */
	NODE_SCALAR(bool,security_barrier)		/* is from security_barrier view? */

	/*
	 * Fields valid for a join RTE (else NULL/zero):
	 *
	 * joinaliasvars is a list of (usually) Vars corresponding to the columns
	 * of the join result.  An alias Var referencing column K of the join
	 * result can be replaced by the K'th element of joinaliasvars --- but to
	 * simplify the task of reverse-listing aliases correctly, we do not do
	 * that until planning time.  In detail: an element of joinaliasvars can
	 * be a Var of one of the join's input relations, or such a Var with an
	 * implicit coercion to the join's output column type, or a COALESCE
	 * expression containing the two input column Vars (possibly coerced).
	 * Within a Query loaded from a stored rule, it is also possible for
	 * joinaliasvars items to be null pointers, which are placeholders for
	 * (necessarily unreferenced) columns dropped since the rule was made.
	 * Also, once planning begins, joinaliasvars items can be almost anything,
	 * as a result of subquery-flattening substitutions.
	 */
	NODE_ENUM(JoinType,jointype)		/* type of join */
	NODE_NODE(List,joinaliasvars)	/* list of alias-var expansions */

	/*
	 * Fields valid for a function RTE (else NULL):
	 *
	 * If the function returns RECORD, funccoltypes lists the column types
	 * declared in the RTE's column type specification, funccoltypmods lists
	 * their declared typmods, funccolcollations their collations.  Otherwise,
	 * those fields are NIL.
	 */
	NODE_NODE(Node,funcexpr)		/* expression tree for func call */
	NODE_NODE(List,funccoltypes)	/* OID list of column type OIDs */
	NODE_NODE(List,funccoltypmods) /* integer list of column typmods */
	NODE_NODE(List,funccolcollations)		/* OID list of column collation OIDs */

	/*
	 * Fields valid for a values RTE (else NIL):
	 */
	NODE_NODE(List,values_lists)	/* list of expression lists */
	NODE_NODE(List,values_collations)		/* OID list of column collation OIDs */

	/*
	 * Fields valid for a CTE RTE (else NULL/zero):
	 */
	NODE_STRING(ctename)		/* name of the WITH list item */
	NODE_SCALAR(Index,ctelevelsup)	/* number of query levels up */
	NODE_SCALAR(bool,self_reference) /* is this a recursive self-reference? */
	NODE_NODE(List,ctecoltypes)	/* OID list of column type OIDs */
	NODE_NODE(List,ctecoltypmods)	/* integer list of column typmods */
	NODE_NODE(List,ctecolcollations)		/* OID list of column collation OIDs */

	/*
	 * Fields valid in all RTEs:
	 */
	NODE_NODE(Alias,alias)			/* user-written alias clause, if any */
	NODE_NODE(Alias,eref)			/* expanded reference names */
	NODE_SCALAR(bool,lateral)		/* subquery, function, or values is LATERAL? */
	NODE_SCALAR(bool,inh)			/* inheritance requested? */
	NODE_SCALAR(bool,inFromCl)		/* present in FROM clause? */
	NODE_SCALAR(AclMode,requiredPerms)	/* bitmask of required access permissions */
	NODE_SCALAR(Oid,checkAsUser)	/* if valid, check access as this role */
	NODE_BITMAPSET(Bitmapset,selectedCols)	/* columns needing SELECT permission */
	NODE_BITMAPSET(Bitmapset,modifiedCols)	/* columns needing INSERT/UPDATE permission */
END_NODE(RangeTblEntry)
#endif /* NO_NODE_RangeTblEntry */
/*
 * SortGroupClause -
 *		representation of ORDER BY, GROUP BY, PARTITION BY,
 *		DISTINCT, DISTINCT ON items
 *
 * You might think that ORDER BY is only interested in defining ordering,
 * and GROUP/DISTINCT are only interested in defining equality.  However,
 * one way to implement grouping is to sort and then apply a "uniq"-like
 * filter.  So it's also interesting to keep track of possible sort operators
 * for GROUP/DISTINCT, and in particular to try to sort for the grouping
 * in a way that will also yield a requested ORDER BY ordering.  So we need
 * to be able to compare ORDER BY and GROUP/DISTINCT lists, which motivates
 * the decision to give them the same representation.
 *
 * tleSortGroupRef must match ressortgroupref of exactly one entry of the
 *		query's targetlist; that is the expression to be sorted or grouped by.
 * eqop is the OID of the equality operator.
 * sortop is the OID of the ordering operator (a "<" or ">" operator),
 *		or InvalidOid if not available.
 * nulls_first means about what you'd expect.  If sortop is InvalidOid
 *		then nulls_first is meaningless and should be set to false.
 * hashable is TRUE if eqop is hashable (note this condition also depends
 *		on the datatype of the input expression).
 *
 * In an ORDER BY item, all fields must be valid.  (The eqop isn't essential
 * here, but it's cheap to get it along with the sortop, and requiring it
 * to be valid eases comparisons to grouping items.)  Note that this isn't
 * actually enough information to determine an ordering: if the sortop is
 * collation-sensitive, a collation OID is needed too.  We don't store the
 * collation in SortGroupClause because it's not available at the time the
 * parser builds the SortGroupClause; instead, consult the exposed collation
 * of the referenced targetlist expression to find out what it is.
 *
 * In a grouping item, eqop must be valid.  If the eqop is a btree equality
 * operator, then sortop should be set to a compatible ordering operator.
 * We prefer to set eqop/sortop/nulls_first to match any ORDER BY item that
 * the query presents for the same tlist item.  If there is none, we just
 * use the default ordering op for the datatype.
 *
 * If the tlist item's type has a hash opclass but no btree opclass, then
 * we will set eqop to the hash equality operator, sortop to InvalidOid,
 * and nulls_first to false.  A grouping item of this kind can only be
 * implemented by hashing, and of course it'll never match an ORDER BY item.
 *
 * The hashable flag is provided since we generally have the requisite
 * information readily available when the SortGroupClause is constructed,
 * and it's relatively expensive to get it again later.  Note there is no
 * need for a "sortable" flag since OidIsValid(sortop) serves the purpose.
 *
 * A query might have both ORDER BY and DISTINCT (or DISTINCT ON) clauses.
 * In SELECT DISTINCT, the distinctClause list is as long or longer than the
 * sortClause list, while in SELECT DISTINCT ON it's typically shorter.
 * The two lists must match up to the end of the shorter one --- the parser
 * rearranges the distinctClause if necessary to make this true.  (This
 * restriction ensures that only one sort step is needed to both satisfy the
 * ORDER BY and set up for the Unique step.  This is semantically necessary
 * for DISTINCT ON, and presents no real drawback for DISTINCT.)
 */
#ifndef NO_NODE_SortGroupClause
BEGIN_NODE(SortGroupClause)
	NODE_SCALAR(Index,tleSortGroupRef)	/* reference into targetlist */
	NODE_OID(operator,eqop)			/* the equality operator ('=' op) */
	NODE_OID(operator,sortop)			/* the ordering operator ('<' op), or 0 */
	NODE_SCALAR(bool,nulls_first)	/* do NULLs come before normal values? */
	NODE_SCALAR(bool,hashable)		/* can eqop be implemented by hashing? */
END_NODE(SortGroupClause)
#endif /* NO_NODE_SortGroupClause */
/*
 * WindowClause -
 *		transformed representation of WINDOW and OVER clauses
 *
 * A parsed Query's windowClause list contains these structs.  "name" is set
 * if the clause originally came from WINDOW, and is NULL if it originally
 * was an OVER clause (but note that we collapse out duplicate OVERs).
 * partitionClause and orderClause are lists of SortGroupClause structs.
 * winref is an ID number referenced by WindowFunc nodes; it must be unique
 * among the members of a Query's windowClause list.
 * When refname isn't null, the partitionClause is always copied from there;
 * the orderClause might or might not be copied (see copiedOrder); the framing
 * options are never copied, per spec.
 */
#ifndef NO_NODE_WindowClause
BEGIN_NODE(WindowClause)
	NODE_STRING(name)			/* window name (NULL in an OVER clause) */
	NODE_STRING(refname)		/* referenced window name, if any */
	NODE_NODE(List,partitionClause)	/* PARTITION BY list */
	NODE_NODE(List,orderClause)	/* ORDER BY list */
	NODE_SCALAR(int,frameOptions)	/* frame_clause options, see WindowDef */
	NODE_NODE(Node,startOffset)	/* expression for starting bound, if any */
	NODE_NODE(Node,endOffset)		/* expression for ending bound, if any */
	NODE_SCALAR(Index,winref)			/* ID referenced by window functions */
	NODE_SCALAR(bool,copiedOrder)	/* did we copy orderClause from refname? */
END_NODE(WindowClause)
#endif /* NO_NODE_WindowClause */
/*
 * RowMarkClause -
 *	   parser output representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * Query.rowMarks contains a separate RowMarkClause node for each relation
 * identified as a FOR [KEY] UPDATE/SHARE target.  If one of these clauses
 * is applied to a subquery, we generate RowMarkClauses for all normal and
 * subquery rels in the subquery, but they are marked pushedDown = true to
 * distinguish them from clauses that were explicitly written at this query
 * level.  Also, Query.hasForUpdate tells whether there were explicit FOR
 * UPDATE/SHARE/KEY SHARE clauses in the current query level.
 */
#ifndef NO_NODE_RowMarkClause
BEGIN_NODE(RowMarkClause)
	NODE_SCALAR(Index,rti)			/* range table index of target relation */
	NODE_ENUM(LockClauseStrength,strength)
	NODE_SCALAR(bool,noWait)			/* NOWAIT option */
	NODE_SCALAR(bool,pushedDown)		/* pushed down from higher query level? */
END_NODE(RowMarkClause)
#endif /* NO_NODE_RowMarkClause */
/*
 * WithClause -
 *	   representation of WITH clause
 *
 * Note: WithClause does not propagate into the Query representation;
 * but CommonTableExpr does.
 */
#ifndef NO_NODE_WithClause
BEGIN_NODE(WithClause)
	NODE_NODE(List,ctes)			/* list of CommonTableExprs */
	NODE_SCALAR(bool,recursive)		/* true = WITH RECURSIVE */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(WithClause)
#endif /* NO_NODE_WithClause */
/*
 * CommonTableExpr -
 *	   representation of WITH list element
 *
 * We don't currently support the SEARCH or CYCLE clause.
 */
#ifndef NO_NODE_CommonTableExpr
BEGIN_NODE(CommonTableExpr)
	NODE_STRING(ctename)		/* query name (never qualified) */
	NODE_NODE(List,aliascolnames)	/* optional list of column names */
	/* SelectStmt/InsertStmt/etc before parse analysis, Query afterwards: */
	NODE_NODE(Node,ctequery)		/* the CTE's subquery */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
	/* These fields are set during parse analysis: */
	NODE_SCALAR(bool,cterecursive)	/* is this CTE actually recursive? */
	NODE_SCALAR(int,cterefcount)	/* number of RTEs referencing this CTE
								 * (excluding internal self-references) */
	NODE_NODE(List,ctecolnames)	/* list of output column names */
	NODE_NODE(List,ctecoltypes)	/* OID list of output column type OIDs */
	NODE_NODE(List,ctecoltypmods)	/* integer list of output column typmods */
	NODE_NODE(List,ctecolcollations)		/* OID list of column collation OIDs */
END_NODE(CommonTableExpr)
#endif /* NO_NODE_CommonTableExpr */
/*
 * oracle rownum expr
 */
#ifndef NO_NODE_RownumExpr
BEGIN_NODE(RownumExpr)
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */
END_NODE(RownumExpr)
#endif /* NO_NODE_RownumExpr */

/*****************************************************************************
 *		Optimizable Statements
 *****************************************************************************/

/* ----------------------
 *		Insert Statement
 *
 * The source expression is represented by SelectStmt for both the
 * SELECT and VALUES cases.  If selectStmt is NULL, then the query
 * is INSERT ... DEFAULT VALUES.
 * ----------------------
 */
#ifndef NO_NODE_InsertStmt
BEGIN_NODE(InsertStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,relation)		/* relation to insert into */
	NODE_NODE(List,cols)			/* optional: names of the target columns */
	NODE_NODE(Node,selectStmt)		/* the source SELECT/VALUES, or NULL */
	NODE_NODE(List,returningList)	/* list of expressions to return */
	NODE_NODE(WithClause,withClause)		/* WITH clause */
END_NODE(InsertStmt)
#endif /* NO_NODE_InsertStmt */
/* ----------------------
 *		Delete Statement
 * ----------------------
 */
#ifndef NO_NODE_DeleteStmt
BEGIN_NODE(DeleteStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,relation)		/* relation to delete from */
	NODE_NODE(List,usingClause)	/* optional using clause for more tables */
	NODE_NODE(Node,whereClause)	/* qualifications */
	NODE_NODE(List,returningList)	/* list of expressions to return */
	NODE_NODE(WithClause,withClause)		/* WITH clause */
END_NODE(DeleteStmt)
#endif /* NO_NODE_DeleteStmt */
/* ----------------------
 *		Update Statement
 * ----------------------
 */
#ifndef NO_NODE_UpdateStmt
BEGIN_NODE(UpdateStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,relation)		/* relation to update */
	NODE_NODE(List,targetList)		/* the target list (of ResTarget) */
	NODE_NODE(Node,whereClause)	/* qualifications */
	NODE_NODE(List,fromClause)		/* optional from clause for more tables */
	NODE_NODE(List,returningList)	/* list of expressions to return */
	NODE_NODE(WithClause,withClause)		/* WITH clause */
END_NODE(UpdateStmt)
#endif /* NO_NODE_UpdateStmt */
/* ----------------------
 *		Select Statement
 *
 * A "simple" SELECT is represented in the output of gram.y by a single
 * SelectStmt node; so is a VALUES construct.  A query containing set
 * operators (UNION, INTERSECT, EXCEPT) is represented by a tree of SelectStmt
 * nodes, in which the leaf nodes are component SELECTs and the internal nodes
 * represent UNION, INTERSECT, or EXCEPT operators.  Using the same node
 * type for both leaf and internal nodes allows gram.y to stick ORDER BY,
 * LIMIT, etc, clause values into a SELECT statement without worrying
 * whether it is a simple or compound SELECT.
 * ----------------------
 */

#ifndef NO_NODE_SelectStmt
BEGIN_NODE(SelectStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	/*
	 * These fields are used only in "leaf" SelectStmts.
	 */
	NODE_NODE(List,distinctClause) /* NULL, list of DISTINCT ON exprs, or
								 * lcons(NIL,NIL) for all (SELECT DISTINCT) */
	NODE_NODE(IntoClause,intoClause)		/* target for SELECT INTO */
	NODE_NODE(List,targetList)		/* the target list (of ResTarget) */
	NODE_NODE(List,fromClause)		/* the FROM clause */
	NODE_NODE(Node,whereClause)	/* WHERE qualification */
	NODE_NODE(List,groupClause)	/* GROUP BY clauses */
	NODE_NODE(Node,havingClause)	/* HAVING conditional-expression */
	NODE_NODE(List,windowClause)	/* WINDOW window_name AS (...), ... */

	/*
	 * In a "leaf" node representing a VALUES list, the above fields are all
	 * null, and instead this field is set.  Note that the elements of the
	 * sublists are just expressions, without ResTarget decoration. Also note
	 * that a list element can be DEFAULT (represented as a SetToDefault
	 * node), regardless of the context of the VALUES list. It's up to parse
	 * analysis to reject that where not valid.
	 */
	NODE_NODE(List,valuesLists)	/* untransformed list of expression lists */

	/*
	 * These fields are used in both "leaf" SelectStmts and upper-level
	 * SelectStmts.
	 */
	NODE_NODE(List,sortClause)		/* sort clause (a list of SortBy's) */
	NODE_NODE(Node,limitOffset)	/* # of result tuples to skip */
	NODE_NODE(Node,limitCount)		/* # of result tuples to return */
	NODE_NODE(List,lockingClause)	/* FOR UPDATE (list of LockingClause's) */
	NODE_NODE(WithClause,withClause)		/* WITH clause */

	/*
	 * These fields are used only in upper-level SelectStmts.
	 */
	NODE_ENUM(SetOperation,op)			/* type of set op */
	NODE_SCALAR(bool,all)			/* ALL specified? */
	NODE_NODE(SelectStmt,larg)	/* left child */
	NODE_NODE(SelectStmt,rarg)	/* right child */
	/* Eventually add fields for CORRESPONDING spec here */
END_NODE(SelectStmt)
#endif /* NO_NODE_SelectStmt */

/* ----------------------
 *		Set Operation node for post-analysis query trees
 *
 * After parse analysis, a SELECT with set operations is represented by a
 * top-level Query node containing the leaf SELECTs as subqueries in its
 * range table.  Its setOperations field shows the tree of set operations,
 * with leaf SelectStmt nodes replaced by RangeTblRef nodes, and internal
 * nodes replaced by SetOperationStmt nodes.  Information about the output
 * column types is added, too.  (Note that the child nodes do not necessarily
 * produce these types directly, but we've checked that their output types
 * can be coerced to the output column type.)  Also, if it's not UNION ALL,
 * information about the types' sort/group semantics is provided in the form
 * of a SortGroupClause list (same representation as, eg, DISTINCT).
 * The resolved common column collations are provided too; but note that if
 * it's not UNION ALL, it's okay for a column to not have a common collation,
 * so a member of the colCollations list could be InvalidOid even though the
 * column has a collatable type.
 * ----------------------
 */
#ifndef NO_NODE_SetOperationStmt
BEGIN_NODE(SetOperationStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(SetOperation,op)			/* type of set op */
	NODE_SCALAR(bool,all)			/* ALL specified? */
	NODE_NODE(Node,larg)			/* left child */
	NODE_NODE(Node,rarg)			/* right child */
	/* Eventually add fields for CORRESPONDING spec here */

	/* Fields derived during parse analysis: */
	NODE_NODE(List,colTypes)		/* OID list of output column type OIDs */
	NODE_NODE(List,colTypmods)		/* integer list of output column typmods */
	NODE_NODE(List,colCollations)	/* OID list of output column collation OIDs */
	NODE_NODE(List,groupClauses)	/* a list of SortGroupClause's */
	/* groupClauses is NIL if UNION ALL, but must be set otherwise */
END_NODE(SetOperationStmt)
#endif /* NO_NODE_SetOperationStmt */

/*****************************************************************************
 *		Other Statements (no optimizations required)
 *
 *		These are not touched by parser/analyze.c except to put them into
 *		the utilityStmt field of a Query.  This is eventually passed to
 *		ProcessUtility (by-passing rewriting and planning).  Some of the
 *		statements do need attention from parse analysis, and this is
 *		done by routines in parser/parse_utilcmd.c after ProcessUtility
 *		receives the command for execution.
 *****************************************************************************/

/*
 * When a command can act on several kinds of objects with only one
 * parse structure required, use these constants to designate the
 * object type.  Note that commands typically don't support all the types.
 */


/* ----------------------
 *		Create Schema Statement
 *
 * NOTE: the schemaElts list contains raw parsetrees for component statements
 * of the schema, such as CREATE TABLE, GRANT, etc.  These are analyzed and
 * executed after the schema itself is created.
 * ----------------------
 */
#ifndef NO_NODE_CreateSchemaStmt
BEGIN_NODE(CreateSchemaStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(schemaname)		/* the name of the schema to create */
	NODE_STRING(authid)			/* the owner of the created schema */
	NODE_NODE(List,schemaElts)		/* schema components (list of parsenodes) */
	NODE_SCALAR(bool,if_not_exists)	/* just do nothing if schema already exists? */
END_NODE(CreateSchemaStmt)
#endif /* NO_NODE_CreateSchemaStmt */

/* ----------------------
 *	Alter Table
 * ----------------------
 */
#ifndef NO_NODE_AlterTableStmt
BEGIN_NODE(AlterTableStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
	NODE_ENUM(ParseGrammar, grammar)
#endif /* ADB */
	NODE_NODE(RangeVar,relation)		/* table to work on */
	NODE_NODE(List,cmds)			/* list of subcommands */
	NODE_ENUM(ObjectType,relkind)		/* type of object */
	NODE_SCALAR(bool,missing_ok)		/* skip error if table missing */
END_NODE(AlterTableStmt)
#endif /* NO_NODE_AlterTableStmt */

#ifndef NO_NODE_AlterTableCmd
BEGIN_NODE(AlterTableCmd)	/* one subcommand of an ALTER TABLE */
	NODE_ENUM(AlterTableType,subtype)		/* Type of table alteration to apply */
	NODE_STRING(name)			/* column, constraint, or trigger to act on,
								 * or new owner or tablespace */
	NODE_NODE(Node,def)			/* definition of new column, index,
								 * constraint, or parent table */
	NODE_ENUM(DropBehavior,behavior)		/* RESTRICT or CASCADE for DROP cases */
	NODE_SCALAR(bool,missing_ok)		/* skip error if missing? */
END_NODE(AlterTableCmd)
#endif /* NO_NODE_AlterTableCmd */

/* ----------------------
 *	Alter Domain
 *
 * The fields are used in different ways by the different variants of
 * this command.
 * ----------------------
 */
#ifndef NO_NODE_AlterDomainStmt
BEGIN_NODE(AlterDomainStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_SCALAR(char,subtype)		/*------------
								 *	T = alter column default
								 *	N = alter column drop not null
								 *	O = alter column set not null
								 *	C = add constraint
								 *	X = drop constraint
								 *------------
								 */
	NODE_NODE(List,typeName)		/* domain to work on */
	NODE_STRING(name)			/* column or constraint name to act on */
	NODE_NODE(Node,def)			/* definition of default or constraint */
	NODE_ENUM(DropBehavior,behavior)		/* RESTRICT or CASCADE for DROP cases */
	NODE_SCALAR(bool,missing_ok)		/* skip error if missing? */
END_NODE(AlterDomainStmt)
#endif /* NO_NODE_AlterDomainStmt */

/* ----------------------
 *		Grant|Revoke Statement
 * ----------------------
 */


#ifndef NO_NODE_GrantStmt
BEGIN_NODE(GrantStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_SCALAR(bool,is_grant)		/* true = GRANT, false = REVOKE */
	NODE_ENUM(GrantTargetType,targtype)	/* type of the grant target */
	NODE_ENUM(GrantObjectType,objtype)	/* kind of object being operated on */
	NODE_NODE(List,objects)		/* list of RangeVar nodes, FuncWithArgs nodes,
								 * or plain names (as Value strings) */
	NODE_NODE(List,privileges)		/* list of AccessPriv nodes */
	/* privileges == NIL denotes ALL PRIVILEGES */
	NODE_NODE(List,grantees)		/* list of PrivGrantee nodes */
	NODE_SCALAR(bool,grant_option)	/* grant or revoke grant option */
	NODE_ENUM(DropBehavior,behavior)		/* drop behavior (for REVOKE) */
END_NODE(GrantStmt)
#endif /* NO_NODE_GrantStmt */
#ifndef NO_NODE_PrivGrantee
BEGIN_NODE(PrivGrantee)
	NODE_STRING(rolname)		/* if NULL then PUBLIC */
END_NODE(PrivGrantee)
#endif /* NO_NODE_PrivGrantee */
/*
 * Note: FuncWithArgs carries only the types of the input parameters of the
 * function.  So it is sufficient to identify an existing function, but it
 * is not enough info to define a function nor to call it.
 */
#ifndef NO_NODE_FuncWithArgs
BEGIN_NODE(FuncWithArgs)
	NODE_NODE(List,funcname)		/* qualified name of function */
	NODE_NODE(List,funcargs)		/* list of Typename nodes */
END_NODE(FuncWithArgs)
#endif /* NO_NODE_FuncWithArgs */
/*
 * An access privilege, with optional list of column names
 * priv_name == NULL denotes ALL PRIVILEGES (only used with a column list)
 * cols == NIL denotes "all columns"
 * Note that simple "ALL PRIVILEGES" is represented as a NIL list, not
 * an AccessPriv with both fields null.
 */
#ifndef NO_NODE_AccessPriv
BEGIN_NODE(AccessPriv)
	NODE_STRING(priv_name)		/* string name of privilege */
	NODE_NODE(List,cols)			/* list of Value strings */
END_NODE(AccessPriv)
#endif /* NO_NODE_AccessPriv */
/* ----------------------
 *		Grant/Revoke Role Statement
 *
 * Note: because of the parsing ambiguity with the GRANT <privileges>
 * statement, granted_roles is a list of AccessPriv; the execution code
 * should complain if any column lists appear.  grantee_roles is a list
 * of role names, as Value strings.
 * ----------------------
 */
#ifndef NO_NODE_GrantRoleStmt
BEGIN_NODE(GrantRoleStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,granted_roles)	/* list of roles to be granted/revoked */
	NODE_NODE(List,grantee_roles)	/* list of member roles to add/delete */
	NODE_SCALAR(bool,is_grant)		/* true = GRANT, false = REVOKE */
	NODE_SCALAR(bool,admin_opt)		/* with admin option */
	NODE_STRING(grantor)		/* set grantor to other than current role */
	NODE_ENUM(DropBehavior,behavior)		/* drop behavior (for REVOKE) */
END_NODE(GrantRoleStmt)
#endif /* NO_NODE_GrantRoleStmt */
/* ----------------------
 *	Alter Default Privileges Statement
 * ----------------------
 */
#ifndef NO_NODE_AlterDefaultPrivilegesStmt
BEGIN_NODE(AlterDefaultPrivilegesStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,options)		/* list of DefElem */
	NODE_NODE(GrantStmt,action)			/* GRANT/REVOKE action (with objects=NIL) */
END_NODE(AlterDefaultPrivilegesStmt)
#endif /* NO_NODE_AlterDefaultPrivilegesStmt */
/* ----------------------
 *		Copy Statement
 *
 * We support "COPY relation FROM file", "COPY relation TO file", and
 * "COPY (query) TO file".  In any given CopyStmt, exactly one of "relation"
 * and "query" must be non-NULL.
 * ----------------------
 */
#ifndef NO_NODE_CopyStmt
BEGIN_NODE(CopyStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,relation)		/* the relation to copy */
	NODE_NODE(Node,query)			/* the SELECT query to copy */
	NODE_NODE(List,attlist)		/* List of column names (as Strings), or NIL
								 * for all columns */
	NODE_SCALAR(bool,is_from)		/* TO or FROM */
	NODE_SCALAR(bool,is_program)		/* is 'filename' a program to popen? */
	NODE_STRING(filename)		/* filename, or NULL for STDIN/STDOUT */
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(CopyStmt)
#endif /* NO_NODE_CopyStmt */
/* ----------------------
 * SET Statement (includes RESET)
 *
 * "SET var TO DEFAULT" and "RESET var" are semantically equivalent, but we
 * preserve the distinction in VariableSetKind for CreateCommandTag().
 * ----------------------
 */

#ifndef NO_NODE_VariableSetStmt
BEGIN_NODE(VariableSetStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(VariableSetKind,kind)
	NODE_STRING(name)			/* variable to be set */
	NODE_NODE(List,args)			/* List of A_Const nodes */
	NODE_SCALAR(bool,is_local)		/* SET LOCAL? */
END_NODE(VariableSetStmt)
#endif /* NO_NODE_VariableSetStmt */
/* ----------------------
 * Show Statement
 * ----------------------
 */
#ifndef NO_NODE_VariableShowStmt
BEGIN_NODE(VariableShowStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(name)
END_NODE(VariableShowStmt)
#endif /* NO_NODE_VariableShowStmt */
/* ----------------------
 *		Create Table Statement
 *
 * NOTE: in the raw gram.y output, ColumnDef and Constraint nodes are
 * intermixed in tableElts, and constraints is NIL.  After parse analysis,
 * tableElts contains just ColumnDefs, and constraints contains just
 * Constraint nodes (in fact, only CONSTR_CHECK nodes, in the present
 * implementation).
 * ----------------------
 */

#ifndef NO_NODE_CreateStmt
BEGIN_NODE(CreateStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
	NODE_ENUM(ParseGrammar, grammar)
#endif
	NODE_NODE(RangeVar,relation)		/* relation to create */
	NODE_NODE(List,tableElts)		/* column definitions (list of ColumnDef) */
	NODE_NODE(List,inhRelations)	/* relations to inherit from (list of
								 * inhRelation) */
	NODE_NODE(TypeName,ofTypename)		/* OF typename */
	NODE_NODE(List,constraints)	/* constraints (list of Constraint nodes) */
	NODE_NODE(List,options)		/* options from WITH clause */
	NODE_ENUM(OnCommitAction,oncommit)	/* what do we do at COMMIT? */
	NODE_STRING(tablespacename) /* table space to use, or NULL */
	NODE_SCALAR(bool,if_not_exists)	/* just do nothing if it already exists? */
#ifdef PGXC
	NODE_NODE(DistributeBy,distributeby) 	/* distribution to use, or NULL */
	NODE_NODE(PGXCSubCluster,subcluster)		/* subcluster of table */
#endif
END_NODE(CreateStmt)
#endif /* NO_NODE_CreateStmt */
/* ----------
 * Definitions for constraints in CreateStmt
 *
 * Note that column defaults are treated as a type of constraint,
 * even though that's a bit odd semantically.
 *
 * For constraints that use expressions (CONSTR_CHECK, CONSTR_DEFAULT)
 * we may have the expression in either "raw" form (an untransformed
 * parse tree) or "cooked" form (the nodeToString representation of
 * an executable expression tree), depending on how this Constraint
 * node was created (by parsing, or by inheritance from an existing
 * relation).  We should never have both in the same node!
 *
 * FKCONSTR_ACTION_xxx values are stored into pg_constraint.confupdtype
 * and pg_constraint.confdeltype columns; FKCONSTR_MATCH_xxx values are
 * stored into pg_constraint.confmatchtype.  Changing the code values may
 * require an initdb!
 *
 * If skip_validation is true then we skip checking that the existing rows
 * in the table satisfy the constraint, and just install the catalog entries
 * for the constraint.  A new FK constraint is marked as valid iff
 * initially_valid is true.  (Usually skip_validation and initially_valid
 * are inverses, but we can set both true if the table is known empty.)
 *
 * Constraint attributes (DEFERRABLE etc) are initially represented as
 * separate Constraint nodes for simplicity of parsing.  parse_utilcmd.c makes
 * a pass through the constraints list to insert the info into the appropriate
 * Constraint node.
 * ----------
 */


/* Foreign key action codes */
#define FKCONSTR_ACTION_NOACTION	'a'
#define FKCONSTR_ACTION_RESTRICT	'r'
#define FKCONSTR_ACTION_CASCADE		'c'
#define FKCONSTR_ACTION_SETNULL		'n'
#define FKCONSTR_ACTION_SETDEFAULT	'd'

/* Foreign key matchtype codes */
#define FKCONSTR_MATCH_FULL			'f'
#define FKCONSTR_MATCH_PARTIAL		'p'
#define FKCONSTR_MATCH_SIMPLE		's'

#ifndef NO_NODE_Constraint
BEGIN_NODE(Constraint)
	NODE_ENUM(ConstrType,contype)		/* see above */
	/* Fields used for most/all constraint types: */
	NODE_STRING(conname)		/* Constraint name, or NULL if unnamed */
	NODE_SCALAR(bool,deferrable)		/* DEFERRABLE? */
	NODE_SCALAR(bool,initdeferred)	/* INITIALLY DEFERRED? */
	NODE_LOCATION(int,location)		/* token location, or -1 if unknown */

	/* Fields used for constraints with expressions (CHECK and DEFAULT): */
	NODE_SCALAR(bool,is_no_inherit)	/* is constraint non-inheritable? */
	NODE_NODE(Node,raw_expr)		/* expr, as untransformed parse tree */
	NODE_STRING(cooked_expr)	/* expr, as nodeToString representation */

	/* Fields used for unique constraints (UNIQUE and PRIMARY KEY): */
	NODE_NODE(List,keys)			/* String nodes naming referenced column(s) */

	/* Fields used for EXCLUSION constraints: */
	NODE_NODE(List,exclusions)		/* list of (IndexElem, operator name) pairs */

	/* Fields used for index constraints (UNIQUE, PRIMARY KEY, EXCLUSION): */
	NODE_NODE(List,options)		/* options from WITH clause */
	NODE_STRING(indexname)		/* existing index to use; otherwise NULL */
	NODE_STRING(indexspace)		/* index tablespace; NULL for default */
	/* These could be, but currently are not, used for UNIQUE/PKEY: */
	NODE_STRING(access_method)	/* index access method; NULL for default */
	NODE_NODE(Node,where_clause)	/* partial index predicate */

	/* Fields used for FOREIGN KEY constraints: */
	NODE_NODE(RangeVar,pktable)		/* Primary key table */
	NODE_NODE(List,fk_attrs)		/* Attributes of foreign key */
	NODE_NODE(List,pk_attrs)		/* Corresponding attrs in PK table */
	NODE_SCALAR(char,fk_matchtype)	/* FULL, PARTIAL, SIMPLE */
	NODE_SCALAR(char,fk_upd_action)	/* ON UPDATE action */
	NODE_SCALAR(char,fk_del_action)	/* ON DELETE action */
	NODE_NODE(List,old_conpfeqop)	/* pg_constraint.conpfeqop of my former self */

	/* Fields used for constraints that allow a NOT VALID specification */
	NODE_SCALAR(bool,skip_validation)	/* skip validation of existing rows? */
	NODE_SCALAR(bool,initially_valid)	/* mark the new constraint as valid? */

	NODE_SCALAR(Oid,old_pktable_oid) /* pg_constraint.confrelid of my former self */
END_NODE(Constraint)
#endif /* NO_NODE_Constraint */
/* ----------------------
 *		Create/Drop Table Space Statements
 * ----------------------
 */

#ifndef NO_NODE_CreateTableSpaceStmt
BEGIN_NODE(CreateTableSpaceStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(tablespacename)
	NODE_STRING(owner)
	NODE_STRING(location)
END_NODE(CreateTableSpaceStmt)
#endif /* NO_NODE_CreateTableSpaceStmt */
#ifndef NO_NODE_DropTableSpaceStmt
BEGIN_NODE(DropTableSpaceStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(tablespacename)
	NODE_SCALAR(bool,missing_ok)		/* skip error if missing? */
END_NODE(DropTableSpaceStmt)
#endif /* NO_NODE_DropTableSpaceStmt */
#ifndef NO_NODE_AlterTableSpaceOptionsStmt
BEGIN_NODE(AlterTableSpaceOptionsStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(tablespacename)
	NODE_NODE(List,options)
	NODE_SCALAR(bool,isReset)
END_NODE(AlterTableSpaceOptionsStmt)
#endif /* NO_NODE_AlterTableSpaceOptionsStmt */
/* ----------------------
 *		Create/Alter Extension Statements
 * ----------------------
 */

#ifndef NO_NODE_CreateExtensionStmt
BEGIN_NODE(CreateExtensionStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(extname)
	NODE_SCALAR(bool,if_not_exists)	/* just do nothing if it already exists? */
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(CreateExtensionStmt)
#endif /* NO_NODE_CreateExtensionStmt */
/* Only used for ALTER EXTENSION UPDATE; later might need an action field */
#ifndef NO_NODE_AlterExtensionStmt
BEGIN_NODE(AlterExtensionStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(extname)
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(AlterExtensionStmt)
#endif /* NO_NODE_AlterExtensionStmt */
#ifndef NO_NODE_AlterExtensionContentsStmt
BEGIN_NODE(AlterExtensionContentsStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(extname)		/* Extension's name */
	NODE_SCALAR(int,action)			/* +1 = add object, -1 = drop object */
	NODE_ENUM(ObjectType,objtype)		/* Object's type */
	NODE_NODE(List,objname)		/* Qualified name of the object */
	NODE_NODE(List,objargs)		/* Arguments if needed (eg, for functions) */
END_NODE(AlterExtensionContentsStmt)
#endif /* NO_NODE_AlterExtensionContentsStmt */
/* ----------------------
 *		Create/Alter FOREIGN DATA WRAPPER Statements
 * ----------------------
 */

#ifndef NO_NODE_CreateFdwStmt
BEGIN_NODE(CreateFdwStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(fdwname)		/* foreign-data wrapper name */
	NODE_NODE(List,func_options)	/* HANDLER/VALIDATOR options */
	NODE_NODE(List,options)		/* generic options to FDW */
END_NODE(CreateFdwStmt)
#endif /* NO_NODE_CreateFdwStmt */
#ifndef NO_NODE_AlterFdwStmt
BEGIN_NODE(AlterFdwStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(fdwname)		/* foreign-data wrapper name */
	NODE_NODE(List,func_options)	/* HANDLER/VALIDATOR options */
	NODE_NODE(List,options)		/* generic options to FDW */
END_NODE(AlterFdwStmt)
#endif /* NO_NODE_AlterFdwStmt */
/* ----------------------
 *		Create/Alter FOREIGN SERVER Statements
 * ----------------------
 */

#ifndef NO_NODE_CreateForeignServerStmt
BEGIN_NODE(CreateForeignServerStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(servername)		/* server name */
	NODE_STRING(servertype)		/* optional server type */
	NODE_STRING(version)		/* optional server version */
	NODE_STRING(fdwname)		/* FDW name */
	NODE_NODE(List,options)		/* generic options to server */
END_NODE(CreateForeignServerStmt)
#endif /* NO_NODE_CreateForeignServerStmt */
#ifndef NO_NODE_AlterForeignServerStmt
BEGIN_NODE(AlterForeignServerStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(servername)		/* server name */
	NODE_STRING(version)		/* optional server version */
	NODE_NODE(List,options)		/* generic options to server */
	NODE_SCALAR(bool,has_version)	/* version specified */
END_NODE(AlterForeignServerStmt)
#endif /* NO_NODE_AlterForeignServerStmt */
/* ----------------------
 *		Create FOREIGN TABLE Statements
 * ----------------------
 */

#ifndef NO_NODE_CreateForeignTableStmt
BEGIN_NODE(CreateForeignTableStmt)
	NODE_BASE2(CreateStmt,base)
	NODE_STRING(servername)
	NODE_NODE(List,options)
END_NODE(CreateForeignTableStmt)
#endif /* NO_NODE_CreateForeignTableStmt */

#ifndef NO_NODE_CreateUserMappingStmt
BEGIN_NODE(CreateUserMappingStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(username)		/* username or PUBLIC/CURRENT_USER */
	NODE_STRING(servername)		/* server name */
	NODE_NODE(List,options)		/* generic options to server */
END_NODE(CreateUserMappingStmt)
#endif /* NO_NODE_CreateUserMappingStmt */
#ifndef NO_NODE_AlterUserMappingStmt
BEGIN_NODE(AlterUserMappingStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(username)		/* username or PUBLIC/CURRENT_USER */
	NODE_STRING(servername)		/* server name */
	NODE_NODE(List,options)		/* generic options to server */
END_NODE(AlterUserMappingStmt)
#endif /* NO_NODE_AlterUserMappingStmt */
#ifndef NO_NODE_DropUserMappingStmt
BEGIN_NODE(DropUserMappingStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(username)		/* username or PUBLIC/CURRENT_USER */
	NODE_STRING(servername)		/* server name */
	NODE_SCALAR(bool,missing_ok)		/* ignore missing mappings */
END_NODE(DropUserMappingStmt)
#endif /* NO_NODE_DropUserMappingStmt */
/* ----------------------
 *		Create TRIGGER Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateTrigStmt
BEGIN_NODE(CreateTrigStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(trigname)		/* TRIGGER's name */
	NODE_NODE(RangeVar,relation)		/* relation trigger is on */
	NODE_NODE(List,funcname)		/* qual. name of function to call */
	NODE_NODE(List,args)			/* list of (T_String) Values or NIL */
	NODE_SCALAR(bool,row)			/* ROW/STATEMENT */
	/* timing uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
	NODE_SCALAR(int16,timing)			/* BEFORE, AFTER, or INSTEAD */
	/* events uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
	NODE_SCALAR(int16,events)			/* "OR" of INSERT/UPDATE/DELETE/TRUNCATE */
	NODE_NODE(List,columns)		/* column names, or NIL for all columns */
	NODE_NODE(Node,whenClause)		/* qual expression, or NULL if none */
	NODE_SCALAR(bool,isconstraint)	/* This is a constraint trigger */
	/* The remaining fields are only used for constraint triggers */
	NODE_SCALAR(bool,deferrable)		/* [NOT] DEFERRABLE */
	NODE_SCALAR(bool,initdeferred)	/* INITIALLY {DEFERRED|IMMEDIATE} */
	NODE_NODE(RangeVar,constrrel)		/* opposite relation, if RI trigger */
END_NODE(CreateTrigStmt)
#endif /* NO_NODE_CreateTrigStmt */
/* ----------------------
 *		Create EVENT TRIGGER Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateEventTrigStmt
BEGIN_NODE(CreateEventTrigStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(trigname)		/* TRIGGER's name */
	NODE_STRING(eventname)		/* event's identifier */
	NODE_NODE(List,whenclause)		/* list of DefElems indicating filtering */
	NODE_NODE(List,funcname)		/* qual. name of function to call */
END_NODE(CreateEventTrigStmt)
#endif /* NO_NODE_CreateEventTrigStmt */
/* ----------------------
 *		Alter EVENT TRIGGER Statement
 * ----------------------
 */
#ifndef NO_NODE_AlterEventTrigStmt
BEGIN_NODE(AlterEventTrigStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(trigname)		/* TRIGGER's name */
	NODE_SCALAR(char,tgenabled)		/* trigger's firing configuration WRT
								 * session_replication_role */
END_NODE(AlterEventTrigStmt)
#endif /* NO_NODE_AlterEventTrigStmt */
/* ----------------------
 *		Create/Drop PROCEDURAL LANGUAGE Statements
 *		Create PROCEDURAL LANGUAGE Statements
 * ----------------------
 */
#ifndef NO_NODE_CreatePLangStmt
BEGIN_NODE(CreatePLangStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_SCALAR(bool,replace)		/* T => replace if already exists */
	NODE_STRING(plname)			/* PL name */
	NODE_NODE(List,plhandler)		/* PL call handler function (qual. name) */
	NODE_NODE(List,plinline)		/* optional inline function (qual. name) */
	NODE_NODE(List,plvalidator)	/* optional validator function (qual. name) */
	NODE_SCALAR(bool,pltrusted)		/* PL is trusted */
END_NODE(CreatePLangStmt)
#endif /* NO_NODE_CreatePLangStmt */
/* ----------------------
 *	Create/Alter/Drop Role Statements
 *
 * Note: these node types are also used for the backwards-compatible
 * Create/Alter/Drop User/Group statements.  In the ALTER and DROP cases
 * there's really no need to distinguish what the original spelling was,
 * but for CREATE we mark the type because the defaults vary.
 * ----------------------
 */

#ifndef NO_NODE_CreateRoleStmt
BEGIN_NODE(CreateRoleStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(RoleStmtType,stmt_type)		/* ROLE/USER/GROUP */
	NODE_STRING(role)			/* role name */
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(CreateRoleStmt)
#endif /* NO_NODE_CreateRoleStmt */
#ifndef NO_NODE_AlterRoleStmt
BEGIN_NODE(AlterRoleStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(role)			/* role name */
	NODE_NODE(List,options)		/* List of DefElem nodes */
	NODE_SCALAR(int,action)			/* +1 = add members, -1 = drop members */
END_NODE(AlterRoleStmt)
#endif /* NO_NODE_AlterRoleStmt */
#ifndef NO_NODE_AlterRoleSetStmt
BEGIN_NODE(AlterRoleSetStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(role)			/* role name */
	NODE_STRING(database)		/* database name, or NULL */
	NODE_NODE(VariableSetStmt,setstmt)	/* SET or RESET subcommand */
END_NODE(AlterRoleSetStmt)
#endif /* NO_NODE_AlterRoleSetStmt */
#ifndef NO_NODE_DropRoleStmt
BEGIN_NODE(DropRoleStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,roles)			/* List of roles to remove */
	NODE_SCALAR(bool,missing_ok)		/* skip error if a role is missing? */
END_NODE(DropRoleStmt)
#endif /* NO_NODE_DropRoleStmt */
/* ----------------------
 *		{Create|Alter} SEQUENCE Statement
 * ----------------------
 */

#ifndef NO_NODE_CreateSeqStmt
BEGIN_NODE(CreateSeqStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,sequence)		/* the sequence to create */
	NODE_NODE(List,options)
	NODE_SCALAR(Oid,ownerId)		/* ID of owner, or InvalidOid for default */
#ifdef PGXC
	NODE_SCALAR(bool,is_serial)		/* Indicates if this sequence is part of SERIAL process */
#endif
END_NODE(CreateSeqStmt)
#endif /* NO_NODE_CreateSeqStmt */
#ifndef NO_NODE_AlterSeqStmt
BEGIN_NODE(AlterSeqStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,sequence)		/* the sequence to alter */
	NODE_NODE(List,options)
	NODE_SCALAR(bool,missing_ok)		/* skip error if a role is missing? */
#ifdef PGXC
	NODE_SCALAR(bool,is_serial)		/* Indicates if this sequence is part of SERIAL process */
#endif
END_NODE(AlterSeqStmt)
#endif /* NO_NODE_AlterSeqStmt */
/* ----------------------
 *		Create {Aggregate|Operator|Type} Statement
 * ----------------------
 */
#ifndef NO_NODE_DefineStmt
BEGIN_NODE(DefineStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,kind)			/* aggregate, operator, type */
	NODE_SCALAR(bool,oldstyle)		/* hack to signal old CREATE AGG syntax */
	NODE_NODE(List,defnames)		/* qualified name (list of Value strings) */
	NODE_NODE(List,args)			/* a list of TypeName (if needed) */
	NODE_NODE(List,definition)		/* a list of DefElem */
END_NODE(DefineStmt)
#endif /* NO_NODE_DefineStmt */
/* ----------------------
 *		Create Domain Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateDomainStmt
BEGIN_NODE(CreateDomainStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,domainname)		/* qualified name (list of Value strings) */
	NODE_NODE(TypeName,typeName)		/* the base type */
	NODE_NODE(CollateClause,collClause)	/* untransformed COLLATE spec, if any */
	NODE_NODE(List,constraints)	/* constraints (list of Constraint nodes) */
END_NODE(CreateDomainStmt)
#endif /* NO_NODE_CreateDomainStmt */
/* ----------------------
 *		Create Operator Class Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateOpClassStmt
BEGIN_NODE(CreateOpClassStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,opclassname)	/* qualified name (list of Value strings) */
	NODE_NODE(List,opfamilyname)	/* qualified name (ditto); NIL if omitted */
	NODE_STRING(amname)			/* name of index AM opclass is for */
	NODE_NODE(TypeName,datatype)		/* datatype of indexed column */
	NODE_NODE(List,items)			/* List of CreateOpClassItem nodes */
	NODE_SCALAR(bool,isDefault)		/* Should be marked as default for type? */
END_NODE(CreateOpClassStmt)
#endif /* NO_NODE_CreateOpClassStmt */
#define OPCLASS_ITEM_OPERATOR		1
#define OPCLASS_ITEM_FUNCTION		2
#define OPCLASS_ITEM_STORAGETYPE	3

#ifndef NO_NODE_CreateOpClassItem
BEGIN_NODE(CreateOpClassItem)
	NODE_SCALAR(int,itemtype)		/* see codes above */
	/* fields used for an operator or function item: */
	NODE_NODE(List,name)			/* operator or function name */
	NODE_NODE(List,args)			/* argument types */
	NODE_SCALAR(int,number)			/* strategy num or support proc num */
	NODE_NODE(List,order_family)	/* only used for ordering operators */
	NODE_NODE(List,class_args)		/* only used for functions */
	/* fields used for a storagetype item: */
	NODE_NODE(TypeName,storedtype)		/* datatype stored in index */
END_NODE(CreateOpClassItem)
#endif /* NO_NODE_CreateOpClassItem */
/* ----------------------
 *		Create Operator Family Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateOpFamilyStmt
BEGIN_NODE(CreateOpFamilyStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,opfamilyname)	/* qualified name (list of Value strings) */
	NODE_STRING(amname)			/* name of index AM opfamily is for */
END_NODE(CreateOpFamilyStmt)
#endif /* NO_NODE_CreateOpFamilyStmt */
/* ----------------------
 *		Alter Operator Family Statement
 * ----------------------
 */
#ifndef NO_NODE_AlterOpFamilyStmt
BEGIN_NODE(AlterOpFamilyStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,opfamilyname)	/* qualified name (list of Value strings) */
	NODE_STRING(amname)			/* name of index AM opfamily is for */
	NODE_SCALAR(bool,isDrop)			/* ADD or DROP the items? */
	NODE_NODE(List,items)			/* List of CreateOpClassItem nodes */
END_NODE(AlterOpFamilyStmt)
#endif /* NO_NODE_AlterOpFamilyStmt */
/* ----------------------
 *		Drop Table|Sequence|View|Index|Type|Domain|Conversion|Schema Statement
 * ----------------------
 */

#ifndef NO_NODE_DropStmt
BEGIN_NODE(DropStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,objects)		/* list of sublists of names (as Values) */
	NODE_NODE(List,arguments)		/* list of sublists of arguments (as Values) */
	NODE_ENUM(ObjectType,removeType)		/* object type */
	NODE_ENUM(DropBehavior,behavior)		/* RESTRICT or CASCADE behavior */
	NODE_SCALAR(bool,missing_ok)		/* skip error if object is missing? */
	NODE_SCALAR(bool,concurrent)		/* drop index concurrently? */
END_NODE(DropStmt)
#endif /* NO_NODE_DropStmt */
/* ----------------------
 *				Truncate Table Statement
 * ----------------------
 */
#ifndef NO_NODE_TruncateStmt
BEGIN_NODE(TruncateStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,relations)		/* relations (RangeVars) to be truncated */
	NODE_SCALAR(bool,restart_seqs)	/* restart owned sequences? */
	NODE_ENUM(DropBehavior,behavior)		/* RESTRICT or CASCADE behavior */
END_NODE(TruncateStmt)
#endif /* NO_NODE_TruncateStmt */

#ifndef NO_NODE_CommentStmt
BEGIN_NODE(CommentStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,objtype)		/* Object's type */
	NODE_NODE(List,objname)		/* Qualified name of the object */
	NODE_NODE(List,objargs)		/* Arguments if needed (eg, for functions) */
	NODE_STRING(comment)		/* Comment to insert, or NULL to remove */
END_NODE(CommentStmt)
#endif /* NO_NODE_CommentStmt */
/* ----------------------
 *				SECURITY LABEL Statement
 * ----------------------
 */
#ifndef NO_NODE_SecLabelStmt
BEGIN_NODE(SecLabelStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,objtype)		/* Object's type */
	NODE_NODE(List,objname)		/* Qualified name of the object */
	NODE_NODE(List,objargs)		/* Arguments if needed (eg, for functions) */
	NODE_STRING(provider)		/* Label provider (or NULL) */
	NODE_STRING(label)			/* New security label to be assigned */
END_NODE(SecLabelStmt)
#endif /* NO_NODE_SecLabelStmt */
/* ----------------------
 *		Declare Cursor Statement
 *
 * Note: the "query" field of DeclareCursorStmt is only used in the raw grammar
 * output.  After parse analysis it's set to null, and the Query points to the
 * DeclareCursorStmt, not vice versa.
 * ----------------------
 */
#define CURSOR_OPT_BINARY		0x0001	/* BINARY */
#define CURSOR_OPT_SCROLL		0x0002	/* SCROLL explicitly given */
#define CURSOR_OPT_NO_SCROLL	0x0004	/* NO SCROLL explicitly given */
#define CURSOR_OPT_INSENSITIVE	0x0008	/* INSENSITIVE */
#define CURSOR_OPT_HOLD			0x0010	/* WITH HOLD */
/* these planner-control flags do not correspond to any SQL grammar: */
#define CURSOR_OPT_FAST_PLAN	0x0020	/* prefer fast-start plan */
#define CURSOR_OPT_GENERIC_PLAN 0x0040	/* force use of generic plan */
#define CURSOR_OPT_CUSTOM_PLAN	0x0080	/* force use of custom plan */

#ifndef NO_NODE_DeclareCursorStmt
BEGIN_NODE(DeclareCursorStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(portalname)		/* name of the portal (cursor) */
	NODE_SCALAR(int,options)		/* bitmask of options (see above) */
	NODE_NODE(Node,query)			/* the raw SELECT query */
END_NODE(DeclareCursorStmt)
#endif /* NO_NODE_DeclareCursorStmt */
/* ----------------------
 *		Close Portal Statement
 * ----------------------
 */
#ifndef NO_NODE_ClosePortalStmt
BEGIN_NODE(ClosePortalStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(portalname)		/* name of the portal (cursor) */
	/* NULL means CLOSE ALL */
END_NODE(ClosePortalStmt)
#endif /* NO_NODE_ClosePortalStmt */

#ifndef NO_NODE_FetchStmt
BEGIN_NODE(FetchStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(FetchDirection,direction)	/* see above */
	NODE_SCALAR(long,howMany)		/* number of rows, or position argument */
	NODE_STRING(portalname)		/* name of portal (cursor) */
	NODE_SCALAR(bool,ismove)			/* TRUE if MOVE */
END_NODE(FetchStmt)
#endif /* NO_NODE_FetchStmt */
/* ----------------------
 *		Create Index Statement
 *
 * This represents creation of an index and/or an associated constraint.
 * If isconstraint is true, we should create a pg_constraint entry along
 * with the index.  But if indexOid isn't InvalidOid, we are not creating an
 * index, just a UNIQUE/PKEY constraint using an existing index.  isconstraint
 * must always be true in this case, and the fields describing the index
 * properties are empty.
 * ----------------------
 */
#ifndef NO_NODE_IndexStmt
BEGIN_NODE(IndexStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
	NODE_ENUM(ParseGrammar, grammar)
#endif /* ADB */
	NODE_STRING(idxname)		/* name of new index, or NULL for default */
	NODE_NODE(RangeVar,relation)		/* relation to build index on */
	NODE_STRING(accessMethod)	/* name of access method (eg. btree) */
	NODE_STRING(tableSpace)		/* tablespace, or NULL for default */
	NODE_NODE(List,indexParams)	/* columns to index: a list of IndexElem */
	NODE_NODE(List,options)		/* WITH clause options: a list of DefElem */
	NODE_NODE(Node,whereClause)	/* qualification (partial-index predicate) */
	NODE_NODE(List,excludeOpNames) /* exclusion operator names, or NIL if none */
	NODE_STRING(idxcomment)		/* comment to apply to index, or NULL */
	NODE_SCALAR(Oid,indexOid)		/* OID of an existing index, if any */
	NODE_SCALAR(Oid,oldNode)		/* relfilenode of existing storage, if any */
	NODE_SCALAR(bool,unique)			/* is index unique? */
	NODE_SCALAR(bool,primary)		/* is index a primary key? */
	NODE_SCALAR(bool,isconstraint)	/* is it for a pkey/unique constraint? */
	NODE_SCALAR(bool,deferrable)		/* is the constraint DEFERRABLE? */
	NODE_SCALAR(bool,initdeferred)	/* is the constraint INITIALLY DEFERRED? */
	NODE_SCALAR(bool,concurrent)		/* should this be a concurrent index build? */
END_NODE(IndexStmt)
#endif /* NO_NODE_IndexStmt */
/* ----------------------
 *		Create Function Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateFunctionStmt
BEGIN_NODE(CreateFunctionStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_SCALAR(bool,replace)		/* T => replace if already exists */
	NODE_NODE(List,funcname)		/* qualified name of function to create */
	NODE_NODE(List,parameters)		/* a list of FunctionParameter */
	NODE_NODE(TypeName,returnType)		/* the return type */
	NODE_NODE(List,options)		/* a list of DefElem */
	NODE_NODE(List,withClause)		/* a list of DefElem */
END_NODE(CreateFunctionStmt)
#endif /* NO_NODE_CreateFunctionStmt */

#ifndef NO_NODE_FunctionParameter
BEGIN_NODE(FunctionParameter)
	NODE_STRING(name)			/* parameter name, or NULL if not given */
	NODE_NODE(TypeName,argType)		/* TypeName for parameter type */
	NODE_ENUM(FunctionParameterMode,mode) /* IN/OUT/etc */
	NODE_NODE(Node,defexpr)		/* raw default expr, or NULL if not given */
END_NODE(FunctionParameter)
#endif /* NO_NODE_FunctionParameter */
#ifndef NO_NODE_AlterFunctionStmt
BEGIN_NODE(AlterFunctionStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(FuncWithArgs,func)			/* name and args of function */
	NODE_NODE(List,actions)		/* list of DefElem */
END_NODE(AlterFunctionStmt)
#endif /* NO_NODE_AlterFunctionStmt */
/* ----------------------
 *		DO Statement
 *
 * DoStmt is the raw parser output, InlineCodeBlock is the execution-time API
 * ----------------------
 */
#ifndef NO_NODE_DoStmt
BEGIN_NODE(DoStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,args)			/* List of DefElem nodes */
END_NODE(DoStmt)
#endif /* NO_NODE_DoStmt */
#ifndef NO_NODE_InlineCodeBlock
BEGIN_NODE(InlineCodeBlock)
	NODE_STRING(source_text)	/* source text of anonymous code block */
	NODE_SCALAR(Oid,langOid)		/* OID of selected language */
	NODE_SCALAR(bool,langIsTrusted)	/* trusted property of the language */
END_NODE(InlineCodeBlock)
#endif /* NO_NODE_InlineCodeBlock */
/* ----------------------
 *		Alter Object Rename Statement
 * ----------------------
 */
#ifndef NO_NODE_RenameStmt
BEGIN_NODE(RenameStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,renameType)		/* OBJECT_TABLE, OBJECT_COLUMN, etc */
	NODE_ENUM(ObjectType,relationType)	/* if column name, associated relation type */
	NODE_NODE(RangeVar,relation)		/* in case it's a table */
	NODE_NODE(List,object)			/* in case it's some other object */
	NODE_NODE(List,objarg)			/* argument types, if applicable */
	NODE_STRING(subname)		/* name of contained object (column, rule,
								 * trigger, etc) */
	NODE_STRING(newname)		/* the new name */
	NODE_ENUM(DropBehavior,behavior)		/* RESTRICT or CASCADE behavior */
	NODE_SCALAR(bool,missing_ok)		/* skip error if missing? */
END_NODE(RenameStmt)
#endif /* NO_NODE_RenameStmt */
/* ----------------------
 *		ALTER object SET SCHEMA Statement
 * ----------------------
 */
#ifndef NO_NODE_AlterObjectSchemaStmt
BEGIN_NODE(AlterObjectSchemaStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,objectType)		/* OBJECT_TABLE, OBJECT_TYPE, etc */
	NODE_NODE(RangeVar,relation)		/* in case it's a table */
	NODE_NODE(List,object)			/* in case it's some other object */
	NODE_NODE(List,objarg)			/* argument types, if applicable */
	NODE_STRING(newschema)		/* the new schema */
	NODE_SCALAR(bool,missing_ok)		/* skip error if missing? */
END_NODE(AlterObjectSchemaStmt)
#endif /* NO_NODE_AlterObjectSchemaStmt */

#ifndef NO_NODE_AlterOwnerStmt
BEGIN_NODE(AlterOwnerStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,objectType)		/* OBJECT_TABLE, OBJECT_TYPE, etc */
	NODE_NODE(RangeVar,relation)		/* in case it's a table */
	NODE_NODE(List,object)			/* in case it's some other object */
	NODE_NODE(List,objarg)			/* argument types, if applicable */
	NODE_STRING(newowner)		/* the new owner */
END_NODE(AlterOwnerStmt)
#endif /* NO_NODE_AlterOwnerStmt */

/* ----------------------
 *		Create Rule Statement
 * ----------------------
 */
#ifndef NO_NODE_RuleStmt
BEGIN_NODE(RuleStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,relation)		/* relation the rule is for */
	NODE_STRING(rulename)		/* name of the rule */
	NODE_NODE(Node,whereClause)	/* qualifications */
	NODE_ENUM(CmdType,event)			/* SELECT, INSERT, etc */
	NODE_SCALAR(bool,instead)		/* is a 'do instead'? */
	NODE_NODE(List,actions)		/* the action statements */
	NODE_SCALAR(bool,replace)		/* OR REPLACE */
END_NODE(RuleStmt)
#endif /* NO_NODE_RuleStmt */
/* ----------------------
 *		Notify Statement
 * ----------------------
 */
#ifndef NO_NODE_NotifyStmt
BEGIN_NODE(NotifyStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(conditionname)	/* condition name to notify */
	NODE_STRING(payload)		/* the payload string, or NULL if none */
END_NODE(NotifyStmt)
#endif /* NO_NODE_NotifyStmt */
/* ----------------------
 *		Listen Statement
 * ----------------------
 */
#ifndef NO_NODE_ListenStmt
BEGIN_NODE(ListenStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(conditionname)	/* condition name to listen on */
END_NODE(ListenStmt)
#endif /* NO_NODE_ListenStmt */
/* ----------------------
 *		Unlisten Statement
 * ----------------------
 */
#ifndef NO_NODE_UnlistenStmt
BEGIN_NODE(UnlistenStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(conditionname)	/* name to unlisten on, or NULL for all */
END_NODE(UnlistenStmt)
#endif /* NO_NODE_UnlistenStmt */
/* ----------------------
 *		{Begin|Commit|Rollback} Transaction Statement
 * ----------------------
 */


#ifndef NO_NODE_TransactionStmt
BEGIN_NODE(TransactionStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
#if defined(ADB) || defined(AGTM)
	NODE_SCALAR(bool,missing_ok)
#endif
	NODE_ENUM(TransactionStmtKind,kind)	/* see above */
	NODE_NODE(List,options)		/* for BEGIN/START and savepoint commands */
	NODE_STRING(gid)			/* for two-phase-commit related commands */
END_NODE(TransactionStmt)
#endif /* NO_NODE_TransactionStmt */
/* ----------------------
 *		Create Type Statement, composite types
 * ----------------------
 */
#ifndef NO_NODE_CompositeTypeStmt
BEGIN_NODE(CompositeTypeStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,typevar)		/* the composite type to be created */
	NODE_NODE(List,coldeflist)		/* list of ColumnDef nodes */
END_NODE(CompositeTypeStmt)
#endif /* NO_NODE_CompositeTypeStmt */
/* ----------------------
 *		Create Type Statement, enum types
 * ----------------------
 */
#ifndef NO_NODE_CreateEnumStmt
BEGIN_NODE(CreateEnumStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,typeName)		/* qualified name (list of Value strings) */
	NODE_NODE(List,vals)			/* enum values (list of Value strings) */
END_NODE(CreateEnumStmt)
#endif /* NO_NODE_CreateEnumStmt */
/* ----------------------
 *		Create Type Statement, range types
 * ----------------------
 */
#ifndef NO_NODE_CreateRangeStmt
BEGIN_NODE(CreateRangeStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,typeName)		/* qualified name (list of Value strings) */
	NODE_NODE(List,params)			/* range parameters (list of DefElem) */
END_NODE(CreateRangeStmt)
#endif /* NO_NODE_CreateRangeStmt */
/* ----------------------
 *		Alter Type Statement, enum types
 * ----------------------
 */
#ifndef NO_NODE_AlterEnumStmt
BEGIN_NODE(AlterEnumStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,typeName)		/* qualified name (list of Value strings) */
	NODE_STRING(newVal)			/* new enum value's name */
	NODE_STRING(newValNeighbor) /* neighboring enum value, if specified */
	NODE_SCALAR(bool,newValIsAfter)	/* place new enum value after neighbor? */
	NODE_SCALAR(bool,skipIfExists)	/* no error if label already exists */
END_NODE(AlterEnumStmt)
#endif /* NO_NODE_AlterEnumStmt */
/* ----------------------
 *		Create View Statement
 * ----------------------
 */
#ifndef NO_NODE_ViewStmt
BEGIN_NODE(ViewStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
	NODE_ENUM(ParseGrammar, grammar)
#endif
	NODE_NODE(RangeVar,view)			/* the view to be created */
	NODE_NODE(List,aliases)		/* target column names */
	NODE_NODE(Node,query)			/* the SELECT query */
	NODE_SCALAR(bool,replace)		/* replace an existing view? */
	NODE_NODE(List,options)		/* options from WITH clause */
END_NODE(ViewStmt)
#endif /* NO_NODE_ViewStmt */
/* ----------------------
 *		Load Statement
 * ----------------------
 */
#ifndef NO_NODE_LoadStmt
BEGIN_NODE(LoadStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(filename)		/* file to load */
END_NODE(LoadStmt)
#endif /* NO_NODE_LoadStmt */
/* ----------------------
 *		Createdb Statement
 * ----------------------
 */
#ifndef NO_NODE_CreatedbStmt
BEGIN_NODE(CreatedbStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(dbname)			/* name of database to create */
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(CreatedbStmt)
#endif /* NO_NODE_CreatedbStmt */
/* ----------------------
 *	Alter Database
 * ----------------------
 */
#ifndef NO_NODE_AlterDatabaseStmt
BEGIN_NODE(AlterDatabaseStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(dbname)			/* name of database to alter */
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(AlterDatabaseStmt)
#endif /* NO_NODE_AlterDatabaseStmt */
#ifndef NO_NODE_AlterDatabaseSetStmt
BEGIN_NODE(AlterDatabaseSetStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(dbname)			/* database name */
	NODE_NODE(VariableSetStmt,setstmt)	/* SET or RESET subcommand */
END_NODE(AlterDatabaseSetStmt)
#endif /* NO_NODE_AlterDatabaseSetStmt */
/* ----------------------
 *		Dropdb Statement
 * ----------------------
 */
#ifndef NO_NODE_DropdbStmt
BEGIN_NODE(DropdbStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(dbname)			/* database to drop */
	NODE_SCALAR(bool,missing_ok)		/* skip error if db is missing? */
END_NODE(DropdbStmt)
#endif /* NO_NODE_DropdbStmt */
/* ----------------------
 *		Cluster Statement (support pbrown's cluster index implementation)
 * ----------------------
 */
#ifndef NO_NODE_ClusterStmt
BEGIN_NODE(ClusterStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(RangeVar,relation)		/* relation being indexed, or NULL if all */
	NODE_STRING(indexname)		/* original index defined */
	NODE_SCALAR(bool,verbose)		/* print progress info */
END_NODE(ClusterStmt)
#endif /* NO_NODE_ClusterStmt */
/* ----------------------
 *		Vacuum and Analyze Statements
 *
 * Even though these are nominally two statements, it's convenient to use
 * just one node type for both.  Note that at least one of VACOPT_VACUUM
 * and VACOPT_ANALYZE must be set in options.  VACOPT_FREEZE is an internal
 * convenience for the grammar and is not examined at runtime --- the
 * freeze_min_age and freeze_table_age fields are what matter.
 * ----------------------
 */

#ifndef NO_NODE_VacuumStmt
BEGIN_NODE(VacuumStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_SCALAR(int,options)		/* OR of VacuumOption flags */
	NODE_SCALAR(int,freeze_min_age) /* min freeze age, or -1 to use default */
	NODE_SCALAR(int,freeze_table_age)		/* age at which to scan whole table */
	NODE_NODE(RangeVar,relation)		/* single table to process, or NULL */
	NODE_NODE(List,va_cols)		/* list of column names, or NIL for all */
	/* place these at the end, to avoid ABI break within 9.3 branch */
	NODE_SCALAR(int,multixact_freeze_min_age)		/* min multixact freeze age,
												 * or -1 to use default */
	NODE_SCALAR(int,multixact_freeze_table_age)		/* multixact age at which to
												 * scan whole table */
END_NODE(VacuumStmt)
#endif /* NO_NODE_VacuumStmt */
#ifdef PGXC
/*
 * ----------------------
 *      Barrier Statement
 */
#ifndef NO_NODE_BarrierStmt
BEGIN_NODE(BarrierStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(id)			/* User supplied barrier id, if any */
END_NODE(BarrierStmt)
#endif /* NO_NODE_BarrierStmt */

#endif

/* ----------------------
 *		Explain Statement
 *
 * The "query" field is either a raw parse tree (SelectStmt, InsertStmt, etc)
 * or a Query node if parse analysis has been done.  Note that rewriting and
 * planning of the query are always postponed until execution of EXPLAIN.
 * ----------------------
 */
#ifndef NO_NODE_ExplainStmt
BEGIN_NODE(ExplainStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(Node,query)			/* the query (see comments above) */
	NODE_NODE(List,options)		/* list of DefElem nodes */
END_NODE(ExplainStmt)
#endif /* NO_NODE_ExplainStmt */
/* ----------------------
 *		CREATE TABLE AS Statement (a/k/a SELECT INTO)
 *
 * A query written as CREATE TABLE AS will produce this node type natively.
 * A query written as SELECT ... INTO will be transformed to this form during
 * parse analysis.
 * A query written as CREATE MATERIALIZED view will produce this node type,
 * during parse analysis, since it needs all the same data.
 *
 * The "query" field is handled similarly to EXPLAIN, though note that it
 * can be a SELECT or an EXECUTE, but not other DML statements.
 * ----------------------
 */
#ifndef NO_NODE_CreateTableAsStmt
BEGIN_NODE(CreateTableAsStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
	NODE_ENUM(ParseGrammar, grammar)
#endif
	NODE_NODE(Node,query)			/* the query (see comments above) */
	NODE_NODE(IntoClause,into)			/* destination table */
	NODE_ENUM(ObjectType,relkind)		/* OBJECT_TABLE or OBJECT_MATVIEW */
	NODE_SCALAR(bool,is_select_into) /* it was written as SELECT INTO */
END_NODE(CreateTableAsStmt)
#endif /* NO_NODE_CreateTableAsStmt */
/* ----------------------
 *		REFRESH MATERIALIZED VIEW Statement
 * ----------------------
 */
#ifndef NO_NODE_RefreshMatViewStmt
BEGIN_NODE(RefreshMatViewStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_SCALAR(bool,skipData)		/* true for WITH NO DATA */
	NODE_NODE(RangeVar,relation)		/* relation to insert into */
END_NODE(RefreshMatViewStmt)
#endif /* NO_NODE_RefreshMatViewStmt */
/* ----------------------
 * Checkpoint Statement
 * ----------------------
 */
#ifndef NO_NODE_CheckPointStmt
BEGIN_NODE(CheckPointStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
END_NODE(CheckPointStmt)
#endif /* NO_NODE_CheckPointStmt */
/* ----------------------
 * Discard Statement
 * ----------------------
 */


#ifndef NO_NODE_DiscardStmt
BEGIN_NODE(DiscardStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(DiscardMode,target)
END_NODE(DiscardStmt)
#endif /* NO_NODE_DiscardStmt */
/* ----------------------
 *		LOCK Statement
 * ----------------------
 */
#ifndef NO_NODE_LockStmt
BEGIN_NODE(LockStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,relations)		/* relations to lock */
	NODE_SCALAR(int,mode)			/* lock mode */
	NODE_SCALAR(bool,nowait)			/* no wait mode */
END_NODE(LockStmt)
#endif /* NO_NODE_LockStmt */
/* ----------------------
 *		SET CONSTRAINTS Statement
 * ----------------------
 */
#ifndef NO_NODE_ConstraintsSetStmt
BEGIN_NODE(ConstraintsSetStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,constraints)	/* List of names as RangeVars */
	NODE_SCALAR(bool,deferred)
END_NODE(ConstraintsSetStmt)
#endif /* NO_NODE_ConstraintsSetStmt */
/* ----------------------
 *		REINDEX Statement
 * ----------------------
 */
#ifndef NO_NODE_ReindexStmt
BEGIN_NODE(ReindexStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_ENUM(ObjectType,kind)			/* OBJECT_INDEX, OBJECT_TABLE, etc. */
	NODE_NODE(RangeVar,relation)		/* Table or index to reindex */
	NODE_STRING(name)			/* name of database to reindex */
	NODE_SCALAR(bool,do_system)		/* include system tables in database case */
	NODE_SCALAR(bool,do_user)		/* include user tables in database case */
END_NODE(ReindexStmt)
#endif /* NO_NODE_ReindexStmt */
/* ----------------------
 *		CREATE CONVERSION Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateConversionStmt
BEGIN_NODE(CreateConversionStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,conversion_name)	/* Name of the conversion */
	NODE_STRING(for_encoding_name)		/* source encoding name */
	NODE_STRING(to_encoding_name)		/* destination encoding name */
	NODE_NODE(List,func_name)		/* qualified conversion function name */
	NODE_SCALAR(bool,def)			/* is this a default conversion? */
END_NODE(CreateConversionStmt)
#endif /* NO_NODE_CreateConversionStmt */
/* ----------------------
 *	CREATE CAST Statement
 * ----------------------
 */
#ifndef NO_NODE_CreateCastStmt
BEGIN_NODE(CreateCastStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(TypeName,sourcetype)
	NODE_NODE(TypeName,targettype)
	NODE_NODE(FuncWithArgs,func)
	NODE_ENUM(CoercionContext,context)
	NODE_SCALAR(bool,inout)
END_NODE(CreateCastStmt)
#endif /* NO_NODE_CreateCastStmt */
/* ----------------------
 *		PREPARE Statement
 * ----------------------
 */
#ifndef NO_NODE_PrepareStmt
BEGIN_NODE(PrepareStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(name)			/* Name of plan, arbitrary */
	NODE_NODE(List,argtypes)		/* Types of parameters (List of TypeName) */
	NODE_NODE(Node,query)			/* The query itself (as a raw parsetree) */
END_NODE(PrepareStmt)
#endif /* NO_NODE_PrepareStmt */

/* ----------------------
 *		EXECUTE Statement
 * ----------------------
 */

#ifndef NO_NODE_ExecuteStmt
BEGIN_NODE(ExecuteStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(name)			/* The name of the plan to execute */
	NODE_NODE(List,params)			/* Values to assign to parameters */
END_NODE(ExecuteStmt)
#endif /* NO_NODE_ExecuteStmt */

/* ----------------------
 *		DEALLOCATE Statement
 * ----------------------
 */
#ifndef NO_NODE_DeallocateStmt
BEGIN_NODE(DeallocateStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_STRING(name)			/* The name of the plan to remove */
	/* NULL means DEALLOCATE ALL */
END_NODE(DeallocateStmt)
#endif /* NO_NODE_DeallocateStmt */
/*
 *		DROP OWNED statement
 */
#ifndef NO_NODE_DropOwnedStmt
BEGIN_NODE(DropOwnedStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,roles)
	NODE_ENUM(DropBehavior,behavior)
END_NODE(DropOwnedStmt)
#endif /* NO_NODE_DropOwnedStmt */
/*
 *		REASSIGN OWNED statement
 */
#ifndef NO_NODE_ReassignOwnedStmt
BEGIN_NODE(ReassignOwnedStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,roles)
	NODE_STRING(newrole)
END_NODE(ReassignOwnedStmt)
#endif /* NO_NODE_ReassignOwnedStmt */
/*
 * TS Dictionary stmts: DefineStmt, RenameStmt and DropStmt are default
 */
#ifndef NO_NODE_AlterTSDictionaryStmt
BEGIN_NODE(AlterTSDictionaryStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,dictname)		/* qualified name (list of Value strings) */
	NODE_NODE(List,options)		/* List of DefElem nodes */
END_NODE(AlterTSDictionaryStmt)
#endif /* NO_NODE_AlterTSDictionaryStmt */
/*
 * TS Configuration stmts: DefineStmt, RenameStmt and DropStmt are default
 */
#ifndef NO_NODE_AlterTSConfigurationStmt
BEGIN_NODE(AlterTSConfigurationStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,cfgname)		/* qualified name (list of Value strings) */
	NODE_NODE(List,tokentype)		/* list of Value strings */
	NODE_NODE(List,dicts)			/* list of list of Value strings */
	NODE_SCALAR(bool,override)		/* if true - remove old variant */
	NODE_SCALAR(bool,replace)		/* if true - replace dictionary by another */
	NODE_SCALAR(bool,missing_ok)		/* for DROP - skip error if missing? */
END_NODE(AlterTSConfigurationStmt)
#endif /* NO_NODE_AlterTSConfigurationStmt */
/* PGXC_BEGIN */
/*
 * EXECUTE DIRECT statement
 */
#ifndef NO_NODE_ExecDirectStmt
BEGIN_NODE(ExecDirectStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,node_names)
	NODE_STRING(query)
END_NODE(ExecDirectStmt)
#endif /* NO_NODE_ExecDirectStmt */
/*
 * CLEAN CONNECTION statement
 */
#ifndef NO_NODE_CleanConnStmt
BEGIN_NODE(CleanConnStmt)
#ifdef ADB
	NODE_SCALAR(int,endpos)
#endif
	NODE_NODE(List,nodes)		/* list of nodes dropped */
	NODE_STRING(dbname)	/* name of database to drop connections */
	NODE_STRING(username)	/* name of user whose connections are dropped */
	NODE_SCALAR(bool,is_coord)	/* type of connections dropped */
	NODE_SCALAR(bool,is_force)	/* option force  */
END_NODE(CleanConnStmt)
#endif /* NO_NODE_CleanConnStmt *//* PGXC_END */

#ifdef ADBMGRD

#ifndef NO_NODE_MGRAddHost
BEGIN_NODE(MGRAddHost)
	NODE_SCALAR(bool, if_not_exists)
	NODE_STRING(name)
	NODE_NODE(List, options)
END_NODE(MGRAddHost)
#endif /* NO_NODE_MGRAddHost */

#ifndef NO_NODE_MGRDropHost
BEGIN_NODE(MGRDropHost)
	NODE_SCALAR(bool, if_exists)
	NODE_NODE(List, hosts)
END_NODE(MGRDropHost)
#endif /* NO_NODE_MGRDropHost */

#ifndef NO_NODE_MGRAlterHost
BEGIN_NODE(MGRAlterHost)
	NODE_SCALAR(bool, if_not_exists)
	NODE_STRING(name)
	NODE_NODE(List, options)
END_NODE(MGRAlterHost)
#endif /* NO_NODE_MGRAlterHost */

#ifndef NO_NODE_MGRAddNode
BEGIN_NODE(MGRAddNode)
	NODE_SCALAR(bool, if_not_exists)
	NODE_STRING(name)
	NODE_NODE(List, options)
END_NODE(MGRAddNode)
#endif /* NO_NODE_MGRAddNode */

#ifndef NO_NODE_MGRAlterNode
BEGIN_NODE(MGRAlterNode)
	NODE_SCALAR(bool, if_not_exists)
	NODE_STRING(name)
	NODE_NODE(List, options)
END_NODE(MGRAlterNode)
#endif /* NO_NODE_MGRAlterNode */

#ifndef NO_NODE_MGRDropNode
BEGIN_NODE(MGRDropNode)
	NODE_SCALAR(bool, if_exists)
	NODE_NODE(List, names)
END_NODE(MGRDropNode)
#endif /* NO_NODE_MGRDropNode */

#ifndef NO_NODE_MGRDeplory
BEGIN_NODE(MGRDeplory)
	NODE_NODE(List, hosts)
	NODE_STRING(password)
END_NODE(MGRDeplory)
#endif /* NO_NODE_MGRDeplory */

#ifndef NO_NODE_MGRUpdateparm
BEGIN_NODE(MGRUpdateparm)
	NODE_SCALAR(char, parmtype)
	NODE_STRING(nodename)
	NODE_SCALAR(char, nodetype)
	NODE_STRING(key)
	NODE_STRING(value)
	NODE_NODE(List, options)
END_NODE(MGRUpdateparm)
#endif /* NO_NODE_MGRUpdateparm */

#ifndef NO_NODE_MGRUpdateparmReset
BEGIN_NODE(MGRUpdateparmReset)
	NODE_SCALAR(char, parmtype)
	NODE_STRING(nodename)
	NODE_SCALAR(char, nodetype)
	NODE_STRING(key)
	NODE_NODE(List, options)
END_NODE(MGRUpdateparmReset)
#endif /* NO_NODE_MGRUpdateparmReset */

#ifndef NO_NODE_MGRStartAgent
BEGIN_NODE(MGRStartAgent)
	NODE_NODE(List, hosts)
	NODE_STRING(password)
END_NODE(MGRStartAgent)
#endif /* NO_NODE_MGRStartAgent */

#ifndef NO_NODE_MGRStopAgent
BEGIN_NODE(MGRStopAgent)
	NODE_NODE(List, hosts)
END_NODE(MGRStopAgent)
#endif /* NO_NODE_MGRStopAgent */

#endif /* ADBMGRD */
