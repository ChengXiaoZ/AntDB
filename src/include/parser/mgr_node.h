/*
 * nodes for manager
 */

#ifndef MGR_NODE_H
#define MGR_NODE_H

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
typedef struct MGRAddHba
{
	NodeTag		type;
	char		*name;		/* host name */
	List		*options;	/* list of DefElem */
}MGRAddHba;
typedef struct MGRAddHost
{
	NodeTag		type;
	bool		if_not_exists;
	char		*name;		/* host name */
	List		*options;	/* list of DefElem */
}MGRAddHost;

typedef struct MGRDropHost
{
	NodeTag		type;
	bool		if_exists;
	List		*hosts;		/* list of A_Const(String) */
}MGRDropHost;

typedef struct MGRAlterHost
{
	NodeTag		type;
	bool		if_not_exists;
	char		*name;		/* host name */
	List		*options;	/* list of DefElem */
}MGRAlterHost;

typedef struct MGRAlterParm
{
	NodeTag		type;
	bool		if_not_exists;	
	char		*parmkey;		/* parm name */
	char		*parmnode;		/* parm node */
	List		*options;	/* list of DefElem */
}MGRAlterParm;

/*for datanode coordinator*/
typedef struct MGRAddNode
{
	NodeTag		type;
	bool		if_not_exists;
	char		nodetype;	/*gtm/coordinator/datanode master/slave/extern*/
	char		*name;			/* node name */
	List		*options;		/* list of DefElem */
}MGRAddNode;

typedef struct MGRAlterNode
{
	NodeTag		type;
	bool		if_not_exists;
	char		nodetype;	/*gtm/coordinator/datanode master/slave/extern*/
	char		*name;			/* node name */
	List		*options;		/* list of DefElem */
}MGRAlterNode;

typedef struct MGRDropNode
{
	NodeTag		type;
	bool		if_exists;
	char		nodetype;	/*gtm/coordinator/datanode master/slave/extern*/
	List		*names;		/* list of A_Const(String) */
}MGRDropNode;

typedef struct MGRDeplory
{
	NodeTag		type;
	List		*hosts;		/* list of Value(String), NIL for ALL */
	char		*password;
}MGRDeplory;

typedef struct MGRUpdateparm
{
	NodeTag		type;
	char		parmtype;
	char		*nodename;
	char		nodetype;
	char		*key;
	char		*value;
	bool		is_force;
	List		*options;		/* list of DefElem */
}MGRUpdateparm;

typedef struct MGRUpdateparmReset
{
	NodeTag		type;
	char		parmtype;
	char		*nodename;
	char		nodetype;
	char		*key;
	bool		is_force;
	List		*options;		/* list of DefElem */
}MGRUpdateparmReset;

typedef struct MGRShowParam
{
	NodeTag		type;
	char		*nodename;
	char		*param;		/*parameter of node*/
}MGRShowParam;

typedef struct MGRMonitorAgent
{
	NodeTag		type;
	List		*hosts;		/* list of Value(String), NIL for ALL */
}MGRMonitorAgent;

typedef struct MGRStartAgent
{
	NodeTag		type;
	List		*hosts;		/* list of Value(String), NIL for ALL */
	char		*password;
}MGRStartAgent;

typedef struct MGRStopAgent
{
	NodeTag		type;
	List		*hosts;		/* list of Value(String), NIL for ALL */
}MGRStopAgent;

#endif /* MGR_NODE_H */
