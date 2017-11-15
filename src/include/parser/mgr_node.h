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
	char		nodetype;	/*gtm/coordinator/datanode master/slave/extern*/
	char		*mastername;
	char		*name;			/* node name */
	List		*options;		/* list of DefElem */
}MGRAddNode;

typedef struct MGRAlterNode
{
	NodeTag		type;
	char		nodetype;	/*gtm/coordinator/datanode master/slave/extern*/
	char		*name;			/* node name */
	List		*options;		/* list of DefElem */
}MGRAlterNode;

typedef struct MGRDropNode
{
	NodeTag		type;
	char		nodetype;	/*gtm/coordinator/datanode master/slave/extern*/
	char		*name;		/* list of A_Const(String) */
}MGRDropNode;

typedef struct MGRUpdateparm
{
	NodeTag		type;
	char		parmtype;
	char		*nodename;
	char		nodetype;
	bool		is_force;
	List		*options;		/* list of DefElem */
}MGRUpdateparm;

typedef struct MGRUpdateparmReset
{
	NodeTag		type;
	char		parmtype;
	char		*nodename;
	char		nodetype;
	bool		is_force;
	List		*options;		/* list of DefElem */
}MGRUpdateparmReset;

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

typedef struct MGRFlushHost
{
	NodeTag		type;
}MGRFlushHost;

typedef struct MonitorJobitemAdd
{
	NodeTag		type;
	bool			if_not_exists;
	char			*name;
	List		*options;
}MonitorJobitemAdd;

typedef struct MonitorJobitemAlter
{
	NodeTag		type;
	char			*name;
	List			*options;
}MonitorJobitemAlter;

typedef struct MonitorJobitemDrop
{
	NodeTag		type;
	bool			if_exists;
	List			*namelist;
}MonitorJobitemDrop;

typedef struct MonitorJobAdd
{
	NodeTag		type;
	bool			if_not_exists;
	char			*name;
	List			*options;
}MonitorJobAdd;

typedef struct MonitorJobAlter
{
	NodeTag		type;
	char			*name;
	List			*options;
}MonitorJobAlter;

typedef struct MonitorJobDrop
{
	NodeTag		type;
	bool			if_exists;
	List			*namelist;
}MonitorJobDrop;

typedef struct MgrExtensionAdd
{
	NodeTag         type;
	char		cmdtype;
	char		*name;
}MgrExtensionAdd;

typedef struct MgrExtensionDrop
{
	NodeTag         type;
	char            cmdtype;
	char            *name;
}MgrExtensionDrop;

typedef struct MgrRemoveNode
{
	NodeTag         type;
	char            nodetype;
	List            *names;
}MgrRemoveNode;

typedef struct MGRSetClusterInit
{
        NodeTag         type;
}MGRSetClusterInit;

typedef struct MonitorDeleteData
{
	NodeTag         type;
	int32		days;
}MonitorDeleteData;

typedef struct ClusterSlotInitStmt
{
	NodeTag		type;
	List		*options;
}ClusterSlotInitStmt;

#endif /* MGR_NODE_H */
