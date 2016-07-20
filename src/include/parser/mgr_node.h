/*
 * nodes for manager
 */

#ifndef MGR_NODE_H
#define MGR_NODE_H

#include "nodes/nodes.h"
#include "nodes/pg_list.h"

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
	int			nodetype;  /*0 gtm/1 coordinator/2 datanode*/
	int			innertype; /*0 master/1 slave/2 extern*/
	char		*name;			/* node name */
	char		*mastername;	/*master name*/
	List		*options;		/* list of DefElem */
}MGRAddNode;

typedef struct MGRAlterNode
{
	NodeTag		type;
	bool		if_not_exists;
	int			nodetype;  /*0 gtm/1 coordinator/2 datanode*/
	int			innertype; /*0 master/1 slave/2 extern*/
	char		*name;			/* node name */
	char		*mastername;	/*master name*/
	List		*options;		/* list of DefElem */
}MGRAlterNode;

typedef struct MGRDropNode
{
	NodeTag		type;
	bool		if_exists;
	int			nodetype;  /*0 gtm/1 coordinator/2 datanode*/
	int			innertype; /*0 master/1 slave/2 extern*/
	List		*hosts;		/* list of A_Const(String) */
}MGRDropNode;

typedef struct MGRDeplory
{
	NodeTag		type;
	List		*hosts;		/* list of Value(String), NIL for ALL */
	char		*password;
}MGRDeplory;

#endif /* MGR_NODE_H */
