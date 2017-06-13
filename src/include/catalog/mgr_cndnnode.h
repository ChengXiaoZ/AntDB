
#ifndef MGR_CNDNNODE_H
#define MGR_CNDNNODE_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */


#define NodeRelationId 4948

CATALOG(mgr_node,4948)
{
	NameData	nodename;		/* node name */
	Oid			nodehost;		/* node hostoid from host*/
	char		nodetype;		/* node type */
	NameData		nodesync;		/* node sync for slave/extra */
	int32		nodeport;		/* node port */
	bool		nodeinited;		/* is initialized */
	Oid			nodemasternameoid;	/* 0 stands for the node is not slave*/
	bool		nodeincluster;		/*check the node in cluster*/
#ifdef CATALOG_VARLEN
	text		nodepath;		/* node data path */
#endif						/* CATALOG_VARLEN */
} FormData_mgr_node;

/* ----------------
 *		Form_mgr_node corresponds to a pointer to a tuple with
 *		the format of mgr_nodenode relation.
 * ----------------
 */
typedef FormData_mgr_node *Form_mgr_node;

/* ----------------
 *		compiler constants for mgr_node
 * ----------------
 */
#define Natts_mgr_node							9
#define Anum_mgr_node_nodename					1
#define Anum_mgr_node_nodehost					2
#define Anum_mgr_node_nodetype					3
#define Anum_mgr_node_nodesync					4
#define Anum_mgr_node_nodeport					5
#define Anum_mgr_node_nodeinited				6
#define Anum_mgr_node_nodemasternameOid			7
#define Anum_mgr_node_nodeincluster				8
#define Anum_mgr_node_nodepath					9

#define CNDN_TYPE_COORDINATOR_MASTER		'c'
#define CNDN_TYPE_COORDINATOR_SLAVE			's'
#define CNDN_TYPE_DATANODE_MASTER			'd'
#define CNDN_TYPE_DATANODE_SLAVE			'b'
#define CNDN_TYPE_DATANODE_EXTRA			'n'

/*no nodetype has this type '\0'*/
#define CNDN_TYPE_NONE_TYPE '\0'

#define GTM_TYPE_GTM_MASTER			'g'
#define GTM_TYPE_GTM_SLAVE			'p'
#define GTM_TYPE_GTM_EXTRA			'e'

/*CNDN_TYPE_DATANODE include : datanode master,slave ,extra*/
#define CNDN_TYPE_DATANODE		'D'
#define CNDN_TYPE_GTM			'G'

#define SHUTDOWN_S  "smart"
#define SHUTDOWN_F  "fast"
#define SHUTDOWN_I  "immediate"
#define TAKEPLAPARM_N  "none"

typedef enum AGENT_STATUS
{
	AGENT_DOWN = 4, /*the number is enum PGPing max_value + 1*/
	AGENT_RUNNING
}agent_status;

struct enum_sync_state
{
	int type;
	char *name;
};

typedef enum SYNC_STATE
{
	SYNC_STATE_SYNC,
	SYNC_STATE_ASYNC,
	SYNC_STATE_POTENTIAL,
}sync_state;

extern bool with_data_checksums;
#endif /* MGR_CNDNNODE_H */
