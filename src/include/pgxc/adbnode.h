#ifndef ADB_NODE_H_
#define ADB_NODE_H_

#include "libpq/libpq-fe.h"
#include "nodes/pg_list.h"

typedef enum ADBNodeType
{
	ADB_NODE_TYPE_COORD = 1,
	ADB_NODE_TYPE_DATA = 2
}ADBNodeType

typedef struct AdbNodeConnInfo
{
	ADBNodeType	node_type;
	Oid			node_oid;
	NameData	node_name;
	PGconn	   *node_conn;
}AdbNodeConnInfo;

typedef struct ADBNodeAllConn
{
	int					dn_count;
	int					co_count;
	AdbNodeConnInfo	  **dn_conns;
	AdbNodeConnInfo	  **co_conns;
	AdbNodeConnInfo	  *primary_conn;
	AdbNodeConnInfo	  *all_conns[1];
} ADBNodeAllConn;

/* don't change it */
extern List *intlist_datanodes;
extern List *intlist_coord;

extern void InitMultinodeExecutor(bool is_force);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);

extern ADBNodeAllConn *adb_get_connections(const List *datanodelist, const List *coordlist, int primary_node);
extern PGresult* adb_get_result(const List *list, AdbNodeConnInfo **ppconn_from);
extern void pfree_adb_all_connections(ADBNodeAllConn *handles);
extern void adb_release_connections(void);

extern void adb_cancel_query(void);
extern void adb_clear_connections(void);

#endif /* ADB_NODE_H_ */
