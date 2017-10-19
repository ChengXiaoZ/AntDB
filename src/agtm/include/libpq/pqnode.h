#ifndef PQ_NODE_H
#define PQ_NODE_H

typedef struct pq_comm_node pq_comm_node;

extern List* get_all_pq_node(void);
extern pgsocket socket_pq_node(pq_comm_node *node);
extern bool pq_node_send_pending(pq_comm_node *node);
extern bool pq_node_is_write_only(pq_comm_node *node);
extern int	pq_node_flush_sock(pq_comm_node *node);
extern void pq_node_new(pgsocket sock);
extern int	pq_node_recvbuf(pq_comm_node *node);
extern void pq_node_close(pq_comm_node *node);
extern int	pq_node_get_msg(StringInfo s, pq_comm_node *node);
extern void pq_node_switch_to(pq_comm_node *node);

#endif /* PQ_NODE_H */
