#ifndef ADB_LOADER_QUEUE
#define ADB_LOADER_QUEUE

#include "linebuf.h"

typedef struct QueueElement
{
  dlist_node  node;
  LineBuffer *lineBuffer;
} QueueElement;

typedef struct MessageQueue
{
	char 		*	name;
	int 			max_size;
	int 			cur_size;
	dlist_head 		queue_head;
	pthread_cond_t  product_cond;
	pthread_mutex_t queue_mutex;
	pthread_mutex_t product_mtx;
} MessageQueue;

extern  void mq_init (MessageQueue *queue, int max_size, char *name);

/**
* @brief put
* 
* add new LineBuffer pointer to tail of msg queue, if msg queue is full, wait
* @param  MessageQueue * : message queue
* @param  LineBuffer     : message value
* @return int 1 : sucess; 0: failed 
*/
extern int mq_put (MessageQueue *queue, LineBuffer* lineBuffer);

extern int mq_put_element (MessageQueue *queue, QueueElement *element);


/**
* @brief put_batch
* 
* add new LineBuffer pointer to tail of msg queue, if msg queue is full, wait
* @param  MessageQueue * queue	: message queue
* @param  LineBuffer lineBuffer	: message values
* @param  int   size			: batch size
* @return int 1 : sucess; 0: failed 
*/
extern int mq_put_batch(MessageQueue *queue, LineBuffer ** lineBuffer, int size);

/**
* @brief poll
* 
* return LineBuffer pointer from head of msg queue, if msg queue is empty, return null
* caller need to pfree element memory
* @param  MessageQueue * : message queue
* @return QueueElement * : get QueueElement from queue head
*         NULL			 : queue is empty  
*/
extern QueueElement* mq_poll(MessageQueue *queue);

/**
* @brief poll_batch
* 
* return LineBuffer pointer from head of msg queue, if msg queue is empty, return null
* caller need to pfree element memory
* @param  MessageQueue * queue : 
*						message queue
* @param  QueueElement *element_array:
*						get batch message from queue
* @param  int	size	 	   : 
*						batch size
* @return int * 	    > 0    :
*						size of element_array	
*						= 0    :    			 	   
*						queue is empty  
*/
extern int mq_poll_batch(MessageQueue *queue, QueueElement **element_array, int size);

extern bool mq_empty (MessageQueue *queue);

extern void mq_destory(MessageQueue *queue);
#endif
