#include "postgres_fe.h"

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include "msg_queue.h"
#include "lib/ilist.h"
#include "adbloader_log.h"

static bool full (MessageQueue *queue);

bool
full (MessageQueue *queue)
{
	return (queue->cur_size == queue->max_size);
}

bool
mq_empty (MessageQueue *queue)
{
	return dlist_is_empty(&queue->queue_head);
}

void
mq_init (MessageQueue *queue, int max_size, char *name)
{
	Assert(queue != NULL && NULL != name);
	if(pthread_mutex_init(&queue->queue_mutex, NULL) != 0 ||
		pthread_mutex_init(&queue->product_mtx, NULL) != 0)
	{
		fprintf(stderr, "Can not initialize mutex:%s\n", strerror(errno));
		exit(1);
	}
	//queue->product_cond = PTHREAD_COND_INITIALIZER;
	queue->max_size = max_size;
	queue->name = (char *)palloc0(strlen(name) + 1);
	sprintf(queue->name, "%s", name);
	queue->name[strlen(name)] = '\0';
}

int
mq_put (MessageQueue *queue, LineBuffer* lineBuffer)
{
	QueueElement *element;

	Assert(queue != NULL && lineBuffer != NULL);
	element = (QueueElement *)palloc0(sizeof(QueueElement));
	element->lineBuffer = lineBuffer;
	mq_put_element(queue, element);
/*
	if(full(queue))
	{
		fprintf(stderr, "queue %s is full, max size is :%d \n", queue->name, queue->max_size);
		pthread_mutex_lock(&queue->product_mtx);
		pthread_cond_wait(&queue->product_cond, &queue->product_mtx);
		fprintf(stderr, "queue %s continue working \n", queue->name);
		pthread_mutex_unlock(&queue->product_mtx);
	}
	pthread_mutex_lock(&queue->queue_mutex);
	dlist_push_tail(&queue->queue_head, &element->node);
	queue->cur_size++;
	pthread_mutex_unlock(&queue->queue_mutex);
*/
	return 1;
}

int 
mq_put_element (MessageQueue *queue, QueueElement *element)
{
	Assert(queue != NULL && element != NULL);
	if(full(queue))
	{
		ADBLOADER_LOG(LOG_DEBUG,
			"queue %s is full, max size is :%d", queue->name, queue->max_size);
		pthread_mutex_lock(&queue->product_mtx);
		pthread_cond_wait(&queue->product_cond, &queue->product_mtx);
		ADBLOADER_LOG(LOG_DEBUG,
			"queue %s continue working", queue->name);
		pthread_mutex_unlock(&queue->product_mtx);
	}
	pthread_mutex_lock(&queue->queue_mutex);
	dlist_push_tail(&queue->queue_head, &element->node);
	queue->cur_size++;
	pthread_mutex_unlock(&queue->queue_mutex);
	return 1;
}

int
mq_put_batch (MessageQueue *queue, LineBuffer ** lineBuffer, int size)
{
	QueueElement *element;
	int			  flag;

	Assert(queue != NULL && lineBuffer != NULL && size > 0);	
	pthread_mutex_lock(&queue->queue_mutex);
	for(flag = 0; flag < size; flag++)
	{
		element = (QueueElement *)palloc0(sizeof(QueueElement));
		element->lineBuffer = lineBuffer[flag];
		if(full(queue))
		{
			pthread_mutex_unlock(&queue->queue_mutex);

			ADBLOADER_LOG(LOG_DEBUG,
					"queue %s is full, max size is :%d", queue->name, queue->max_size);
			pthread_mutex_lock(&queue->product_mtx);
			pthread_cond_wait(&queue->product_cond, &queue->product_mtx);
			ADBLOADER_LOG(LOG_DEBUG,
					"queue %s continue working", queue->name);
			pthread_mutex_unlock(&queue->product_mtx);

			pthread_mutex_lock(&queue->queue_mutex);
		}
		dlist_push_tail(&queue->queue_head, &element->node);
		queue->cur_size++;
	}
	pthread_mutex_unlock(&queue->queue_mutex);
	return 1;
}

QueueElement*
mq_poll (MessageQueue *queue)
{
	dlist_node 	 *node;
	QueueElement *element;

	Assert(queue != NULL);
	pthread_mutex_lock(&queue->queue_mutex);
	if(mq_empty(queue))
	{
		pthread_mutex_unlock(&queue->queue_mutex);
		return NULL;
	}
	node = dlist_pop_head_node(&queue->queue_head);
	queue->cur_size--;
	pthread_mutex_unlock(&queue->queue_mutex);
	/* wakeup producer */
	pthread_cond_signal(&queue->product_cond);

	ADBLOADER_LOG(LOG_DEBUG,
		"queue %s send singnal to put message ", queue->name);
	element = dlist_container(QueueElement, node, node);
	return element;
}

int
mq_poll_batch (MessageQueue *queue, QueueElement **element_array, int size)
{
	int 		 flag ;
	dlist_node 	 *node;	

	Assert(queue != NULL && element_array != NULL && size > 0);
	pthread_mutex_lock(&queue->queue_mutex);	
	for(flag = 0; flag < size; flag++)
	{
		if(mq_empty(queue))
		{
			pthread_mutex_unlock(&queue->queue_mutex);
			return flag;		
		}
		node = dlist_pop_head_node(&queue->queue_head);
		queue->cur_size--;
		element_array[flag] = dlist_container(QueueElement, node, node);
	}
	pthread_mutex_unlock(&queue->queue_mutex);
	return flag;
}

void
mq_destory (MessageQueue *queue)
{
	Assert(queue != NULL);
	pthread_mutex_unlock(&queue->queue_mutex);
	while(!dlist_is_empty(&queue->queue_head))
	{
		dlist_node 	 *node;
		QueueElement *element;
		node = dlist_pop_head_node(&queue->queue_head);
		element = dlist_container(QueueElement, node, node);
		if (NULL != element->lineBuffer)
			release_linebuf(element->lineBuffer);
		element->lineBuffer = NULL;
		pfree(element);
	}
	pfree(queue->name);
	pthread_mutex_unlock(&queue->queue_mutex);
	pfree(queue);
	queue = NULL;
}

