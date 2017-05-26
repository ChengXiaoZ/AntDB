#ifndef ADB_LOAD_MSG_QUEUE_PIPE_H
#define ADB_LOAD_MSG_QUEUE_PIPE_H

#include "linebuf.h"

typedef struct QueueElementPipe
{
  LineBuffer *lineBuffer;
  LineBuffer *hash;
  bool		 finish;
} QueueElementPipe;

typedef struct MessageQueuePipe
{
	char 			*name;
	int				fd[2];
	bool			write_lock;
	pthread_mutex_t write_queue_mutex;
	pthread_mutex_t read_queue_mutex;
} MessageQueuePipe;

/* main thread need to init queue */
extern  void mq_pipe_init (MessageQueuePipe *queue, char *name);

extern int mq_pipe_put (MessageQueuePipe *queue, LineBuffer* lineBuffer);

extern int mq_pipe_put_element (MessageQueuePipe *queue, QueueElementPipe *element);

extern int mq_pipe_put_batch (MessageQueuePipe *queue, LineBuffer ** lineBuffer, int size);

extern LineBuffer * mq_pipe_poll (MessageQueuePipe *queue);

extern void mq_pipe_destory (MessageQueuePipe *queue);

#endif /* ADB_LOAD_MSG_QUEUE_PIPE_H */
