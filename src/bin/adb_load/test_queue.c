#include "postgres_fe.h"

#include <pthread.h>
#include <unistd.h>

#include "linebuf.h"
#include "msg_queue.h"

static void * thread_func(void *argp);

void * thread_func(void *argp)
{
	MessageQueue * msgQueue = (MessageQueue *)argp;	
	while(1)
	{
		QueueElement* element = NULL;
		sleep(10);
		element = mq_poll(msgQueue);
		if(NULL != element)
			fprintf(stderr, "buff content : %s \n", element->lineBuffer->data);
	}
	return NULL;
}
int main(int argc, char **argv)
{
	MessageQueue *queue = (MessageQueue*)palloc0(sizeof(MessageQueue));
	pthread_t thread_id = 0;
	const char * string = "abcdefg";
	int errno;
	LineBuffer* buf = NULL;
	init_linebuf(2);
	buf = get_linebuf();
	appendLineBufInfoString(buf, string);
	mq_init(queue, 1, "test_queue");
	if (errno =  pthread_create(&thread_id, NULL,
									thread_func, queue))
	{
		fprintf(stderr, "create thread error");
		return 0;
	}
/*	errno = pthread_mutex_lock(&queue->product_mtx);
	errno = pthread_cond_wait(&queue->product_cond, &queue->product_mtx);
	errno = pthread_mutex_lock(&queue->product_mtx);
*/
	mq_put(queue, buf);
	mq_put(queue, buf);
	pthread_mutex_lock(&queue->product_mtx);
	sleep(10000);
	return 0;
}
