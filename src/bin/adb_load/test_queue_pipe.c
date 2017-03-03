#include "postgres_fe.h"

#include <pthread.h>
#include <unistd.h>

#include "linebuf.h"
#include "msg_queue_pipe.h"
static void * thread_func(void *argp);

void * thread_func(void *argp)
{
	MessageQueuePipe * msgQueue = (MessageQueuePipe *)argp;	
	while(1)
	{
		LineBuffer* linebuffer = mq_pipe_poll(msgQueue);
		sleep(10);		
		if(NULL != linebuffer)
			fprintf(stderr, "buff content : %s \n", linebuffer->data);
	}
	return NULL;
}

int main(int argc, char **argv)
{
	MessageQueuePipe *queue = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	pthread_t thread_id = 0;
	const char * string = "abcdefg";
	LineBuffer* buf = NULL;
	int errno;
	init_linebuf(2);
	buf = get_linebuf();
	appendLineBufInfoString(buf, string);
	mq_pipe_init(queue, "test_queue");
	if ((errno =  pthread_create(&thread_id, NULL, thread_func, queue)) < 0)
	{
		fprintf(stderr, "create thread error");
		return 0;
	}

	mq_pipe_put(queue, buf);
	mq_pipe_put(queue, buf);
	sleep(10000);
	return 0;
}


