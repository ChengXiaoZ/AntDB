#include "postgres_fe.h"

#include "read_producer.h"
#include "msg_queue_pipe.h"
#include "linebuf.h"


int main(int argc, char **argv)
{
	char *filepath = "test.txt";
	int   datanode_num = 1;
	int   flag;
	MessageQueuePipe 	 *	input_queue;
	MessageQueuePipe	 ** output_queue;

	output_queue = (MessageQueuePipe**)palloc0(sizeof(MessageQueuePipe*) * datanode_num);
	for (flag = 0 ; flag < datanode_num; flag ++)
	{
		output_queue[flag] = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
		mq_pipe_init(output_queue[flag], "output_queue");
	}
	init_linebuf(2);
	input_queue = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	mq_pipe_init(input_queue, "input_queue");
	init_read_thread(filepath, input_queue, output_queue, datanode_num, true, 0, "start");
	for (;;)
	{
	}
	return 0;
}

