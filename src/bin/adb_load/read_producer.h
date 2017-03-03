#ifndef READ_PRODUCER_H
#define READ_PRODUCER_H

#include "msg_queue_pipe.h"

#define	READ_PRODUCER_ERROR		0
#define	READ_PRODUCER_OK		1

typedef enum ReadProducerState
{
	READ_PRODUCER_PROCESS_DEFAULT,
	READ_PRODUCER_PROCESS_OK,
	READ_PRODUCER_PROCESS_ERROR,
	READ_PRODUCER_PROCESS_EXIT_BY_CALLER,
	READ_PRODUCER_PROCESS_COMPLETE
} ReadProducerState;

int InitReadProducer(char *filepath, MessageQueuePipe *input_queue, MessageQueuePipe **output_queue,
							int out_queue_size, bool replication, int end_flag_num, char * start_cmd);

ReadProducerState GetReadModule(void);

void SetReadProducerExit(void);
#endif