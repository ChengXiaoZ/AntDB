#ifndef READ_PRODUCER_H
#define READ_PRODUCER_H

#include "msg_queue_pipe.h"

#define READ_PRODUCER_ERROR    0
#define READ_PRODUCER_OK       1

typedef struct ReadInfo
{
	char             *filepath;
	MessageQueuePipe *input_queue;
	
	MessageQueuePipe **output_queue;
	int              output_queue_num;
	
	int              datanodes_num;
	int              threads_num_per_datanode;
	
	bool             replication;
	int              end_flag_num;
	char            *start_cmd;
	int              read_file_buffer;
	
	int             *redo_queue_index;
	int              redo_queue_total;
	bool             redo_queue;
} ReadInfo;

typedef enum ReadProducerState
{
	READ_PRODUCER_PROCESS_DEFAULT,
	READ_PRODUCER_PROCESS_OK,
	READ_PRODUCER_PROCESS_ERROR,
	READ_PRODUCER_PROCESS_EXIT_BY_CALLER,
	READ_PRODUCER_PROCESS_COMPLETE
} ReadProducerState;

#if 0
int
init_read_thread(char *filepath, MessageQueuePipe *input_queue, 
				MessageQueuePipe **output_queue, int output_queue_num, 
				int datanodes_num, int threads_num_per_datanode,
				bool replication, int end_flag_num, char * start_cmd,
				int read_file_buffer, int *redo_queue_index, int redo_queue_total,
				bool redo_queue);
#endif

int
init_read_thread(ReadInfo *read_info);

ReadProducerState GetReadModule(void);

bool check_need_redo_queue(int redo_queue_total, int *redo_queue_index, int flag);

int get_filter_queue_file_fd_index(int redo_queue_total, int *redo_queue_index, int flag);

void set_read_producer_exit(void);
#endif