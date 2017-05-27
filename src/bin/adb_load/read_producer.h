#ifndef ADB_LOAD_READ_PRODUCER_H
#define ADB_LOAD_READ_PRODUCER_H

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
	bool             roundrobin;
	int              end_flag_num;
	char            *start_cmd;
	int              read_file_buffer;

	int             *redo_queue_index;
	int              redo_queue_total;
	bool             redo_queue;
	bool             filter_first_line;
	bool             stream_mode;
} ReadInfo;

typedef enum ReadProducerState
{
	READ_PRODUCER_PROCESS_DEFAULT,
	READ_PRODUCER_PROCESS_OK,
	READ_PRODUCER_PROCESS_ERROR,
	READ_PRODUCER_PROCESS_EXIT_BY_CALLER,
	READ_PRODUCER_PROCESS_COMPLETE
} ReadProducerState;

extern int init_read_thread(ReadInfo *read_info);
extern ReadProducerState GetReadModule(void);
extern bool check_need_redo_queue(int redo_queue_total, int *redo_queue_index, int flag);
extern int get_filter_queue_file_fd_index(int redo_queue_total, int *redo_queue_index, int flag);
extern void stop_read_thread(void);

#endif /* ADB_LOAD_READ_PRODUCER_H */