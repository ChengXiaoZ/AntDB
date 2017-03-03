#include "postgres_fe.h"
#include "dispatch.h"
#include "msg_queue_pipe.h"
#include "adbloader_log.h"
#include "read_write_file.h"
int main(int argc, char **argv)
{
	int 				 flag;
	MessageQueuePipe	 **output_queue;
	int					 datanode_size = 1;
	DatanodeInfo		 *datanode_info;
	char				 *connect_str_dn1 = "user=adb2.2 host=10.1.226.202 port=17998 dbname=postgres options='-c grammar=postgres -c remotetype=coordinator  -c lc_monetary=C -c DateStyle=iso,mdy -c	timezone=prc -c	geqo=on	-c intervalstyle=postgres'user=adb2.2 host=10.1.226.202 port=17998 dbname=postgres options='-c grammar=postgres -c remotetype=coordinator  -c lc_monetary=C -c DateStyle=iso,mdy -c	timezone=prc -c	geqo=on	-c intervalstyle=postgres'";
//	char				 *connect_str = "user=lvcx host=localhost port=15436 dbname=postgres";
	DispatchInfo 		 *dispatch = NULL;
	LineBuffer		 	* buf1 = NULL;
	LineBuffer		 	* buf2 = NULL;
	LineBuffer		 	* buf3 = NULL;
	const char 		 	* string1 = "123\n";
	const char 			* string2 = "abc\n";
//	const char 			* string2 = "456\n";
	const char			* string3 = "789\n";

	init_linebuf(2);
	output_queue = (MessageQueuePipe**)palloc0(sizeof(MessageQueuePipe*) * datanode_size);
	datanode_info = (DatanodeInfo*)palloc0(sizeof(DatanodeInfo));
	datanode_info->node_nums = datanode_size;
	datanode_info->datanode = (Oid *)palloc0(sizeof(Oid) * datanode_size);
	datanode_info->datanode[0] = 12345;
//	datanode_info->datanode[1] = 23456;
//	datanode_info->datanode[2] = 34567;
	datanode_info->conninfo = (char **)palloc0(sizeof(char *) * datanode_size);
	datanode_info->conninfo[0] = connect_str_dn1;
//	datanode_info->conninfo[1] = connect_str;
//	datanode_info->conninfo[2] = connect_str;
	for (flag = 0; flag < datanode_size; flag++)
	{
		output_queue[flag] = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	}
	mq_pipe_init(output_queue[0], "datanode0");
//	mq_pipe_init(output_queue[1], "datanode1");
//	mq_pipe_init(output_queue[2], "datanode2");

	dispatch = (DispatchInfo *)palloc0(sizeof(DispatchInfo));
	dispatch->conninfo_agtm	= "user=postgres host=10.1.226.202 port=12998 dbname=postgres options='-c lc_monetary=C -c DateStyle=iso,mdy -c	timezone=prc -c	geqo=on	-c intervalstyle=postgres'";
	dispatch->thread_nums = datanode_size;
	dispatch->output_queue = output_queue;
	dispatch->datanode_info = datanode_info;
	dispatch->table_name = "test";
	dispatch->copy_options = "with (DELIMITER ',')";

 	adbLoader_log_init(NULL, LOG_INFO);
	fopen_error_file(NULL);
	InitDispatch(dispatch, TABLE_REPLICATION);

	buf1 = get_linebuf();
	buf2 = get_linebuf();
	buf3 = get_linebuf();	

	appendLineBufInfoString(buf1, string1);
	appendLineBufInfoString(buf2, string2);
	appendLineBufInfoString(buf3, string3);

	mq_pipe_put(output_queue[0], buf1);
	mq_pipe_put(output_queue[0], buf2);
	mq_pipe_put(output_queue[0], buf3);
	mq_pipe_put(output_queue[0], NULL);

	for (;;)
	{

	}
	pfree(dispatch);
	dispatch = NULL;
	fclose_error_file();
	adbLoader_log_end();
	return 0;
}

