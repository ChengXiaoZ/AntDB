#include "postgres_fe.h"

#include "compute_hash.h"
#include "msg_queue_pipe.h"
#include "nodes/pg_list.h"
#include "read_write_file.h"

int main(int argc, char **argv)
{
	MessageQueuePipe 	 *	input_queue;
	MessageQueuePipe 	 **	output_queue;
	char             *	func_name = "add";
	char 			 *	conninfo;
	int					thread_nums = 1;
	int					output_queue_size = 3;
	HashField		 *  field;
	int 			 	loc[2] = {3,1};
	Oid				 	type[2] = {5433,5434};
	Oid				 	node[2] = {15433,14561};
	const char 		 *  string1 = "123,2,\"54,32\",a,b,c,d,e,f,g,abcdefg";
	const char 		 *  string2 = "321,2,\"45\",a,b,c,d,e,f,g,abcdefg";
	const char		 *	string3 = "flag";
	LineBuffer		 * buf1 = NULL;
	LineBuffer		 * buf2 = NULL;
	LineBuffer		 * buf3 = NULL;

	init_linebuf(2);
	buf1 = get_linebuf();
	buf2 = get_linebuf();
	buf3 = get_linebuf();	

	appendLineBufInfoString(buf1, string1);
	appendLineBufInfoString(buf2, string2);
	appendLineBufInfoString(buf3, string3);

	input_queue = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	output_queue = (MessageQueuePipe**)palloc0(sizeof(MessageQueuePipe*) * output_queue_size);
	mq_pipe_init(input_queue, "input_queue");
	output_queue[0] = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	output_queue[0]->write_lock = true;
	mq_pipe_init(output_queue[0], "output_queue0");
	output_queue[1] = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	output_queue[1]->write_lock = true;
	mq_pipe_init(output_queue[1], "output_queue1");
	output_queue[2] = (MessageQueuePipe*)palloc0(sizeof(MessageQueuePipe));
	output_queue[2]->write_lock = true;
	mq_pipe_init(output_queue[2], "output_queue2");
	mq_pipe_put(input_queue, buf1);
	mq_pipe_put(input_queue, buf2);

	field = (HashField *)palloc0(sizeof(HashField));
	conninfo = "user=lvcx host=localhost port=15436 dbname=postgres";
	field->field_nums = 2;
	field->field_loc = loc;
	field->field_type = type;
	field->datanodes_num = 2;
	field->node_list = node;
	field->delim = ",";
	field->hash_delim = ",";
	field->copy_options = "with (DELIMITER ',', FORMAT csv, QUOTE '\"')";
	field->quotec = '"';
	field->has_qoute = true;

	fopen_error_file(NULL);
	init_hash_compute(thread_nums, func_name, conninfo, input_queue,
		output_queue, output_queue_size, field);

	mq_pipe_put(input_queue, NULL);
	
	for(;;)
	{}
	fclose_error_file();
	return 0;
}

