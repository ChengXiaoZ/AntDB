#include "postgres_fe.h"

#include <libpq-fe.h>
#include <unistd.h>
#include <sys/stat.h>

#include "getopt_long.h"
#include "loadsetting.h"
#include "linebuf.h"
#include "properties.h"
#include "utility.h"

static const char *SERVER_PORT = "SERVER_PORT";
static const char *SERVER_IP   = "SERVER_IP";
static const char *SERVER_USER = "SERVER_USER";
static const char *SERVER_DB   = "SERVER_DB";

static const char *AGTM_PORT   = "AGTM_PORT";
static const char *AGTM_IP     = "AGTM_IP";
static const char *__AGTM_USER = "AGTM_USER";
static const char *AGTM_DB     = "AGTM_DB";

static const char *COORDINATOR_PORT = "COORDINATOR_PORT";
static const char *COORDINATOR_IP   = "COORDINATOR_IP";
static const char *COORDINATOR_USER = "COORDINATOR_USER";
static const char *COORDINATOR_DB   = "COORDINATOR_DB";

static const char *DATANODE_VALID   = "DATANODE_VALID";
static const char *DATANODE_NUM     = "DATANODE_NUM";
static const char *THREADS_NUM_PER_DATANODE = "THREADS_NUM_PER_DATANODE";
static const char *DATANODE1_NAME   = "DATANODE1_NAME";
static const char *DATANODE1_IP     = "DATANODE1_IP";
static const char *DATANODE1_PORT   = "DATANODE1_PORT";
static const char *DATANODE1_USER   = "DATANODE1_USER";
static const char *DATANODE1_DB     = "DATANODE1_DB";

static const char *HASH_THREAD_NUM  = "HASH_THREAD_NUM";
static const char *COPY_DELIMITER   = "COPY_DELIMITER";
static const char *COPY_NULL        = "COPY_NULL";

/* LOG INFO WARNNING, ERROR */
static const char *LOG_LEVEL         = "LOG_LEVEL";
static const char *LOG_PATH          = "LOG_PATH";
static const char *FILTER_QUEUE_FILE_PATH = "FILTER_QUEUE_FILE_PATH";
static const char *ERROR_DATA_FILE_PATH = "ERROR_DATA_FILE_PATH";
static const char *PROCESS_BAR       = "PROCESS_BAR";
static const char *COPY_CMD_COMMENT  = "COPY_CMD_COMMENT";
static const char *COPY_CMD_COMMENT_STR = "COPY_CMD_COMMENT_STR";
static const char *FILTER_FIRST_LINE = "FILTER_FIRST_LINE";
static const char *READ_FILE_BUFFER  = "READ_FILE_BUFFER";
static const char *ERROR_THRESHOLD   = "ERROR_THRESHOLD";

static void print_help(FILE *fd);
static char *replace_string(const char *string, const char *replace, const char *replacement);
static char *get_config_file_value(const char *key);
static void check_configfile(ADBLoadSetting *setting);
static void pg_free_hash_config(HashConfig *hash_config);
static void pg_free_log_field(LogField *logfield);
static int get_redo_queue_total(char *optarg);
static int *get_redo_queue(char *optarg, int redo_queue_num);
static char * get_copy_options(char *text_delim, char *copy_null);
static char * get_text_delim(char *text_delim);

#define DEFAULT_CONFIGFILENAME "./adb_load.conf"
#define HASH_DELIM             ","

ADBLoadSetting *
cmdline_adb_load_setting(int argc, char **argv)
{
	static struct option long_options[] = {
		{"copy_cmd_comment",          no_argument, NULL, 'p'},
		{"copy_cmd_comment_str",required_argument, NULL, 'm'},
		{"configfile",          required_argument, NULL, 'c'},
		{"dbname",              required_argument, NULL, 'd'},
		{"dynamic",                   no_argument, NULL, 'y'},
		{"filter_first_line",		  no_argument, NULL, 'n'},
		{"filter_queue_file",         no_argument, NULL, 'e'},
		{"hash_threads",        required_argument, NULL, 'h'},
		{"inputdir",            required_argument, NULL, 'i'},
		{"inputfile",           required_argument, NULL, 'f'},
		{"just_check",                no_argument, NULL, 'j'},
		{"outputdir",           required_argument, NULL, 'o'},
		{"password",            required_argument, NULL, 'W'},
		{"queue",               required_argument, NULL, 'Q'},
		{"static",                    no_argument, NULL, 's'},
		{"stream",                    no_argument, NULL, 'x'},
		{"singlefile",                no_argument, NULL, 'g'},
		{"table",               required_argument, NULL, 't'},
		{"threads_per_datanode",required_argument, NULL, 'r'},
		{"username",            required_argument, NULL, 'U'},
		{"help",                      no_argument, NULL, '?'},
		{"version",                   no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int c;
	int option_index;
	ADBLoadSetting * setting = NULL;

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			print_help(stdout);
			exit(EXIT_SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("adb_load 2.0 (based on " PG_VERSION ")");
			exit(EXIT_SUCCESS);
		}
	}

	setting = (ADBLoadSetting *)palloc0(sizeof(ADBLoadSetting));
	while((c = getopt_long(argc, argv, "c:d:yi:f:o:W:sgt:U:Q:r:h:ejpm:nx", long_options, &option_index)) != -1)
	{
		switch(c)
		{
		case 'c': //configfile
			setting->config_file_path = pg_strdup(optarg);
			break;
		case 'd': //dbname
			setting->database_name = pg_strdup(optarg);
			break;
		case 'e':
			setting->filter_queue_file = true;
			break;
		case 'f': //inputfile
			setting->input_file = pg_strdup(optarg);
			break;
		case 'g': //single file mode
			setting->single_file_mode = true;
			break;
		case 'h':
			setting->hash_thread_num = atoi(optarg);
			break;
		case 'i': //inputdir
			{
				//remove last '/' if user special
				if (optarg[strlen(optarg) - 1] == '/')
					optarg[strlen(optarg) - 1] = '\0';
				setting->input_directory = pg_strdup(optarg);
				break;
			}
		case 'j': //just for check tool
			setting->just_check = true;
			break;
		case 'm':
			{
				if (check_copy_comment_str_valid(optarg))
				{
					setting->copy_cmd_comment_str = pg_strdup(optarg);
				}
				else
				{
					fprintf(stderr, "Error: option -m/copy_cmd_comment_str must be two same char.\n");
					exit(EXIT_FAILURE);
				}
				break;
			}
		case 'n':
			setting->filter_first_line = true;
			break;
		case 'o': //outputdir
			setting->output_directory = pg_strdup(optarg);
			break;
		case 'p':
			setting->copy_cmd_comment = true;
			break;
		case 'Q': //queue
			{
				setting->redo_queue = true;
				setting->redo_queue_total= get_redo_queue_total(optarg);
				setting->redo_queue_index= get_redo_queue(optarg, setting->redo_queue_total);
				break;
			}
		case 'r':
			setting->threads_num_per_datanode = atoi(optarg);
			break;
		case 's': //static mode
			setting->static_mode = true;
			break;
		case 't': //table
			setting->table_name = pg_strdup(adb_load_tolower(optarg));
			break;
		case 'U': //username
			setting->user_name = pg_strdup(optarg);
			break;
		case 'V':
			puts("adb_load 2.0 (based on " PG_VERSION ")");
			exit(EXIT_SUCCESS);
		case 'W': //password
			setting->password = pg_strdup(optarg);
			break;
		case 'x': // data from stdin
			setting->stream_mode = true;
			break;
		case 'y': //dynamic mode
			setting->dynamic_mode = true;
			break;
		case '?':
			print_help(stdout);
			exit(EXIT_SUCCESS);
			break;
		default:
			fprintf(stderr, "Error: unrecognized option \"%c\".\n", c);
			exit(EXIT_FAILURE);
		}
	}

	setting->program = pg_strdup(argv[0]);

	if (setting->filter_queue_file && !setting->redo_queue)
	{
		fprintf(stderr, "Error: options -e/--filter_queue_file and -Q/--queue must be used together.\n");
		exit(EXIT_FAILURE);
	}

	if (setting->threads_num_per_datanode < 0)
	{
		fprintf(stderr, "Error: option -r/--thrds_num_per_dn can not less then 0.\n");
		exit(EXIT_FAILURE);
	}

	if (setting->hash_thread_num < 0)
	{
		fprintf(stderr, "Error: option -h/--hash_thread_num can not less then 0.\n");
		exit(EXIT_FAILURE);
	}

	if (setting->database_name == NULL || setting->user_name == NULL)
	{
		fprintf(stderr, "Error: options -d/--dbname and -U/--username must be given.\n");
		exit(EXIT_FAILURE);
	}

	if ((setting->single_file_mode && setting->static_mode) ||
		(setting->static_mode && setting->dynamic_mode)||
		(setting->single_file_mode && setting->dynamic_mode)||
		(setting->single_file_mode && setting->static_mode && setting->dynamic_mode))
	{
		fprintf(stderr, "Error: options -s/--static, -y/--dysnamic and -g/--singlefile cannot be used together.\n");
		exit(EXIT_FAILURE);
	}

	if (!setting->single_file_mode && !setting->static_mode && !setting->dynamic_mode && !setting->stream_mode)
	{
		fprintf(stderr, "Error: you should specify one of the options -g/--singlefile, -s/--static, -y/--dysnamic and -x/--stream.\n");
		exit(EXIT_FAILURE);
	}

	if (setting->static_mode || setting->dynamic_mode)
	{
		if (setting->input_directory == NULL)
		{
			fprintf(stderr, "Error: you should use option -i/--inputdir to specify input directory.\n");
			exit(EXIT_FAILURE);
		}
		else
		{
			if (!directory_exists(setting->input_directory))
			{
				fprintf(stderr, "Error: inputdir \"%s\" does not exist.\n", setting->input_directory);
				exit(EXIT_FAILURE);
			}
		}
	}

	if (setting->static_mode)
	{
		if (setting->input_file != NULL || setting->table_name != NULL)
		{
			fprintf(stderr, "Error: options -s/--static, -f/--inputfile and -t/--table cannot be used together.\n");
			exit(EXIT_FAILURE);
		}
	}

	if (setting->dynamic_mode)
	{
		if (setting->input_file != NULL || setting->table_name != NULL)
		{
			fprintf(stderr, "Error: options -y/--dysnamic, -f/--inputfile and -t/--table cannot be used together.\n");
			exit(EXIT_FAILURE);
		}
	}

	if (setting->single_file_mode)
	{
		if (setting->input_file == NULL || setting->table_name == NULL)
		{
			fprintf(stderr, "Error: options -f/--inputfile and -t/--table must be used together.\n");
			exit(EXIT_FAILURE);
		}
	}

	if (setting->stream_mode)
	{
		if (setting->input_file != NULL)
		{
			fprintf(stderr, "Error: options -f/--inputfile should not used in stream mode.\n");
			exit(EXIT_FAILURE);
		}
		if (setting->table_name == NULL)
		{
			fprintf(stderr, "Error: options -t/----table should be used in stream mode.\n");
			exit(EXIT_FAILURE);
		}
	}

	check_configfile(setting);

	// check -o option
	if (setting->output_directory != NULL)
	{
		if (!directory_exists(setting->output_directory))
			make_directory(setting->output_directory);
	}

	if (setting->single_file_mode)
	{
		if ((setting->input_file != NULL && setting->table_name == NULL) ||
			(setting->input_file == NULL && setting->table_name != NULL))
		{
			fprintf(stderr, "Error: options -f/--inputfile and -t/--table should be used together.\n");
			exit(EXIT_FAILURE);
		}
	}

	if (setting->input_file != NULL)
	{
		if (!file_exists(setting->input_file))
		{
			fprintf(stderr, "Error: inputfile \"%s\" does not exist.\n", setting->input_file);
			exit(EXIT_FAILURE);
		}
	}

	return setting;
}

static int
get_redo_queue_total(char *optarg)
{
	char *local_optarg = NULL;
	char *token = NULL;
	int   redo_queue_total = 0;
	char *strtok_r_ptr = NULL;

	local_optarg = pg_strdup(optarg);

	token = strtok_r(local_optarg, ",", &strtok_r_ptr);
	while (token != NULL)
	{
		redo_queue_total++;

		token = strtok_r(NULL, ",", &strtok_r_ptr);
	}

	if (redo_queue_total == 0)
	{
		pg_free(local_optarg);
		local_optarg = NULL;

		fprintf(stderr, "Error: please check -Q/--queue option.\n");
		exit(EXIT_FAILURE);
	}

	pg_free(local_optarg);
	local_optarg = NULL;

	return redo_queue_total;
}

static int *
get_redo_queue(char *optarg, int redo_queue_total)
{
	char *local_optarg = NULL;
	int  *redo_queue = NULL;
	char *strtok_r_ptr = NULL;
	char *token = NULL;
	int   i = 0;

	local_optarg = pg_strdup(optarg);
	redo_queue = (int *)palloc0(redo_queue_total * sizeof(int));

	token = strtok_r(local_optarg, ",", &strtok_r_ptr);
	while (token != NULL)
	{
		if (i < redo_queue_total)
		{
			redo_queue[i] = atoi(token);
			i++;
		}
		else
		{
			fprintf(stderr, "Error: please check -Q/--queue option.\n");
			exit(EXIT_FAILURE);
		}

		token = strtok_r(NULL, ",", &strtok_r_ptr);
	}

	return redo_queue;
}

static void
check_configfile(ADBLoadSetting *setting)
{
	int ret;

	if (setting->config_file_path != NULL)
	{
		if (!file_exists(setting->config_file_path))
		{
			fprintf(stderr, "Error: configuration file \"%s\" does not exist.\n", setting->config_file_path);
			exit(EXIT_FAILURE);
		}
		else if ((ret = access(setting->config_file_path, 4)) == -1)
		{
			fprintf(stderr, "Error: No permission to read your config file.\n");
			exit(EXIT_FAILURE);
		}
	}
	else
	{
		if (!file_exists(DEFAULT_CONFIGFILENAME))
		{
			fprintf(stderr, "Error: default config file is not exit, please use -c/--configfile.\n");
			exit(EXIT_FAILURE);
		}
		else if ((ret = access(DEFAULT_CONFIGFILENAME, 4)) == -1)
		{
			fprintf(stderr, "Error: no permission read default config file \"%s\" .\n", DEFAULT_CONFIGFILENAME);
			exit(EXIT_FAILURE);
		}
		else
		{
			setting->config_file_path = pg_strdup(DEFAULT_CONFIGFILENAME);
		}
	}

    return;
}

void
get_node_conn_info(ADBLoadSetting *setting)
{
	char conn_info_string[1024];
	int i = 0;
	sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s "
							"options=\'-c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
							,setting->server_info->user_name
							,setting->server_info->node_host
							,setting->server_info->node_port
							,setting->server_info->database_name);
	setting->server_info->connection = pstrdup(conn_info_string);
	check_node_connection_valid(setting->server_info->node_host, setting->server_info->node_port, setting->server_info->connection);

	sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s "
							"options=\'-c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
							,setting->agtm_info->user_name
							,setting->agtm_info->node_host
							,setting->agtm_info->node_port
							,setting->agtm_info->database_name);
	setting->agtm_info->connection = pstrdup(conn_info_string);
	check_node_connection_valid(setting->agtm_info->node_host, setting->agtm_info->node_port, setting->agtm_info->connection);

	sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s "
							"options=\'-c grammar=postgres -c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
							,setting->coordinator_info->user_name
							,setting->coordinator_info->node_host
							,setting->coordinator_info->node_port
							,setting->coordinator_info->database_name);
	setting->coordinator_info->connection = pstrdup(conn_info_string);
	check_node_connection_valid(setting->coordinator_info->node_host, setting->coordinator_info->node_port, setting->coordinator_info->connection);

	if (setting->config_datanodes_valid)
	{
		if (setting->password != NULL)
		{
			for(i = 0; i < setting->datanodes_num; ++i)
			{
				sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s password=%s "
									"options=\'-c grammar=postgres -c remotetype=coordinator -c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
									,setting->datanodes_info[i]->user_name
									,setting->datanodes_info[i]->node_host
									,setting->datanodes_info[i]->node_port
									,setting->datanodes_info[i]->database_name
									,setting->password);
				setting->datanodes_info[i]->connection = pstrdup(conn_info_string);
			}
		}
		else
		{
			for(i = 0; i < setting->datanodes_num; ++i)
			{
				sprintf(conn_info_string, "user=%s host=%s port=%s dbname=%s "
									"options=\'-c grammar=postgres -c remotetype=coordinator -c lc_monetary=C -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres\'"
									,setting->datanodes_info[i]->user_name
									,setting->datanodes_info[i]->node_host
									,setting->datanodes_info[i]->node_port
									,setting->datanodes_info[i]->database_name);
				setting->datanodes_info[i]->connection = pstrdup(conn_info_string);
			}
		}

		for(i = 0; i < setting->datanodes_num; ++i)
		{
			check_node_connection_valid(setting->datanodes_info[i]->node_host, setting->datanodes_info[i]->node_port, setting->datanodes_info[i]->connection);
		}
	}
}

void
check_node_connection_valid(const char *host_ip, const char *host_port, const char *connection_str)
{
	PGconn *conn = NULL;
	conn = PQconnectdb(connection_str);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Error: Connection to node with(ip = %s, port = %s) failed. \n Error:%s\n",
						host_ip, host_port, PQerrorMessage(conn));
		PQfinish(conn);
		conn = NULL;
		exit(EXIT_FAILURE);
	}

	PQfinish(conn);
	conn = NULL;
}

void
get_settings_by_config_file(ADBLoadSetting *setting)
{
	char num_str[4];
	char *datanode_valid = NULL;
	char *process_bar_config = NULL;
	char *str_ptr = NULL;
	int i = 0;

	if(setting->config_file_path == NULL)
	{
		fprintf(stderr, "Error: the config file must be given.\n");
		exit(EXIT_FAILURE);
	}

	InitConfig(setting->config_file_path);

	setting->server_info = (ServerNodeInfo *)palloc0(sizeof(ServerNodeInfo));
	setting->agtm_info = (NodeInfoData *)palloc0(sizeof(NodeInfoData));
	setting->coordinator_info = (NodeInfoData *)palloc0(sizeof(NodeInfoData));

	setting->server_info->node_host = get_config_file_value(SERVER_IP);
	setting->server_info->node_port = get_config_file_value(SERVER_PORT);
	setting->server_info->user_name = get_config_file_value(SERVER_USER);
	setting->server_info->database_name = get_config_file_value(SERVER_DB);

	setting->agtm_info->node_host = get_config_file_value(AGTM_IP);
	setting->agtm_info->node_port = get_config_file_value(AGTM_PORT);
	setting->agtm_info->user_name = get_config_file_value(__AGTM_USER);
	setting->agtm_info->database_name = get_config_file_value(AGTM_DB);

	setting->coordinator_info->node_host = get_config_file_value(COORDINATOR_IP);
	setting->coordinator_info->node_port = get_config_file_value(COORDINATOR_PORT);
	setting->coordinator_info->user_name = get_config_file_value(COORDINATOR_USER);
	setting->coordinator_info->database_name = get_config_file_value(COORDINATOR_DB);

	process_bar_config = get_config_file_value(PROCESS_BAR);
	if (strcasecmp(process_bar_config, "on") == 0   ||
		strcasecmp(process_bar_config, "true") == 0 ||
		strcasecmp(process_bar_config, "1") == 0)
	{
		setting->process_bar = true;
	}
	else if (strcasecmp(process_bar_config, "off") == 0   ||
		    strcasecmp(process_bar_config, "false") == 0  ||
		    strcasecmp(process_bar_config, "0") == 0)
	{
		setting->process_bar = false;
	}

	datanode_valid = get_config_file_value(DATANODE_VALID);
	if (strcasecmp(datanode_valid, "on") == 0   ||
		strcasecmp(datanode_valid, "true") == 0 ||
		strcasecmp(datanode_valid, "1") == 0)
	{
		str_ptr = get_config_file_value(DATANODE_NUM);
		setting->datanodes_num = atoi(str_ptr);
		pfree(str_ptr);
		str_ptr = NULL;

		setting->datanodes_info = (NodeInfoData **)palloc0(setting->datanodes_num * sizeof(NodeInfoData*));
		for (i = 0; i < setting->datanodes_num; i++)
			setting->datanodes_info[i] = (NodeInfoData *)palloc0(sizeof(NodeInfoData));

		setting->config_datanodes_valid = true;

		for(i=0; i< setting->datanodes_num; ++i)
		{
			sprintf(num_str,"%d",i+1);
			DATANODE1_NAME = replace_string(DATANODE1_NAME, "1", num_str);
			setting->datanodes_info[i]->node_name = get_config_file_value(DATANODE1_NAME);
			DATANODE1_IP = replace_string(DATANODE1_IP, "1", num_str);
			setting->datanodes_info[i]->node_host = get_config_file_value(DATANODE1_IP);
			DATANODE1_PORT = replace_string(DATANODE1_PORT, "1", num_str);
			setting->datanodes_info[i]->node_port = get_config_file_value(DATANODE1_PORT);
			DATANODE1_USER = replace_string(DATANODE1_USER, "1", num_str);
			setting->datanodes_info[i]->user_name = get_config_file_value(DATANODE1_USER);
			DATANODE1_DB = replace_string(DATANODE1_DB, "1", num_str);
			setting->datanodes_info[i]->database_name = get_config_file_value(DATANODE1_DB);
		}

	}

	if (setting->threads_num_per_datanode == 0)
	{
		str_ptr = get_config_file_value(THREADS_NUM_PER_DATANODE);
		setting->threads_num_per_datanode = atoi(str_ptr);

		if (setting->threads_num_per_datanode <= 0)
		{
			pfree(str_ptr);
			str_ptr = NULL;

			fprintf(stderr, "Error: the value for \"THREADS_NUM_PER_DATANODE\" must be greater than 0.\n");
			exit(EXIT_FAILURE);
		}

		pfree(str_ptr);
		str_ptr = NULL;
	}

	setting->hash_config = (HashConfig *)palloc0(sizeof(HashConfig ));
	if (setting->hash_thread_num == 0)
	{
		str_ptr = get_config_file_value(HASH_THREAD_NUM);
		setting->hash_config->hash_thread_num = atoi(str_ptr);

		if (setting->hash_config->hash_thread_num <= 0)
		{
			pfree(str_ptr);
			str_ptr = NULL;

			fprintf(stderr, "Error: the value for \"HASH_THREAD_NUM\" must be greater than 0.\n");
			exit(EXIT_FAILURE);
		}

		pfree(str_ptr);
		str_ptr = NULL;
	}
	else
	{
		setting->hash_config->hash_thread_num = setting->hash_thread_num;
	}

	setting->hash_config->text_delim = get_text_delim(get_config_file_value(COPY_DELIMITER));
	setting->hash_config->hash_delim = pstrdup(HASH_DELIM);

	if (!setting->filter_first_line)
	{
		str_ptr = get_config_file_value(FILTER_FIRST_LINE);
		if (strcasecmp(str_ptr, "on") == 0	 ||
			strcasecmp(str_ptr, "true") == 0 ||
			strcasecmp(str_ptr, "1") == 0)
		{
			setting->filter_first_line = true;
		}
		else if (strcasecmp(str_ptr, "off") == 0  ||
				strcasecmp(str_ptr, "false") == 0 ||
				strcasecmp(str_ptr, "0") == 0)
		{
			setting->filter_first_line = false;
		}
		else
		{
			pfree(str_ptr);
			str_ptr = NULL;
			fprintf(stderr, "Error: the value for \"FILTER_FIRST_LINE\" must be one of \"on/off, true/false, 1/0\".\n");
			exit(EXIT_FAILURE);
		}

		pfree(str_ptr);
		str_ptr = NULL;
	}

	setting->hash_config->copy_quotec = pstrdup("\"");
	setting->hash_config->copy_escapec = pstrdup("NO");
	setting->hash_config->copy_option = get_copy_options(setting->hash_config->text_delim,
                                                        get_config_file_value(COPY_NULL));

	setting->log_field = (LogField *)palloc0(sizeof(LogField));
	setting->log_field->log_level = get_config_file_value(LOG_LEVEL);
	setting->log_field->log_path = get_config_file_value(LOG_PATH);

	str_ptr = get_config_file_value(FILTER_QUEUE_FILE_PATH);
	if (str_ptr[strlen(str_ptr) - 1] == '/')
		str_ptr[strlen(str_ptr) - 1] = '\0';
	setting->filter_queue_file_path = pstrdup(str_ptr);
	pfree(str_ptr);
	str_ptr = NULL;

	if (!directory_exists(setting->filter_queue_file_path))
		make_directory(setting->filter_queue_file_path);

	str_ptr = get_config_file_value(ERROR_DATA_FILE_PATH);
	if (str_ptr[strlen(str_ptr) - 1] == '/')
		str_ptr[strlen(str_ptr) - 1] = '\0';
	setting->error_data_file_path = pstrdup(str_ptr);
	pfree(str_ptr);
	str_ptr = NULL;

	if (!directory_exists(setting->error_data_file_path))
		make_directory(setting->error_data_file_path);

	str_ptr = get_config_file_value(READ_FILE_BUFFER);
	setting->read_file_buffer = atoi(str_ptr);
	pfree(str_ptr);
	str_ptr = NULL;

	if (!setting->copy_cmd_comment)
	{
		str_ptr = get_config_file_value(COPY_CMD_COMMENT);
		if (strcasecmp(str_ptr, "on") == 0	 ||
			strcasecmp(str_ptr, "true") == 0 ||
			strcasecmp(str_ptr, "1") == 0)
		{
			setting->copy_cmd_comment = true;
		}
		else if (strcasecmp(str_ptr, "off") == 0   ||
				strcasecmp(str_ptr, "false") == 0  ||
				strcasecmp(str_ptr, "0") == 0)
		{
			setting->copy_cmd_comment = false;
		}
		else
		{
			fprintf(stderr, "Error: the value for \"copy_cmd_comment\" must be one of \"on/off, true/false, 1/0\".\n");
			exit(EXIT_FAILURE);
		}

		pfree(str_ptr);
		str_ptr = NULL;
	}

	if (setting->copy_cmd_comment_str == NULL)
	{
		str_ptr = get_config_file_value(COPY_CMD_COMMENT_STR);
		if (check_copy_comment_str_valid(str_ptr))
		{
			setting->copy_cmd_comment_str = pg_strdup(str_ptr);
		}
		else
		{
			fprintf(stderr, "Error: the config parameter \"copy_cmd_comment_str\" set wrong.\n");
			exit(EXIT_FAILURE);
		}

		pfree(str_ptr);
		str_ptr = NULL;
	}

	if (setting->copy_cmd_comment && setting->copy_cmd_comment_str == NULL)
	{
		fprintf(stderr, "Error: the value for \'COPY_CMD_COMMENT_STR\' must be given when \'COPY_CMD_COMMENT\' is set on.\n");
		exit(EXIT_FAILURE);
	}

	setting->error_threshold = atoi(get_config_file_value(ERROR_THRESHOLD));

	return;
}

static char *
get_text_delim(char *text_delim)
{
	char *local_text_delim = NULL;

	local_text_delim = pstrdup(text_delim);

	if (strlen(local_text_delim) == 1)
	{
		return local_text_delim;
	}
	else if (strlen(local_text_delim) == 2)
	{
		if (strcasecmp(local_text_delim, "\\t") == 0)
		{
			local_text_delim[0] = '\t';
			local_text_delim[1] = '\0';
			return local_text_delim;
		}
	}
	else
	{
		fprintf(stderr, "Error: the value for \"COPY_DELIMITER\" must be a single character.\n");
		exit(EXIT_FAILURE);
	}

	return NULL;
}

static char *
get_copy_options(char *text_delim, char *copy_null)
{
	LineBuffer *buf = NULL;
	char *copy_options = NULL;

	init_linebuf(3);
	buf = get_linebuf();

	appendLineBufInfo(buf, " with ( ");

	/*for COPY_DELIMITER */
	appendLineBufInfo(buf, "DELIMITER \'%c\', ", text_delim[0]);

	/*for COPY_NULL */
	appendLineBufInfo(buf, "NULL \'%s\' ", copy_null);
	appendLineBufInfo(buf, ")");

	copy_options = pstrdup(buf->data);

	release_linebuf(buf);
	end_linebuf();

	return copy_options;
}

static char *
replace_string(const char *string, const char *replace, const char *replacement)
{
	char *ptr = NULL;
	int new_str_len = 0;
	char *new_str = NULL;

	if((ptr = strstr(string, replace)) == NULL)
		return (char*)string;
	new_str_len = strlen(string) + strlen(replacement) - strlen(replace);
	new_str = palloc0(new_str_len + 1);
	strncat(new_str, string, ptr - string);
	strcat(new_str, replacement);
	strcat(new_str, &(ptr[strlen(replace)]));

	return new_str;
}

static char *
get_config_file_value(const char *key)
{
	char *get_value = GetConfValue(key);
	if(get_value == NULL)
	{
		fprintf(stderr, "Error: the value for \"%s\" must be given.\n", key);
		exit(EXIT_FAILURE);
	}
	return pstrdup(get_value);
}

static void
print_help(FILE *fd)
{
	fprintf(fd, _("adb_load import data to datanode.\n\n"));
	fprintf(fd, _("Usage:\n"));
	fprintf(fd, _("  adb_load [OPTION]...\n"));
	fprintf(fd, _("\nOptions:\n"));
	fprintf(fd, _("  -d, --dbname                database name to connect to (no default value)\n"));
	fprintf(fd, _("  -U, --username              database user name (no default value)\n"));
	fprintf(fd, _("  -W, --password              use it when connect to database\n\n"));

	fprintf(fd, _("  -h, --hash_threads          hash threads num\n"));
	fprintf(fd, _("  -r, --threads_per_datanode  threads num per datanode\n\n"));

	fprintf(fd, _("  -g, --singlefile            import data using single file mode\n"));
	fprintf(fd, _("  -s, --static                import data using static mode\n"));
	fprintf(fd, _("  -y, --dynamic               import data using dynamic mode\n"));
	fprintf(fd, _("  -x, --stream                import data using stream mode\n\n"));

	fprintf(fd, _("  -c, --configfile            config file path (default:adb_load.conf in the current directory)\n"));
	fprintf(fd, _("  -o, --outputdir             output directory for log file and error file\n"));
	fprintf(fd, _("  -i, --inputdir              data file directory\n"));
	fprintf(fd, _("  -f, --inputfile             data file\n"));
	fprintf(fd, _("  -t, --table                 table name\n\n"));

	fprintf(fd, _("  -p, --copy_cmd_comment      enable copy command comment\n"));
	fprintf(fd, _("  -m, --copy_cmd_comment_str  comment mark, must be two same characters, such as '\\\\','##'\n"));
	fprintf(fd, _("  -n, --filter_first_line     filter the first line\n\n"));

	fprintf(fd, _("  -Q, --queue                 queues that need to be re-imported\n"));
	fprintf(fd, _("  -e, --filter_queue_file     redirect specific queue data into files\n"));
	fprintf(fd, _("  -j, --just_check            just check if the data is correct, no importing data into the database\n"));
	fprintf(fd, _("  -?, --help                  show this help, then exit\n"));
	fprintf(fd, _("  -V, --version               output version information, then exit\n"));

	return;
}

void
pg_free_adb_load_setting(ADBLoadSetting *setting)
{
	int i = 0;

	pg_free(setting->input_file);
	setting->input_file = NULL;

	pg_free(setting->input_directory);
	setting->input_directory = NULL;

	pg_free(setting->config_file_path);
	setting->config_file_path = NULL;

	pg_free(setting->output_directory);
	setting->output_directory = NULL;

	pg_free(setting->database_name);
	setting->database_name = NULL;

	pg_free(setting->user_name);
	setting->user_name = NULL;

	pg_free(setting->password);
	setting->password = NULL;

	pg_free(setting->table_name);
	setting->table_name = NULL;

	pg_free(setting->program);
	setting->program = NULL;

	pg_free_NodeInfoData(setting->server_info);
	pg_free(setting->server_info);
	setting->server_info = NULL;

	pg_free_NodeInfoData(setting->agtm_info);
	pg_free(setting->agtm_info);
	setting->agtm_info = NULL;
#if 0
	pg_free_NodeInfoData(setting->coordinator_info);
	pg_free(setting->coordinator_info);
	setting->coordinator_info = NULL;
#endif

	for (i = 0; i < setting->datanodes_num; i++)
	{
		pg_free_NodeInfoData(setting->datanodes_info[i]);
		pg_free(setting->datanodes_info[i]);
		setting->datanodes_info[i] = NULL;
	}
	pg_free(setting->datanodes_info);
	setting->datanodes_info = NULL;

	pg_free_hash_config(setting->hash_config);
	pg_free(setting->hash_config);
	setting->hash_config = NULL;

	pg_free_log_field(setting->log_field);
	pg_free(setting->log_field);
	setting->hash_config = NULL;

	pg_free(setting->redo_queue_index);
	setting->redo_queue_index = NULL;

	pg_free(setting->filter_queue_file_path);
	setting->filter_queue_file_path = NULL;

	pg_free(setting);
	setting = NULL;

	return;
}

static void
pg_free_log_field(LogField *logfield)
{
	Assert(logfield != NULL);

	pg_free(logfield->log_level);
	logfield->log_level = NULL;

	pg_free(logfield->log_path);
	logfield->log_path = NULL;

	return;
}

static void
pg_free_hash_config(HashConfig *hash_config)
{
	if (hash_config == NULL)
		return;

	pg_free(hash_config->text_delim);
	hash_config->text_delim = NULL;

	pg_free(hash_config->hash_delim);
	hash_config->hash_delim = NULL;

	pg_free(hash_config->copy_quotec);
	hash_config->copy_quotec = NULL;

	pg_free(hash_config->copy_escapec);
	hash_config->copy_escapec = NULL;

	pg_free(hash_config->copy_option);
	hash_config->copy_option = NULL;

	return;
}

void
pg_free_NodeInfoData(NodeInfoData *pt)
{
	Assert(pt != NULL);

	pg_free(pt->node_name);
	pt->node_name = NULL;

	pg_free(pt->node_host);
	pt->node_host = NULL;

	pg_free(pt->node_port);
	pt->node_port = NULL;

	pg_free(pt->user_name);
	pt->user_name = NULL;

	pg_free(pt->database_name);
	pt->database_name = NULL;

	pg_free(pt->connection);
	pt->connection = NULL;

	return;
}

