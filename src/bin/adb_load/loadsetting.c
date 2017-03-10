#include "postgres_fe.h"

#include "getopt_long.h"
#include "loadsetting.h"
#include "properties.h"
#include <libpq-fe.h>
#include <unistd.h>
#include <sys/stat.h>

static const char*  SERVER_PORT =		"SERVER_PORT";
static const char*  SERVER_IP   =		"SERVER_IP";
static const char*  SERVER_USER =		"SERVER_USER";
static const char*  SERVER_DB   =		"SERVER_DB";

static const char*  AGTM_PROT   =			"AGTM_PROT";
static const char*  AGTM_IP     =			"AGTM_IP";
static const char*  __AGTM_USER =			"AGTM_USER";
static const char*  AGTM_DB     =			"AGTM_DB";

static const char*  COORDINATOR_PROT =		"COORDINATOR_PROT";
static const char*  COORDINATOR_IP   =		"COORDINATOR_IP";
static const char*  COORDINATOR_USER =		"COORDINATOR_USER";
static const char*  COORDINATOR_DB   =		"COORDINATOR_DB";

static const char*  DATANODE_VALID   =		"DATANODE_VALID";
static const char*  DATANODE_NUM     =		"DATANODE_NUM";
static const char*  DATANODE1_PROT   =		"DATANODE1_PROT";
static const char*  DATANODE1_IP     =		"DATANODE1_IP";
static const char*  DATANODE1_USER   =		"DATANODE1_USER";
static const char*  DATANODE1_DB     =		"DATANODE1_DB";

static const char*  HASH_THREAD_NUM  =		"HASH_THREAD_NUM";
static const char*  TEXT_DELIM       =		"TEXT_DELIM";
static const char*  HASH_DELIM       =		"HASH_DELIM";
static const char*  COPY_QUOTEC      =		"COPY_QUOTEC";
static const char*  COPY_ESCAPEC     =		"COPY_ESCAPEC";
static const char*  COPY_OPTIONS     =		"COPY_OPTIONS";

/* LOG INFO WARNNING, ERROR */
static const char* LOG_LEVEL         =		"LOG_LEVEL";
static const char* LOG_PATH          =		"LOG_PATH";

static const char* PROCESS_BAR       =		"PROCESS_BAR";

/* ERROR FILE */
// static const char*	HASH_ERROR_FILE  =		"HASH_ERROR_FILE";
// static const char*	DISPATCH_ERROR_FILE  =  "DISPATCH_ERROR_FILE";
// static const char*	READ_ERROR_FILE	 =		"READ_ERROR_FILE";
// static const char*  ERROR_FILE_DIRECTORY =  "ERROR_FILE_DIRECTORY";

static void print_help(FILE *fd);
//static void get_node_conn_info(ADBLoadSetting *setting);
//static void get_settings_by_config_file(ADBLoadSetting *setting);
static char *replace_string(const char *string, const char *replace, const char *replacement);
static char *get_config_file_value(const char *key);
static void check_configfile(ADBLoadSetting *setting);
static void make_directory(const char *dir);
static bool directory_exists(char *dir);
static bool file_exists(char *file);

#define DEFAULT_CONFIGFILENAME "./adb_load.conf"

ADBLoadSetting *cmdline_adb_load_setting(int argc, char **argv, ADBLoadSetting *setting)
{
	static struct option long_options[] = {
		{"configfile", required_argument, NULL, 'c'},
		{"dbname",     required_argument, NULL, 'd'},
		{"dynamic",          no_argument, NULL, 'y'},
		{"inputdir",   required_argument, NULL, 'i'},
		{"inputfile",  required_argument, NULL, 'f'},
		{"outputdir",  required_argument, NULL, 'o'},
		{"password",   required_argument, NULL, 'W'},
		{"static",           no_argument, NULL, 's'},
		{"singlefile",       no_argument, NULL, 'g'},
		{"table",      required_argument, NULL, 't'},
		{"username",   required_argument, NULL, 'U'},
		{"help",             no_argument, NULL, '?'},
		{"version",          no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int c;
	int option_index;

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0 ||
			strcmp(argv[1], "-?") == 0)
		{
			print_help(stdout);
			exit(EXIT_SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("adb_load (PostgreSQL) " PG_VERSION);
			exit(EXIT_SUCCESS);
		}
	}

	setting = (ADBLoadSetting *)palloc0(sizeof(ADBLoadSetting));
	while((c = getopt_long(argc, argv, "c:d:yi:f:o:W:sgt:U:", long_options, &option_index)) != -1)
	{
		switch(c)
		{
		case 'c': //configfile
			setting->config_file_path = pg_strdup(optarg);
			break;
		case 'o': //outputdir
			setting->output_directory = pg_strdup(optarg);
			break;
		case 'd': //dbname
			setting->database_name = pg_strdup(optarg);
			break;
		case 'y': //dynamic mode
			setting->dynamic_mode = true;
			break;
		case 'i': //inputdir
			{
				//remove last '/' if user special
				if (optarg[strlen(optarg) - 1] == '/')
					optarg[strlen(optarg) - 1] = '\0';
				setting->input_directory = pg_strdup(optarg);
				break;
			}
		case 'f': //inputfile
			setting->input_file = pg_strdup(optarg);
			break;
		case 'W': //password
			setting->password = pg_strdup(optarg);
			break;
		case 's': //static mode
			setting->static_mode = true;
			break;
		case 'g': //single file mode
			setting->single_file = true;
			break;
		case 't': // table
			setting->table_name = pg_strdup(optarg);
			break;
		case 'U': // username
			setting->user_name = pg_strdup(optarg);
			break;
		case '?':
			print_help(stdout);
			exit(EXIT_SUCCESS);
			break;
		case 'V':
			puts("adb_load (PostgreSQL) " PG_VERSION);
			exit(EXIT_SUCCESS);
		default:
			fprintf(stderr, "Try \"adb_load --help\" for more information.\n");
			exit(EXIT_FAILURE);
		}
	}

	if (setting->database_name == NULL || setting->user_name == NULL)
	{
		fprintf(stderr, "options -d/--dbname and -U/--username you must use.\n");
		exit(EXIT_FAILURE);
	}

	if ((setting->single_file && setting->static_mode) ||
		(setting->static_mode && setting->dynamic_mode)||
		(setting->single_file && setting->dynamic_mode)||
		(setting->single_file && setting->static_mode && setting->dynamic_mode))
	{
		fprintf(stderr, "options -s/--static, -y/--dysnamic and -g/--singlefile cannot be used together.\n");
		exit(EXIT_FAILURE);
	}

	if (!setting->single_file && !setting->static_mode && !setting->dynamic_mode)
	{
		fprintf(stderr, "you should specify one of the options -g/--singlefile, -s/--static and -y/--dysnamic.\n");
		exit(EXIT_FAILURE);
	}

	if (setting->static_mode || setting->dynamic_mode)
	{
		if (setting->input_directory == NULL)
		{
			fprintf(stderr, "you should use option -i/--inputdir to specify input directory.\n");
			exit(EXIT_FAILURE);
		}
		else
		{
			if (!directory_exists(setting->input_directory))
			{
				fprintf(stderr, "option -i/-inputdir you specify is not exist.\n");
				exit(EXIT_FAILURE);
			}
		}
	}

	if (setting->static_mode || setting->dynamic_mode)
	{
		if (setting->input_file != NULL || setting->table_name != NULL)
		{
			fprintf(stderr, "options -y/--dysnamic, -f/--inputfile and -t/--table cannot be used together.\n");
			exit(EXIT_FAILURE);
		}
	}

	if (setting->single_file)
	{
		if (setting->input_file == NULL || setting->table_name == NULL)
		{
			fprintf(stderr, "you should use -f/--inputfile or -t/--table option.\n");
			exit(EXIT_FAILURE);
		}
	}

	check_configfile(setting);

	// checkout -o option
	if (setting->output_directory != NULL)
	{
		if (!directory_exists(setting->output_directory))
			make_directory(setting->output_directory);
	}
	else
	{
		setting->output_directory = pg_strdup("./");
	}

	if ((setting->input_file != NULL && setting->table_name == NULL) ||
		(setting->input_file == NULL && setting->table_name != NULL))
	{
		fprintf(stderr, "options -f/--inputfile and -t/--table should be used together.\n");
		exit(EXIT_FAILURE);
	}

	if (setting->input_file != NULL)
	{
		if (!file_exists(setting->input_file))
		{
			fprintf(stderr, "file \"%s\" is not exist.\n", setting->input_file);
			exit(EXIT_FAILURE);
		}
	}

	return setting;
}

static bool directory_exists(char *dir)
{
	struct stat st;

	if (stat(dir, &st) != 0)
		return false;
	if (S_ISDIR(st.st_mode))
		return true;
	return false;
}

/* Create a directory */
static void make_directory(const char *dir)
{
	if (mkdir(dir, S_IRWXU | S_IRWXG | S_IRWXO) < 0)
	{
		fprintf(stderr, _("could not create directory \"%s\": %s\n"),
				dir, strerror(errno));
		exit(2);
	}
}

static void check_configfile(ADBLoadSetting *setting)
{
	int ret;

	if (setting->config_file_path != NULL)
	{
		if (!file_exists(setting->config_file_path))
		{
			fprintf(stderr, "you specify is a directory or do not exit for option -c/--configfile.\n");
			exit(EXIT_FAILURE);
		}
		else if ((ret = access(setting->config_file_path, 4)) == -1)
		{
			fprintf(stderr, "No permission to read your specify config file.\n");
			exit(EXIT_FAILURE);
		}
	}
	else
	{
		if (!file_exists(DEFAULT_CONFIGFILENAME))
		{
			fprintf(stderr, "default config file is not exit, please use -c/--configfile.\n");
			exit(EXIT_FAILURE);
		}
		else if ((ret = access(DEFAULT_CONFIGFILENAME, 4)) == -1)
		{
			fprintf(stderr, "no permission read default config file \"%s\" .\n", DEFAULT_CONFIGFILENAME);
			exit(EXIT_FAILURE);
		}
		else
		{
			setting->config_file_path = pg_strdup(DEFAULT_CONFIGFILENAME);
		}
	}
}

static bool file_exists(char *file)
{
	FILE  *f = fopen(file, "r");

	if (!f)
		return false;

	fclose(f);
	return true;
}

void get_node_conn_info(ADBLoadSetting *setting)
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

void check_node_connection_valid(const char *host_ip, const char *host_port, const char *connection_str)
{
	PGconn     *conn;	
	conn = PQconnectdb(connection_str);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to node with(ip = %s, port = %s) failed;\n       \
					    Error:%s", host_ip, host_port, PQerrorMessage(conn));
		PQfinish(conn);
		exit(1);
	}
	PQfinish(conn);
}

void get_settings_by_config_file(ADBLoadSetting *setting)
{
	char num_str[4];
	char *datanode_valid = NULL;
	char *process_bar_config = NULL;
	char *str_ptr;
	int i = 0;
	if(setting->config_file_path == NULL)				
	{
		fprintf(stderr, "the input parameter must have config_file_path \n");
		print_help(stdout);
		exit(1);
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
	setting->agtm_info->node_port = get_config_file_value(AGTM_PROT);
	setting->agtm_info->user_name = get_config_file_value(__AGTM_USER);
	setting->agtm_info->database_name = get_config_file_value(AGTM_DB);
	
	setting->coordinator_info->node_host = get_config_file_value(COORDINATOR_IP);
	setting->coordinator_info->node_port = get_config_file_value(COORDINATOR_PROT);
	setting->coordinator_info->user_name = get_config_file_value(COORDINATOR_USER);
	setting->coordinator_info->database_name = get_config_file_value(COORDINATOR_DB);

	process_bar_config = get_config_file_value(PROCESS_BAR);
	if (strcmp(process_bar_config, "on") == 0)
	{
		setting->process_bar = true;
	}
	else
	{
		setting->process_bar = false;
	}

	datanode_valid = get_config_file_value(DATANODE_VALID);
	if (strcmp(datanode_valid, "on") == 0)
	{
	
		str_ptr = get_config_file_value(DATANODE_NUM);
		setting->datanodes_num = atoi(str_ptr);
		pfree(str_ptr);

		setting->datanodes_info = (NodeInfoData **)palloc0(setting->datanodes_num * sizeof(NodeInfoData*));
		for (i = 0; i < setting->datanodes_num; i++)
			setting->datanodes_info[i] = (NodeInfoData *)palloc0(sizeof(NodeInfoData));
		
		setting->config_datanodes_valid = true;
	
		for(i=0; i< setting->datanodes_num; ++i)
		{
			sprintf(num_str,"%d",i+1);
			DATANODE1_IP = replace_string(DATANODE1_IP, "1", num_str);
			setting->datanodes_info[i]->node_host = get_config_file_value(DATANODE1_IP);
			DATANODE1_PROT = replace_string(DATANODE1_PROT, "1", num_str);
			setting->datanodes_info[i]->node_port = get_config_file_value(DATANODE1_PROT);
			DATANODE1_USER = replace_string(DATANODE1_USER, "1", num_str);
			setting->datanodes_info[i]->user_name = get_config_file_value(DATANODE1_USER);
			DATANODE1_DB = replace_string(DATANODE1_DB, "1", num_str);
			setting->datanodes_info[i]->database_name = get_config_file_value(DATANODE1_DB);
		}

	}
	
	setting->hash_config = (HashConfig *)palloc0(sizeof(HashConfig ));
	str_ptr = get_config_file_value(HASH_THREAD_NUM);
	setting->hash_config->hash_thread_num = atoi(str_ptr);
	pfree(str_ptr);
	
	setting->hash_config->test_delim = get_config_file_value(TEXT_DELIM);
	setting->hash_config->hash_delim = get_config_file_value(HASH_DELIM);
	setting->hash_config->copy_quotec = get_config_file_value(COPY_QUOTEC);
	setting->hash_config->copy_escapec = get_config_file_value(COPY_ESCAPEC);
	setting->hash_config->copy_option = get_config_file_value(COPY_OPTIONS);
	
	setting->log_field = (LogField *)palloc0(sizeof(LogField));
	setting->log_field->log_level = get_config_file_value(LOG_LEVEL);
	setting->log_field->log_path = get_config_file_value(LOG_PATH);
}

static char *replace_string(const char *string, const char *replace, const char *replacement)
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

static char *get_config_file_value(const char *key)
{
	char *get_value = GetConfValue(key);
	if(get_value == NULL)
	{
		fprintf(stderr, "the config parameter \"%s\" need a value\n",key);
		exit(1);
	}
	return pstrdup(get_value);
}

static void print_help(FILE *fd)
{
	fprintf(fd, _("adb_load command agen\n"));
	fprintf(fd, _("Usage:\n"));
	fprintf(fd, _("  adb_load [OPTION]\n"));
	fprintf(fd, _("\nOptions:\n"));
}

void free_adb_load_setting(ADBLoadSetting *setting)
{
	int i;

	pg_free(setting->input_file);
	setting->input_file = NULL;

	pg_free(setting->config_file_path);
	setting->config_file_path = NULL;

	pg_free(setting->input_directory);
	setting->input_directory = NULL;

	pg_free(setting->output_directory);
	setting->output_directory = NULL;

	pg_free(setting->table_name);
	setting->table_name = NULL;
	
	pg_free(setting->program);
	setting->program = NULL;

	free_NodeInfoData(setting->server_info);
	pg_free(setting->server_info);
	setting->server_info = NULL;

	free_NodeInfoData(setting->agtm_info);
	pg_free(setting->agtm_info);
	setting->agtm_info = NULL;

	free_NodeInfoData(setting->coordinator_info);
	pg_free(setting->coordinator_info);
	setting->coordinator_info = NULL;

	for (i = 0; i < setting->datanodes_num; i++)
	{
		free_NodeInfoData(setting->datanodes_info[i]);
		pg_free(setting->datanodes_info[i]);
		setting->datanodes_info[i] = NULL;
	}
	pg_free(setting->datanodes_info);
	setting->datanodes_info = NULL;

	pg_free(setting->hash_config);
	setting->hash_config = NULL;

	pg_free(setting->log_field);
	setting->hash_config = NULL;

	pfree(setting);
	setting = NULL;
}

void free_NodeInfoData(NodeInfoData *pt)
{
	pfree(pt->node_name);
	pt->node_name = NULL;

	pfree(pt->node_host);
	pt->node_host = NULL;

	pfree(pt->node_port);
	pt->node_port = NULL;

	pfree(pt->user_name);
	pt->user_name = NULL;

	pfree(pt->database_name);
	pt->database_name = NULL;

	pfree(pt->connection);
	pt->connection = NULL;
}

