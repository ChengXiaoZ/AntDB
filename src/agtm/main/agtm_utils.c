#include "postgres.h"

#include "agtm/agtm_msg.h"
#include "agtm/agtm_utils.h"
#include "c.h"

static void gtm_util_init_nametabs(void);

struct enum_name
{
	int type;
	char *name;
};

static struct enum_name message_name_tab[] =
{
	{AGTM_MSG_GET_GXID,"AGTM_MSG_GET_GXID"},
	{AGTM_MSG_GET_TIMESTAMP,"AGTM_MSG_GET_TIMESTAMP"},
	{AGTM_MSG_GXID_LIST,"AGTM_MSG_GXID_LIST"},
	{AGTM_MSG_SNAPSHOT_GET,"AGTM_MSG_SNAPSHOT_GET"},
	{AGTM_MSG_SEQUENCE_GET_NEXT,"AGTM_MSG_SEQUENCE_GET_NEXT"},
	{AGTM_MSG_SEQUENCE_GET_CUR,"AGTM_MSG_SEQUENCE_GET_CUR"},
	{AGTM_MSG_SEQUENCE_GET_LAST,"AGTM_MSG_SEQUENCE_GET_LAST"},
	{AGTM_MSG_SEQUENCE_SET_VAL,"AGTM_MSG_SEQUENCE_SET_VAL"},
	{AGTM_MSG_GET_STATUS,"AGTM_MSG_GET_STATUS"},
	{-1, NULL}
};

static struct enum_name result_name_tab[] =
{
	{AGTM_GET_GXID_RESULT,"AGTM_GET_GXID_RESULT"},
	{AGTM_GET_TIMESTAMP_RESULT,"AGTM_GET_TIMESTAMP_RESULT"},
	{AGTM_GXID_LIST_RESULT,"AGTM_GXID_LIST_RESULT"},
	{AGTM_SNAPSHOT_GET_RESULT,"AGTM_SNAPSHOT_GET_RESULT"},
	{AGTM_SEQUENCE_GET_NEXT_RESULT,"AGTM_SEQUENCE_GET_NEXT_RESULT"},
	{AGTM_MSG_SEQUENCE_GET_CUR_RESULT,"AGTM_MSG_SEQUENCE_GET_CUR_RESULT"},
	{AGTM_SEQUENCE_GET_LAST_RESULT,"AGTM_SEQUENCE_GET_LAST_RESULT"},
	{AGTM_SEQUENCE_SET_VAL_RESULT,"AGTM_SEQUENCE_SET_VAL_RESULT"},
	{AGTM_GET_STATUS_RESULT,"AGTM_GET_STATUS_RESULT"},
	{-1, NULL}
};



static char **message_name = NULL;
static int message_max;
static char **result_name = NULL;
static int result_max;


static void
gtm_util_init_nametabs(void)
{
	int ii;

	if (message_name)
		free(message_name);
	if (result_name)
		free(result_name);
	for (ii = 0, message_max = 0; message_name_tab[ii].type >= 0; ii++)
	{
		if (message_max < message_name_tab[ii].type)
			message_max = message_name_tab[ii].type;
	}
	message_name = (char **)malloc(sizeof(char *) * (message_max + 1));
	memset(message_name, 0, sizeof(char *) * (message_max + 1));
	for (ii = 0; message_name_tab[ii].type >= 0; ii++)
	{
		message_name[message_name_tab[ii].type] = message_name_tab[ii].name;
	}

	for (ii = 0, result_max = 0; result_name_tab[ii].type >= 0; ii++)
	{
		if (result_max < result_name_tab[ii].type)
			result_max = result_name_tab[ii].type;
	}
	result_name = (char **)malloc(sizeof(char *) * (result_max + 1));
	memset(result_name, 0, sizeof(char *) * (result_max + 1));
	for (ii = 0; result_name_tab[ii].type >= 0; ii++)
	{
		result_name[result_name_tab[ii].type] = result_name_tab[ii].name;
	}
}

char *gtm_util_message_name(AGTM_MessageType type)
{
		if (message_name == NULL)
		gtm_util_init_nametabs();
		if (type > message_max)
			return "UNKNOWN_MESSAGE";
		return message_name[type];

}
char *gtm_util_result_name(AGTM_ResultType type)
{
		if (result_name == NULL)
		gtm_util_init_nametabs();
		if (type > result_max)
			return "UNKNOWN_RESULT";
		return result_name[type];
}

