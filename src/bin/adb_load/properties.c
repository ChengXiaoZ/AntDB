#include "postgres_fe.h"

#include "properties.h"
#include "nodes/pg_list.h"
#include "adbloader_log.h"

#define KEY_SIZE        128 /* key cache size */
#define VALUE_SIZE      128 /* value cache size */ 
#define LINE_BUF_SIZE   256 /* line cache size */

typedef struct PropertieNode {
    char *key;
    char *value;
	struct PropertieNode *pNext;
} PropertieNode;

typedef struct PROPS_HANDLE {
    PropertieNode *pHead;  /* attribute head point */
    char *filepath;		   /* config file path */
} PROPS_HANDLE;

static PropertieNode *create_props_node (void);
static void			  clean_props_node(PropertieNode *node);
/* wipe off space and \t \r from line*/
static void trime_symbol_copy (const char *src, char **dest);
static int saveConfig(const char *filepath);

PROPS_HANDLE *Properties;

int
InitConfig (const char *filepath)
{
	int ret = 0;
	FILE *fp = NULL;
	PropertieNode *pHead = NULL, *pCurrent = NULL;

	char line[LINE_BUF_SIZE];	/*cache every line data */
	char keybuff[KEY_SIZE] = { 0 };         /* cache key data */
	char *pLine = NULL;
	PropertieNode	*node;

	Assert(NULL != filepath);
	Properties = (PROPS_HANDLE *)palloc0(sizeof(PROPS_HANDLE));
	if (NULL == Properties)
	{
		ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] palloc0 memory to PROPS_HANDLE failed");
		exit(1);
	}
	Properties->filepath = (char*)palloc0(strlen(filepath) + 1);
	memcpy(Properties->filepath, filepath, strlen(filepath));
	Properties->filepath[strlen(filepath)] = '\0';

	/* open file */
	fp = fopen(filepath, "r");
	if (!fp) 
	{
	    ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] open file failed, filepath : %s",filepath);
		exit(1);
	} 
	pHead = create_props_node();
	Properties->pHead = pHead;
	pCurrent = pHead;
	/* read all data from config file */
	while (!feof(fp))
	{
		if(fgets(line, LINE_BUF_SIZE, fp) == NULL)   
			break;

		/* wipe off \n or \t */
		if (line[strlen(line) -1] == '\n' || line[strlen(line) -1] == '\t' ||
							line[strlen(line) -1] == '\r')
			line[strlen(line) -1] = '\0';

		 /* find "=" */
		if ((pLine = strstr(line, "=")) == NULL) 
		   /* if "=" unexist continue next line */
		    continue;

		node = create_props_node();

		/* set key */
		memcpy(keybuff, line, pLine-line);
		trime_symbol_copy(keybuff, &node->key);

		/* set value */
		pLine += 1;
		trime_symbol_copy(pLine, &node->value);

		node->pNext = NULL;
		pCurrent->pNext = node;
		pCurrent = node;

		/* reset buffer */
		memset(keybuff, 0, KEY_SIZE);
	}
	/* close file */
	fclose(fp);     
	return ret;
}

char *
GetConfValue (const char *key)
{
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurrent = NULL;
	ph = Properties;
	pCurrent = ph->pHead->pNext;
	while (pCurrent != NULL)
	{
		if (strcmp(pCurrent->key,key) == 0)
			break;
		pCurrent = pCurrent->pNext;
	}
	if (pCurrent == NULL)
	{
		ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] can't find config key :%s", key);
		return NULL;
	}
	return pCurrent->value;
}

void
GetConfValues (const char **key, char **values, int count)
{
	int flag;
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurrent = NULL;
	values = (char**)palloc0(sizeof(char*) * count);
	for (flag = 0; flag < count; flag++)
	{
		ph = Properties;
		pCurrent = ph->pHead->pNext;
		while (pCurrent != NULL)
		{
			if (strcmp(pCurrent->key, key[flag]) == 0)
				break;
			pCurrent = pCurrent->pNext;
		}
		if (pCurrent == NULL)
		{
			values[flag] = NULL;
			ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] can't find config key :%s", key[flag]);
		}
		else
		{
			values[flag] = pg_strdup(pCurrent->value);
		}
	}
}

int
SetValue(const char *key, const char *value)
{
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurrent = NULL;

	Assert(NULL != key && NULL != value);
	ph = Properties;
	pCurrent = ph->pHead->pNext;
	while (pCurrent != NULL)
	{
		if (strcmp(pCurrent->key,key) == 0)
			break;
		pCurrent = pCurrent->pNext;
	}
	if (pCurrent == NULL)
	{
		ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] can't find config key :%s",key);
		return -1;
	}
	pfree(pCurrent->value);
	pCurrent->value = NULL;
	pCurrent->value = (char*)palloc0(strlen(value) + 1);
	memcpy(pCurrent->value, value, strlen(value));
	pCurrent->value[strlen(value)] = '\0';
	saveConfig(Properties->filepath);
	return 0;
}

PropertieNode *
create_props_node (void)
{
	PropertieNode * node = (PropertieNode*)palloc0(sizeof(PropertieNode));
	if (NULL == node)
	{
		ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] palloc0 memory to PropertieNode failed");
		exit(1);
	}
	return node;
}

void 
clean_props_node(PropertieNode *node)
{
	Assert(NULL != node);
	if (NULL != node->key)
		pfree(node->key);
	if (NULL != node->value)
		pfree(node->value);

	node->key = NULL;
	node->value = NULL;
	pfree(node);
	node = NULL;
}

int
saveConfig(const char *filepath)
{
	int	 writeLen = 0;
	FILE *fp = NULL;
	PropertieNode *pCurrent = NULL;
	Assert(NULL != filepath);

	fp = fopen(filepath,"w");
	if (NULL == fp)
	{
		ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] open file failed ,file path :%s",filepath);
		return -1;
	}
	pCurrent = Properties->pHead->pNext;
	while (NULL != pCurrent)
	{
		writeLen = fprintf(fp, "%s=%s", pCurrent->key, pCurrent->value);
		if (writeLen < 0)
		{
			ADBLOADER_LOG(LOG_ERROR, "[CONFIG][thread main ] save key :%s , save value :%s failed",
				pCurrent->key, pCurrent->value);
			return -1;
		}
		pCurrent = pCurrent->pNext;
	}
	fclose(fp);
	return 0;
}

void 
trime_symbol_copy (const char *src, char **dest)
{
    const char *psrc = src;
    unsigned long i = 0,j = strlen(psrc) - 1,len;

	Assert(NULL != src && NULL != dest);
    while (psrc[i] == ' '){
        i++;
    }

    while (psrc[j] == ' ' || psrc[j] == '\t' || psrc[j] == '\r') {
        j--;
    }
 
	if (psrc[i] == '\"' && psrc[j] == '\"' && i != j)
	{
		i++;
		j--;
	}

	len = j - i + 1;	
    *dest = (char*)palloc0(len + 1);
    memcpy(*dest, psrc+i, len);	
    *(*dest+len) = '\0';   
}

int 
PropsDel(const char *key)
{
	PROPS_HANDLE *ph = NULL;
    PropertieNode *pCurrent = NULL, *pPrev = NULL;
	Assert(NULL != key);
	ph = Properties;
	pPrev = ph->pHead;
    pCurrent = ph->pHead->pNext;

    while (pCurrent != NULL) 
	{
	    if (strcmp(pCurrent->key, key) == 0)
	        break;
	    pPrev = pCurrent;
	    pCurrent = pCurrent->pNext;
	}
	if (pCurrent == NULL)
		return -1;
	pPrev->pNext = pCurrent->pNext;
	pfree(pCurrent);
	pCurrent = NULL;

	return (saveConfig(Properties->filepath));
}

int	
PropsAdd(const char *key, const char *value)
{
	PROPS_HANDLE *ph = NULL;
    PropertieNode *pCurrent = NULL;
	Assert(NULL != key && NULL != value);
	ph = Properties;
	pCurrent = ph->pHead;
	while (pCurrent->pNext != NULL)
	{
		if (strcmp(pCurrent->pNext->key,key) == 0)
			break;
		pCurrent = pCurrent->pNext;
	}
	if (pCurrent->pNext != NULL)
		return SetValue(key,value);
	return -1;
}

int
PropsRelease(void)
{
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurr = NULL;
	PropertieNode *pTemp = NULL;
	ph = Properties;
	pCurr = ph->pHead;

	while (pCurr != NULL)
	{
		if (pCurr->key != NULL)
		{
            pfree(pCurr->key);
            pCurr->key = NULL;
        }

		if (pCurr->value != NULL)
		{
            pfree(pCurr->value);
            pCurr->value = NULL;
        }
		pTemp = pCurr->pNext;
		pfree(pCurr);
		pCurr = pTemp;
	}

	if(ph->filepath != NULL)
    {
        pfree(ph->filepath);
        ph->filepath = NULL;
    }
	pfree(ph);
	return 0;
}

int
PropsGetCount(void)
{
	int count = 0;
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurrent = NULL;
	ph = Properties;
	pCurrent = ph->pHead->pNext;
	while (pCurrent != NULL)
	{
		count++;
		pCurrent = pCurrent->pNext;
	}
	return count;
}

void 	
PrintConfig(void)
{
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurrent = NULL;
	ph = Properties;
	pCurrent = ph->pHead->pNext;
	while (pCurrent != NULL)
	{
		printf("%s=%s\n", pCurrent->key, pCurrent->value);
		pCurrent = pCurrent->pNext;
	}
}

void
DestoryConfig (void)
{
	PROPS_HANDLE *ph = NULL;
	PropertieNode *pCurrent = NULL;

	ph = Properties;
	pCurrent = ph->pHead->pNext;
	while (pCurrent != NULL)
	{
		PropertieNode *tmp = pCurrent->pNext;
		clean_props_node(pCurrent);
		pCurrent = tmp;
	}
	if (Properties->filepath)
		pfree(Properties->filepath);
	Properties->filepath = NULL;
	if (Properties->pHead)
		clean_props_node(Properties->pHead);		
	Properties->pHead = NULL;
	pfree(Properties);
	Properties = NULL;
}

