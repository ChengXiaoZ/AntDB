
#ifndef CONF_SCAN_H_
#define CONF_SCAN_H_

typedef struct ConfInfo
{
	char *filename;
	char *line;
	char *name;		/* null if comment line */
	char *value;
	int name_loc;
	int name_len;
	int value_loc;
	int value_len;
	struct ConfInfo *next;
}ConfInfo;

ConfInfo * parse_conf_file(const char *filename);

#endif /* CONF_SCAN_H_ */
