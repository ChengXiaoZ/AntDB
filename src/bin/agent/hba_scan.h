#ifndef HBA_SCAN_H_
#define HBA_SCAN_H_

typedef enum HbaType
{
	HBA_TYPE_EMPTY = 0,
	HBA_TYPE_LOCAL,
	HBA_TYPE_HOST,
	HBA_TYPE_HOSTSSL,
	HBA_TYPE_HOSTNOSSL
}HbaType;

typedef struct HbaInfo
{
	struct HbaInfo *next;
	char *line;
	char *database;
	char *user;
	char *addr;			/* address, null if type==HBA_TYPE_LOCAL */
	char *auth_method;	/* auth method */
	char *options;		/* options, format is NAME=VALUE */
	int addr_mark;		/* address mark length, ipv4:0-32, ipv6:0-128 */
	bool addr_is_ipv6;	/* address is ip version 6? */
	HbaType type;
	int type_loc;	/* type location */
	int type_len;	/* type length */
	int db_loc;		/* database location */
	int db_len;		/* database length */
	int user_loc;	/* user location */
	int user_len;	/* user length */
	int addr_loc;	/* address location */
	int addr_len;	/* address length */
	int mark_loc;	/* addr_mark locathion */
	int mark_len;	/* addr_mark length */
	int method_loc;	/* method location */
	int method_len;	/* method length */
	int opt_loc;	/* options location */
	int opt_len;	/* options length */
}HbaInfo;

HbaInfo* parse_hba_file(const char *filename);

#endif /* HBA_SCAN_H_ */
