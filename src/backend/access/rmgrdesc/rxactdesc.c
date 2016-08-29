#include "postgres.h"

#include "access/rxact_mgr.h"
#include "access/rxact_msg.h"

//static const char* remote_type_string(RemoteXactType type);

void rxact_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	/*const char *gid;
	RemoteXactType type;
	int count;
	Oid db_oid;
	Oid *oids;

	uint8		info = xl_info & ~XLR_INFO_MASK;
	switch(info)
	{
	case RXACT_MSG_DO:
		memcpy(&db_oid, rec, sizeof(db_oid));
		rec += sizeof(db_oid);
		type = (RemoteXactType)(*rec++);
		memcpy(&count, rec, sizeof(count));
		rec+=sizeof(count);
		oids = (Oid*)rec;
		rec += count*sizeof(oids[0]);
		gid = rec;
		appendStringInfoString(buf, "RXACT ");
		switch(type)
		{
		case RX_PREPARE:
			appendStringInfo(buf, "");
		}
		break;
	}*/
}

/*static const char* remote_type_string(RemoteXactType type)
{
	switch(type)
	{
	case RX_PREPARE:
		return "PREPARE";
	case RX_ROLLBACK:
		return "ROLLBACK";
	case RX_COMMIT:
		return "COMMIT";
	}
	return "UNKNOWN";
}
*/