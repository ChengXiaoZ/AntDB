#ifndef AGTMPROTOCOL_H
#define AGTMPROTOCOL_H

#include "postgres.h"

#include "agtm/agtm_msg.h"
#include "datatype/timestamp.h"
#include "utils/snapshot.h"

typedef int64	AGTM_Sequence;	/* a 64-bit sequence */

typedef struct AGTM_ResultData
{
	TransactionId			grd_gxid;			/* TXN_PREPARE
												 * TXN_START_PREPARED
												 * TXN_COMMIT
												 * TXN_COMMIT_PREPARED
												 * TXN_ROLLBACK
												 */
	Timestamp 				grd_timestamp;
	GlobalSnapshot			snapshot;
	AGTM_Sequence           gsq_val;
} AGTM_ResultData;

typedef struct AGTM_Result
{
	AGTM_ResultType		gr_type;
	int					gr_msglen;
	int					gr_status;
	AGTM_ResultData		gr_resdata;
} AGTM_Result;

#endif
