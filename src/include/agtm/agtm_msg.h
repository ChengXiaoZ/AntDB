/*-------------------------------------------------------------------------
 *
 * agtm_msg.h
 *	
 *   Definitions for message type between agtm and coordinator or datanode
 *
 * Portions Copyright (c) 2016, ASIAINFO BDX ADB Group
 *
 * src/include/agtm/agtm_msg.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef AGTM_MSG_H
#define AGTM_MSG_H

/*
 * The following enum symbols are also used in message_name_tab structure
 * in agtm_utils.c.   Modification of the following enum should reflect
 * changes to message_name_tab structure as well.
 */
typedef enum AGTM_MessageType
{
	AGTM_MSG_GET_GXID,		/* Get a GXID for a transaction */
	AGTM_MSG_GET_TIMESTAMP,
	AGTM_MSG_GXID_LIST,
	AGTM_MSG_SNAPSHOT_GET,		/* Get a global snapshot */
	AGTM_MSG_SEQUENCE_GET_NEXT,		/* Get the next sequence value of sequence */
	AGTM_MSG_SEQUENCE_GET_CUR,
	AGTM_MSG_SEQUENCE_GET_LAST,	/* Get the last sequence value of sequence */
	AGTM_MSG_SEQUENCE_SET_VAL,		/* Set values for sequence */
	AGTM_MSG_GET_STATUS,		/* Get status of a given transaction */
	AGTM_MSG_XACT_LOCK_TABLE_WAIT, /* Wait for the specified transaction(s) to commit or abort */
	AGTM_MSG_LOCK_TRANSACTION		/* lock or release TransactionID with locktype */
} AGTM_MessageType;
#define AGTM_MSG_TYPE_COUNT (AGTM_MSG_LOCK_TRANSACTION+1)

/*
 * Symbols in the following enum are usd in result_name_tab defined in agtm_utils.c.
 * Modifictaion to the following enum should be reflected to result_name_tab as well.
 */
typedef enum AGTM_ResultType
{
	AGTM_NONE_RESULT,		/* for initinal */
	AGTM_GET_GXID_RESULT,
	AGTM_GET_TIMESTAMP_RESULT,
	AGTM_GXID_LIST_RESULT,
	AGTM_SNAPSHOT_GET_RESULT,
	AGTM_SEQUENCE_GET_NEXT_RESULT,
	AGTM_MSG_SEQUENCE_GET_CUR_RESULT,
	AGTM_SEQUENCE_GET_LAST_RESULT,
	AGTM_SEQUENCE_SET_VAL_RESULT,
	AGTM_COMPLETE_RESULT			/* for no message result */
} AGTM_ResultType;
#define AGTM_RESULT_TYPE_COUNT (AGTM_COMPLETE_RESULT+1)

#endif
