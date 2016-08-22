/*-------------------------------------------------------------------------
 *
 * agtm_transaction.h
 *
 *	  Definitions for deal transaction gxid/snapshot/timestamp command message form coordinator
 *
 * Portions Copyright (c) 2016, ASIAINFO BDX ADB Group
 *
 * src/include/agtm/agtm_transaction.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef AGTM_TRANSACTION_H
#define AGTM_TRANSACTION_H

#include "postgres.h"

#include "lib/stringinfo.h"

StringInfo ProcessGetGXIDCommand(StringInfo message, StringInfo output);

StringInfo ProcessGetTimestamp(StringInfo message, StringInfo output);

StringInfo ProcessGetSnapshot(StringInfo message, StringInfo output);

StringInfo ProcessGetXactStatus(StringInfo message, StringInfo output);

StringInfo ProcessSequenceInit(StringInfo message, StringInfo output);

StringInfo ProcessSequenceDrop(StringInfo message, StringInfo output);

#endif