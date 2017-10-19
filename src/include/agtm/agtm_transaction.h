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

void ProcessGetGXIDCommand(StringInfo message);

void ProcessGetTimestamp(StringInfo message);

void ProcessGetSnapshot(StringInfo message);

#endif