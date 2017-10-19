/*-------------------------------------------------------------------------
 *
 * agtm_sequence.h
 *
 *	  Definitions for deal sequence next/current/last command message form coordinator
 *
 * Portions Copyright (c) 2016, ASIAINFO BDX ADB Group
 *
 * src/include/agtm/agtm_sequence.h
 *
 *-------------------------------------------------------------------------
 */
 
#ifndef AGTM_SEQUENCE_H
#define AGTM_SEQUENCE_H

#include "postgres.h"

#include "lib/stringinfo.h"

void ProcessNextSeqCommand(StringInfo message);

/*
 *  select currval('seq1') will call this fucntion.function currval('sequence') called
 *  must after nextval('sequence') called and in the same session .otherwise function
 *  currval must be ereport(error)
 */
void ProcessCurSeqCommand(StringInfo message);

/*
 *  select lastval() will call this fucntion.function currval('sequence') called
 *  must after nextval('sequence') called and in the same session .otherwise function
 *  currval must be ereport(error)
 */
void PorcessLastSeqCommand(StringInfo message);

void ProcessSetSeqCommand(StringInfo message); 

#endif