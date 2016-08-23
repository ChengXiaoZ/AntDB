#ifndef AGTM_CATALOG_SEQUENCE_H
#define AGTM_CATALOG_SEQUENCE_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#include "agtm/agtm_msg.h"

#define AgtmSequenceRelationId 4053

CATALOG(agtm_sequence,4053) //BKI_SHARED_RELATION
{
	NameData database;
	NameData schema;
	NameData sequence;
} FormData_agtm_sequence;

/* ----------------
 *		Form_agtm_sequence corresponds to a pointer to a tuple with
 *		the format of agtm_sequence relation.
 * ----------------
 */
typedef FormData_agtm_sequence *Form_agtm_sequence;

/* ----------------
 *		compiler constants for agtm_sequence
 * ----------------
 */
 
#define Natts_agtm_sequence				3		
#define Anum_agtm_sequence_database		1
#define Anum_agtm_sequence_schema		2
#define Anum_agtm_sequence_sequence		3

extern Oid AddAgtmSequence(const char* database,
				const char* schema, const char* sequence);

extern Oid DelAgtmSequence(const char* database,
				const char* schema, const char* sequence);

extern bool SequenceIsExist(const char* database,
				const char* schema, const char* sequence);

extern Oid SequenceSystemClassOid(const char* database,
				const char* schema, const char* sequence);

extern void UpdateSequenceInfo(const char* database,
				const char* schema, const char* sequence, const char * value, AgtmNodeTag type);

#endif

