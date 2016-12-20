
#ifndef MGR_HBA_H
#define MGR_HBA_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */


#define HbaRelationId 3191

CATALOG(mgr_hba,3191)
{
	int32		row_id;			/*  */
	NameData	nodename;		/* node name */
	text		hbavalue;		/* storing a line of pg_hba.conf */
} FormData_mgr_hba;

/* ----------------
 *		Form_mgr_updateparm corresponds to a pointer to a tuple with
 *		the format of mgr_updateparm relation.
 * ----------------
 */
typedef FormData_mgr_hba *Form_mgr_hba;

/* ----------------
 *		compiler constants for mgr_updateparm
 * ----------------
 */
#define Natts_mgr_hba				3
#define Anum_mgr_hba_id				1
#define Anum_mgr_hba_nodename		2
#define Anum_mgr_hba_value			3



#endif /* MGR_HBA_H */
