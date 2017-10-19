
#ifndef MGR_PARM_H
#define MGR_PARM_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */


#define ParmRelationId 4928

CATALOG(mgr_parm,4928)
{
	NameData	parmnode;			/* parm type:c/d/g */
	NameData	parmkey;			/* parm name */
#ifdef CATALOG_VARLEN
	text		parmvalue;			/* parm value */
	text		parmconfigtype;		/* parm type */	
	text		parmcomment;		/* parm comment */	
#endif								/* CATALOG_VARLEN */
} FormData_mgr_parm;

/* ----------------
 *		Form_mgr_parm corresponds to a pointer to a tuple with
 *		the format of mgr_parm relation.
 * ----------------
 */
typedef FormData_mgr_parm *Form_mgr_parm;

/* ----------------
 *		compiler constants for mgr_parm
 * ----------------
 */
#define Natts_mgr_parm			 		5
#define Anum_mgr_parm_parmnode			1
#define Anum_mgr_parm_parmkey			2
#define Anum_mgr_parm_parmvalue			3
#define Anum_mgr_parm_parmconfigtype	4
#define Anum_mgr_parm_parmcomment		5
#endif /* MGR_GTM_H */
