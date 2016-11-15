
#ifndef MGR_PARM_H
#define MGR_PARM_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */


#define ParmRelationId 4928

CATALOG(mgr_parm,4928) BKI_WITHOUT_OIDS
{
	char		parmtype;		/* parm type:c/d/g/'*' for all/'#' for datanode and coordinator*/
	NameData	parmname;			/* parm name */
	NameData	parmvalue;			/* parm value */
	NameData	parmcontext;		/*backend, user, internal, postmaster, superuser, sighup*/
	NameData	parmvartype;		/*bool, enum, string, integer, real*/	
#ifdef CATALOG_VARLEN
	text		parmunit;
	text		parmminval;			/* parm comment */
	text		parmmaxval;
	text		parmenumval;
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
#define Natts_mgr_parm				9
#define Anum_mgr_parm_type			1
#define Anum_mgr_parm_name			2
#define Anum_mgr_parm_value			3
#define Anum_mgr_parm_context		4
#define Anum_mgr_parm_vartype		5
#define Anum_mgr_parm_unit			6
#define Anum_mgr_parm_minval		7
#define Anum_mgr_parm_maxval		8
#define Anum_mgr_parm_enumval		9

#define PARM_TYPE_GTM 				'G'
#define PARM_TYPE_COORDINATOR		'C'
#define PARM_TYPE_DATANODE			'D'

#endif /* MGR_PARM_H */
