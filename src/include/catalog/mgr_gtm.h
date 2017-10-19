
#ifndef MGR_GTM_H
#define MGR_GTM_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */


#define GtmRelationId 4918

CATALOG(mgr_gtm,4918)
{
	NameData	gtmname;		/* gtm name */
	int32		gtmport;		/* gtm port */
	Oid			gtmhost;		/* get hostoid from host*/
	char		gtmtype;		/* gtm type */
	bool		gtminited;		/* is initialized */
#ifdef CATALOG_VARLEN
	text		gtmpath;		/* gtm data path */
#endif						/* CATALOG_VARLEN */
} FormData_mgr_gtm;

/* ----------------
 *		Form_mgr_gtm corresponds to a pointer to a tuple with
 *		the format of mgr_gtm relation.
 * ----------------
 */
typedef FormData_mgr_gtm *Form_mgr_gtm;

/* ----------------
 *		compiler constants for mgr_gtm
 * ----------------
 */
#define Natts_mgr_gtm			6
#define Anum_mgr_gtm_gtmname	1
#define Anum_mgr_gtm_gtmport	2
#define Anum_mgr_gtm_gtmhost	3
#define Anum_mgr_gtm_gtmtype	4
#define Anum_mgr_gtm_gtminited	5
#define Anum_mgr_gtm_gtmpath	6

#define GTM_TYPE_GTM		'g'
#define GTM_TYPE_STANDBY	's'
#define GTM_TYPE_PROXY		'p'

#ifndef GTM_DEFAULT_PORT
#	define GTM_DEFAULT_PORT 6666
#endif

#endif /* MGR_GTM_H */
