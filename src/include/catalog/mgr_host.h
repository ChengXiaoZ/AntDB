
#ifndef MGR_HOST_H
#define MGR_HOST_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define HostRelationId 4908

CATALOG(mgr_host,4908)
{
	NameData	hostname;		/* host name */
	NameData	hostuser;		/* host user */
	int32		hostport;		/* host port */
	char		hostproto;		/* host protocol of connection */
	int32		hostagentport;	/* agent port */
#ifdef CATALOG_VARLEN
	text		hostaddr;		/* host address */
	text		hostadbhome; 	/*host home*/
#endif /* CATALOG_VARLEN */
} FormData_mgr_host;

/* ----------------
 *		Form_mgr_host corresponds to a pointer to a tuple with
 *		the format of mgr_host relation.
 * ----------------
 */
typedef FormData_mgr_host *Form_mgr_host;

/* ----------------
 *		compiler constants for mgr_host
 * ----------------
 */
#define Natts_mgr_host					7
#define Anum_mgr_host_hostname			1
#define Anum_mgr_host_hostuser			2
#define Anum_mgr_host_hostport			3
#define Anum_mgr_host_hostproto			4
#define Anum_mgr_host_hostagentport		5
#define Anum_mgr_host_hostaddr			6
#define Anum_mgr_host_hostadbhome		7

#define HOST_PROTOCOL_TELNET			't'
#define HOST_PROTOCOL_SSH				's'
#define AGENTDEFAULTPORT				5430

#endif /* MGR_HOST_H */
