
#ifndef MONITOR_USER_H
#define MONITOR_USER_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#define timestamptz int
#endif /* BUILD_BKI */

#define MuserRelationId 4953

CATALOG(monitor_user,4953)
{
	NameData				username;				/*the user name*/
	int32					usertype;				/*1: ordinary users, 2: db manager*/
	timestamptz				userstarttime;
	timestamptz				userendtime;
	NameData				usertel;
	NameData				useremail;
	NameData				usercompany;
	NameData				userdepart;
	NameData				usertitle;
#ifdef CATALOG_VARLEN
	text						userpassword;
	text						userdesc;		/*plan for the query*/
#endif /* CATALOG_VARLEN */	
} FormData_monitor_user;

/* ----------------
 *		FormData_monitor_user corresponds to a pointer to a tuple with
 *		the format of Form_monitor_user relation.
 * ----------------
 */
typedef FormData_monitor_user *Form_monitor_user;

#ifndef BUILD_BKI
#undef timestamptz
#endif

/* ----------------
 *		compiler constants for Form_monitor_user
 * ----------------
 */
#define Natts_monitor_user								7
#define Anum_monitor_user_name							1
#define Anum_monitor_user_type							2
#define Anum_monitor_user_starttime						3
#define Anum_monitor_user_endtime						4
#define Anum_monitor_user_tel							5
#define Anum_monitor_user_email							6
#define Anum_monitor_user_company						7
#define Anum_monitor_user_depart						8
#define Anum_monitor_user_title							9
#endif /* MONITOR_USER_H */
