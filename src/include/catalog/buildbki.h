/*
 * catalog/buildbki.h
 */

#ifndef BUILD_BKI_H
#define BUILD_BKI_H

#undef CATALOG

#define CATALOG_VARLEN 1

/* Options that may appear after CATALOG (on the same line) */
#undef BKI_BOOTSTRAP
#undef BKI_SHARED_RELATION
#undef BKI_WITHOUT_OIDS
#undef BKI_ROWTYPE_OID
#undef BKI_SCHEMA_MACRO

#undef DATA
#undef DESCR
#undef SHDESCR

#undef DECLARE_TOAST

#endif /* BUILD_BKI_H */