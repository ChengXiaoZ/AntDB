#----------------------------------------------------------------------------
#
# Postgres-XC backend method makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/backend/pgxc/Makefile
#
#-----------------------------------------------------------------------------
subdir = src/backend/pgxc
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_expansion), yes)
SUBDIRS	    = locator pool nodemgr copy xc_maintenance_mode cluster slot barrier
endif

ifeq ($(enable_expansion), no)
SUBDIRS	    = locator pool nodemgr copy xc_maintenance_mode cluster barrier
endif


include $(top_srcdir)/src/backend/common.mk
