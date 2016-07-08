#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/rxact
#
# IDENTIFICATION
#    src/backend/access/rxact/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/rxact
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = rxact_mgr.o rxact_comm.o

include $(top_srcdir)/src/backend/common.mk

libpg-fe.h:
	$(LN_S) $(top_builddir)/contrib/gtm/client/libpg-fe.h $(top_srcdir)/src/include/
