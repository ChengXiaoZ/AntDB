#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/transam
#
# IDENTIFICATION
#    src/agtm/access/transam/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/access/transam
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = clog.o transam.o varsup.o xact.o rmgr.o slru.o subtrans.o multixact.o \
	timeline.o twophase.o twophase_rmgr.o xlog.o xlogarchive.o xlogfuncs.o \
	xlogreader.o xlogutils.o

include $(top_srcdir)/src/agtm/common.mk

# ensure that version checks in xlog.c get recompiled when catversion.h changes
xlog.o: xlog.c $(top_srcdir)/src/include/catalog/catversion.h

libpg-fe.h:
	$(LN_S) $(top_builddir)/contrib/gtm/client/libpg-fe.h $(top_srcdir)/src/include/
