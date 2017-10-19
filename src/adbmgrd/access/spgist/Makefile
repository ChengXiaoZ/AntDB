#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/spgist
#
# IDENTIFICATION
#    src/adbmgrd/access/spgist/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/access/spgist
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = spgutils.o spginsert.o spgscan.o spgvacuum.o \
	spgdoinsert.o spgxlog.o \
	spgtextproc.o spgquadtreeproc.o spgkdtreeproc.o

include $(top_srcdir)/src/adbmgrd/common.mk
