#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/spgist
#
# IDENTIFICATION
#    src/agtm/access/spgist/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/access/spgist
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = spgutils.o spginsert.o spgscan.o spgvacuum.o \
	spgdoinsert.o spgxlog.o \
	spgtextproc.o spgquadtreeproc.o spgkdtreeproc.o

include $(top_srcdir)/src/agtm/common.mk
