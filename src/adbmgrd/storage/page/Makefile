#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/page
#
# IDENTIFICATION
#    src/adbmgrd/storage/page/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/storage/page
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS =  bufpage.o checksum.o itemptr.o

include $(top_srcdir)/src/adbmgrd/common.mk

# important optimizations flags for checksum.c
checksum.o: CFLAGS += ${CFLAGS_VECTOR}
