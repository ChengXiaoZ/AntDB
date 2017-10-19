#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/page
#
# IDENTIFICATION
#    src/agtm/storage/page/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/storage/page
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS =  bufpage.o checksum.o itemptr.o

include $(top_srcdir)/src/agtm/common.mk

# important optimizations flags for checksum.c
checksum.o: CFLAGS += ${CFLAGS_VECTOR}
