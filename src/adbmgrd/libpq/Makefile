#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for libpq subsystem (adbmgrd half of libpq interface)
#
# IDENTIFICATION
#    src/adbmgrd/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/libpq
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# be-fsstubs is here for historical reasons, probably belongs elsewhere

OBJS = be-fsstubs.o be-secure.o auth.o crypt.o hba.o ip.o md5.o pqcomm.o \
       pqformat.o pqsignal.o

include $(top_srcdir)/src/adbmgrd/common.mk
