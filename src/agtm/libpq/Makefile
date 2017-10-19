#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for libpq subsystem (agtm half of libpq interface)
#
# IDENTIFICATION
#    src/agtm/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/libpq
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# be-fsstubs is here for historical reasons, probably belongs elsewhere

OBJS = be-fsstubs.o be-secure.o auth.o crypt.o hba.o ip.o md5.o pqcomm.o \
       pqformat.o pqsignal.o pqnode.o pqnone.o

include $(top_srcdir)/src/agtm/common.mk
