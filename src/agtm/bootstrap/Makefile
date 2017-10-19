#-------------------------------------------------------------------------
#
# Makefile for the bootstrap module
#
# src/agtm/bootstrap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/bootstrap
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS= bootparse.o bootstrap.o

include $(top_srcdir)/src/agtm/common.mk

# bootscanner is compiled as part of bootparse
bootparse.o: bootscanner.c

# bootparse.c and bootscanner.c are in the distribution tarball, so
# they are not cleaned here.
