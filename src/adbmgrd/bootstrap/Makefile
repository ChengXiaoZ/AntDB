#-------------------------------------------------------------------------
#
# Makefile for the bootstrap module
#
# src/adbmgrd/bootstrap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/bootstrap
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS= bootparse.o bootstrap.o

include $(top_srcdir)/src/adbmgrd/common.mk

# bootscanner is compiled as part of bootparse
bootparse.o: bootscanner.c

# bootparse.c and bootscanner.c are in the distribution tarball, so
# they are not cleaned here.
