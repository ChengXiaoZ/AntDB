#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for tcop
#
# IDENTIFICATION
#    src/adbmgrd/tcop/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/tcop
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS= dest.o fastpath.o postgres.o pquery.o utility.o utility_mgr.o

ifneq (,$(filter $(PORTNAME),cygwin win32))
override CPPFLAGS += -DWIN32_STACK_RLIMIT=$(WIN32_STACK_RLIMIT)
endif

include $(top_srcdir)/src/adbmgrd/common.mk
