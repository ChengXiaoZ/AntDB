#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for agtm/port/win32
#
# IDENTIFICATION
#    src/agtm/port/win32/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/port/win32
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = timer.o socket.o signal.o security.o mingwcompat.o
ifeq ($(have_win32_dbghelp), yes)
OBJS += crashdump.o
endif

include $(top_srcdir)/src/agtm/common.mk
