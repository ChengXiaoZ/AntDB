#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/adbmgrd/replication
#
# IDENTIFICATION
#    src/adbmgrd/replication/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/replication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = walsender.o walreceiverfuncs.o walreceiver.o basebackup.o \
	repl_gram.o syncrep.o

include $(top_srcdir)/src/adbmgrd/common.mk

# repl_scanner is compiled as part of repl_gram
repl_gram.o: repl_scanner.c

# repl_gram.c and repl_scanner.c are in the distribution tarball, so
# they are not cleaned here.
