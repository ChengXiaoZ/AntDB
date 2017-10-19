#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/agtm/replication
#
# IDENTIFICATION
#    src/agtm/replication/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/replication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = walsender.o walreceiverfuncs.o walreceiver.o basebackup.o \
	repl_gram.o syncrep.o

include $(top_srcdir)/src/agtm/common.mk

# repl_scanner is compiled as part of repl_gram
repl_gram.o: repl_scanner.c

# repl_gram.c and repl_scanner.c are in the distribution tarball, so
# they are not cleaned here.
