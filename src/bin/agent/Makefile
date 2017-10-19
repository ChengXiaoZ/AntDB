#-------------------------------------------------------------------------
#
# Makefile for src/bin/agent
#
# Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/agent/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "agent - ADB cluster manager command agent"
PGAPPICON=win32

subdir = src/bin/agent
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS= agent.o agent_elog.o globals.o \
	agt_msg.o agt_cmd.o backend.o \
	agent_utility.o \
	assert.o aset.o mcxt.o stringinfo.o \
	conf_scan.o hba_scan.o \
	$(top_builddir)/src/port/libpgport_srv.a \
	$(top_builddir)/src/common/libpgcommon_srv.a

CFLAGS += -I$(top_srcdir)/$(subdir)

# We put libpgport and libpgcommon into OBJS, so remove it from LIBS
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS))

# The agent doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))

all: submake-libpgport agent

assert.c: % : $(top_srcdir)/src/backend/utils/error/%
	rm -f $@ && $(LN_S) $< .

aset.c mcxt.c: % : $(top_srcdir)/src/backend/utils/mmgr/%
	rm -f $@ && $(LN_S) $< .

stringinfo.c: % : $(top_srcdir)/src/backend/lib/%
	rm -f $@ && $(LN_S) $< .

agent:	$(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
 
install: all installdirs
	$(INSTALL_PROGRAM) agent$(X) '$(DESTDIR)$(bindir)/agent$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/agent$(X)'

distclean: clean
	rm -f assert.c aset.c mcxt.c stringinfo.c conf_scan.c hba_scan.c

clean maintainer-clean:
	rm -f agent$(X) $(OBJS)  


