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
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

# We need libpython as a shared library.  In Python >=2.5, configure
# asks Python directly.  But because this has been broken in Debian
# for a long time (http://bugs.debian.org/695979), and to support
# older Python versions, we see if there is a file that is named like
# a shared library as a fallback.
ifeq (1,$(python_enable_shared))
shared_libpython = yes
else
ifeq ($(PORTNAME), darwin)
# OS X does supply a .dylib even though Py_ENABLE_SHARED does not get set
shared_libpython = yes
else
ifneq (,$(wildcard $(python_libdir)/libpython*$(DLSUFFIX)*))
shared_libpython = yes
endif
endif
endif

# Python on win32 ships with import libraries only for Microsoft Visual C++,
# which are not compatible with mingw gcc. Therefore we need to build a
# new import library to link with.
ifeq ($(PORTNAME), win32)
pytverstr=$(subst .,,${python_version})
OBJS += libpython${pytverstr}.a
libpython${pytverstr}.a: python${pytverstr}.def
	dlltool --dllname python${pytverstr}.dll --def python${pytverstr}.def --output-lib  libpython${pytverstr}.a
WD=$(subst \,/,$(WINDIR))
python${pytverstr}.def:
	pexports $(WD)/system32/python${pytverstr}.dll > python${pytverstr}.def
endif
OBJS= agent.o agent_elog.o globals.o \
	agt_msg.o agt_cmd.o backend.o \
	agent_utility.o agent_cmd_python.o\
	assert.o aset.o mcxt.o stringinfo.o \
	conf_scan.o hba_scan.o get_uptime.o \
	$(top_builddir)/src/port/libpgport_srv.a \
	$(top_builddir)/src/common/libpgcommon_srv.a

CFLAGS += -I$(top_srcdir)/$(subdir) $(python_includespec)

ifeq ($(shared_libpython),yes)
# We put libpgport and libpgcommon into OBJS, so remove it from LIBS
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS) $(libpq_pgport))

# The agent doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))

LIBS += $(python_libspec) $(python_additional_libs)
all: submake-libpgport agent
else #($(shared_libpython),yes)
all:
	@echo ""; \
	 echo "*** Cannot build PL/Python because libpython is not a shared library." ; \
	 echo "*** You might have to rebuild your Python installation.  Refer to"; \
	 echo "*** the documentation for details."; \
	 echo ""
endif

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
	$(INSTALL_DATA) $(srcdir)/host_info.py '$(DESTDIR)$(datadir)/host_info.py'
	
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/agent$(X)'

distclean: clean
	rm -f assert.c aset.c mcxt.c stringinfo.c conf_scan.c hba_scan.c

clean maintainer-clean:
	rm -f agent$(X) $(OBJS)  


