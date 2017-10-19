#-------------------------------------------------------------------------
#
# Makefile for src/bin/initdb
#
# Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/initdb/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "initdb - initialize a new database cluster"
PGAPPICON=win32

subdir = src/bin/initdb
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) -I$(top_srcdir)/src/timezone $(CPPFLAGS)

# use system timezone data?
ifneq (,$(with_system_tzdata))
override CPPFLAGS += '-DSYSTEMTZDIR="$(with_system_tzdata)"'
endif

OBJ_S=	initdb.o findtimezone.o localtime.o encnames.o
OBJS= $(OBJ_S) $(WIN32RES)
OBJS_MGR= $(OBJ_S:%.o=%_mgr.o) $(WIN32RES)
OBJS_AGTM= $(OBJ_S:%.o=%_agtm.o) $(WIN32RES)

all: initdb initmgr initagtm

initdb: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

initmgr: $(OBJS_MGR) | submake-libpgport
	$(CC) $(OBJS_MGR) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

initagtm: $(OBJS_AGTM) | submake-libpgport
	$(CC) $(OBJS_AGTM) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

$(OBJS_MGR): %_mgr.o : %.c
	$(CC) $(subst -DPGXC,, $(subst -DADB,, $(CFLAGS))) $(CPPFLAGS) -DINITMGR -c -o $@ $<

$(OBJS_AGTM): %_agtm.o : %.c
	$(CC) $(subst -DPGXC,, $(subst -DADB,, $(CFLAGS))) $(CPPFLAGS) -DINITAGTM -c -o $@ $<

# We used to pull in all of libpq to get encnames.c, but that
# exposes us to risks of version skew if we link to a shared library.
# Do it the hard way, instead, so that we're statically linked.

encnames.c: % : $(top_srcdir)/src/backend/utils/mb/%
	rm -f $@ && $(LN_S) $< .

# Likewise, pull in localtime.c from src/timezones

localtime.c: % : $(top_srcdir)/src/timezone/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) initdb$(X) '$(DESTDIR)$(bindir)/initdb$(X)'
	$(INSTALL_PROGRAM) initmgr$(X) '$(DESTDIR)$(bindir)/initmgr$(X)'
	$(INSTALL_PROGRAM) initagtm$(X) '$(DESTDIR)$(bindir)/initagtm$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/initdb$(X)'
	rm -f '$(DESTDIR)$(bindir)/initmgr$(X)'
	rm -f '$(DESTDIR)$(bindir)/initagtm$(X)'

clean distclean maintainer-clean:
	rm -f initdb$(X) initmgr$(X) initagtm$(X) $(OBJS) $(OBJS_MGR) $(OBJS_AGTM) encnames.c localtime.c


# ensure that changes in datadir propagate into object file
initdb.o: initdb.c $(top_builddir)/src/Makefile.global
initmgr.o: initdb.c $(top_builddir)/src/Makefile.global
initagtm.o: initdb.c $(top_builddir)/src/Makefile.global
