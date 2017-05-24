#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for src/common
#
# This makefile generates two outputs:
#
#	libpgcommon.a - contains object files with FRONTEND defined,
#		for use by client application and libraries
#
#	libpgcommon_srv.a - contains object files without FRONTEND defined,
#		for use only by the backend binaries
#
# IDENTIFICATION
#    src/common/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

LIBS += $(PTHREAD_LIBS)

OBJS_COMMON = relpath.o string.o psprintf.o

OBJS_FRONTEND = $(OBJS_COMMON) fe_memutils.o

OBJS_PG_FRONTEND = $(OBJS_FRONTEND:%.o=%_pg.o)

OBJS_SRV = $(OBJS_COMMON:%.o=%_srv.o)

OBJS_MGR = $(OBJS_COMMON:%.o=%_mgr.o)

OBJS_AGTM = $(OBJS_COMMON:%.o=%_agtm.o)

all: libpgcommon.a libpgcommon_srv.a libpgcommon_mgr.a libpgcommon_agtm.a libpgcommon_pg.a

# libpgcommon is needed by some contrib
install: all installdirs
	$(INSTALL_STLIB) libpgcommon.a '$(DESTDIR)$(libdir)/libpgcommon.a'
	$(INSTALL_STLIB) libpgcommon_pg.a '$(DESTDIR)$(libdir)/libpgcommon_pg.a'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libpgcommon.a'
	rm -f '$(DESTDIR)$(libdir)/libpgcommon_pg.a'

libpgcommon.a: $(OBJS_FRONTEND)
	rm -f $@
	$(AR) $(AROPT) $@ $^

libpgcommon_pg.a: $(OBJS_PG_FRONTEND)
	rm -f $@
	$(AR) $(AROPT) $@ $^

#
# Server versions of object files
#

libpgcommon_srv.a: $(OBJS_SRV)
	rm -f $@
	$(AR) $(AROPT) $@ $^

libpgcommon_mgr.a: $(OBJS_MGR)
	rm -f $@
	$(AR) $(AROPT) $@ $^

libpgcommon_agtm.a: $(OBJS_AGTM)
	rm -f $@
	$(AR) $(AROPT) $@ $^

# Because this uses its own compilation rule, it doesn't use the
# dependency tracking logic from Makefile.global.  To make sure that
# dependency tracking works anyway for the *_srv.o files, depend on
# their *.o siblings as well, which do have proper dependencies.  It's
# a hack that might fail someday if there is a *_srv.o without a
# corresponding *.o, but it works for now.
%_pg.o: %.c %.o
	$(CC) $(filter-out -DADB, $(CFLAGS)) $(CPPFLAGS) -c $< -o $@

%_srv.o: %.c %.o
	$(CC) $(CFLAGS) $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@

%_mgr.o: %.c %.o
	$(CC) $(patsubst -DPGXC,,$(patsubst -DADB,, $(CFLAGS))) -DADBMGRD $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@

%_agtm.o: %.c %.o
	$(CC) $(patsubst -DPGXC,,$(patsubst -DADB,, $(CFLAGS))) -DAGTM $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@

$(OBJS_SRV) $(OBJS_MGR) $(OBJS_AGTM): | submake-errcodes

.PHONY: submake-errcodes

submake-errcodes:
	$(MAKE) -C ../backend submake-errcodes

clean distclean maintainer-clean:
	rm -f libpgcommon.a libpgcommon_srv.a libpgcommon_mgr.a libpgcommon_agtm.a $(OBJS_FRONTEND) $(OBJS_SRV) $(OBJS_MGR) $(OBJS_AGTM)
	rm -f libpgcommon_pg.a $(OBJS_PG_FRONTEND)
