#-------------------------------------------------------------------------
#
# Makefile for ADB transaction manager
#
# Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/agtm/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "agtm - the ADB global transaction manager"
# This is a daemon process, which is why it is not labeled as an executable
#PGAPPICON=win32

subdir = src/agtm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
backend_src_dir = $(abs_top_srcdir)/src/backend
backend_obj_dir = $(top_builddir)/src/backend
agtm_inc_dir = $(top_builddir)/src/agtm/include

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

SUBDIRS = access bootstrap catalog parser commands executor foreign lib libpq \
	main nodes optimizer port postmaster regex replication rewrite \
	storage tcop tsearch utils \
	$(top_builddir)/src/timezone $(top_builddir)/src/interfaces/libpq

include $(backend_src_dir)/common.mk

# As of 1/2010:
# The probes.o file is necessary for dtrace support on Solaris, and on recent
# versions of systemtap.  (Older systemtap releases just produce an empty
# file, but that's okay.)  However, OS X's dtrace doesn't use it and doesn't
# even recognize the -G option.  So, build probes.o except on Darwin.
# This might need adjustment as other platforms add dtrace support.
ifneq ($(PORTNAME), darwin)
ifeq ($(enable_dtrace), yes)
LOCALOBJS += utils/probes.o
endif
endif

OBJS = $(SUBDIROBJS) $(LOCALOBJS) $(top_builddir)/src/port/libpgport_srv.a \
	   $(top_builddir)/src/common/libpgcommon_agtm.a \
       $(top_builddir)/src/interfaces/libpq/libpq.a


# We put libpgport and libpgcommon into OBJS, so remove it from LIBS; also add
# libldap
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS)) $(LDAP_LIBS_BE)

# The backend doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))

##########################################################################

all: submake-libpgport submake-schemapg agtm $(POSTGRES_IMP)

ifneq ($(PORTNAME), cygwin)
ifneq ($(PORTNAME), win32)
ifneq ($(PORTNAME), aix)

agtm: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o $@

endif
endif
endif

ifeq ($(PORTNAME), cygwin)

agtm: $(OBJS) agtm.def libagtm.a
	$(DLLTOOL) --dllname $@$(X) --output-exp $@.exp --def agtm.def
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X) -Wl,--base-file,$@.base $@.exp $(call expand_subsys,$(OBJS)) $(LIBS)
	$(DLLTOOL) --dllname $@$(X) --base-file $@.base --output-exp $@.exp --def agtm.def
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack,$(WIN32_STACK_RLIMIT) -o $@$(X) $@.exp $(call expand_subsys,$(OBJS)) $(LIBS)
	rm -f $@.exp $@.base

agtm.def: $(OBJS)
	$(DLLTOOL) --export-all --output-def $@ $(call expand_subsys,$^)

libagtm.a: agtm.def
	$(DLLTOOL) --dllname agtm.exe --def agtm.def --output-lib $@

endif # cygwin

ifeq ($(PORTNAME), win32)
LIBS += -lsecur32

agtm: $(OBJS) agtm.def libagtm.a $(WIN32RES)
	$(DLLTOOL) --dllname $@$(X) --output-exp $@.exp --def agtm.def
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X) -Wl,--base-file,$@.base $@.exp $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS)
	$(DLLTOOL) --dllname $@$(X) --base-file $@.base --output-exp $@.exp --def agtm.def
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack=$(WIN32_STACK_RLIMIT) -o $@$(X) $@.exp $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS)
	rm -f $@.exp $@.base

agtm.def: $(OBJS)
	$(DLLTOOL) --export-all --output-def $@ $(call expand_subsys,$^)

libagtm.a: agtm.def
	$(DLLTOOL) --dllname agtm.exe --def agtm.def --output-lib $@

endif # win32

ifeq ($(PORTNAME), aix)

agtm: $(POSTGRES_IMP)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$(OBJS)) -Wl,-bE:$(top_builddir)/src/agtm/$(POSTGRES_IMP) $(LIBS) -o $@

$(POSTGRES_IMP): $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(call expand_subsys,$^)
ifeq ($(host_os), aix3.2.5)
	$(MKLDEXPORT) SUBSYS.o $(bindir)/agtm > $@
else
ifneq (,$(findstring aix4.1, $(host_os)))
	$(MKLDEXPORT) SUBSYS.o $(bindir)/agtm > $@
else
	$(MKLDEXPORT) SUBSYS.o . > $@
endif
endif
	@rm -f SUBSYS.o

endif # aix

# Update the commonly used headers before building the subdirectories
$(SUBDIRS:%=%-recursive): $(agtm_inc_dir)/parser/gram.h $(agtm_inc_dir)/catalog/schemapg.h $(agtm_inc_dir)/utils/fmgroids.h $(agtm_inc_dir)/utils/errcodes.h $(agtm_inc_dir)/utils/probes.h

# run this unconditionally to avoid needing to know its dependencies here:
submake-schemapg:
	$(MAKE) -C catalog schemapg.h

# src/port needs a convenient way to force errcodes.h to get built
submake-errcodes: $(agtm_inc_dir)/utils/errcodes.h

.PHONY: submake-schemapg submake-errcodes

catalog/schemapg.h: | submake-schemapg

$(top_builddir)/src/port/libpgport_srv.a: | submake-libpgport

# Those are rules to create dependent GTM libraries automatically
$(top_builddir)/src/interfaces/libpq/libpq.a:
	$(MAKE) -C $(top_builddir)/src/interfaces/libpq libpq.a

# The agtm.o target is needed by the rule in Makefile.global that
# creates the exports file when MAKE_EXPORTS = true.
agtm.o: $(OBJS)
	$(CC) $(LDREL) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$^) $(LIBS) -o $@


# The following targets are specified in make commands that appear in
# the make files in our subdirectories. Note that it's important we
# match the dependencies shown in the subdirectory makefiles!

parser/gram.h: $(backend_src_dir)/parser/gram.y
	$(MAKE) -C parser gram.h

utils/fmgroids.h: $(backend_src_dir)/utils/Gen_fmgrtab.pl $(backend_src_dir)/catalog/Catalog.pm $(top_srcdir)/src/include/catalog/pg_proc.h
	$(MAKE) -C utils fmgroids.h

utils/errcodes.h: $(backend_obj_dir)/utils/errcodes.h
	rm -f $@
	$(LN_S) `cd '$(backend_obj_dir)/utils' >/dev/null && pwd`/errcodes.h $@

$(backend_obj_dir)/utils/errcodes.h: $(backend_src_dir)/utils/generate-errcodes.pl $(backend_src_dir)/utils/errcodes.txt
	$(MAKE) -C $(backend_obj_dir)/utils errcodes.h

utils/probes.h: $(backend_src_dir)/utils/probes.d
	$(MAKE) -C utils probes.h

# Make symlinks for these headers in the include directory. That way
# we can cut down on the -I options. Also, a symlink is automatically
# up to date when we update the base file.
#
# The point of the prereqdir incantation in some of the rules below is to
# force the symlink to use an absolute path rather than a relative path.
# For headers which are generated by make distprep, the actual header within
# src/backend will be in the source tree, while the symlink in src/include
# will be in the build tree, so a simple ../.. reference won't work.
# For headers generated during regular builds, we prefer a relative symlink.

$(agtm_inc_dir)/parser/gram.h: parser/gram.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(agtm_inc_dir)/catalog/schemapg.h: catalog/schemapg.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(agtm_inc_dir)/utils/errcodes.h: utils/errcodes.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(agtm_inc_dir)/utils/fmgroids.h: utils/fmgroids.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(agtm_inc_dir)/utils/probes.h: utils/probes.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .


utils/probes.o: utils/probes.d $(SUBDIROBJS)
	$(DTRACE) $(DTRACEFLAGS) -C -G -s $(call expand_subsys,$^) -o $@


##########################################################################

# Be sure that these files get removed by the maintainer-clean target
distprep:
	$(MAKE) -C parser	gram.c gram.h scan.c
	$(MAKE) -C bootstrap	bootparse.c bootscanner.c
	$(MAKE) -C catalog	schemapg.h agtm.bki agtm.description agtm.shdescription
	$(MAKE) -C replication	repl_gram.c repl_scanner.c
	$(MAKE) -C utils	fmgrtab.c fmgroids.h errcodes.h
	$(MAKE) -C utils/misc	guc-file.c
	$(MAKE) -C utils/sort	qsort_tuple.c


##########################################################################

install: all installdirs install-bin
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(INSTALL_DATA) libagtm.a '$(DESTDIR)$(libdir)/libagtm.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(INSTALL_DATA) libagtm.a '$(DESTDIR)$(libdir)/libagtm.a'
endif
endif
	$(MAKE) -C catalog install-data
	$(INSTALL_DATA) $(srcdir)/utils/misc/postgresql.conf.sample '$(DESTDIR)$(datadir)/agtm.conf.sample'

install-bin: agtm $(POSTGRES_IMP) installdirs
	$(INSTALL_PROGRAM) agtm$(X) '$(DESTDIR)$(bindir)/agtm$(X)'

.PHONY: install-bin

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(MAKE_EXPORTS), true)
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)'
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(MKLDEXPORT_DIR)'
endif


##########################################################################

uninstall:
	rm -f '$(DESTDIR)$(bindir)/agtm$(X)'
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libagtm.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libagtm.a'
endif
endif
	$(MAKE) -C catalog uninstall-data
	$(MAKE) -C tsearch uninstall-data
	rm -f '$(DESTDIR)$(datadir)/agtm.conf.sample' '$(DESTDIR)$(datadir)/manager_create.sql'

##########################################################################

clean:
	rm -f $(LOCALOBJS) postgres$(X) $(POSTGRES_IMP) \
		$(agtm_inc_dir)/parser/gram.h \
		$(agtm_inc_dir)/catalog/schemapg.h \
		$(agtm_inc_dir)/utils/fmgroids.h \
		$(agtm_inc_dir)/utils/probes.h
ifeq ($(PORTNAME), cygwin)
	rm -f agtm.dll agtm.def libagtm.a
endif
ifeq ($(PORTNAME), win32)
	rm -f agtm.dll agtm.def libagtm.a $(WIN32RES)
endif

distclean: clean
	rm -f port/tas.s port/dynloader.c port/pg_sema.c port/pg_shmem.c \
	      port/pg_latch.c

maintainer-clean: distclean
	rm -f bootstrap/bootparse.c \
	      bootstrap/bootscanner.c \
	      parser/gram.c \
	      parser/gram.h \
	      parser/scan.c \
	      catalog/schemapg.h \
	      catalog/agtm.bki \
	      catalog/agtm.description \
	      catalog/agtm.shdescription \
	      replication/repl_gram.c \
	      replication/repl_scanner.c \
	      utils/fmgroids.h \
	      utils/fmgrtab.c \
	      utils/errcodes.h \
	      utils/misc/guc-file.c \
	      utils/sort/qsort_tuple.c


##########################################################################
#
# Support for code development.
#
# Use target "quick" to build "postgres" when you know all the subsystems
# are up to date.  It saves the time of doing all the submakes.
.PHONY: quick
quick: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o postgres
