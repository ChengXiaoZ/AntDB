#
# Makefile for utils
#
# src/adbmgrd/utils/Makefile
#

subdir = src/adbmgrd/utils
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS        = fmgrtab.o
SUBDIRS     = adt cache error fmgr hash init mb misc mmgr resowner sort time

# location of Catalog.pm
catalogdir  = $(backend_src_dir)/catalog

include $(top_srcdir)/src/adbmgrd/common.mk

all: errcodes.h fmgroids.h probes.h

$(SUBDIRS:%=%-recursive): fmgroids.h

# see explanation in ../parser/Makefile
fmgroids.h: fmgrtab.c ;

fmgrtab.c: $(backend_src_dir)/utils/Gen_fmgrtab.pl $(catalogdir)/Catalog.pm ../catalog/pg_proc.h
	$(PERL) -I $(catalogdir) $< ../catalog/pg_proc.h

errcodes.h: $(backend_src_dir)/utils/errcodes.txt generate-errcodes.pl
	$(PERL) $(backend_src_dir)/utils/generate-errcodes.pl $< > $@

../catalog/pg_proc.h: $(top_srcdir)/src/include/catalog/pg_proc.h
	$(MAKE) -C ../catalog pg_proc.h

ifneq ($(enable_dtrace), yes)
probes.h: Gen_dummy_probes.sed
endif

probes.h: probes.d
ifeq ($(enable_dtrace), yes)
	$(DTRACE) -C -h -s $< -o $@.tmp
	sed -e 's/POSTGRESQL_/TRACE_POSTGRESQL_/g' $@.tmp >$@
	rm $@.tmp
else
	sed -f $(backend_src_dir)/utils/Gen_dummy_probes.sed $< >$@
endif


# fmgroids.h, fmgrtab.c and errcodes.h are in the distribution tarball, so they
# are not cleaned here.
clean:
	rm -f probes.h

maintainer-clean: clean
	rm -f fmgroids.h fmgrtab.c errcodes.h
