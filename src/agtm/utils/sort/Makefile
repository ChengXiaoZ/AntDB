#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/sort
#
# IDENTIFICATION
#    src/agtm/utils/sort/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/utils/sort
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = logtape.o sortsupport.o tuplesort.o tuplestore.o

tuplesort.o: qsort_tuple.c

qsort_tuple.c: gen_qsort_tuple.pl
	$(PERL) $< $< > $@

include $(top_srcdir)/src/agtm/common.mk

maintainer-clean:
	rm -f qsort_tuple.c
