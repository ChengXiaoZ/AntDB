#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for adbmgrd/regex
#
# IDENTIFICATION
#    src/adbmgrd/regex/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/regex
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = regcomp.o regerror.o regexec.o regfree.o regprefix.o regexport.o

include $(top_srcdir)/src/adbmgrd/common.mk

# mark inclusion dependencies between .c files explicitly
regcomp.o: regcomp.c regc_lex.c regc_color.c regc_nfa.c regc_cvec.c \
        regc_locale.c regc_pg_locale.c

regexec.o: regexec.c rege_dfa.c
