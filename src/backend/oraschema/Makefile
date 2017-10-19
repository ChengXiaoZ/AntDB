#-------------------------------------------------------------------------
#
# Makefile for backend/oraschema
#
# src/backend/oraschema/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/oraschema
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = convert.o nvarchar2.o orastr.o oradate.o others.o random.o varchar2.o oracoerce.o
DATA = oracle_schema.sql
SQL_FILES = oracle_type.sql oracle_proc.sql oracle_random.sql oracle_operator.sql 

include $(top_srcdir)/src/backend/common.mk

all: $(DATA)

$(DATA):$(SQL_FILES)
	echo "GRANT USAGE ON SCHEMA oracle TO PUBLIC;" > $@
	cat $^ >> $@

clean:
	-rm -f $(DATA)
