# src/bin/pg_xlogdump/Makefile

PGFILEDESC = "pg_clogdump - decode and display transaction status of any transaction id"
PGAPPICON=win32

subdir = src/bin/pg_clogdump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = pg_clogdump.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

all: pg_clogdump

pg_clogdump: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_clogdump$(X) '$(DESTDIR)$(bindir)/pg_clogdump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_clogdump$(X)'

clean distclean maintainer-clean:
	rm -f pg_clogdump$(X) $(OBJS)
