#-------------------------------------------------------------------------
#
# Makefile for src/bin/adb_load
#
# src/bin/adb_load/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "adb_load - the ADB load tool"
PGAPPICON=win32

subdir = src/bin/adb_load
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)

LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))

OBJS = adb_load.o loadsetting.o linebuf.o ilist.o compute_hash.o msg_queue.o \
       read_write_file.o msg_queue_pipe.o adbloader_log.o dispatch.o read_producer.o \
       properties.o utility.o \
	   $(WIN32RES)

OBJ_TEST_QUEUE = test_queue.o msg_queue.o linebuf.o adbloader_log.o
OBJ_TEST_QUEUE_PIPE = test_queue_pipe.o msg_queue_pipe.o linebuf.o adbloader_log.o
OBJ_TEST_COMPUTE = test_compute_hash.o msg_queue.o linebuf.o compute_hash.o read_write_file.o msg_queue_pipe.o adbloader_log.o utility.o
OBJ_TEST_DISPATCH = msg_queue.o linebuf.o read_write_file.o msg_queue_pipe.o adbloader_log.o dispatch.o test_dispatch.o utility.o
OBJ_TEST_LOG = test_log.o adbloader_log.o
OBJ_TEST_WRITE_FILE = test_write_file.o linebuf.o msg_queue.o read_write_file.o adbloader_log.o
OBJ_TEST_COMMUNICATE = test_communicat.o
OBJ_TEST_READ = test_read.o msg_queue_pipe.o read_producer.o adbloader_log.o linebuf.o utility.o read_write_file.o
OBJ_TEST_PROPERTIES = adbloader_log.o test_properties.o properties.o
OBJ_TEST_LIST = test_list.o
all: adb_load

adb_load: $(OBJS) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) adb_load$(X) '$(DESTDIR)$(bindir)/adb_load$(X)'
	$(INSTALL_DATA) $(srcdir)/adb_load.conf.example '$(DESTDIR)$(datadir)/adb_load.conf.example'

test: test_queue test_compute test_queue_pipe test_dispatch test_log test_write_file test_communicate test_read test_properties test_list
#	test_queue$(X)

test_queue: $(OBJ_TEST_QUEUE) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_QUEUE) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_compute: $(OBJ_TEST_COMPUTE) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_COMPUTE) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_queue_pipe:$(OBJ_TEST_QUEUE_PIPE) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_QUEUE_PIPE) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)	

test_dispatch:$(OBJ_TEST_DISPATCH) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_DISPATCH) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_log:$(OBJ_TEST_LOG) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_LOG) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_write_file:$(OBJ_TEST_WRITE_FILE) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_WRITE_FILE) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_communicate:$(OBJ_TEST_COMMUNICATE) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_COMMUNICATE) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_read:$(OBJ_TEST_READ) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_READ) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_properties:$(OBJ_TEST_PROPERTIES) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_PROPERTIES) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

test_list:$(OBJ_TEST_LIST) | submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(OBJ_TEST_LIST) $(LDFLAGS) $(LDFLAGS_EX) -lpthread $(LIBS) $(libpq) -o $@$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/adb_load$(X)' '$(DESTDIR)$(datadir)/adb_load.sample'

clean distclean:
	rm -f adb_load$(X) $(OBJS)
	rm -f $(OBJ_TEST_QUEUE)
	rm -f $(OBJ_TEST_QUEUE_PIPE)
	rm -f $(OBJ_TEST_COMPUTE)
	rm -f $(OBJ_TEST_DISPATCH)
	rm -f $(OBJ_TEST_LOG)
	rm -f $(OBJ_TEST_WRITE_FILE)
	rm -f $(OBJ_TEST_COMMUNICATE)
	rm -f $(OBJ_TEST_READ)
	rm -f $(OBJ_TEST_PROPERTIES)
	rm -f $(OBJ_TEST_LIST)
	rm -f test_queue
	rm -f test_compute
	rm -f test_queue_pipe
	rm -f test_dispatch
	rm -f test_log
	rm -f test_write_file
	rm -f test_communicate
	rm -f test_read
	rm -f test_properties
	rm -f test_list
#maintainer-clean: distclean
#	rm -f 

