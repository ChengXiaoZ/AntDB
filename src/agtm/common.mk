
backend_src_dir = $(abs_top_srcdir)/src/backend
backend_obj_dir = $(top_builddir)/src/backend
agtm_inc_dir = $(top_builddir)/src/agtm/include 

override CFLAGS := $(patsubst -DPGXC,, $(CFLAGS))
override CFLAGS := $(patsubst -DADB,, $(CFLAGS))
override CFLAGS += -DAGTM -I$(agtm_inc_dir) -I$(top_srcdir)/src/agtm/include -I$(top_srcdir)/$(subdir) -I$(top_srcdir)/src/interfaces

cur_dir = $(subdir:src/agtm/%=%)

override VPATH = $(abs_top_srcdir)/$(subdir):$(backend_src_dir)/$(cur_dir)
include $(backend_src_dir)/common.mk

