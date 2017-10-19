
backend_src_dir = $(abs_top_srcdir)/src/backend
backend_obj_dir = $(top_builddir)/src/backend
mgr_inc_dir = $(top_builddir)/src/adbmgrd/include

override CFLAGS := $(subst -DPGXC,, $(CFLAGS))
override CFLAGS := $(subst -DADB,, $(CFLAGS))
override CFLAGS += -DADBMGRD -I$(mgr_inc_dir)

cur_dir = $(subdir:src/adbmgrd/%=%)

override VPATH = $(abs_top_srcdir)/$(subdir):$(backend_src_dir)/$(cur_dir)
include $(backend_src_dir)/common.mk

