#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for adbmgrd/manager
#
# IDENTIFICATION
#    src/adbmgrd/manager/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/manager
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

DATA = adbmgr_init.sql

OBJS = cmd_host.o cmd_updateparm.o mgr_agent.o ssh2_exec.o cmd_node.o mgr_common.o monitor_hostpage.o monitor_databaseitem.o monitor_slowlog.o monitor_dbthreshold.o mgr_hba.o monitor_jobitem.o

include $(top_srcdir)/src/adbmgrd/common.mk


