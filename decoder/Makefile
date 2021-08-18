MODULE_big = pg_pb3_ld
OBJS = pg_pb3_ld.o protobuf.o fsd.o utils.o

#REGRESS = TODO

ifdef NO_PGXS
subdir = contrib/pg_pb3_ld
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
