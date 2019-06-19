bin_PROGRAMS = qperf

if RDMA
AM_CFLAGS = -Wall -O -DRDMA
if HAS_XRC
AM_CFLAGS += -DHAS_XRC=1
endif
qperf_SOURCES = qperf.c socket.c rds.c rdma.c support.c help.c qperf.h
qperf_LDADD = -libverbs
else
AM_CFLAGS = -Wall -O
qperf_SOURCES = qperf.c socket.c rds.c support.c help.c qperf.h
endif
EXTRA_DIST = help.txt mkman mkhelp

man_MANS = qperf.1

qperf.1: $(srcdir)/help.txt
	srcdir=$(srcdir) $(srcdir)/mkman >qperf.1

help.c: $(srcdir)/help.txt
if RDMA
	srcdir=$(srcdir) $(srcdir)/mkhelp RDMA
else
	srcdir=$(srcdir) $(srcdir)/mkhelp
endif

clean-local:
	-rm -f qperf.1
