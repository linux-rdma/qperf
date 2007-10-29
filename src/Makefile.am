bin_PROGRAMS = qperf

AM_CFLAGS = -Wall -O -DRDMA

if HAVE_LIBIBVERBS
qperf_SOURCES = qperf.c socket.c rdma.c help.c qperf.h
qperf_LDADD = -libverbs
else
qperf_SOURCES = qperf.c socket.c help.c qperf.h
endif
man_MANS = qperf.1

qperf.1:	help.txt
	./mkman >qperf.1

help.c: help.txt
if HAVE_LIBIBVERBS
	./mkhelp RDMA
else
	./mkhelp
endif

clean-local:
	-rm -f qperf.1
