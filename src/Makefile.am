bin_PROGRAMS = qperf

AM_CFLAGS = -Wall -O -DRDMA

qperf_SOURCES = qperf.c ip.c ib.c help.c qperf.h
qperf_LDADD = -libverbs
man_MANS = qperf.1

qperf.1:	help.txt
	./mkman >qperf.1

help.c: help.txt
	./mkhelp RDMA

clean-local:
	-rm -f qperf.1
