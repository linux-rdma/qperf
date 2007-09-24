CC	= gcc 
CFLAGS	= -Wall -O -DRDMA

all:	qperf

qperf:	qperf.o ip.o ib.o help.o
	$(CC) -DRDMA -o $@ $^ -libverbs

help.c: help.txt
	./mkhelp RDMA

.PHONY: clean
clean:
	rm -f *.o help.c qperf

.PHONY: install
install:
	cp qperf /usr/local/bin
