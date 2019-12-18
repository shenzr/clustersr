CC = g++ -std=c++11 
CFLAGS = -g -Wall -O2 -lm -lrt

all : common.o gene_placement

common.o: common.c
	gcc -c common.c

gene_placement : gene_placement.cc common.o
	$(CC) -o $@ $^ $(CFLAGS)

clean: 
	rm -rf gene_placement common.o 
