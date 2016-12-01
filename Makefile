all: bfs

bfs: bfs.o
	gcc bfs.o -o bfs -lhiredis -lfuse -Wall

bfs.o: bfs.c
	gcc -c bfs.c -I /usr/include/hiredis/ -D_FILE_OFFSET_BITS=64  

clean:
	rm -rf *.o bfs

run:
	./bfs /tmp/fuse
