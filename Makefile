all: server client

server: bw_template.o tests.o
	gcc bw_template.o tests.o -libverbs -o server

client: server
	ln -sf server client

bw_template.o: bw_template.c
	gcc -c bw_template.c -o bw_template.o

tests.o: tests.c
	gcc -c tests.c -o tests.o

clean:
	rm -f server client bw_template.o tests.o
