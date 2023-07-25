all: server client
 
server: bw_template.c map.o
	gcc bw_template.c map.o -libverbs -o server

client: server
	ln -sf server client
 
map.o: map.c map.h
	gcc -c map.c -o map.o

clean:
	rm -f server client map.o
