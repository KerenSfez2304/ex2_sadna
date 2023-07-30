all: server client
 
server: bw_template_us.c map.o
	gcc bw_template.c -libverbs -o server

client: server
	ln -sf server client


clean:
	rm -f server client
