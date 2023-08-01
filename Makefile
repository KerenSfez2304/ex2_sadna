all: server client

server: bw_template_o.c
	gcc bw_template_o.c -libverbs -o server

client: server
	ln -sf server client

clean:
	rm -f server client
