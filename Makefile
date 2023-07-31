all: tests server client

server: bw_template.c
	gcc bw_template.c -libverbs -o server

tests: tests.c
	gcc tests.c -o tests

client: server
	ln -sf server client

clean:
	rm -f server client tests
