# Default target
default: all

all: clean bw_template

bw_template: bw_template.c
	gcc bw_template.c -o server -lrdmacm -libverbs && ln -s server client

clean:
	rm -rf ./client ./server
