# Default target
default: all

all: clean bw_template

bw_template: bw_template.c
	gcc bw_template.c -libverbs -o server && ln -s server client

clean:
	rm -rf ./client ./server