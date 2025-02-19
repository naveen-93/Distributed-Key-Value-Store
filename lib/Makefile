.PHONY: all clean test

all: my-libkv.so

my-libkv.so: libkv.go
	go build -buildmode=c-shared -o my-libkv.so libkv.go

test: my-libkv.so
	ln -sf my-libkv.so libkv.so
	gcc -o test/test_libkv test/test_libkv.c -L. -lkv -I. -lpthread
	LD_LIBRARY_PATH=. ./test/test_libkv

clean:
	rm -f my-libkv.so test/test_libkv libkv.so



