.PHONY: all clean test

all: libkv.so

# Build the shared library from libkv.go
libkv.so: libkv.go
	go build -buildmode=c-shared -o libkv.so libkv.go

test: libkv.so test/test_libkv.c
	# Compile the test program with proper flags
	gcc -o test/test_libkv test/test_libkv.c -L. -lkv -I. -lpthread -ldl
	# Run the test with library path set
	LD_LIBRARY_PATH=. ./test/test_libkv

clean:
	rm -f libkv.so libkv.h test/test_libkv



