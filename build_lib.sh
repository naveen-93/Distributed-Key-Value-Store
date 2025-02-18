#!/bin/bash

# Build the shared library
go build -buildmode=c-shared -o my-libkv.so pkg/client/c_bindings.go 