CWD := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROTOFILE_PATH := $(CWD)/../pg_pb3.proto

all: check

check: pg_pb3_test.pb.go
	go test -v .

pg_pb3_test.proto: $(PROTOFILE_PATH)
	go generate

pg_pb3_test.pb.go: pg_pb3_test.proto
	protoc --go_out=. pg_pb3_test.proto && sed -i '' -e 's/^package __$$/package test/' pg_pb3_test.pb.go

clean:
	rm -f pg_pb3_test.proto pg_pb3_test.pb.go

.PHONY: check clean
