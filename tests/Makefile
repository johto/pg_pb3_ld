CWD := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROTOFILE_PATH := $(CWD)/../pg_pb3.proto

all: check

check: proto.pb.go
	go test -v .

proto.proto: $(PROTOFILE_PATH)
	(head -2 $(PROTOFILE_PATH) > proto.proto) && (echo 'package test;' >> proto.proto) && (tail -n +2 $(PROTOFILE_PATH) >> proto.proto)

proto.pb.go: proto.proto
	protoc --go_out=. proto.proto

clean:
	rm -f proto.pb.go proto.proto

.PHONY: check clean
