all: check

check: proto.pb.go
	go test -v .

proto.pb.go: proto.proto
	protoc --go_out=. proto.proto

clean:
	rm -f proto.pb.go

.PHONY: check clean
