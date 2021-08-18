package main

import (
	"bufio"
	"io"
	"path/filepath"
	"strings"
	"os"
)

func main() {
	protoSource := filepath.Join("..", "pg_pb3.proto")
	fh, err := os.Open(protoSource)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(fh)
	var lines []string
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		line = strings.TrimSuffix(line, "\n")
		if strings.HasPrefix(line, "package ") {
			line = `package test;`
		} else if strings.HasPrefix(line, "option go_package = ") {
			line = `option go_package = "./";`
		}
		lines = append(lines, line)
	}
	data := strings.Join(lines, "\n") + "\n"
	err = os.WriteFile("pg_pb3_test.proto", []byte(data), 0644)
	if err != nil {
		panic(err)
	}
}
