package main

import (
	"fmt"
	proto "github.com/golang/protobuf/proto"
	"strings"
)

type FuzzerError struct {
	Transaction *TestTransaction
	ExpectedMessages []proto.Message
	ReceivedMessages []proto.Message
	Err error
}

func (fe *FuzzerError) DescribeExpectedMessages() string {
	var descriptions []string
	for _, msg := range fe.ExpectedMessages {
		desc := fmt.Sprintf("%T%s", msg, proto.MarshalTextString(msg))
		descriptions = append(descriptions, desc)
	}
	return strings.Join(descriptions, "\n")
}

func (fe *FuzzerError) DescribeReceivedMessages() string {
	var descriptions []string
	for _, msg := range fe.ReceivedMessages {
		desc := fmt.Sprintf("%T%s", msg, proto.MarshalTextString(msg))
		descriptions = append(descriptions, desc)
	}
	return strings.Join(descriptions, "\n")
}

func (fe *FuzzerError) Error() string {
	return fe.Err.Error()
}

