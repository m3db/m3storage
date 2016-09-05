// Code generated by protoc-gen-go.
// source: configtest.proto
// DO NOT EDIT!

/*
Package configtest is a generated protocol buffer package.

It is generated from these files:
	configtest.proto

It has these top-level messages:
	TestConfig
*/
package configtest

import proto "github.com/golang/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

type TestConfig struct {
	Hosts     []string `protobuf:"bytes,1,rep,name=hosts" json:"hosts,omitempty"`
	BaseValue float64  `protobuf:"fixed64,2,opt,name=base_value" json:"base_value,omitempty"`
	IncValue  float64  `protobuf:"fixed64,3,opt,name=inc_value" json:"inc_value,omitempty"`
	StepSize  int32    `protobuf:"varint,4,opt,name=step_size" json:"step_size,omitempty"`
}

func (m *TestConfig) Reset()         { *m = TestConfig{} }
func (m *TestConfig) String() string { return proto.CompactTextString(m) }
func (*TestConfig) ProtoMessage()    {}

func init() {
}
