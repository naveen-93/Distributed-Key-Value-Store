// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.4
// 	protoc        v5.28.3
// source: proto/wal.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type WALEntry_Operation int32

const (
	WALEntry_PUT    WALEntry_Operation = 0
	WALEntry_DELETE WALEntry_Operation = 1
)

// Enum value maps for WALEntry_Operation.
var (
	WALEntry_Operation_name = map[int32]string{
		0: "PUT",
		1: "DELETE",
	}
	WALEntry_Operation_value = map[string]int32{
		"PUT":    0,
		"DELETE": 1,
	}
)

func (x WALEntry_Operation) Enum() *WALEntry_Operation {
	p := new(WALEntry_Operation)
	*p = x
	return p
}

func (x WALEntry_Operation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (WALEntry_Operation) Descriptor() protoreflect.EnumDescriptor {
	return file_proto_wal_proto_enumTypes[0].Descriptor()
}

func (WALEntry_Operation) Type() protoreflect.EnumType {
	return &file_proto_wal_proto_enumTypes[0]
}

func (x WALEntry_Operation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use WALEntry_Operation.Descriptor instead.
func (WALEntry_Operation) EnumDescriptor() ([]byte, []int) {
	return file_proto_wal_proto_rawDescGZIP(), []int{0, 0}
}

type WALEntry struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Op            WALEntry_Operation     `protobuf:"varint,1,opt,name=op,proto3,enum=kvstore.WALEntry_Operation" json:"op,omitempty"`
	Key           string                 `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Value         string                 `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
	Timestamp     uint64                 `protobuf:"varint,4,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Ttl           uint64                 `protobuf:"varint,5,opt,name=ttl,proto3" json:"ttl,omitempty"`
	Checksum      uint32                 `protobuf:"varint,6,opt,name=checksum,proto3" json:"checksum,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WALEntry) Reset() {
	*x = WALEntry{}
	mi := &file_proto_wal_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WALEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WALEntry) ProtoMessage() {}

func (x *WALEntry) ProtoReflect() protoreflect.Message {
	mi := &file_proto_wal_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WALEntry.ProtoReflect.Descriptor instead.
func (*WALEntry) Descriptor() ([]byte, []int) {
	return file_proto_wal_proto_rawDescGZIP(), []int{0}
}

func (x *WALEntry) GetOp() WALEntry_Operation {
	if x != nil {
		return x.Op
	}
	return WALEntry_PUT
}

func (x *WALEntry) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *WALEntry) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *WALEntry) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *WALEntry) GetTtl() uint64 {
	if x != nil {
		return x.Ttl
	}
	return 0
}

func (x *WALEntry) GetChecksum() uint32 {
	if x != nil {
		return x.Checksum
	}
	return 0
}

var File_proto_wal_proto protoreflect.FileDescriptor

var file_proto_wal_proto_rawDesc = string([]byte{
	0x0a, 0x0f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x77, 0x61, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x07, 0x6b, 0x76, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x22, 0xcd, 0x01, 0x0a, 0x08, 0x57,
	0x41, 0x4c, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x2b, 0x0a, 0x02, 0x6f, 0x70, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0e, 0x32, 0x1b, 0x2e, 0x6b, 0x76, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x57, 0x41,
	0x4c, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x02, 0x6f, 0x70, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1c, 0x0a, 0x09,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x10, 0x0a, 0x03, 0x74, 0x74,
	0x6c, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x03, 0x74, 0x74, 0x6c, 0x12, 0x1a, 0x0a, 0x08,
	0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08,
	0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x22, 0x20, 0x0a, 0x09, 0x4f, 0x70, 0x65, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x07, 0x0a, 0x03, 0x50, 0x55, 0x54, 0x10, 0x00, 0x12, 0x0a,
	0x0a, 0x06, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x10, 0x01, 0x42, 0x0f, 0x5a, 0x0d, 0x6b, 0x76,
	0x73, 0x74, 0x6f, 0x72, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
})

var (
	file_proto_wal_proto_rawDescOnce sync.Once
	file_proto_wal_proto_rawDescData []byte
)

func file_proto_wal_proto_rawDescGZIP() []byte {
	file_proto_wal_proto_rawDescOnce.Do(func() {
		file_proto_wal_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_proto_wal_proto_rawDesc), len(file_proto_wal_proto_rawDesc)))
	})
	return file_proto_wal_proto_rawDescData
}

var file_proto_wal_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_proto_wal_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_proto_wal_proto_goTypes = []any{
	(WALEntry_Operation)(0), // 0: kvstore.WALEntry.Operation
	(*WALEntry)(nil),        // 1: kvstore.WALEntry
}
var file_proto_wal_proto_depIdxs = []int32{
	0, // 0: kvstore.WALEntry.op:type_name -> kvstore.WALEntry.Operation
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_proto_wal_proto_init() }
func file_proto_wal_proto_init() {
	if File_proto_wal_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_proto_wal_proto_rawDesc), len(file_proto_wal_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_wal_proto_goTypes,
		DependencyIndexes: file_proto_wal_proto_depIdxs,
		EnumInfos:         file_proto_wal_proto_enumTypes,
		MessageInfos:      file_proto_wal_proto_msgTypes,
	}.Build()
	File_proto_wal_proto = out.File
	file_proto_wal_proto_goTypes = nil
	file_proto_wal_proto_depIdxs = nil
}
