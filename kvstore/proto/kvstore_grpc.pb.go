// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.3
// source: proto/kvstore.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	KVStore_Get_FullMethodName          = "/kvstore.KVStore/Get"
	KVStore_Put_FullMethodName          = "/kvstore.KVStore/Put"
	KVStore_GetRingState_FullMethodName = "/kvstore.KVStore/GetRingState"
)

// KVStoreClient is the client API for KVStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// Client-facing service
type KVStoreClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	GetRingState(ctx context.Context, in *RingStateRequest, opts ...grpc.CallOption) (*RingStateResponse, error)
}

type kVStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewKVStoreClient(cc grpc.ClientConnInterface) KVStoreClient {
	return &kVStoreClient{cc}
}

func (c *kVStoreClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, KVStore_Get_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVStoreClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, KVStore_Put_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVStoreClient) GetRingState(ctx context.Context, in *RingStateRequest, opts ...grpc.CallOption) (*RingStateResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RingStateResponse)
	err := c.cc.Invoke(ctx, KVStore_GetRingState_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVStoreServer is the server API for KVStore service.
// All implementations must embed UnimplementedKVStoreServer
// for forward compatibility.
//
// Client-facing service
type KVStoreServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Put(context.Context, *PutRequest) (*PutResponse, error)
	GetRingState(context.Context, *RingStateRequest) (*RingStateResponse, error)
	mustEmbedUnimplementedKVStoreServer()
}

// UnimplementedKVStoreServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedKVStoreServer struct{}

func (UnimplementedKVStoreServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedKVStoreServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (UnimplementedKVStoreServer) GetRingState(context.Context, *RingStateRequest) (*RingStateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRingState not implemented")
}
func (UnimplementedKVStoreServer) mustEmbedUnimplementedKVStoreServer() {}
func (UnimplementedKVStoreServer) testEmbeddedByValue()                 {}

// UnsafeKVStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KVStoreServer will
// result in compilation errors.
type UnsafeKVStoreServer interface {
	mustEmbedUnimplementedKVStoreServer()
}

func RegisterKVStoreServer(s grpc.ServiceRegistrar, srv KVStoreServer) {
	// If the following call pancis, it indicates UnimplementedKVStoreServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&KVStore_ServiceDesc, srv)
}

func _KVStore_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVStoreServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KVStore_Get_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVStoreServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVStore_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVStoreServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KVStore_Put_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVStoreServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVStore_GetRingState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RingStateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVStoreServer).GetRingState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KVStore_GetRingState_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVStoreServer).GetRingState(ctx, req.(*RingStateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// KVStore_ServiceDesc is the grpc.ServiceDesc for KVStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var KVStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "kvstore.KVStore",
	HandlerType: (*KVStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _KVStore_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _KVStore_Put_Handler,
		},
		{
			MethodName: "GetRingState",
			Handler:    _KVStore_GetRingState_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/kvstore.proto",
}

const (
	NodeInternal_Replicate_FullMethodName             = "/kvstore.NodeInternal/Replicate"
	NodeInternal_SyncKeys_FullMethodName              = "/kvstore.NodeInternal/SyncKeys"
	NodeInternal_Heartbeat_FullMethodName             = "/kvstore.NodeInternal/Heartbeat"
	NodeInternal_GarbageCollect_FullMethodName        = "/kvstore.NodeInternal/GarbageCollect"
	NodeInternal_GetReplica_FullMethodName            = "/kvstore.NodeInternal/GetReplica"
	NodeInternal_AddNode_FullMethodName               = "/kvstore.NodeInternal/AddNode"
	NodeInternal_GetReplicationMetrics_FullMethodName = "/kvstore.NodeInternal/GetReplicationMetrics"
)

// NodeInternalClient is the client API for NodeInternal service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// Node-to-node service
type NodeInternalClient interface {
	Replicate(ctx context.Context, in *ReplicateRequest, opts ...grpc.CallOption) (*ReplicateResponse, error)
	SyncKeys(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (*SyncResponse, error)
	Heartbeat(ctx context.Context, in *Ping, opts ...grpc.CallOption) (*Pong, error)
	GarbageCollect(ctx context.Context, in *GarbageCollectRequest, opts ...grpc.CallOption) (*GarbageCollectResponse, error)
	GetReplica(ctx context.Context, in *GetReplicaRequest, opts ...grpc.CallOption) (*GetReplicaResponse, error)
	AddNode(ctx context.Context, in *AddNodeRequest, opts ...grpc.CallOption) (*Empty, error)
	GetReplicationMetrics(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*ReplicationMetricsResponse, error)
}

type nodeInternalClient struct {
	cc grpc.ClientConnInterface
}

func NewNodeInternalClient(cc grpc.ClientConnInterface) NodeInternalClient {
	return &nodeInternalClient{cc}
}

func (c *nodeInternalClient) Replicate(ctx context.Context, in *ReplicateRequest, opts ...grpc.CallOption) (*ReplicateResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReplicateResponse)
	err := c.cc.Invoke(ctx, NodeInternal_Replicate_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeInternalClient) SyncKeys(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (*SyncResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SyncResponse)
	err := c.cc.Invoke(ctx, NodeInternal_SyncKeys_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeInternalClient) Heartbeat(ctx context.Context, in *Ping, opts ...grpc.CallOption) (*Pong, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Pong)
	err := c.cc.Invoke(ctx, NodeInternal_Heartbeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeInternalClient) GarbageCollect(ctx context.Context, in *GarbageCollectRequest, opts ...grpc.CallOption) (*GarbageCollectResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GarbageCollectResponse)
	err := c.cc.Invoke(ctx, NodeInternal_GarbageCollect_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeInternalClient) GetReplica(ctx context.Context, in *GetReplicaRequest, opts ...grpc.CallOption) (*GetReplicaResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetReplicaResponse)
	err := c.cc.Invoke(ctx, NodeInternal_GetReplica_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeInternalClient) AddNode(ctx context.Context, in *AddNodeRequest, opts ...grpc.CallOption) (*Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Empty)
	err := c.cc.Invoke(ctx, NodeInternal_AddNode_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeInternalClient) GetReplicationMetrics(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*ReplicationMetricsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReplicationMetricsResponse)
	err := c.cc.Invoke(ctx, NodeInternal_GetReplicationMetrics_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// NodeInternalServer is the server API for NodeInternal service.
// All implementations must embed UnimplementedNodeInternalServer
// for forward compatibility.
//
// Node-to-node service
type NodeInternalServer interface {
	Replicate(context.Context, *ReplicateRequest) (*ReplicateResponse, error)
	SyncKeys(context.Context, *SyncRequest) (*SyncResponse, error)
	Heartbeat(context.Context, *Ping) (*Pong, error)
	GarbageCollect(context.Context, *GarbageCollectRequest) (*GarbageCollectResponse, error)
	GetReplica(context.Context, *GetReplicaRequest) (*GetReplicaResponse, error)
	AddNode(context.Context, *AddNodeRequest) (*Empty, error)
	GetReplicationMetrics(context.Context, *Empty) (*ReplicationMetricsResponse, error)
	mustEmbedUnimplementedNodeInternalServer()
}

// UnimplementedNodeInternalServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedNodeInternalServer struct{}

func (UnimplementedNodeInternalServer) Replicate(context.Context, *ReplicateRequest) (*ReplicateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Replicate not implemented")
}
func (UnimplementedNodeInternalServer) SyncKeys(context.Context, *SyncRequest) (*SyncResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SyncKeys not implemented")
}
func (UnimplementedNodeInternalServer) Heartbeat(context.Context, *Ping) (*Pong, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedNodeInternalServer) GarbageCollect(context.Context, *GarbageCollectRequest) (*GarbageCollectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GarbageCollect not implemented")
}
func (UnimplementedNodeInternalServer) GetReplica(context.Context, *GetReplicaRequest) (*GetReplicaResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetReplica not implemented")
}
func (UnimplementedNodeInternalServer) AddNode(context.Context, *AddNodeRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddNode not implemented")
}
func (UnimplementedNodeInternalServer) GetReplicationMetrics(context.Context, *Empty) (*ReplicationMetricsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetReplicationMetrics not implemented")
}
func (UnimplementedNodeInternalServer) mustEmbedUnimplementedNodeInternalServer() {}
func (UnimplementedNodeInternalServer) testEmbeddedByValue()                      {}

// UnsafeNodeInternalServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NodeInternalServer will
// result in compilation errors.
type UnsafeNodeInternalServer interface {
	mustEmbedUnimplementedNodeInternalServer()
}

func RegisterNodeInternalServer(s grpc.ServiceRegistrar, srv NodeInternalServer) {
	// If the following call pancis, it indicates UnimplementedNodeInternalServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&NodeInternal_ServiceDesc, srv)
}

func _NodeInternal_Replicate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).Replicate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_Replicate_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).Replicate(ctx, req.(*ReplicateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeInternal_SyncKeys_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SyncRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).SyncKeys(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_SyncKeys_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).SyncKeys(ctx, req.(*SyncRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeInternal_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Ping)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_Heartbeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).Heartbeat(ctx, req.(*Ping))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeInternal_GarbageCollect_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GarbageCollectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).GarbageCollect(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_GarbageCollect_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).GarbageCollect(ctx, req.(*GarbageCollectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeInternal_GetReplica_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetReplicaRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).GetReplica(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_GetReplica_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).GetReplica(ctx, req.(*GetReplicaRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeInternal_AddNode_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddNodeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).AddNode(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_AddNode_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).AddNode(ctx, req.(*AddNodeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeInternal_GetReplicationMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeInternalServer).GetReplicationMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NodeInternal_GetReplicationMetrics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeInternalServer).GetReplicationMetrics(ctx, req.(*Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// NodeInternal_ServiceDesc is the grpc.ServiceDesc for NodeInternal service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var NodeInternal_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "kvstore.NodeInternal",
	HandlerType: (*NodeInternalServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Replicate",
			Handler:    _NodeInternal_Replicate_Handler,
		},
		{
			MethodName: "SyncKeys",
			Handler:    _NodeInternal_SyncKeys_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _NodeInternal_Heartbeat_Handler,
		},
		{
			MethodName: "GarbageCollect",
			Handler:    _NodeInternal_GarbageCollect_Handler,
		},
		{
			MethodName: "GetReplica",
			Handler:    _NodeInternal_GetReplica_Handler,
		},
		{
			MethodName: "AddNode",
			Handler:    _NodeInternal_AddNode_Handler,
		},
		{
			MethodName: "GetReplicationMetrics",
			Handler:    _NodeInternal_GetReplicationMetrics_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/kvstore.proto",
}
