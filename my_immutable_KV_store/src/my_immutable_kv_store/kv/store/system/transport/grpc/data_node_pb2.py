# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: data_node.proto
# Protobuf Python Version: 5.27.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    27,
    2,
    '',
    'data_node.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0f\x64\x61ta_node.proto\x12\tdata_node\":\n\nPutRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t\x12\x10\n\x08keyspace\x18\x03 \x01(\t\"\x1d\n\x0bPutResponse\x12\x0e\n\x06status\x18\x01 \x01(\t\"+\n\nGetRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x10\n\x08keyspace\x18\x02 \x01(\t\",\n\x0bGetResponse\x12\x0e\n\x06status\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t\"\x12\n\x10HeartbeatRequest\"#\n\x11HeartbeatResponse\x12\x0e\n\x06status\x18\x01 \x01(\t2\xbe\x01\n\x08\x44\x61taNode\x12\x34\n\x03Put\x12\x15.data_node.PutRequest\x1a\x16.data_node.PutResponse\x12\x34\n\x03Get\x12\x15.data_node.GetRequest\x1a\x16.data_node.GetResponse\x12\x46\n\tHeartbeat\x12\x1b.data_node.HeartbeatRequest\x1a\x1c.data_node.HeartbeatResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'data_node_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_PUTREQUEST']._serialized_start=30
  _globals['_PUTREQUEST']._serialized_end=88
  _globals['_PUTRESPONSE']._serialized_start=90
  _globals['_PUTRESPONSE']._serialized_end=119
  _globals['_GETREQUEST']._serialized_start=121
  _globals['_GETREQUEST']._serialized_end=164
  _globals['_GETRESPONSE']._serialized_start=166
  _globals['_GETRESPONSE']._serialized_end=210
  _globals['_HEARTBEATREQUEST']._serialized_start=212
  _globals['_HEARTBEATREQUEST']._serialized_end=230
  _globals['_HEARTBEATRESPONSE']._serialized_start=232
  _globals['_HEARTBEATRESPONSE']._serialized_end=267
  _globals['_DATANODE']._serialized_start=270
  _globals['_DATANODE']._serialized_end=460
# @@protoc_insertion_point(module_scope)
