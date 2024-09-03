from abc import ABC, abstractmethod

import grpc

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config
from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.grpc import master_node_pb2_grpc, \
    master_node_pb2


class ChunkProcessorInterface(ABC):
    @abstractmethod
    def process_chunk(self, chunk, keyspace):
        pass


class ChunkProcessor(ChunkProcessorInterface):
    def __init__(self, keyspace):
        self.keyspace = keyspace

    def process_chunk(self, chunk, keyspace):
        from my_immutable_KV_store.src.my_immutable_kv_store.my_imm_kv_store import logger
        channel = grpc.insecure_channel(f'localhost:{Config.GRPC_MASTER_PORT}')
        stub = master_node_pb2_grpc.MasterNodeStub(channel)
        for line in chunk:
            if not line:
                continue
            try:
                key, value = line.strip().split(maxsplit=1)
                stub.Put(master_node_pb2.NodeRequest(key=key, value=value))
            except ValueError:
                logger.warning(f"Skipping invalid line: {line}")
