import multiprocessing
from contextlib import asynccontextmanager

import grpc
from concurrent import futures

import uvicorn
from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from grpc_reflection.v1alpha import reflection
from time import sleep
import threading

from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.grpc import master_node_pb2, \
    master_node_pb2_grpc

from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.sharding.consistent_hashing import ConsistentHashing
import my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.grpc.data_node_pb2_grpc as \
    data_node_pb2_grpc
import my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.grpc.data_node_pb2 as data_node_pb2

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MasterNodeServicer(master_node_pb2_grpc.MasterNodeServicer):
    def __init__(self):
        self.consistent_hashing = ConsistentHashing(
            virtual_nodes=Config.VIRTUAL_NODES,
            replication_factor=Config.REPLICATION_FACTOR,
            nodes=Config.NODE_COUNT
        )
        self.node_status = {f"node_{i}": {"port": Config.DATA_PORT+i, "grpc_port": Config.GRPC_PORT+i} \
                            for i in range(Config.NODE_COUNT)}
        self.failed_nodes = set()

    def Put(self, request, context):
        shard_id = self.consistent_hashing.get_shard_id(f"{request.keyspace}#{request.key}")
        node_id = self.consistent_hashing.get_node_for_shard(shard_id)
        node = self.node_status[f"node_{node_id}"]

        channel = grpc.insecure_channel(f'localhost:{node["grpc_port"]}')
        stub = data_node_pb2_grpc.DataNodeStub(channel)
        response = stub.Put(data_node_pb2.PutRequest(key=request.key, value=request.value, keyspace=request.keyspace))

        return master_node_pb2.NodeResponse(status=response.status)

    def Get(self, request, context):
        shard_id = self.consistent_hashing.get_shard_id(f"{request.keyspace}#{request.key}")
        node_id = self.consistent_hashing.get_node_for_shard(shard_id)
        node = self.node_status[f"node_{node_id}"]

        channel = grpc.insecure_channel(f'localhost:{node["grpc_port"]}')
        stub = data_node_pb2_grpc.DataNodeStub(channel)
        response = stub.Get(data_node_pb2.GetRequest(key=request.key, keyspace=request.keyspace))

        return master_node_pb2.NodeResponse(status=response.status, value=response.value)

    def Rebalance(self, request, context):
        # new_shard_map = self.consistent_hashing.rebalance_shards(failed_node_id=None)
        #
        # for node_id, shards in new_shard_map.items():
        #     node = self.node_status[node_id]
        #     channel = grpc.insecure_channel(f'{node["address"]}:{node["port"]}')
        #     stub = data_node_pb2_grpc.DataNodeStub(channel)
        #     for shard_hash in shards:
        #         # TODO: Implement re-balancing of shards in case of new node added/existing deleted
        #         pass
        return master_node_pb2.Status(message="Rebalancing completed.")

    def check_heartbeats(self):
        while True:
            for node_id, node_info in self.node_status.items():
                channel = grpc.insecure_channel(f'localhost:{node_info["grpc_port"]}')
                stub = data_node_pb2_grpc.DataNodeStub(channel)
                try:
                    response = stub.Heartbeat(master_node_pb2.Empty())
                    if response.status != "OK":
                        raise Exception("Heartbeat failed")
                except Exception as e:
                    logger.error(f"Node {node_id} failed: {e}")
                    self.failed_nodes.add(node_id)
                    self.node_status[node_id]['is_healthy'] = False
            if self.failed_nodes:
                self.Rebalance(master_node_pb2.Empty(), context=None)
                self.failed_nodes.clear()
            sleep(Config.HEARTBEAT_INTERVAL)


def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_node = MasterNodeServicer()
    master_node_pb2_grpc.add_MasterNodeServicer_to_server(master_node, server)

    SERVICE_NAMES = (
        master_node_pb2.DESCRIPTOR.services_by_name['MasterNode'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port(f'[::]:{Config.GRPC_MASTER_PORT}')
    server.start()

    # Start heartbeat checking in a separate thread
    threading.Thread(target=master_node.check_heartbeats).start()

    server.wait_for_termination()


@asynccontextmanager
async def lifespan(_: FastAPI):
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    p = multiprocessing.Process(target=serve_grpc, args=())
    p.start()
    yield


app = FastAPI(lifespan=lifespan)


# FastAPI endpoints for management or monitoring
@app.get("/health")
async def health_check():
    return {"status": "OK"}


@app.get("/get")
@cache(expire=60)
async def retrieve(key=None, keyspace=None):
    keyspace = Config.DEFAULT_KEYSPACE if not keyspace else keyspace
    channel = grpc.insecure_channel(f'localhost:{Config.GRPC_MASTER_PORT}')
    stub = master_node_pb2_grpc.MasterNodeStub(channel)
    response = stub.Get(master_node_pb2.NodeRequest(key=key, value=None, keyspace=keyspace))
    if response.value:
        return {"status": "success", "value": response.value}
    else:
        return {"status": "failure", "value": None}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=Config.MASTER_PORT)
