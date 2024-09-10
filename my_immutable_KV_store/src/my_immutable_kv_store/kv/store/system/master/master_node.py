import multiprocessing
from collections import defaultdict
from contextlib import asynccontextmanager
import random

import grpc
from concurrent import futures

import uvicorn
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
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

from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.pool.grpc_pool_utils import GRPCPoolUtils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MasterNodeServicer(master_node_pb2_grpc.MasterNodeServicer):
    def __init__(self):
        self.consistent_hashing = ConsistentHashing(
            virtual_nodes=Config.VIRTUAL_NODES,
            replication_factor=Config.REPLICATION_FACTOR,
            nodes=Config.NODE_COUNT
        )
        self.node_status = {f"{GRPCPoolUtils.get_node_addr_key(i)}": {
                "port": Config.DATA_PORT+i,
                "grpc_port": Config.GRPC_PORT+i
            } for i in range(Config.NODE_COUNT)
        }
        self.failed_nodes = set()
        # initialize gRPC connection pool
        self.pool_size = Config.GRPC_DATANODE_POOL_SIZE
        self.connection_pool = defaultdict(list)
        self._initialize_connection_pool()

    # not a Single Responsibility Rule
    def _initialize_connection_pool(self):
        for node, details in self.node_status.items():
            addr_key = f"localhost:{details['grpc_port']}"
            for _ in range(self.pool_size):
                channel = grpc.insecure_channel(addr_key)
                stub = data_node_pb2_grpc.DataNodeStub(channel)
                self.connection_pool[node].append(stub)

    def get_stub_from_pool(self, node):
        if node not in self.connection_pool:
            raise ValueError(f"No pool found for address {node}")
        return random.choice(self.connection_pool[node])

    def Put(self, request, context):
        shard_id = self.consistent_hashing.get_shard_id(request.keyspace, request.key)
        node_id = self.consistent_hashing.get_node_for_shard(request.keyspace, shard_id)
        stub = self.get_stub_from_pool(GRPCPoolUtils.get_node_addr_key(node_id))

        response = stub.Put(data_node_pb2.PutRequest(key=request.key, value=request.value, keyspace=request.keyspace, shard=str(shard_id)))

        return master_node_pb2.NodeResponse(status=response.status)

    def Get(self, request, context):
        shard_id = self.consistent_hashing.get_shard_id(request.keyspace, request.key)
        if not shard_id:
            return master_node_pb2.NodeResponse(status=Config.NOT_FOUND, value=None)
        node_id = self.consistent_hashing.get_node_for_shard(request.keyspace, shard_id)
        stub = self.get_stub_from_pool(GRPCPoolUtils.get_node_addr_key(node_id))

        response = stub.Get(data_node_pb2.GetRequest(key=request.key, keyspace=request.keyspace, shard=str(shard_id)))

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
                    if response.status != Config.SUCCESS:
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
    global grpc_channel, grpc_stub
    grpc_channel = grpc.insecure_channel(f'localhost:{Config.GRPC_MASTER_PORT}')
    grpc_stub = master_node_pb2_grpc.MasterNodeStub(grpc_channel)
    yield
    p.kill()
    if grpc_channel:
        grpc_channel.close()
    #  TODO: Implement graceful Shutdown of GRPC and FastAPI services


app = FastAPI(lifespan=lifespan)

grpc_channel = None
grpc_stub = None


# FastAPI endpoints for management or monitoring
@app.get("/health")
async def health_check():
    return {"status": Config.SUCCESS}


@app.get("/get")
@cache(expire=Config.CACHE_TTL)
async def retrieve(key=None, keyspace=None):
    keyspace = Config.DEFAULT_KEYSPACE if not keyspace else keyspace
    # channel = grpc.insecure_channel(f'localhost:{Config.GRPC_MASTER_PORT}')
    # grpc_stub = master_node_pb2_grpc.MasterNodeStub(channel)
    response = grpc_stub.Get(master_node_pb2.NodeRequest(key=key, value=None, keyspace=keyspace))
    if response.value:
        return JSONResponse(status_code=status.HTTP_200_OK, content={"status": Config.SUCCESS, "value": response.value})
    else:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"status": Config.NOT_FOUND, "value": None})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=Config.MASTER_PORT, timeout_keep_alive=Config.KEEP_ALIVE_HTTP)
