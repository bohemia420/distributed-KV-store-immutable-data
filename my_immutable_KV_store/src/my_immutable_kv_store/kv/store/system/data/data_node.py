import argparse
import logging
import multiprocessing
from concurrent import futures
from contextlib import asynccontextmanager

import grpc
from grpc_reflection.v1alpha import reflection
from fastapi import FastAPI

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config

from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.grpc import data_node_pb2, \
    data_node_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="Start a DataNode service.")
parser.add_argument("--node-id", type=int, default=1, help="The ID of this DataNode.")
args = parser.parse_args()
NODE_ID = args.node_id


class DataNodeServicer(data_node_pb2_grpc.DataNodeServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.storage = {}

    def Put(self, request, context):
        if request.keyspace not in self.storage:
            self.storage[request.keyspace] = {request.shard: {}}
        self.storage[request.keyspace][request.shard][request.key] = request.value
        return data_node_pb2.PutResponse(status=Config.SUCCESS)

    def Get(self, request, context):
        if request.keyspace not in self.storage:
            return data_node_pb2.GetResponse(status=Config.NOT_FOUND, value=None)
        value = self.storage[request.keyspace][request.shard].get(request.key, None)
        return data_node_pb2.GetResponse(status=Config.SUCCESS if value else Config.NOT_FOUND, value=value)

    def Heartbeat(self, request, context):
        return data_node_pb2.HeartbeatResponse(status=Config.SUCCESS)


def serve_grpc(node_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_node_servicer = DataNodeServicer(node_id)
    data_node_pb2_grpc.add_DataNodeServicer_to_server(data_node_servicer, server)
    SERVICE_NAMES = (
        data_node_pb2.DESCRIPTOR.services_by_name['DataNode'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port(f'[::]:{Config.GRPC_PORT+node_id}')
    server.start()
    logger.debug(f"GRPC server started at port: {Config.GRPC_PORT+node_id}")
    server.wait_for_termination()


@asynccontextmanager
async def lifespan(_: FastAPI):
    p = multiprocessing.Process(target=serve_grpc, args=(NODE_ID,))
    p.start()
    yield
    p.kill()
    #  TODO: Implement graceful Shutdown of GRPC and FastAPI services

app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": Config.SUCCESS}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(Config.DATA_PORT+NODE_ID))
