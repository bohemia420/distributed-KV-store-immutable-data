import os
import grpc
from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config
from my_immutable_KV_store.src.my_immutable_kv_store.my_imm_kv_store import logger
from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.system.transport.grpc import data_node_pb2_grpc, \
    data_node_pb2
from my_immutable_KV_store.src.my_immutable_kv_store.utils.profiling import timeit


class Services:
    PKG_PATH = "my_immutable_kv_store/src/my_immutable_kv_store/kv/store/system/"

    @classmethod
    @timeit(level='DEBUG')
    def start_master_node(cls):
        os.system(f'python {os.path.join(cls.PKG_PATH, "master/master_node.py")}')

    @classmethod
    @timeit(level='DEBUG')
    def start_data_node(cls, node_id):
        os.system(f'python {os.path.join(cls.PKG_PATH, "data/data_node.py")} --node-id {node_id}')

    @classmethod
    def perform_health_checks(cls):
        for node_id in range(Config.NODE_COUNT):
            port = Config.DATA_PORT + node_id
            channel = grpc.insecure_channel(f'localhost:{Config.GRPC_PORT+node_id}')
            stub = data_node_pb2_grpc.DataNodeStub(channel)
            try:
                response = stub.Heartbeat(data_node_pb2.HeartbeatRequest())
                if response.status != Config.SUCCESS:
                    logger.error(f"Node {node_id} at port {port} is not healthy!")
                    return False
            except grpc.RpcError:
                logger.error(f"Node {node_id} at port {port} is unreachable!")
                return False
        return True
