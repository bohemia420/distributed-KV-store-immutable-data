from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config


class GRPCPoolUtils:
    @staticmethod
    def get_node_addr_key(node_id):
        return f"node_{node_id}"
