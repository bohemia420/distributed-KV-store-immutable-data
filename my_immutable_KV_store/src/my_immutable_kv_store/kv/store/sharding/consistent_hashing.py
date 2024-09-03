import hashlib
import bisect
from collections import defaultdict

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config


class ConsistentHashing:
    def __init__(self, nodes=None, virtual_nodes=Config.VIRTUAL_NODES, replication_factor=Config.REPLICATION_FACTOR):
        self.virtual_nodes = virtual_nodes
        self.replication_factor = replication_factor
        self.ring = []
        self.node_map = {}
        self.nodes = nodes
        if nodes:
            for node in range(nodes):
                for vn in range(virtual_nodes):
                    vnode_hash = self._hash(f"Node{node}-VN{vn}")
                    self.ring.append(vnode_hash)
                    self.node_map[vnode_hash] = node
        self.ring.sort()

    @staticmethod
    def _hash(key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def get_shard_id(self, key):
        key_hash = self._hash(key)
        shard_pos = bisect.bisect(self.ring, key_hash) % len(self.ring)
        return self.ring[shard_pos]

    def get_node_for_shard(self, shard_id):
        return self.node_map[shard_id]

    # TODO: re-sharding (balancing in face of a node going down / another coming up
