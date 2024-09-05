import hashlib
import bisect
from collections import defaultdict

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config


class ConsistentHashing:
    def __init__(
            self,
            nodes=None,
            keyspaces=None,
            virtual_nodes=Config.VIRTUAL_NODES,
            replication_factor=Config.REPLICATION_FACTOR):
        self.keyspaces = [Config.DEFAULT_KEYSPACE] if keyspaces is None else keyspaces
        self.virtual_nodes = virtual_nodes
        self.replication_factor = replication_factor
        self.ring = {}
        self.node_map = {}
        self.nodes = nodes
        if nodes:
            for node in range(nodes):
                for vn in range(virtual_nodes):
                    for keyspace in self.keyspaces:
                        vnode_hash = self._hash(f"Node{node}-VN{vn}")
                        if keyspace not in self.ring:
                            self.ring[keyspace] = []
                        self.ring[keyspace].append(vnode_hash)
                        if keyspace not in self.node_map:
                            self.node_map[keyspace] = {}
                        self.node_map[keyspace][vnode_hash] = node
        [self.ring[keyspace].sort() for keyspace in self.ring.keys()]

    @staticmethod
    def _hash(key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def get_shard_id(self, keyspace, key):
        key_hash = self._hash(key)
        if not keyspace in self.ring:
            return None
        shard_pos = bisect.bisect(self.ring[keyspace], key_hash) % len(self.ring)
        return self.ring[keyspace][shard_pos]

    def get_node_for_shard(self, keyspace, shard_id):
        return self.node_map[keyspace][shard_id]

    # TODO: re-sharding (balancing in face of a node going down / another coming up
