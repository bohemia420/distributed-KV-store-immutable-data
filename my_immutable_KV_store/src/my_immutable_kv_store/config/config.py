class Config:
    CACHE_TTL = 60
    DEFAULT_KEYSPACE = 'default'
    GRPC_MASTER_PORT = 50050  # master listens to cli at 9000/REST over HTTP, but the system internally uses gRPC
    GRPC_PORT = 50051  # GRPC Ports for DataNode(s)
    DATA_PORT = 9001  # FWIW, the datanodes were modeled as FastAPI Applications, too
    MASTER_PORT = 9000  # Master served as a FastAPI app on port 9000
    HEARTBEAT_INTERVAL = 10  # Master keeps checking Node Health, eliminates/reshards the failed node/remaining nodes
    SLEEP_WARMUP = 5  # warm up time to allow data nodes to come up
    REPLICATION_FACTOR = 1  # replicas for shards
    CHUNK_SIZE = 1000  # 1000 lines per chunk  # Each data file is read in 'bunches' of lines
    CHECKPOINT_FILE = ".chk_my_imm_kv.ini"  # a checkpoint maintained, in order to speed up uploads upon failures
    SHARD_COUNT = 5  # each Keyspace must be divided into a number of shards
    NODE_COUNT = 5  # number of data nodes that actually store a 'keyspace' comprising of shards
    VIRTUAL_NODES = 10  # Consistent Hashing- so that distribution is agnostic of number of/whereabouts of Nodes
    NOT_FOUND = "NOT_FOUND"  # grpc response status value for key/shard/keyspace not found
    SUCCESS = "SUCCESS"  # grpc response status value when successful
