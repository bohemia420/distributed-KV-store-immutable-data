import time
import argparse
# import logging
from loguru import logger
import multiprocessing

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config
from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.loader.KVstore_loader import KVStoreLoader
from my_immutable_KV_store.src.my_immutable_kv_store.kv.store.loader.processor.chunk_processor import ChunkProcessor
from my_immutable_KV_store.src.my_immutable_kv_store.kv.state.INIcheckpointing import INICheckpointing
import my_immutable_KV_store.src.my_immutable_kv_store.utils.services as services
from my_immutable_KV_store.src.my_immutable_kv_store.utils.profiling import timeit


def parse_args():
    parser = argparse.ArgumentParser(description="My Immutable KV Store")
    parser.add_argument("--data", required=True, help="<keyspace>:<path> or <path>")
    parser.add_argument("--clear", action='store_true', help="Clear checkpoint and lock files before loading data")
    return parser.parse_args()


@timeit(level='DEBUG')
def load_data(keyspace, path, to_clear=False):
    checkpointing = INICheckpointing(Config.CHECKPOINT_FILE)
    if to_clear:
        checkpointing.clear_checkpoints()
    kv_loader = KVStoreLoader(checkpointing, ChunkProcessor)
    kv_loader.load_data(keyspace, path)
    logger.success("Loaded Data into immutable KV Store")


def main():
    args = parse_args()
    data_arg = args.data
    keyspace, path = data_arg.split(':', 1) if ':' in data_arg else Config.DEFAULT_KEYSPACE, data_arg

    processes = [multiprocessing.Process(target=services.Services.start_master_node)]
    for i in range(Config.NODE_COUNT):
        processes.append(multiprocessing.Process(target=services.Services.start_data_node, args=(i,)))
    try:
        for p in processes:
            p.start()
        time.sleep(Config.SLEEP_WARMUP)
        if services.Services.perform_health_checks():
            logger.success("All nodes are healthy. Starting data load ...")
            load_data(keyspace, path, args.clear)
        else:
            logger.critical("Health checks failed. Data load aborted.")
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.error("Shutting down, killing all master and datanode processes ...")
        [p.kill() for p in processes]


if __name__ == "__main__":
    main()
