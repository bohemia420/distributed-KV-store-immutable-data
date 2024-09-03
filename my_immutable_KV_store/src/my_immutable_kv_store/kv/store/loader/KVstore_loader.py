import multiprocessing
import os
from pathlib import Path

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config


class KVStoreLoader:
    def __init__(self, checkpointing, chunk_processor_cls):
        self.checkpointing = checkpointing
        self.chunk_processor_cls = chunk_processor_cls

    def load_file_in_chunks(self, file_path, keyspace):
        from my_immutable_KV_store.src.my_immutable_kv_store.my_imm_kv_store import logger
        from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config
        section = str(file_path)
        config = self.checkpointing.load_checkpoint()
        file_size = os.path.getsize(file_path)
        chunk_index = 0
        with open(file_path, 'r') as f:
            while True:
                chunk_start = chunk_index * Config.CHUNK_SIZE
                if chunk_start >= file_size:
                    break
                chunk_id = f"{chunk_index}"
                if config.has_option(section, chunk_id):
                    chunk_index += 1
                    continue
                f.seek(chunk_start)
                chunk = f.read(Config.CHUNK_SIZE)
                self.process_chunk(chunk, keyspace, section, chunk_id)
                logger.info(f"Processed chunk {chunk_index} of file {file_path} into keyspace {keyspace}")
                chunk_index += 1

    def process_chunk(self, chunk, keyspace, section, chunk_id):
        if not self.checkpointing.is_chunk_processed(section, chunk_id):
            chunk_processor = self.chunk_processor_cls(keyspace)
            chunk_processor.process_chunk(chunk, keyspace)
            self.checkpointing.save_checkpoint(section, chunk_id)

    def load_data(self, keyspace, path):
        path = Path(path)
        if path.is_file():
            self.load_file_in_chunks(path, keyspace)
        elif path.is_dir():
            file_chunks = self._get_file_chunks(path, keyspace)
            with multiprocessing.Pool() as pool:
                pool.starmap(self._process_chunk_task, file_chunks)

    def _get_file_chunks(self, path, keyspace):
        chunks = []
        for file in path.glob("*"):
            section = str(file)
            config = self.checkpointing.load_checkpoint()
            chunk_index = 0
            with open(file, 'r') as f:
                while True:
                    lines = []
                    for _ in range(Config.CHUNK_SIZE):
                        line = f.readline()
                        if not line:  # End of file
                            break
                        lines.append(line.strip())
                    if not lines:
                        break
                    chunk_id = f"{chunk_index}"
                    if not config.has_option(section, chunk_id):
                        chunks.append((lines, keyspace, section, chunk_id))
                    chunk_index += 1
        return chunks

    def _process_chunk_task(self, chunk, keyspace, section, chunk_id):
        self.process_chunk(chunk, keyspace, section, chunk_id)
