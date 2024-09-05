import os
from filelock import FileLock
from configparser import ConfigParser
from my_immutable_KV_store.src.my_immutable_kv_store.kv.state.checkpointing import CheckpointingInterface


class INICheckpointing(CheckpointingInterface):
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.lock_file = f"{checkpoint_file}.lock"

    def _create_checkpoint_file(self):
        with open(self.checkpoint_file, 'w') as file:
            file.write("")

    def load_checkpoint(self):
        config = ConfigParser()
        if os.path.exists(self.checkpoint_file):
            config.read(self.checkpoint_file)
        return config

    def save_checkpoint(self, section, chunk_id):
        with FileLock(self.lock_file):
            config = self.load_checkpoint()
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, chunk_id, 'processed')
            with open(self.checkpoint_file, 'w') as f:
                config.write(f)

    def is_chunk_processed(self, section, chunk_id):
        config = self.load_checkpoint()
        if config.has_section(section):
            return config.has_option(section, str(chunk_id))
        return False
