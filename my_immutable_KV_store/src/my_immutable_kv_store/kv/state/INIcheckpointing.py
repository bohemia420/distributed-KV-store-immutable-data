import os
from configparser import ConfigParser
from my_immutable_KV_store.src.my_immutable_kv_store.kv.state.checkpointing import CheckpointingInterface


class INICheckpointing(CheckpointingInterface):
    def __init__(self, checkpoint_file):
        self.config = ConfigParser()
        self.checkpoint_file = checkpoint_file
        if os.path.exists(self.checkpoint_file):
            self.config.read(self.checkpoint_file)
        else:
            self._create_checkpoint_file()

    def _create_checkpoint_file(self):
        with open(self.checkpoint_file, 'w') as file:
            file.write("")

    def load_checkpoint(self):
        return self.config

    def save_checkpoint(self, section, chunk_id):
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, chunk_id, 'processed')
        with open(self.checkpoint_file, 'w') as f:
            self.config.write(f)

    def is_chunk_processed(self, section, chunk_id):
        if self.config.has_section(section):
            return self.config.has_option(section, str(chunk_id))
        return False
