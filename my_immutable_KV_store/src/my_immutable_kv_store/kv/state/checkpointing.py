from abc import ABC, abstractmethod


class CheckpointingInterface(ABC):
    @abstractmethod
    def load_checkpoint(self):
        pass

    @abstractmethod
    def save_checkpoint(self, section, chunk_id):
        pass
