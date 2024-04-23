from abc import ABC, abstractmethod


class Converter(ABC):
    @abstractmethod
    def convert_to_senml_csv(self, chunk_size):
        pass

    @abstractmethod
    def converter_to_senml_riotbench_csv(self, chunk_size):
        pass
