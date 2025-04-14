from abc import ABC, abstractmethod

class QueueClient(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def read_message(self):
        pass

    @abstractmethod
    def send_message(self, message):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def read_message_blocking(self, timeout=None):
        pass
