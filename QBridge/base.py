from abc import ABC, abstractmethod
from typing import Optional, Any

class QueueClient(ABC):
    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def read_message(self) -> Optional[bytes]:
        pass

    @abstractmethod
    def send_message(self, message: bytes) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def read_message_blocking(self, timeout: Optional[float] = None) -> Optional[bytes]:
        pass

    @abstractmethod
    def ack(self, receipt_handle: Any) -> None:
        pass

    @abstractmethod
    def reject(self, receipt_handle: Any, requeue: bool = True) -> None:
        pass