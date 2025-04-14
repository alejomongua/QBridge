import time
import threading
from typing import Dict, List, Optional, Protocol, Tuple, Iterator, Callable, cast
import pytest
from pytest import MonkeyPatch
import os
import sys
from pika.spec import Basic, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.connection import Parameters
from dotenv import load_dotenv

# Add the parent directory to the system path to import QBridge
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if (parent_dir not in sys.path):
    sys.path.append(parent_dir)

from QBridge.rabbitmq_client import RabbitMQClient # noqa: E402

# Load environment variables from .env.test file
dotenv_path = os.path.join(parent_dir, '.env.test')
load_dotenv(dotenv_path)

# ---------------------------------------------------------------------------
# Fake classes to simulate RabbitMQ behavior
# ---------------------------------------------------------------------------

class MethodFrameProtocol(Protocol):
    delivery_tag: int

ConsumerCallback = Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]

class FakeChannel:
    def __init__(self) -> None:
        self.messages: List[bytes] = []
        self.consumers: Dict[str, ConsumerCallback] = {}
        self.queue: str = ""

    def queue_declare(self, queue: str) -> None:
        self.queue = queue

    def basic_get(self, queue: str, auto_ack: bool = False) -> Tuple[Optional[MethodFrameProtocol], Optional[BasicProperties], Optional[bytes]]:
        if self.messages:
            message = self.messages.pop(0)
            # Create a simple fake method frame with a delivery_tag attribute
            fake_method_frame = type("FakeMethodFrame", (), {"delivery_tag": 1})()
            fake_header_frame = BasicProperties()
            return fake_method_frame, fake_header_frame, message
        else:
            return None, None, None

    def basic_ack(self, delivery_tag: int) -> None:
        pass

    def basic_publish(self, exchange: str, routing_key: str, body: bytes) -> None:
        self.messages.append(body)

    def basic_consume(self, queue: str, on_message_callback: ConsumerCallback, auto_ack: bool) -> str:
        consumer_tag = "fake_consumer"
        self.consumers[consumer_tag] = on_message_callback
        return consumer_tag

    def basic_cancel(self, consumer_tag: str) -> None:
        if consumer_tag in self.consumers:
            del self.consumers[consumer_tag]

    def simulate_delivery(self, message: bytes) -> None:
        # Simulate delivery by appending and invoking the callback
        self.messages.append(message)
        for tag, callback in list(self.consumers.items()):
            # Create a fake method frame with a delivery_tag attribute
            fake_method = type("FakeMethod", (), {"delivery_tag": 1})()
            fake_properties = BasicProperties()
            callback(cast(BlockingChannel, self), fake_method, fake_properties, message)


class FakeBlockingConnection:
    def __init__(self, parameters: Parameters) -> None:
        self.parameters = parameters
        self.closed: bool = False
        self._channel: FakeChannel = FakeChannel()
        self.is_open: bool = True

    def channel(self) -> FakeChannel:
        return self._channel

    def process_data_events(self, time_limit: Optional[int] = None) -> None:
        time.sleep(0.1)

    def close(self) -> None:
        self.closed = True
        self.is_open = False

# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

# This fixture will patch pika.BlockingConnection in QBridge.rabbitmq_client
@pytest.fixture(autouse=True)
def patch_pika(monkeypatch: MonkeyPatch) -> None:
    # Import the module so we can patch its reference to pika.BlockingConnection.
    import QBridge.rabbitmq_client as rabbitmq_client
    monkeypatch.setattr(rabbitmq_client.pika, "BlockingConnection", FakeBlockingConnection)

# Fixture to create and clean up a RabbitMQClient instance.
@pytest.fixture
def client() -> Iterator[RabbitMQClient]:
    client = RabbitMQClient(
        host=os.getenv('RABBITMQ_HOST', 'localhost'),
        username=os.getenv('RABBITMQ_USERNAME', 'guest'),
        password=os.getenv('RABBITMQ_PASSWORD', 'guest'),
        queue_name=os.getenv('RABBITMQ_QUEUE_NAME', 'test_queue'),
    )
    client.connect()
    yield client
    client.disconnect()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_send_and_read_message(client: RabbitMQClient) -> None:
    """Test that a message can be sent and then read using basic_get."""
    test_message = b"Hello, RabbitMQ!"
    client.send_message(test_message)
    message = client.read_message()
    assert message == test_message

def test_read_message_blocking_success(client: RabbitMQClient) -> None:
    """Test read_message_blocking with a message delivered before the timeout."""
    test_message = b"Blocking message"

    # Start a thread that simulates delivering a message after a short delay.
    def deliver_message() -> None:
        time.sleep(0.3)
        if client.channel:  # Add runtime check
            fake_channel = cast(FakeChannel, client.channel)
            fake_channel.simulate_delivery(test_message)

    thread = threading.Thread(target=deliver_message)
    thread.start()
    received = client.read_message_blocking(timeout=2)
    thread.join()
    assert received == test_message

def test_read_message_blocking_timeout(client: RabbitMQClient) -> None:
    """Test read_message_blocking when no message is delivered (should return None)."""
    received = client.read_message_blocking(timeout=1)
    assert received is None
