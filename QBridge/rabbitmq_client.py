import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
import time
from threading import Event
from typing import Optional, List, Tuple, cast
from .base import QueueClient

class RabbitMQClient(QueueClient):
    def __init__(self, host: str, username: str, password: str, queue_name: str):
        self.host: str = host
        self.username: str = username
        self.password: str = password
        self.queue_name: str = queue_name
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[BlockingChannel] = None

    def connect(self) -> None:
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(host=self.host, credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = cast(BlockingChannel, self.connection.channel())
        self.channel.queue_declare(queue=self.queue_name)
        print(f"Connected to RabbitMQ at {self.host}")

    def read_message(self) -> Optional[bytes]:
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
            
        # Note: basic_get in pika actually returns Tuple[Basic.GetOk, BasicProperties, bytes] | Tuple[None, None, None]
        response = cast(Tuple[Optional[Basic.GetOk], Optional[BasicProperties], Optional[bytes]], 
                                   self.channel.basic_get(queue=self.queue_name, auto_ack=False))
        
        print(f"Response: {response}")
        method_frame, header_frame, body = response
        
        if isinstance(method_frame, Basic.GetOk) and isinstance(body, bytes):
            delivery_tag = cast(int, method_frame.delivery_tag)
            self.channel.basic_ack(delivery_tag)
            return body
        return None

    def send_message(self, message: bytes) -> None:
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def disconnect(self) -> None:
        if self.connection and self.connection.is_open:
            self.connection.close()

    def read_message_blocking(self, timeout: Optional[float] = None) -> Optional[bytes]:
        """
        Wait for a new message until the timeout expires.
        Returns the message body if received before timeout; otherwise None.
        """
        message_event = Event()
        result: List[Optional[bytes]] = [None]  # Using a mutable container to hold the result

        def callback(ch: BlockingChannel, method: Basic.Deliver, 
                    properties: BasicProperties, body: bytes) -> None:
            result[0] = body
            delivery_tag = cast(int, method.delivery_tag)
            ch.basic_ack(delivery_tag)
            message_event.set()
            ch.basic_cancel(consumer_tag=consumer_tag)

        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
            
        consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=False
        )

        start_time = time.time()
        while not message_event.is_set():
            if self.connection:
                # Convert float to milliseconds as int
                self.connection.process_data_events(time_limit=int(500))  # 500ms
            if timeout is not None and (time.time() - start_time) > timeout:
                if self.channel:
                    self.channel.basic_cancel(consumer_tag=consumer_tag)
                break

        return result[0]

    def ack(self, receipt_handle: int) -> None:
        self.channel.basic_ack(receipt_handle)

    def reject(self, receipt_handle: int, requeue: bool = True) -> None:
        self.channel.basic_nack(receipt_handle, requeue=requeue)