import pika
import time
from threading import Event
from .base import QueueClient

class RabbitMQClient(QueueClient):
    def __init__(self, host, username, password, queue_name):
        self.host = host
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(host=self.host, credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        print(f"Connected to RabbitMQ at {self.host}")

    def read_message(self):
        method_frame, header_frame, body = self.channel.basic_get(self.queue_name)
        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
            return body
        return None

    def send_message(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def read_message_blocking(self, timeout=None):
        """
        Wait for a new message until the timeout expires.
        Returns the message body if received before timeout; otherwise None.
        """
        message_event = Event()
        result = [None]  # Using a mutable container to hold the result

        def callback(ch, method, properties, body):
            result[0] = body
            ch.basic_ack(method.delivery_tag)
            message_event.set()
            ch.basic_cancel(consumer_tag=consumer_tag)

        consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=False
        )

        start_time = time.time()
        while not message_event.is_set():
            self.connection.process_data_events(time_limit=0.5)
            if timeout is not None and (time.time() - start_time) > timeout:
                self.channel.basic_cancel(consumer_tag=consumer_tag)
                break

        return result[0]
