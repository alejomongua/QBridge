# Message Queue Client

This package provides a common interface for message queue clients along with a RabbitMQ adapter implementation. It’s designed to allow developers to switch the underlying messaging technology seamlessly.

## Installation

```bash
pip install -e .
```

## Usage Example

```python
from QBridge.factory import get_queue_client

client = get_queue_client(
    service_type='rabbitmq',
    host='localhost',
    username='guest',
    password='guest',
    queue_name='my_queue'
)
client.connect()

# Blocking read with a 10-second timeout
message = client.read_message_blocking(timeout=10)
if message:
    print("Received:", message)
else:
    print("No message received within timeout")

client.disconnect()
```
