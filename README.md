# QBridge Task Manager Usage

## Overview

The `TaskManager` class provides a layer on top of a `QueueClient` (like RabbitMQ) to orchestrate the consumption and processing of tasks from a queue. It adds essential features like task timeouts, automatic retries upon failure, and routing to a dead-letter queue (DLQ) if a task consistently fails.

## Prerequisites

1.  **Queue Client:** You need an implementation of the `QueueClient` abstract base class (e.g., `QBridge.rabbitmq_client.RabbitMQClient`). This client must be *already connected* before being passed to the `TaskManager`.
2.  **Task Handler Function:** A Python function that contains your business logic to process a single task's payload.
    *   This function will receive the task payload as its argument.
    *   It **must** raise an `Exception` if the task processing fails. This signals the `TaskManager` to attempt a retry or send to the DLQ.
    *   If you configure `max_workers` > 1, ensure your handler function is thread-safe.

## Installation

(Assuming you have the QBridge package installed, potentially via `pip install .` in the project root if a `setup.py` or `pyproject.toml` is configured for installation.)

## Usage Example

```python
import logging
import time
from QBridge.task_manager import TaskManager
from QBridge.rabbitmq_client import RabbitMQClient # Or your chosen QueueClient implementation

# --- 1. Configure Logging (Recommended) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 2. Define your Task Handler ---
def my_task_handler(payload: dict):
    """
    Processes the incoming task payload.
    Raises an exception on failure.
    """
    logger.info(f"Received task payload: {payload}")
    try:
        # Replace with your actual task processing logic
        required_data = payload['data']
        result = f"Processed data: {required_data}"
        logger.info(result)
        # Simulate work
        time.sleep(2)
        # Simulate a potential failure
        # if payload.get("force_fail"):
        #     raise ValueError("Forced failure for testing!")
    except KeyError as e:
        logger.error(f"Missing key in payload: {e}")
        raise ValueError(f"Invalid task payload: Missing key {e}") from e
    except Exception as e:
        logger.error(f"Task failed: {e}", exc_info=True)
        # Re-raise the exception to signal failure to TaskManager
        raise

# --- 3. Set up and Connect Queue Client ---
# Replace with your actual connection details
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
INPUT_QUEUE = "tasks_to_process"
DEAD_LETTER_QUEUE = "failed_tasks"

# Ensure queues exist on your RabbitMQ server
# (You might need separate setup scripts for this)

queue_client = RabbitMQClient(rabbitmq_url=RABBITMQ_URL)
try:
    # TaskManager expects a connected client
    queue_client.connect()
    logger.info("RabbitMQ client connected.")
except Exception as e:
    logger.error(f"Failed to connect RabbitMQ client: {e}")
    exit(1) # Cannot proceed without a connection

# --- 4. Instantiate TaskManager ---
task_manager = TaskManager(
    queue_client=queue_client,      # The connected client instance
    handler=my_task_handler,        # Your business logic function
    input_queue=INPUT_QUEUE,        # Queue to read tasks from
    dead_letter_queue=DEAD_LETTER_QUEUE, # Queue for failed tasks
    max_retries=3,                  # Retry 3 times *after* the first failure (total 4 attempts)
    task_timeout=10.0,              # Allow 10 seconds per task execution
    max_workers=4                   # Process up to 4 tasks concurrently
)

# --- 5. Start Processing ---
logger.info(f"Starting TaskManager to process queue: {INPUT_QUEUE}")
try:
    # This is a blocking call, it will run until interrupted (e.g., Ctrl+C)
    task_manager.start(poll_timeout=5.0) # Check for new messages every 5 seconds
except Exception as e:
    logger.error(f"TaskManager encountered an unexpected error: {e}", exc_info=True)
finally:
    logger.info("Shutting down...")
    queue_client.disconnect()
    logger.info("RabbitMQ client disconnected.")

```

## How it Works

1.  **Initialization:** You provide the connected `queue_client`, your `handler`, queue names, and configuration options (retries, timeout, concurrency).
2.  **Starting:** Calling `task_manager.start()` begins a loop that continuously polls the `input_queue` for new messages using the `queue_client`.
3.  **Task Dispatch:** When a message is received:
    *   It's parsed as JSON. Malformed JSON messages are immediately rejected and sent to the DLQ.
    *   The `handler` function is called with the message's `payload` in a separate thread (using `ThreadPoolExecutor`).
4.  **Execution & Timeout:**
    *   The `handler` runs.
    *   If it doesn't complete within `task_timeout` seconds, it's considered timed out.
5.  **Success:** If the `handler` completes successfully without raising an exception, the message is acknowledged (`ack`) and removed from the queue.
6.  **Failure/Timeout:** If the `handler` raises an exception or times out:
    *   The `TaskManager` checks the current attempt count (stored implicitly or potentially via message headers/payload depending on the `QueueClient` implementation's retry mechanism).
    *   **Retry:** If `attempt < max_retries`, the message is rejected with `requeue=True` (or the client's equivalent), putting it back in the queue for another try.
    *   **Dead-Letter Queue (DLQ):** If `attempt >= max_retries`, the message is rejected with `requeue=False` (or the client's equivalent), causing it to be routed to the configured `dead_letter_queue` (assuming the underlying queue/broker is configured for DLQ routing).
7.  **Concurrency:** The `ThreadPoolExecutor` manages running up to `max_workers` handler functions concurrently.
8.  **Shutdown:** Pressing `Ctrl+C` triggers a `KeyboardInterrupt`, causing the `start()` loop to exit and the thread pool to shut down gracefully.

## Configuration Parameters

*   `queue_client` (**required**): An *instance* of a connected `QueueClient`.
*   `handler` (**required**): Your task processing function `Callable[[Any], None]`.
*   `input_queue` (**required**): `str`, name of the queue to consume from.
*   `dead_letter_queue` (**required**): `str`, name of the queue for exhausted tasks.
*   `max_retries` (optional): `int`, number of retries *after* the initial attempt (default: `3`).
*   `task_timeout` (optional): `float`, seconds before a task is considered hung (default: `10.0`).
*   `max_workers` (optional): `int`, concurrency level (default: `None`, uses `ThreadPoolExecutor`'s default, often based on CPU cores).
*   `poll_timeout` (optional, in `start()` method): `float`, seconds the `queue_client` waits for a message before the `start` loop checks again (default: `5.0`).

## Message Format

The `TaskManager` expects messages on the `input_queue` to be JSON strings. The JSON object should contain at least a `"payload"` key, which holds the actual data your `handler` function needs.

```json
{
  "payload": {
    "data": "your task specific data",
    "id": 123
  },
  "task_id": "optional-unique-id-for-logging",
  "retry": 0
}
```

*   `payload`: The data passed directly to your handler function.
*   `task_id` (optional): If provided, used for logging. If not, a UUID is generated.
*   `retry` (optional): Used internally by some retry mechanisms. The `TaskManager` itself primarily uses the `max_retries` count.