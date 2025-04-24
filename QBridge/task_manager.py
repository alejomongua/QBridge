import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Callable, Any, Optional

from .base import QueueClient   # the abstract interface


logger = logging.getLogger(__name__)


class TaskManager:
    """
    A thin orchestration layer that adds timeout / retry / DLQ semantics
    on top of a QueueClient implementation.

    Parameters
    ----------
    queue_client : QueueClient
        An *already connected* client (RabbitMQClient, SQSClient, ...).
    handler : Callable[[Any], None]
        Your business function that executes the task payload.
        • It must raise an Exception on failure.
        • It must be thread-safe if you use >1 worker.
    input_queue : str
        Name of the queue from which tasks are consumed.
    dead_letter_queue : str
        Queue where exhausted tasks are routed.
    max_retries : int
        How many times to retry *after* the first attempt.
    task_timeout : float
        Seconds before a running task is considered hung and retried.
    max_workers : int
        Concurrency level (default = #CPU cores).
    """
    def __init__(
        self,
        queue_client: QueueClient,
        handler: Callable[[Any], None],
        *, # -- this means that everything after this must be passed as a keyword
        input_queue: str,
        dead_letter_queue: str,
        max_retries: int = 3,
        task_timeout: float = 10.0,
        max_workers: Optional[int] = None,
    ):
        self.qc = queue_client
        self.handler = handler
        self.input_queue = input_queue
        self.dlq = dead_letter_queue
        self.max_retries = max_retries
        self.task_timeout = task_timeout
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    # --------------------------------------------------------------------- #
    # public API
    # --------------------------------------------------------------------- #
    def start(self, poll_timeout: float = 5.0) -> None:
        """
        Blocking loop: keep pulling tasks and dispatching them.
        poll_timeout: seconds to wait for a new message before checking again.
        """
        logger.info("TaskManager started")
        try:
            while True:
                raw = self.qc.read_message_blocking(timeout=poll_timeout)
                if raw is None:
                    continue  # no message – loop again

                self._handle_incoming(raw)
        except KeyboardInterrupt:
            logger.info("TaskManager interrupted - shutting down ...")
        finally:
            self.executor.shutdown(wait=True)

    # --------------------------------------------------------------------- #
    # internal helpers
    # --------------------------------------------------------------------- #
    def _handle_incoming(self, delivery: tuple[Any, bytes]) -> None:
        receipt, body = delivery
        try:
            msg = json.loads(body)
            task_id = msg.get("task_id") or str(uuid.uuid4())
            attempt = int(msg.get("retry", 0))

            future = self.executor.submit(self.handler, msg)
            try:
                future.result(timeout=self.task_timeout)
            except TimeoutError:
                self._retry_or_dlq(msg, receipt, attempt, "timeout")
            except Exception as exc:  # noqa: BLE001
                self._retry_or_dlq(msg, receipt, attempt, str(exc))
            else:
                self.qc.ack(receipt)
                logger.info("Task %s completed", task_id)

        # Capture json decoding errors
        except json.JSONDecodeError as exc:
            logger.error("Malformed JSON (%s) -> DLQ", exc, exc_info=True)
            self.qc.reject(receipt, requeue=False)

    def _retry_or_dlq(self, msg, receipt, attempt: int, reason: str):
        if attempt < self.max_retries:
            # simplest: requeue the same message
            self.qc.reject(receipt, requeue=True)
            logger.info("Retrying (attempt %s/%s)", attempt + 1, self.max_retries)
        else:
            logger.error("Exhausted retries -> DLQ (%s)", reason)
            self.qc.reject(receipt, requeue=False)
