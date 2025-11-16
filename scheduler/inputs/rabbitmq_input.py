import pika
import time
import logging
from typing import List, Any, Dict, Optional
from ..models import EspressoRabbitMQInputDefinition
from .base import EspressoInputAdapter

logging.getLogger("pika").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


class EspressoRabbitMQInputAdapter(EspressoInputAdapter):
    def __init__(self, input_def: EspressoRabbitMQInputDefinition):
        # Store configuration
        self.url = input_def.url
        self.queue = input_def.queue
        self.prefetch_count = input_def.prefetch_count

        # Lazy initialization - don't connect yet
        self.params = pika.URLParameters(self.url)
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self._is_setup = False

        logger.info(
            f"RabbitMQ adapter initialized for queue '{self.queue}' (connection pending)"
        )

    def _ensure_connected(self, max_retries: int = 3, retry_delay: float = 2.0) -> bool:
        """
        Ensure we have a valid connection and channel. Reconnect if needed.
        """
        if (
            self.connection
            and self.connection.is_open
            and self.channel
            and self.channel.is_open
        ):
            return True

        # Need to (re)connect
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Connecting to RabbitMQ (attempt {attempt}/{max_retries})..."
                )

                self._close_quietly()

                self.connection = pika.BlockingConnection(self.params)
                self.channel = self.connection.channel()

                if not self._is_setup:
                    self._setup_queue()
                    self._is_setup = True

                logger.info(
                    f"✓ Successfully connected to RabbitMQ queue '{self.queue}'"
                )
                return True

            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to connect to RabbitMQ after {max_retries} attempts"
                    )
                    return False
            except Exception as e:
                logger.error(
                    f"Unexpected error connecting to RabbitMQ: {e}", exc_info=True
                )
                return False

        return False

    def _setup_queue(self):
        """Declare queue and set QoS settings."""
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.basic_qos(prefetch_count=self.prefetch_count)

    def _close_quietly(self):
        """Close connection without raising exceptions."""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

    def poll(self) -> List[Any]:
        """Poll for a single message. Returns empty list if RabbitMQ is unavailable."""
        return self.poll_batch(batch_size=1)

    def poll_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """
        Poll for multiple messages from RabbitMQ.

        Returns a list of message dicts:
        {
            "body": bytes,
            "delivery_tag": int,
            "properties": <BasicProperties>,
        }

        If RabbitMQ is unavailable, returns empty list and logs warning.
        """
        # Ensure we're connected before polling
        if not self._ensure_connected():
            logger.warning("Cannot poll: RabbitMQ connection unavailable")
            return []

        items: List[Dict[str, Any]] = []
        try:
            for _ in range(batch_size):
                method_frame, properties, body = self.channel.basic_get(
                    queue=self.queue, auto_ack=False
                )
                if method_frame:
                    items.append(
                        {
                            "body": body,
                            "properties": properties,
                            "delivery_tag": method_frame.delivery_tag,
                        }
                    )
                else:
                    break
        except Exception as e:
            logger.error(f"Error polling messages: {e}", exc_info=True)
            # Mark connection as bad so next operation retries
            self._close_quietly()

        return items

    def poll_all(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []

        while True:
            batch = self.poll_batch(batch_size=10)
            if not batch:
                break
            items.extend(batch)

        return items

    def ack(self, msg: Dict[str, Any]) -> None:
        """
        Acknowledge a single message after successful processing.
        """
        tag = msg["delivery_tag"]
        self.channel.basic_ack(delivery_tag=tag)

    def nack(self, msg: Dict[str, Any], requeue: bool = True) -> None:
        """
        Negative-acknowledge a message (optionally requeue).
        """
        tag = msg["delivery_tag"]
        self.channel.basic_nack(delivery_tag=tag, requeue=requeue)

    def has_data(self) -> bool:
        """
        Check if queue has messages without consuming them.
        Returns False if RabbitMQ is unavailable.
        """
        if not self._ensure_connected():
            return False

        try:
            method = self.channel.queue_declare(
                queue=self.queue,
                durable=True,
                passive=True,  # don't create, just check metadata
            )
            return method.method.message_count > 0
        except Exception as e:
            logger.error(f"Error checking queue status: {e}")
            self._close_quietly()
            return False

    def close(self) -> None:
        try:
            self.channel.close()
        finally:
            self.connection.close()
