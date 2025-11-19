import asyncio
import logging
from typing import List, Any, Dict, Optional
from aio_pika import connect_robust, Channel, Connection
from aio_pika.abc import AbstractIncomingMessage
from ..models import EspressoRabbitMQInputDefinition
from .base import EspressoInputAdapter

logging.getLogger("aio_pika").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


class EspressoRabbitMQInputAdapter(EspressoInputAdapter):
    def __init__(self, input_def: EspressoRabbitMQInputDefinition):
        # Store configuration
        self.url = input_def.url
        self.queue_name = input_def.queue
        self.prefetch_count = input_def.prefetch_count

        # Lazy initialization - don't connect yet
        self.connection: Optional[Connection] = None
        self.channel: Optional[Channel] = None
        self.queue = None
        self._is_setup = False

        logger.info(
            f"RabbitMQ adapter initialized for queue '{self.queue_name}' (connection pending)"
        )

    async def _ensure_connected(
        self, max_retries: int = 3, retry_delay: float = 2.0
    ) -> bool:
        if (
            self.connection
            and not self.connection.is_closed
            and self.channel
            and not self.channel.is_closed
        ):
            return True

        # Need to (re)connect
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Connecting to RabbitMQ (attempt {attempt}/{max_retries})..."
                )

                await self._close_quietly()

                self.connection = await connect_robust(self.url)
                self.channel = await self.connection.channel()

                if not self._is_setup:
                    await self._setup_queue()
                    self._is_setup = True

                logger.info(
                    f"✓ Successfully connected to RabbitMQ queue '{self.queue_name}'"
                )
                return True

            except Exception as e:
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to connect to RabbitMQ after {max_retries} attempts"
                    )
                    return False

        return False

    async def _setup_queue(self):
        await self.channel.set_qos(prefetch_count=self.prefetch_count)
        self.queue = await self.channel.declare_queue(self.queue_name, durable=True)

    async def _close_quietly(self):
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
        except Exception:
            pass
        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
        except Exception:
            pass

    async def poll(self) -> List[Any]:
        return await self.poll_batch(batch_size=1)

    async def poll_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """
        Poll for multiple messages from RabbitMQ.

        Returns a list of message dicts:
        {
            "body": bytes,
            "message": <AbstractIncomingMessage>,
        }
        """
        if not await self._ensure_connected():
            logger.warning("Cannot poll: RabbitMQ connection unavailable")
            return []

        items: List[Dict[str, Any]] = []
        try:
            for _ in range(batch_size):
                message = await self.queue.get(timeout=0.1, fail=False)
                if message:
                    items.append(
                        {
                            "body": message.body,
                            "message": message,
                        }
                    )
                else:
                    break
        except Exception as e:
            logger.error(f"Error polling messages: {e}", exc_info=True)
            # Mark connection as bad so next operation retries
            await self._close_quietly()

        return items

    async def poll_all(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []

        while True:
            batch = await self.poll_batch(batch_size=10)
            if not batch:
                break
            items.extend(batch)

        return items

    async def ack(self, msg: Dict[str, Any]) -> None:
        message: AbstractIncomingMessage = msg["message"]
        await message.ack()

    async def nack(self, msg: Dict[str, Any], requeue: bool = True) -> None:
        message: AbstractIncomingMessage = msg["message"]
        await message.nack(requeue=requeue)

    async def has_data(self) -> bool:
        if not await self._ensure_connected():
            return False

        try:
            queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                passive=False,  # Re-declare to get fresh metadata
            )
            message_count = queue.declaration_result.message_count
            return message_count > 0
        except Exception as e:
            logger.error(f"Error checking queue status: {e}")
            await self._close_quietly()
            return False

    async def close(self) -> None:
        try:
            if self.channel:
                await self.channel.close()
        finally:
            if self.connection:
                await self.connection.close()
