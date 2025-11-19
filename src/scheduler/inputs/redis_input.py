import asyncio
import logging
from typing import List, Any, Dict, Optional, AsyncIterator
import redis.asyncio as redis
from redis.exceptions import ResponseError
from ..models import EspressoRedisStreamsInputDefinition
from .base import EspressoInputAdapter

logger = logging.getLogger(__name__)


class EspressoRedisStreamsInputAdapter(EspressoInputAdapter):
    def __init__(self, input_def: EspressoRedisStreamsInputDefinition):
        # Store configuration
        self.host = input_def.host
        self.port = input_def.port
        self.password = input_def.password
        self.db = input_def.db
        self.stream_name = input_def.stream_name
        self.consumer_group = input_def.consumer_group
        self.consumer_name = input_def.consumer_name
        self.start_id = input_def.start_id

        # Lazy initialization
        self.redis_client: Optional[redis.Redis] = None
        self._is_setup = False

        logger.info(
            f"Redis Streams adapter initialized for stream '{self.stream_name}' "
            f"(group: {self.consumer_group}, consumer: {self.consumer_name})"
        )

    async def _ensure_connected(
        self, max_retries: int = 3, retry_delay: float = 2.0
    ) -> bool:
        if self.redis_client:
            try:
                await self.redis_client.ping()
                return True
            except Exception:
                await self._close_quietly()

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to Redis (attempt {attempt}/{max_retries})...")

                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    decode_responses=True,
                )

                await self.redis_client.ping()

                if not self._is_setup:
                    await self._setup_consumer_group()
                    self._is_setup = True

                logger.info(
                    f"✓ Successfully connected to Redis stream '{self.stream_name}'"
                )
                return True

            except Exception as e:
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to connect to Redis after {max_retries} attempts"
                    )
                    return False

        return False

    async def _setup_consumer_group(self):
        try:
            await self.redis_client.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id=self.start_id,
                mkstream=True,
            )
            logger.info(f"Created consumer group '{self.consumer_group}'")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{self.consumer_group}' already exists")
            else:
                raise

    async def _close_quietly(self):
        try:
            if self.redis_client:
                await self.redis_client.close()
        except Exception:
            pass

    async def poll(self) -> List[Dict[str, Any]]:
        return await self.poll_batch(batch_size=1)

    async def poll_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """
        Poll for multiple messages from Redis Streams.

        Returns a list of message dicts:
        {
            "id": "1637012345678-0",
            "data": {"field1": "value1", ...},
            "stream": "stream_name"
        }

        """
        if not await self._ensure_connected():
            logger.warning("Cannot poll: Redis connection unavailable")
            return []

        items: List[Dict[str, Any]] = []
        try:
            # XREADGROUP returns: [('stream_name', [('msg_id', {'field': 'value'}), ...])]
            response = await self.redis_client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={self.stream_name: ">"},
                count=batch_size,
                block=100,  # Block for 100ms if no messages
            )

            if response:
                for stream_name, messages in response:
                    for message_id, data in messages:
                        items.append(
                            {"id": message_id, "data": data, "stream": stream_name}
                        )

        except Exception as e:
            logger.error(f"Error polling messages: {e}", exc_info=True)
            await self._close_quietly()

        return items

    async def poll_all(self) -> List[Dict[str, Any]]:
        """Get all available messages at once."""
        items: List[Dict[str, Any]] = []

        while True:
            batch = await self.poll_batch(batch_size=100)
            if not batch:
                break
            items.extend(batch)

        return items

    async def poll_stream(
        self, batch_size: int = 10
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        while True:
            batch = await self.poll_batch(batch_size=batch_size)
            if not batch:
                break
            yield batch

    async def has_data(self) -> bool:
        if not await self._ensure_connected():
            return False

        try:
            # Check pending messages for this consumer group
            pending_info = await self.redis_client.xpending(
                name=self.stream_name, groupname=self.consumer_group
            )

            if pending_info and pending_info.get("pending", 0) > 0:
                return True

            # Also check for new messages
            response = await self.redis_client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={self.stream_name: ">"},
                count=1,
                block=0,  # Non-blocking
            )

            return bool(response and response[0][1])

        except Exception as e:
            logger.error(f"Error checking stream status: {e}")
            await self._close_quietly()
            return False

    async def ack(self, msg: Dict[str, Any]) -> None:
        if not await self._ensure_connected():
            logger.warning("Cannot ACK: Redis connection unavailable")
            return

        try:
            message_id = msg["id"]
            stream_name = msg.get("stream", self.stream_name)

            await self.redis_client.xack(stream_name, self.consumer_group, message_id)
        except Exception as e:
            logger.error(f"Error acknowledging message: {e}")

    async def nack(self, msg: Dict[str, Any]) -> None:
        logger.warning(
            f"NACK called for message {msg.get('id')}. "
            "Redis Streams doesn't support NACK - message remains pending."
        )

    async def close(self) -> None:
        """Close the Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis connection closed")
