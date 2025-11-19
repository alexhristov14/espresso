import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class DistributedJobState:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis: Optional[Redis] = None
        self.instance_id = str(uuid.uuid4())[:8]
        logger.info(
            f"Distributed state manager initialized (instance: {self.instance_id})"
        )

    async def connect(self):
        try:
            self.redis = Redis.from_url(
                self.redis_url, encoding="utf-8", decode_responses=True
            )

            await self.redis.ping()
            logger.info(f"✓ Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def close(self):
        if self.redis:
            await self.redis.close()
            logger.info("Redis connection closed")

    def _job_key(self, job_id: str) -> str:
        return f"espresso:job:{job_id}:state"

    def _lock_key(self, job_id: str) -> str:
        return f"espresso:lock:job:{job_id}"

    async def acquire_lock(self, job_id: str, ttl_seconds: int = 300) -> bool:
        lock_key = self._lock_key(job_id)

        acquired = await self.redis.set(
            lock_key, self.instance_id, nx=True, ex=ttl_seconds
        )

        if acquired:
            logger.debug(f"[{self.instance_id}] Acquired lock for job {job_id}")
        else:
            lock_holder = await self.redis.get(lock_key)
            logger.debug(
                f"[{self.instance_id}] Lock for job {job_id} held by {lock_holder}"
            )

        return bool(acquired)

    async def release_lock(self, job_id: str):
        lock_key = self._lock_key(job_id)

        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """

        result = await self.redis.eval(lua_script, 1, lock_key, self.instance_id)

        if result:
            logger.debug(f"[{self.instance_id}] Released lock for job {job_id}")
        else:
            logger.warning(
                f"[{self.instance_id}] Could not release lock for job {job_id} (not owner)"
            )

    async def get_job_state(self, job_id: str) -> Optional[Dict[str, Any]]:
        job_key = self._job_key(job_id)
        state = await self.redis.hgetall(job_key)

        if not state:
            return None

        result = dict(state)

        for time_field in ["next_run_time", "last_run_time", "created_at"]:
            if time_field in result and result[time_field]:
                result[time_field] = datetime.fromisoformat(result[time_field])
            else:
                result[time_field] = None

        if "is_running" in result:
            result["is_running"] = result["is_running"].lower() == "true"

        for num_field in ["retries_attempted", "execution_count"]:
            if num_field in result:
                result[num_field] = int(result[num_field])

        for float_field in ["total_execution_time", "last_execution_duration"]:
            if float_field in result and result[float_field]:
                result[float_field] = float(result[float_field])
            else:
                result[float_field] = (
                    None if float_field == "last_execution_duration" else 0.0
                )

        return result

    async def set_job_state(self, job_id: str, state: Dict[str, Any]):
        job_key = self._job_key(job_id)

        # Convert values to strings for Redis hash
        redis_state = {}
        for key, value in state.items():
            if isinstance(value, datetime):
                redis_state[key] = value.isoformat()
            elif isinstance(value, bool):
                redis_state[key] = str(value)
            elif value is None:
                redis_state[key] = ""
            else:
                redis_state[key] = str(value)

        await self.redis.hset(job_key, mapping=redis_state)

    async def update_job_field(self, job_id: str, field: str, value: Any):
        job_key = self._job_key(job_id)

        # Convert value to string
        if isinstance(value, datetime):
            value_str = value.isoformat()
        elif isinstance(value, bool):
            value_str = str(value)
        elif value is None:
            value_str = ""
        else:
            value_str = str(value)

        await self.redis.hset(job_key, field, value_str)

    async def get_all_job_ids(self) -> list[str]:
        pattern = "espresso:job:*:state"
        keys = []

        async for key in self.redis.scan_iter(match=pattern, count=100):
            job_id = key.split(":")[2]
            keys.append(job_id)

        return keys

    async def delete_job_state(self, job_id: str):
        job_key = self._job_key(job_id)
        lock_key = self._lock_key(job_id)

        await self.redis.delete(job_key, lock_key)
        logger.info(f"Deleted state for job {job_id}")

    async def heartbeat(self, ttl_seconds: int = 30):
        key = f"espresso:instance:{self.instance_id}:heartbeat"
        await self.redis.set(key, datetime.now().isoformat(), ex=ttl_seconds)

    async def get_active_instances(self) -> list[str]:
        pattern = "espresso:instance:*:heartbeat"
        instances = []

        async for key in self.redis.scan_iter(match=pattern, count=100):
            # Extract instance_id from key
            instance_id = key.split(":")[2]
            instances.append(instance_id)

        return instances
