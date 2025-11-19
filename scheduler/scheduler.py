import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from .models import EspressoJobDefinition, EspressoInputDefinition
from .runtime import EspressoJobRuntimeState
from .worker import EspressoJobExecutor
from .input_manager import EspressoInputManager
from .distributed_state import DistributedJobState

logger = logging.getLogger(__name__)


class EspressoScheduler:
    def __init__(
        self,
        jobs: List[EspressoJobDefinition],
        inputs: List[EspressoInputDefinition],
        tick_seconds: int = 1,
        num_workers: int = 5,
        redis_url: Optional[str] = None,  # If set, enables distributed mode
    ):
        self.tick_seconds = tick_seconds
        self.executor = EspressoJobExecutor(num_workers=num_workers)
        self.input_manager = EspressoInputManager(inputs)
        self._lock = asyncio.Lock()
        self._running = False

        self.distributed_mode = redis_url is not None
        self.distributed_state = DistributedJobState(redis_url) if redis_url else None

        now = datetime.now()

        self.job_states: Dict[str, EspressoJobRuntimeState] = {}
        for job in jobs:
            next_run = now
            self.job_states[job.id] = EspressoJobRuntimeState(
                definition=job, next_run_time=next_run
            )

        if self.distributed_mode:
            logger.info("🌐 Scheduler initialized in DISTRIBUTED mode (Redis enabled)")
        else:
            logger.info(
                "🖥️  Scheduler initialized in SINGLE-SERVER mode (local state only)"
            )

    async def _sync_state_to_redis(self, job_id: str):
        if not self.distributed_mode:
            return

        state = self.job_states[job_id]
        await self.distributed_state.set_job_state(
            job_id,
            {
                "next_run_time": state.next_run_time,
                "last_run_time": state.last_run_time,
                "retries_attempted": state.retries_attempted,
                "is_running": state.is_running,
                "last_error": state.last_error or "",
                "status": state.status,
                "execution_count": state.execution_count,
                "total_execution_time": state.total_execution_time,
                "last_execution_duration": state.last_execution_duration,
                "created_at": state.created_at,
            },
        )

    async def _sync_state_from_redis(self, job_id: str):
        if not self.distributed_mode:
            return

        redis_state = await self.distributed_state.get_job_state(job_id)
        if redis_state:
            state = self.job_states[job_id]

            state.next_run_time = redis_state.get("next_run_time")
            state.last_run_time = redis_state.get("last_run_time")
            state.retries_attempted = redis_state.get("retries_attempted", 0)
            state.is_running = redis_state.get("is_running", False)
            state.last_error = redis_state.get("last_error")
            state.status = redis_state.get("status", "active")
            state.execution_count = redis_state.get("execution_count", 0)
            state.total_execution_time = redis_state.get("total_execution_time", 0.0)
            state.last_execution_duration = redis_state.get("last_execution_duration")

    async def _run(self, state: EspressoJobRuntimeState):
        job = state.definition
        state.last_run_time = datetime.now()
        start_time = datetime.now()

        task = await self.executor.submit(state, self.input_manager)

        def _callback(fut):
            try:
                fut.result()

                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                state.execution_count += 1
                state.total_execution_time += duration
                state.last_execution_duration = duration

                if self.distributed_mode:
                    asyncio.create_task(self._sync_state_to_redis(job.id))

            except Exception:
                state.retries_attempted += 1
                if state.retries_attempted > job.max_retries:
                    logger.error(f"Job {job.id} exceeded max retries, disabling")
                    state.disable()
                    state.next_run_time = None
                else:
                    delay = job.retry_delay_seconds
                    state.next_run_time = datetime.now() + timedelta(seconds=delay)

                if self.distributed_mode:
                    asyncio.create_task(self._sync_state_to_redis(job.id))

        task.add_done_callback(_callback)

    def append_to_input(self, input_id: str, item: Any) -> None:
        self.input_manager.append_to_input(input_id, item)

    def append_items_to_input(self, input_id: str, items: List[Any]) -> None:
        self.input_manager.append_items_to_input(input_id, items)

    async def run_forever(self):
        self._running = True

        if self.distributed_mode:
            await self.distributed_state.connect()

            for job_id in self.job_states:
                await self._sync_state_to_redis(job_id)

        logger.info("Scheduler started")

        while self._running:
            now = datetime.now()

            if self.distributed_mode:
                await self.distributed_state.heartbeat()

            async with self._lock:
                for job_id, job_state in list(self.job_states.items()):
                    job = job_state.definition

                    if self.distributed_mode:
                        await self._sync_state_from_redis(job_id)

                    if not job_state.can_execute():
                        continue

                    if self.distributed_mode:
                        should_run = False

                        if job.trigger and job.trigger.kind == "input":
                            input_id = job.trigger.input_id
                            if (
                                job_state.next_run_time
                                and now >= job_state.next_run_time
                            ):
                                if input_id and await self.input_manager.has_data(
                                    input_id
                                ):
                                    should_run = True
                        else:
                            if (
                                job_state.next_run_time
                                and now >= job_state.next_run_time
                            ):
                                should_run = True

                        if not should_run:
                            continue

                        lock_acquired = await self.distributed_state.acquire_lock(
                            job_id, ttl_seconds=300
                        )
                        if not lock_acquired:
                            logger.debug(
                                f"[DISTRIBUTED] Job {job_id} locked by another instance, skipping"
                            )
                            continue

                        await self.distributed_state.update_job_field(
                            job_id, "is_running", True
                        )

                        try:
                            if job.trigger and job.trigger.kind == "input":
                                input_id = job.trigger.input_id
                                logger.info(
                                    f"[DISTRIBUTED] Triggering input-based job {job_id}"
                                )
                                await self._run(job_state)
                            else:
                                logger.info(
                                    f"[DISTRIBUTED] Scheduling job {job_id} for execution"
                                )
                                await self._run(job_state)
                        finally:
                            await self.distributed_state.update_job_field(
                                job_id, "is_running", False
                            )
                            await self.distributed_state.release_lock(job_id)

                    else:
                        if job.trigger and job.trigger.kind == "input":
                            if not job_state.is_running:
                                input_id = job.trigger.input_id

                                if (
                                    job_state.next_run_time
                                    and now >= job_state.next_run_time
                                ):
                                    if input_id and await self.input_manager.has_data(
                                        input_id
                                    ):
                                        logger.info(
                                            f"Triggering input-based job {job_id} (scheduled)"
                                        )
                                        await self._run(job_state)
                                    else:
                                        logger.debug(
                                            f"No data available for job {job_id}, scheduling next check"
                                        )
                                        job_state.schedule_next_run(
                                            now - timedelta(seconds=1)
                                        )
                            continue

                        if job_state.next_run_time is None:
                            continue

                        if now >= job_state.next_run_time:
                            logger.info(f"Scheduling job {job_id} for execution")
                            await self._run(job_state)

            await asyncio.sleep(self.tick_seconds)

    async def stop(self):
        """Stop the scheduler gracefully."""
        logger.info("Stopping scheduler...")
        self._running = False

        if self.distributed_mode:
            await self.distributed_state.close()

    async def get_job(self, job_id: str) -> Optional[EspressoJobRuntimeState]:
        """Get job state by ID."""
        async with self._lock:
            return self.job_states.get(job_id)

    async def list_jobs(self) -> Dict[str, EspressoJobRuntimeState]:
        """Get all job states."""
        async with self._lock:
            return dict(self.job_states)

    async def pause_job(self, job_id: str) -> bool:
        """Pause a job."""
        async with self._lock:
            if job_id in self.job_states:
                self.job_states[job_id].pause()
                logger.info(f"Job {job_id} paused")
                return True
            return False

    async def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        async with self._lock:
            if job_id in self.job_states:
                self.job_states[job_id].resume()
                logger.info(f"Job {job_id} resumed")
                return True
            return False

    async def stop_job(self, job_id: str) -> bool:
        """Stop a job."""
        async with self._lock:
            if job_id in self.job_states:
                self.job_states[job_id].stop()
                logger.info(f"Job {job_id} stopped")
                return True
            return False

    async def enable_job(self, job_id: str) -> bool:
        """Enable a disabled job."""
        async with self._lock:
            if job_id in self.job_states:
                self.job_states[job_id].enable()
                logger.info(f"Job {job_id} enabled")
                return True
            return False

    async def trigger_job(self, job_id: str) -> bool:
        """Manually trigger a job execution."""
        async with self._lock:
            if job_id in self.job_states:
                job_state = self.job_states[job_id]
                if job_state.can_execute():
                    logger.info(f"Manually triggering job {job_id}")
                    await self._run(job_state)
                    return True
                else:
                    logger.warning(
                        f"Cannot trigger job {job_id} - status: {job_state.status}"
                    )
                    return False
            return False
