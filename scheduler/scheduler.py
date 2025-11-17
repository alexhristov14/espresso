import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List
from .models import EspressoJobDefinition, EspressoInputDefinition
from .runtime import EspressoJobRuntimeState
from .worker import EspressoJobExecutor
from .input_manager import EspressoInputManager

logger = logging.getLogger(__name__)


class EspressoScheduler:
    def __init__(
        self,
        jobs: List[EspressoJobDefinition],
        inputs: List[EspressoInputDefinition],
        tick_seconds: int = 1,
        num_workers: int = 5,
    ):
        self.tick_seconds = tick_seconds
        self.executor = EspressoJobExecutor(num_workers=num_workers)
        self.input_manager = EspressoInputManager(inputs)

        now = datetime.now()

        self.job_states: Dict[str, EspressoJobRuntimeState] = {}
        for job in jobs:
            next_run = now
            self.job_states[job.id] = EspressoJobRuntimeState(
                definition=job, next_run_time=next_run
            )

    async def _run(self, state: EspressoJobRuntimeState):
        job = state.definition
        state.last_run_time = datetime.now()

        task = await self.executor.submit(state, self.input_manager)

        def _callback(fut):
            try:
                fut.result()
            except Exception:
                state.retries_attempted += 1
                if state.retries_attempted > job.max_retries:
                    logger.error(f"Job {job.id} exceeded max retries, disabling")
                    state.next_run_time = None
                else:
                    delay = job.retry_delay_seconds
                    state.next_run_time = datetime.now() + timedelta(seconds=delay)

        task.add_done_callback(_callback)

    async def run_forever(self):
        while True:
            now = datetime.now()

            for job_id, job_state in list(self.job_states.items()):
                job = job_state.definition

                if job.trigger and job.trigger.kind == "input":
                    if not job_state.is_running:
                        input_id = job.trigger.input_id

                        if job_state.next_run_time and now >= job_state.next_run_time:

                            if input_id and await self.input_manager.has_data(input_id):
                                logger.info(
                                    f"Triggering input-based job {job_id} (scheduled)"
                                )
                                await self._run(job_state)
                            else:
                                logger.debug(
                                    f"No data available for job {job_id}, scheduling next check"
                                )
                                job_state.schedule_next_run(now - timedelta(seconds=1))
                    continue

                if job_state.next_run_time is None:
                    continue

                if now >= job_state.next_run_time:
                    logger.info(f"Scheduling job {job_id} for execution")
                    await self._run(job_state)

            await asyncio.sleep(self.tick_seconds)
