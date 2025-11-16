import logging

import time
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
    ):
        self.tick_seconds = tick_seconds
        self.executor = EspressoJobExecutor()
        self.input_manager = EspressoInputManager(inputs)

        now = datetime.now()

        self.job_states: Dict[str, EspressoJobRuntimeState] = {}
        for job in jobs:
            next_run = now
            self.job_states[job.id] = EspressoJobRuntimeState(
                definition=job, next_run_time=next_run
            )

    def _run(self, state: EspressoJobRuntimeState):
        job = state.definition
        state.last_run_time = datetime.now()

        future = self.executor.submit(state, self.input_manager)

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

        future.add_done_callback(_callback)

    def run_forever(self):
        while True:
            now = datetime.now()

            # Scheduled based jobs
            for job_id, job_state in list(self.job_states.items()):
                job = job_state.definition

                if job.trigger and job.trigger.kind == "input":
                    if not job_state.is_running:
                        input_id = job.trigger.input_id

                        if job_state.next_run_time and now >= job_state.next_run_time:
                            if input_id and self.input_manager.has_data(input_id):
                                logger.info(
                                    f"Triggering input-based job {job_id} (scheduled)"
                                )
                                self._run(job_state)
                            else:
                                logger.debug(
                                    f"No data available for job {job_id}, scheduling next check"
                                )
                                job_state.schedule_next_run(datetime.now())
                    continue

                # Handle scheduled jobs
                if job_state.next_run_time is None:
                    continue

                if now >= job_state.next_run_time:
                    logger.info(f"Scheduling job {job_id} for execution")
                    self._run(job_state)

            time.sleep(self.tick_seconds)
