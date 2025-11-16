import logging
import importlib
import traceback
import threading
from datetime import datetime
from typing import Callable
from .runtime import EspressoJobRuntimeState
from .input_manager import EspressoInputManager
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


def resolve_callable(module_name: str, function_name: str) -> Callable:
    module = importlib.import_module(module_name)
    func = getattr(module, function_name)
    return func


class EspressoJobExecutor:
    def __init__(self, num_workers: int = 5):
        self.executor = ThreadPoolExecutor(max_workers=num_workers)

    def submit(
        self, job_state: EspressoJobRuntimeState, input_manager: EspressoInputManager
    ):
        def _run():
            thread_id = threading.get_ident()
            job = job_state.definition

            items = []
            input_id = None

            try:
                job_state.is_running = True
                job_state.last_run_time = datetime.now()
                trigger = job.trigger

                func = resolve_callable(job.module, job.function)

                # Handle trigger based jobs
                if trigger:
                    if trigger.kind == "input":
                        input_id = trigger.input_id
                        if not input_id:
                            raise ValueError(
                                f"Input trigger for job {job.id} missing input_id"
                            )

                        batch_size = getattr(job, "batch_size", 10)
                        result = input_manager.poll(batch_size=batch_size)
                        items = result.get(input_id, [])

                        func(items, *job.args, **job.kwargs)

                        # Acknowledge messages after successful processing
                        input_manager.ack_batch(input_id, items)

                # Handle normal scheduled jobs
                else:
                    func(*job.args, **job.kwargs)

                job_state.retries_attempted = 0
                job_state.last_error = None

                logger.info(f"[TID {thread_id}] Successfully executed job {job.id}")

            except Exception:
                # Negative-acknowledge messages on failure (requeue them)
                if input_id and items:
                    input_manager.nack_batch(input_id, items, requeue=True)

                job_state.retries_attempted += 1
                job_state.last_error = traceback.format_exc()
                logger.error(
                    f"[TID {thread_id}] Error executing job {job.id}: {job_state.last_error}"
                )
                raise

            finally:
                job_state.is_running = False
                job_state.schedule_next_run(datetime.now())

        return self.executor.submit(_run)
