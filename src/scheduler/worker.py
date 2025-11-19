import logging
import importlib
import traceback
import asyncio
from datetime import datetime
from typing import Callable
from .runtime import EspressoJobRuntimeState
from .input_manager import EspressoInputManager

logger = logging.getLogger(__name__)


def resolve_callable(module_name: str, function_name: str) -> Callable:
    module = importlib.import_module(module_name)
    func = getattr(module, function_name)
    return func


class EspressoJobExecutor:
    def __init__(self, num_workers: int = 5):
        self.num_workers = num_workers
        self.semaphore = asyncio.Semaphore(num_workers)

    async def submit(
        self, job_state: EspressoJobRuntimeState, input_manager: EspressoInputManager
    ):
        async def _run():
            async with self.semaphore:
                task_id = id(asyncio.current_task())
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
                            result = await input_manager.poll(batch_size=batch_size)
                            items = result.get(input_id, [])

                            if asyncio.iscoroutinefunction(func):
                                await func(items, *job.args, **job.kwargs)
                            else:
                                await asyncio.to_thread(
                                    func, items, *job.args, **job.kwargs
                                )  # Run sync function in thread pool

                            # Acknowledge messages after successful processing
                            await input_manager.ack_batch(input_id, items)

                    # Handle normal scheduled jobs
                    else:
                        if asyncio.iscoroutinefunction(func):
                            await func(*job.args, **job.kwargs)
                        else:
                            await asyncio.to_thread(func, *job.args, **job.kwargs)

                    job_state.retries_attempted = 0
                    job_state.last_error = None

                    logger.info(f"[Task {task_id}] Successfully executed job {job.id}")

                except Exception:
                    # Negative-acknowledge messages on failure (requeue them)
                    if input_id and items:
                        await input_manager.nack_batch(input_id, items, requeue=True)

                    job_state.retries_attempted += 1
                    job_state.last_error = traceback.format_exc()
                    logger.error(
                        f"[Task {task_id}] Error executing job {job.id}: {job_state.last_error}"
                    )
                    raise

                finally:
                    job_state.is_running = False
                    job_state.schedule_next_run(datetime.now())

        return asyncio.create_task(_run())
