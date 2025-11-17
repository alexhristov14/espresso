import sys
import asyncio
import json
import logging
from pathlib import Path
from aio_pika import connect_robust, Message

project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scheduler import yaml_loader  # noqa: E402
from scheduler.scheduler import EspressoScheduler  # noqa: E402

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    inputs, jobs = yaml_loader.load_jobs_from_yaml(
        "jobs_definitions/rabbit_mq_jobs.yaml"
    )

    print(f"Loaded {len(jobs)} jobs and {len(inputs)} inputs.")

    sched = EspressoScheduler(jobs, inputs, tick_seconds=3)
    await sched.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
