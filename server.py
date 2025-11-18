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
from scheduler.api import create_api  # noqa: E402

# Only import if available
try:
    import uvicorn
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


async def run_scheduler(sched: EspressoScheduler):
    """Run the scheduler in the background."""
    await sched.run_forever()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    inputs, jobs = yaml_loader.load_jobs_from_yaml(
        "examples/jobs_definitions/rabbit_mq_jobs.yaml"
    )

    print(f"Loaded {len(jobs)} jobs and {len(inputs)} inputs.")

    sched = EspressoScheduler(jobs, inputs, num_workers=10)
    
    if HAS_FASTAPI:
        # Create API app
        app = create_api(sched)
        
        # Configure uvicorn
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info"
        )
        server = uvicorn.Server(config)
        
        print("Starting Espresso Scheduler with API on http://0.0.0.0:8000")
        print("API Documentation: http://0.0.0.0:8000/docs")
        
        # Run both scheduler and API server concurrently
        async with asyncio.TaskGroup() as tg:
            tg.create_task(run_scheduler(sched))
            tg.create_task(server.serve())
    else:
        print("FastAPI not installed. Running scheduler only (no API).")
        print("Install with: pip install fastapi uvicorn")
        await sched.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
