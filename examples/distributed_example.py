# Example: Distributed Scheduler with Redis
# This shows how to run Espresso across multiple servers

import sys
import asyncio
import logging
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scheduler import yaml_loader
from scheduler.scheduler import EspressoScheduler


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load jobs from YAML
    inputs, jobs = yaml_loader.load_jobs_from_yaml(
        "examples/jobs_definitions/jobs.yaml"
    )

    print(f"Loaded {len(jobs)} jobs and {len(inputs)} inputs.")
    print("\n" + "=" * 60)
    print("DISTRIBUTED MODE ENABLED")
    print("=" * 60)
    print("\n✅ You can now run this script on MULTIPLE SERVERS!")
    print("✅ Each server will coordinate via Redis")
    print("✅ Jobs will NOT be duplicated across servers")
    print("\nHow it works:")
    print("  1. All servers share job state in Redis")
    print("  2. Before running a job, server acquires a distributed lock")
    print("  3. Only ONE server gets the lock and runs the job")
    print("  4. Other servers skip and check again next tick")
    print("\n" + "=" * 60 + "\n")

    sched = EspressoScheduler(
        jobs,
        inputs,
        num_workers=10,
        redis_url="redis://redis-server:6379/0",
    )

    await sched.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
