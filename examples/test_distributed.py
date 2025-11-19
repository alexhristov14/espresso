"""
Quick test to verify distributed coordination works.

This script simulates 3 servers running the same job every 5 seconds.
Watch the output to see that only ONE server runs the job each time.
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scheduler.models import EspressoJobDefinition, EspressoSchedule
from scheduler.scheduler import EspressoScheduler


# Simple test function
async def test_job():
    """This will be called by the scheduler."""
    print(f"    🎯 JOB EXECUTED at {datetime.now().strftime('%H:%M:%S')}")
    await asyncio.sleep(1)  # Simulate work


async def run_instance(instance_name: str, redis_url: str):
    """Run a scheduler instance."""
    logging.basicConfig(level=logging.INFO, format=f"[{instance_name}] %(message)s")

    # Create a simple job that runs every 5 seconds
    job = EspressoJobDefinition(
        id="test_distributed_job",
        type="espresso_job",
        module="__main__",  # Use this module
        function="test_job",
        schedule=EspressoSchedule(kind="interval", every_seconds=5),
    )

    # Create scheduler with Redis
    sched = EspressoScheduler(jobs=[job], inputs=[], num_workers=1, redis_url=redis_url)

    print(f"\n{'=' * 60}")
    print(f"🚀 {instance_name} started!")
    print(f"{'=' * 60}\n")

    await sched.run_forever()


async def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║         DISTRIBUTED MODE TEST                                ║
║                                                              ║
║  This will simulate 3 servers running simultaneously.        ║
║  Watch for [DISTRIBUTED] messages showing coordination.     ║
║                                                              ║
║  Expected behavior:                                          ║
║    ✅ Job runs every 5 seconds                              ║
║    ✅ Only ONE instance executes it each time               ║
║    ✅ Other instances skip with "locked by another"         ║
║                                                              ║
║  Press Ctrl+C to stop all instances.                         ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Check if Redis is available
    print("🔍 Checking Redis connection...")
    try:
        from redis.asyncio import Redis

        redis = Redis.from_url("redis://localhost:6379")
        await redis.ping()
        await redis.close()
        print("✅ Redis is running!\n")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("\n💡 Start Redis first:")
        print("   docker run -d -p 6379:6379 redis:7-alpine")
        print("   OR")
        print("   sudo service redis-server start\n")
        return

    redis_url = "redis://localhost:6379/0"

    # Run 3 scheduler instances in parallel
    async with asyncio.TaskGroup() as tg:
        tg.create_task(run_instance("SERVER-A", redis_url))
        tg.create_task(run_instance("SERVER-B", redis_url))
        tg.create_task(run_instance("SERVER-C", redis_url))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n✋ Test stopped by user\n")
