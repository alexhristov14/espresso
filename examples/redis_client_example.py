import redis.asyncio as redis
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scheduler.yaml_loader import load_jobs_from_yaml
from scheduler.scheduler import EspressoScheduler

async def main():
    # Load inputs and jobs from YAML
    inputs, jobs = load_jobs_from_yaml("jobs_definitions/jobs.yaml")
    
    print(f"✓ Loaded {len(inputs)} input(s) and {len(jobs)} job(s)")
    for inp in inputs:
        print(f"  - Input: {inp.id} (type: {inp.type})")
    for job in jobs:
        print(f"  - Job: {job.id}")

    scheduler = EspressoScheduler(jobs=jobs, inputs=inputs, num_workers=5)
    
    scheduler_task = asyncio.create_task(scheduler.run_forever())
    print("\n✓ Scheduler started")

    print("\n📝 Producing test messages to Redis Streams...")
    redis_client = redis.Redis(
        host='redis-server',  # Docker container name
        port=6379,
        db=0,
        decode_responses=True
    )
    
    try:
        for i in range(15):
            email = f"user_{i+1}@example.com"

            scheduler.append_to_input(
                input_id="email_list_input",
                item=email
            )

            msg_id = await redis_client.xadd(
                'notifications_stream',
                {
                    'type': 'user_signup',
                    'user_id': f'user_{i+1}',
                    'message': f'Welcome user {i+1}!'
                }
            )
            print(f"  ✓ Added message {msg_id}")
        
        print("\n✅ Test messages added to Redis Stream 'notifications_stream'")
        print("   The scheduler will process them every 5 seconds")
        
    except Exception as e:
        print(f"⚠️  Could not add test messages to Redis: {e}")
        print("   Make sure Redis is running and accessible at redis-server:6379")
    finally:
        await redis_client.aclose()  # Use aclose() instead of close()

    print("\n🚀 Scheduler is running. Press Ctrl+C to stop.\n")
    await scheduler_task


if __name__ == "__main__":
    asyncio.run(main())
