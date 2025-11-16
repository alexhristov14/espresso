import sys
import pika
import time
import json
import logging
from pathlib import Path

project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scheduler import yaml_loader  # noqa: E402
from scheduler.scheduler import EspressoScheduler  # noqa: E402


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    connection = pika.BlockingConnection(
        pika.URLParameters("amqp://guest:guest@rabbitmq:5672/")
    )
    channel = connection.channel()
    channel.queue_declare(queue="orders_queue", durable=True)

    for i in range(1, 6):
        order = {
            "order_id": f"ORDER_000{i}",
            "customer": f"test{i}@example.com",
            "amount": 12.12 * i,
            "items": ["Product A", "Product B"],
        }

        channel.basic_publish(
            exchange="",
            routing_key="orders_queue",
            body=json.dumps(order),
            properties=pika.BasicProperties(delivery_mode=2),
        )

        print(f"✓ Sent order: {order['order_id']}")

        time.sleep(3)

    connection.close()

    inputs, jobs = yaml_loader.load_jobs_from_yaml(
        "jobs_definitions/rabbit_mq_jobs.yaml"
    )

    print(f"Loaded {len(jobs)} jobs and {len(inputs)} inputs.")

    sched = EspressoScheduler(jobs, inputs)
    sched.run_forever()


if __name__ == "__main__":
    main()
