import asyncio
import json
from aio_pika import connect_robust, Message

async def main():
    # Connect to RabbitMQ and send test messages
    connection = await connect_robust("amqp://guest:guest@rabbitmq-server:5672/")
    channel = await connection.channel()
    await channel.declare_queue("orders_queue", durable=True)

    for i in range(1, 50):
        order = {
            "order_id": f"ORDER_000{i}",
            "customer": f"test{i}@example.com",
            "amount": 12.12 * i,
            "items": ["Product A", "Product B"],
        }

        await channel.default_exchange.publish(
            Message(
                body=json.dumps(order).encode(),
                delivery_mode=2,
            ),
            routing_key="orders_queue",
        )

        print(f"✓ Sent order: {order['order_id']}")

        await asyncio.sleep(3)

    await connection.close()

if __name__ == "__main__":
    asyncio.run(main())