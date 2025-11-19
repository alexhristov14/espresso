from typing import List
import json


def print_hello_world():
    print("Hello, world!")


def loop_n_times(n: int):
    for i in range(n):
        print(f"Iteration {i + 1}")


def send_welcome_email(ids: List[int]):
    # print(f"Sending welcome emails... to the following user IDs: {ids}")
    for user_id in ids:
        print(f"Sending welcome email to user ID: {user_id}")


def just_run():
    print("Just running the job!")


def process_order(order_data: List[str]):
    for order in order_data:
        order_dict = json.loads(order["body"])
        print(
            f"Processing order: {order_dict['order_id']} for customer {order_dict['customer']} with amount {order_dict['amount']} and items {order_dict['items']}"
        )


def process_notification(notifications: List[dict]):
    """Process notifications from Redis Streams"""
    for notification in notifications:
        msg_id = notification.get("id", "unknown")
        data = notification.get("data", {})

        notification_type = data.get("type", "unknown")
        user_id = data.get("user_id", "N/A")
        message = data.get("message", "No message")

        print(f"[Redis Stream] Processing notification {msg_id}")
        print(f"  Type: {notification_type}")
        print(f"  User: {user_id}")
        print(f"  Message: {message}")
