from ..models import (
    EspressoInputDefinition,
    EspressoListInputDefinition,
    EspressoRabbitMQInputDefinition,
    EspressoRedisStreamsInputDefinition,
)
from typing import List, Optional, Any


def create_input_def(
    id: str,
    type: str,
    items: Optional[List[Any]] = None,
    rabbitmq_url: Optional[str] = None,
    rabbitmq_queue: Optional[str] = None,
    rabbitmq_prefetch_count: Optional[int] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_password: Optional[str] = None,
    redis_db: Optional[int] = None,
    redis_stream_name: Optional[str] = None,
    redis_consumer_group: Optional[str] = None,
    redis_consumer_name: Optional[str] = None,
    redis_start_id: Optional[str] = None,
) -> EspressoInputDefinition:
    """
    Factory function to create input definitions based on type.

    Args:
        id (str): The unique identifier for the input.
        type (str): The type of the input (e.g., "list").
        items (Optional[List[Any]]): The list of items for list inputs.

    Example job definition YAML to use this input creator:

    ```python
    input_def = create_input_def(
        id="example_input",
        type="list",
        items=["item1", "item2", "item3"]
    )
    ```

    ```yaml
    jobs:
        -   id: example_job
            type: example_module.example_function
            module: example_module
            function: example_function
            schedule:
                kind: interval
                every_seconds: 300
            trigger:
                kind: input
                input_id: example_input
            args: []
            kwargs: {}
            max_retries: 3
            retry_delay_seconds: 60
            timeout_seconds: 300
            enabled: true
    ```
    """

    if type == "list":
        return EspressoListInputDefinition(id=id, type=type, items=items or [])
    elif type == "rabbitmq":
        return EspressoRabbitMQInputDefinition(
            id=id,
            type=type,
            url=rabbitmq_url or "amqp://guest:guest@localhost/",
            queue=rabbitmq_queue or "default_queue",
            prefetch_count=rabbitmq_prefetch_count or 10,
        )
    elif type == "redis_streams":
        return EspressoRedisStreamsInputDefinition(
            id=id,
            type=type,
            host=redis_host or "localhost",
            port=redis_port or 6379,
            password=redis_password,
            db=redis_db or 0,
            stream_name=redis_stream_name or "espresso_stream",
            consumer_group=redis_consumer_group or "espresso_group",
            consumer_name=redis_consumer_name or "worker_1",
            start_id=redis_start_id or "0",
        )
    else:
        return EspressoInputDefinition(id=id, type=type)
