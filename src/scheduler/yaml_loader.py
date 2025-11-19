import yaml
from pathlib import Path
from datetime import datetime
from typing import List
from .models import (
    EspressoJobDefinition,
    EspressoSchedule,
    EspressoInputDefinition,
    EspressoTrigger,
    EspressoListInputDefinition,
    EspressoRabbitMQInputDefinition,
    EspressoRedisStreamsInputDefinition,
)


def load_jobs_from_yaml(path: str | Path):
    path = Path(path)

    with path.open("r") as file:
        data = yaml.safe_load(file)

        inputs = []

        for raw_input in data.get("inputs", []):
            if raw_input["type"] == "list":
                input_def = EspressoListInputDefinition(
                    id=raw_input["id"],
                    type=raw_input["type"],
                    items=raw_input.get("items", []),
                )
            elif raw_input["type"] == "rabbitmq":
                input_def = EspressoRabbitMQInputDefinition(
                    id=raw_input["id"],
                    type=raw_input["type"],
                    url=raw_input.get("url"),
                    queue=raw_input.get("queue"),
                    prefetch_count=raw_input.get("prefetch_count", 10),
                )
            elif raw_input["type"] == "redis_streams":
                input_def = EspressoRedisStreamsInputDefinition(
                    id=raw_input["id"],
                    type=raw_input["type"],
                    host=raw_input.get("host", "localhost"),
                    port=raw_input.get("port", 6379),
                    password=raw_input.get("password"),
                    db=raw_input.get("db", 0),
                    stream_name=raw_input.get("stream_name", "espresso_stream"),
                    consumer_group=raw_input.get("consumer_group", "espresso_group"),
                    consumer_name=raw_input.get("consumer_name", "worker_1"),
                    start_id=raw_input.get("start_id", "0"),
                )
            else:
                input_def = EspressoInputDefinition(
                    id=raw_input["id"],
                    type=raw_input["type"],
                )

            inputs.append(input_def)

        jobs = []

        for raw_job in data.get("jobs", []):
            s = raw_job["schedule"]

            trigger = raw_job.get("trigger", None)

            if trigger:
                trigger = EspressoTrigger(
                    kind=trigger["kind"],
                    input_id=trigger.get("input_id"),
                )

            schedule = EspressoSchedule(
                kind=s["kind"],
                cron=s.get("cron"),
                every_seconds=s.get("every_seconds"),
                run_at=datetime.fromisoformat(s["run_at"]) if s.get("run_at") else None,
            )

            job = EspressoJobDefinition(
                id=raw_job["id"],
                type=raw_job["type"],
                module=raw_job["module"],
                function=raw_job["function"],
                batch_size=raw_job.get("batch_size", None),
                schedule=schedule,
                trigger=trigger,
                args=raw_job.get("args", []),
                kwargs=raw_job.get("kwargs", {}),
                max_retries=raw_job.get("max_retries", 3),
                retry_delay_seconds=raw_job.get("retry_delay_seconds", 60),
                timeout_seconds=raw_job.get("timeout_seconds", 300),
                enabled=raw_job.get("enabled", True),
            )

            jobs.append(job)

        return inputs, jobs
