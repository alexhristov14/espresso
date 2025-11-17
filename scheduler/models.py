from dataclasses import dataclass
from typing import Optional, Literal, Any
from datetime import datetime

ScheduleKind = Literal["cron", "interval", "one_off", "on_demand"]
InputType = Literal["list", "rabbitmq"]
TriggerKind = Literal["input"]


@dataclass
class EspressoSchedule:
    kind: ScheduleKind
    cron: Optional[str] = None
    every_seconds: Optional[int] = None
    run_at: Optional[datetime] = None


@dataclass
class EspressoTrigger:
    kind: TriggerKind
    input_id: Optional[str] = None


@dataclass
class EspressoJobDefinition:
    id: str
    type: str
    module: str
    function: str
    schedule: EspressoSchedule
    batch_size: Optional[int] = None
    trigger: Optional[EspressoTrigger] = None
    args: list = None
    kwargs: dict = None
    max_retries: int = 3
    retry_delay_seconds: int = 60
    timeout_seconds: int = 300
    enabled: bool = True


@dataclass
class EspressoInputDefinition:
    id: str
    type: InputType


@dataclass
class EspressoListInputDefinition(EspressoInputDefinition):
    items: list[Any] = None


@dataclass
class EspressoRabbitMQInputDefinition(EspressoInputDefinition):
    url: str = None
    queue: str = None
    prefetch_count: int = 10
