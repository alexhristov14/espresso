from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from .models import EspressoJobDefinition
from .utils import _get_next_cron_time


@dataclass
class EspressoJobRuntimeState:
    definition: EspressoJobDefinition
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None
    retries_attempted: int = 0
    is_running: bool = False
    last_error: Optional[str] = None

    def schedule_next_run(self, current_time: datetime):
        schedule = self.definition.schedule

        if schedule.kind == "cron" and schedule.cron:
            self.next_run_time = _get_next_cron_time(schedule.cron, current_time)
        elif schedule.kind == "interval" and schedule.every_seconds:
            if self.last_run_time:
                self.next_run_time = self.last_run_time + timedelta(
                    seconds=schedule.every_seconds
                )
            else:
                self.next_run_time = current_time + timedelta(
                    seconds=schedule.every_seconds
                )
        elif schedule.kind == "one_off" and schedule.run_at:
            self.next_run_time = schedule.run_at
        else:
            self.next_run_time = None
