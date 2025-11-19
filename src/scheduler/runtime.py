from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Literal
from .models import EspressoJobDefinition
from .utils import _get_next_cron_time

JobStatus = Literal["active", "paused", "stopped", "disabled"]


@dataclass
class EspressoJobRuntimeState:
    definition: EspressoJobDefinition
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None
    retries_attempted: int = 0
    is_running: bool = False
    last_error: Optional[str] = None
    status: JobStatus = "active"
    execution_count: int = 0
    total_execution_time: float = 0.0
    last_execution_duration: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.now)

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
        elif schedule.kind == "on_demand":
            self.next_run_time = current_time
        else:
            self.next_run_time = None

    def can_execute(self) -> bool:
        """Check if job can be executed based on status."""
        return self.status == "active" and not self.is_running

    def pause(self):
        """Pause the job."""
        if self.status == "active":
            self.status = "paused"

    def resume(self):
        """Resume a paused job."""
        if self.status == "paused":
            self.status = "active"

    def stop(self):
        """Stop the job (can be resumed later)."""
        self.status = "stopped"

    def disable(self):
        """Disable the job permanently."""
        self.status = "disabled"

    def enable(self):
        """Enable a disabled job."""
        if self.status == "disabled":
            self.status = "active"
