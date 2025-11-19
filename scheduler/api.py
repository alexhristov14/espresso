"""
REST API for Espresso Job Scheduler runtime control.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from .scheduler import EspressoScheduler
from .runtime import EspressoJobRuntimeState

logger = logging.getLogger(__name__)


# Response models
class JobStateResponse(BaseModel):
    id: str
    type: str
    module: str
    function: str
    status: str
    is_running: bool
    execution_count: int
    last_run_time: Optional[datetime]
    next_run_time: Optional[datetime]
    last_error: Optional[str]
    retries_attempted: int
    total_execution_time: float
    last_execution_duration: Optional[float]
    created_at: datetime
    schedule_kind: str
    enabled: bool

    class Config:
        from_attributes = True


class JobListResponse(BaseModel):
    jobs: Dict[str, JobStateResponse]
    total: int


class JobActionResponse(BaseModel):
    success: bool
    message: str
    job_id: str


class HealthResponse(BaseModel):
    status: str
    scheduler_running: bool
    total_jobs: int
    active_jobs: int
    paused_jobs: int
    running_jobs: int
    num_workers: int
    tick_seconds: int


def create_api(scheduler: EspressoScheduler) -> FastAPI:
    """Create FastAPI application with scheduler control endpoints."""

    app = FastAPI(
        title="Espresso Job Scheduler API",
        description="Runtime control API for managing scheduled jobs",
        version="0.1.0",
    )

    def _state_to_response(
        job_id: str, state: EspressoJobRuntimeState
    ) -> JobStateResponse:
        """Convert runtime state to API response model."""
        return JobStateResponse(
            id=job_id,
            type=state.definition.type,
            module=state.definition.module,
            function=state.definition.function,
            status=state.status,
            is_running=state.is_running,
            execution_count=state.execution_count,
            last_run_time=state.last_run_time,
            next_run_time=state.next_run_time,
            last_error=state.last_error,
            retries_attempted=state.retries_attempted,
            total_execution_time=state.total_execution_time,
            last_execution_duration=state.last_execution_duration,
            created_at=state.created_at,
            schedule_kind=state.definition.schedule.kind,
            enabled=state.definition.enabled,
        )

    @app.get("/", tags=["General"])
    async def root():
        """API root endpoint."""
        return {
            "message": "Espresso Job Scheduler API",
            "version": "0.1.0",
            "docs": "/docs",
        }

    @app.get("/health", response_model=HealthResponse, tags=["General"])
    async def health():
        """Health check endpoint with scheduler configuration."""
        jobs = await scheduler.list_jobs()
        active = sum(1 for s in jobs.values() if s.status == "active")
        paused = sum(1 for s in jobs.values() if s.status == "paused")
        running = sum(1 for s in jobs.values() if s.is_running)

        return HealthResponse(
            status="healthy",
            scheduler_running=scheduler._running,
            total_jobs=len(jobs),
            active_jobs=active,
            paused_jobs=paused,
            running_jobs=running,
            num_workers=scheduler.executor.num_workers,
            tick_seconds=scheduler.tick_seconds,
        )

    @app.get("/jobs", response_model=JobListResponse, tags=["Jobs"])
    async def list_jobs():
        """List all jobs with their current state."""
        jobs = await scheduler.list_jobs()
        job_responses = {
            job_id: _state_to_response(job_id, state) for job_id, state in jobs.items()
        }
        return JobListResponse(jobs=job_responses, total=len(job_responses))

    @app.get("/jobs/{job_id}", response_model=JobStateResponse, tags=["Jobs"])
    async def get_job(job_id: str):
        """Get detailed information about a specific job."""
        state = await scheduler.get_job(job_id)
        if not state:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job '{job_id}' not found",
            )
        return _state_to_response(job_id, state)

    @app.post(
        "/jobs/{job_id}/pause", response_model=JobActionResponse, tags=["Job Control"]
    )
    async def pause_job(job_id: str):
        """Pause a job (prevents it from running while keeping schedule)."""
        success = await scheduler.pause_job(job_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job '{job_id}' not found",
            )
        return JobActionResponse(
            success=True, message=f"Job '{job_id}' paused successfully", job_id=job_id
        )

    @app.post(
        "/jobs/{job_id}/resume", response_model=JobActionResponse, tags=["Job Control"]
    )
    async def resume_job(job_id: str):
        """Resume a paused job."""
        success = await scheduler.resume_job(job_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job '{job_id}' not found",
            )
        return JobActionResponse(
            success=True, message=f"Job '{job_id}' resumed successfully", job_id=job_id
        )

    @app.post(
        "/jobs/{job_id}/stop", response_model=JobActionResponse, tags=["Job Control"]
    )
    async def stop_job(job_id: str):
        """Stop a job (can be resumed later)."""
        success = await scheduler.stop_job(job_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job '{job_id}' not found",
            )
        return JobActionResponse(
            success=True, message=f"Job '{job_id}' stopped successfully", job_id=job_id
        )

    @app.post(
        "/jobs/{job_id}/enable", response_model=JobActionResponse, tags=["Job Control"]
    )
    async def enable_job(job_id: str):
        """Enable a disabled job."""
        success = await scheduler.enable_job(job_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job '{job_id}' not found",
            )
        return JobActionResponse(
            success=True, message=f"Job '{job_id}' enabled successfully", job_id=job_id
        )

    @app.post(
        "/jobs/{job_id}/trigger", response_model=JobActionResponse, tags=["Job Control"]
    )
    async def trigger_job(job_id: str):
        """Manually trigger a job execution."""
        success = await scheduler.trigger_job(job_id)
        if not success:
            state = await scheduler.get_job(job_id)
            if not state:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Job '{job_id}' not found",
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot trigger job '{job_id}' - current status: {state.status}",
                )
        return JobActionResponse(
            success=True,
            message=f"Job '{job_id}' triggered successfully",
            job_id=job_id,
        )

    return app
