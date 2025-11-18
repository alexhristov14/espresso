"""
Tests for runtime control functionality.
"""
import pytest
import asyncio
from datetime import datetime
from scheduler.models import (
    EspressoJobDefinition,
    EspressoSchedule,
    EspressoInputDefinition,
)
from scheduler.scheduler import EspressoScheduler
from scheduler.runtime import EspressoJobRuntimeState


@pytest.fixture
def sample_job():
    """Create a sample job definition."""
    return EspressoJobDefinition(
        id="test_job",
        type="espresso_job",
        module="testing.test",
        function="print_hello_world",
        schedule=EspressoSchedule(kind="interval", every_seconds=60),
        args=[],
        kwargs={},
    )


@pytest.fixture
def scheduler_with_jobs(sample_job):
    """Create a scheduler with sample jobs."""
    jobs = [sample_job]
    inputs = []
    return EspressoScheduler(jobs, inputs, tick_seconds=1, num_workers=2)


@pytest.mark.asyncio
async def test_job_runtime_state_initialization():
    """Test that job runtime state is initialized correctly."""
    job = EspressoJobDefinition(
        id="test_job",
        type="espresso_job",
        module="test.module",
        function="test_func",
        schedule=EspressoSchedule(kind="interval", every_seconds=10),
    )
    
    state = EspressoJobRuntimeState(definition=job)
    
    assert state.status == "active"
    assert state.is_running is False
    assert state.execution_count == 0
    assert state.retries_attempted == 0
    assert state.last_error is None


@pytest.mark.asyncio
async def test_job_can_execute():
    """Test job can_execute logic."""
    job = EspressoJobDefinition(
        id="test_job",
        type="espresso_job",
        module="test.module",
        function="test_func",
        schedule=EspressoSchedule(kind="interval", every_seconds=10),
    )
    
    state = EspressoJobRuntimeState(definition=job)
    
    # Active job can execute
    assert state.can_execute() is True
    
    # Paused job cannot execute
    state.pause()
    assert state.can_execute() is False
    
    # Resumed job can execute
    state.resume()
    assert state.can_execute() is True
    
    # Running job cannot execute
    state.is_running = True
    assert state.can_execute() is False


@pytest.mark.asyncio
async def test_pause_resume_job(scheduler_with_jobs):
    """Test pausing and resuming a job."""
    job_id = "test_job"
    
    # Initially active
    job_state = await scheduler_with_jobs.get_job(job_id)
    assert job_state.status == "active"
    
    # Pause job
    success = await scheduler_with_jobs.pause_job(job_id)
    assert success is True
    
    job_state = await scheduler_with_jobs.get_job(job_id)
    assert job_state.status == "paused"
    
    # Resume job
    success = await scheduler_with_jobs.resume_job(job_id)
    assert success is True
    
    job_state = await scheduler_with_jobs.get_job(job_id)
    assert job_state.status == "active"


@pytest.mark.asyncio
async def test_stop_enable_job(scheduler_with_jobs):
    """Test stopping and enabling a job."""
    job_id = "test_job"
    
    # Stop job
    success = await scheduler_with_jobs.stop_job(job_id)
    assert success is True
    
    job_state = await scheduler_with_jobs.get_job(job_id)
    assert job_state.status == "stopped"
    
    # Enable job (returns to active)
    success = await scheduler_with_jobs.enable_job(job_id)
    assert success is True
    
    job_state = await scheduler_with_jobs.get_job(job_id)
    assert job_state.status == "active"


@pytest.mark.asyncio
async def test_list_jobs(scheduler_with_jobs):
    """Test listing all jobs."""
    jobs = await scheduler_with_jobs.list_jobs()
    
    assert len(jobs) == 1
    assert "test_job" in jobs
    assert jobs["test_job"].definition.function == "print_hello_world"


@pytest.mark.asyncio
async def test_get_nonexistent_job(scheduler_with_jobs):
    """Test getting a job that doesn't exist."""
    job_state = await scheduler_with_jobs.get_job("nonexistent_job")
    assert job_state is None


@pytest.mark.asyncio
async def test_control_nonexistent_job(scheduler_with_jobs):
    """Test controlling a job that doesn't exist."""
    success = await scheduler_with_jobs.pause_job("nonexistent_job")
    assert success is False
    
    success = await scheduler_with_jobs.resume_job("nonexistent_job")
    assert success is False


@pytest.mark.asyncio
async def test_job_state_transitions():
    """Test various job state transitions."""
    job = EspressoJobDefinition(
        id="test_job",
        type="espresso_job",
        module="test.module",
        function="test_func",
        schedule=EspressoSchedule(kind="interval", every_seconds=10),
    )
    
    state = EspressoJobRuntimeState(definition=job)
    
    # active -> paused
    assert state.status == "active"
    state.pause()
    assert state.status == "paused"
    
    # paused -> active
    state.resume()
    assert state.status == "active"
    
    # active -> stopped
    state.stop()
    assert state.status == "stopped"
    
    # stopped -> active (via enable)
    state.enable()
    assert state.status == "active"
    
    # active -> disabled
    state.disable()
    assert state.status == "disabled"
    
    # disabled -> active
    state.enable()
    assert state.status == "active"


@pytest.mark.asyncio
async def test_execution_metrics():
    """Test that execution metrics are tracked."""
    job = EspressoJobDefinition(
        id="test_job",
        type="espresso_job",
        module="test.module",
        function="test_func",
        schedule=EspressoSchedule(kind="interval", every_seconds=10),
    )
    
    state = EspressoJobRuntimeState(definition=job)
    
    # Initial state
    assert state.execution_count == 0
    assert state.total_execution_time == 0.0
    assert state.last_execution_duration is None
    
    # Simulate execution
    state.execution_count = 5
    state.total_execution_time = 25.5
    state.last_execution_duration = 6.2
    
    assert state.execution_count == 5
    assert state.total_execution_time == 25.5
    assert state.last_execution_duration == 6.2
    
    # Calculate average
    avg_time = state.total_execution_time / state.execution_count
    assert avg_time == pytest.approx(5.1, 0.01)
