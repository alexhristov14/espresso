"""
Example script demonstrating runtime control of Espresso Job Scheduler.

This script shows how to interact with the scheduler API to control jobs.
Make sure the scheduler is running (python server.py) before running this script.
"""
import time
import requests
from typing import Dict, Any

BASE_URL = "http://localhost:8000"


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def check_health() -> Dict[str, Any]:
    """Check scheduler health."""
    response = requests.get(f"{BASE_URL}/health")
    health = response.json()
    print(f"Status: {health['status']}")
    print(f"Scheduler Running: {health['scheduler_running']}")
    print(f"Total Jobs: {health['total_jobs']}")
    print(f"Active Jobs: {health['active_jobs']}")
    print(f"Paused Jobs: {health['paused_jobs']}")
    print(f"Currently Running: {health['running_jobs']}")
    return health


def list_jobs() -> Dict[str, Any]:
    """List all jobs."""
    response = requests.get(f"{BASE_URL}/jobs")
    data = response.json()
    print(f"Total jobs: {data['total']}\n")
    
    for job_id, job in data['jobs'].items():
        print(f"Job ID: {job_id}")
        print(f"  Status: {job['status']}")
        print(f"  Function: {job['module']}.{job['function']}")
        print(f"  Executions: {job['execution_count']}")
        print(f"  Is Running: {job['is_running']}")
        if job['last_execution_duration']:
            print(f"  Last Duration: {job['last_execution_duration']:.2f}s")
        if job['execution_count'] > 0:
            avg_time = job['total_execution_time'] / job['execution_count']
            print(f"  Avg Duration: {avg_time:.2f}s")
        print()
    
    return data


def get_job_details(job_id: str) -> Dict[str, Any]:
    """Get details for a specific job."""
    response = requests.get(f"{BASE_URL}/jobs/{job_id}")
    if response.status_code == 404:
        print(f"Job '{job_id}' not found!")
        return None
    
    job = response.json()
    print(f"Job: {job['id']}")
    print(f"  Module: {job['module']}")
    print(f"  Function: {job['function']}")
    print(f"  Status: {job['status']}")
    print(f"  Schedule: {job['schedule_kind']}")
    print(f"  Enabled: {job['enabled']}")
    print(f"  Execution Count: {job['execution_count']}")
    print(f"  Retries: {job['retries_attempted']}")
    if job['last_run_time']:
        print(f"  Last Run: {job['last_run_time']}")
    if job['next_run_time']:
        print(f"  Next Run: {job['next_run_time']}")
    if job['last_error']:
        print(f"  Last Error: {job['last_error']}")
    
    return job


def pause_job(job_id: str) -> bool:
    """Pause a job."""
    response = requests.post(f"{BASE_URL}/jobs/{job_id}/pause")
    result = response.json()
    print(result['message'])
    return result['success']


def resume_job(job_id: str) -> bool:
    """Resume a paused job."""
    response = requests.post(f"{BASE_URL}/jobs/{job_id}/resume")
    result = response.json()
    print(result['message'])
    return result['success']


def trigger_job(job_id: str) -> bool:
    """Manually trigger a job."""
    response = requests.post(f"{BASE_URL}/jobs/{job_id}/trigger")
    if response.status_code != 200:
        print(f"Failed to trigger job: {response.json()}")
        return False
    result = response.json()
    print(result['message'])
    return result['success']


def stop_job(job_id: str) -> bool:
    """Stop a job."""
    response = requests.post(f"{BASE_URL}/jobs/{job_id}/stop")
    result = response.json()
    print(result['message'])
    return result['success']


def enable_job(job_id: str) -> bool:
    """Enable a disabled job."""
    response = requests.post(f"{BASE_URL}/jobs/{job_id}/enable")
    result = response.json()
    print(result['message'])
    return result['success']


def demo_runtime_control():
    """Demonstrate various runtime control operations."""
    
    print_section("Scheduler Health Check")
    try:
        check_health()
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to scheduler API!")
        print("Make sure the scheduler is running: python server.py")
        return
    
    print_section("List All Jobs")
    jobs_data = list_jobs()
    
    if jobs_data['total'] == 0:
        print("No jobs found. Make sure you have jobs defined in your YAML file.")
        return
    
    # Get the first job ID for demonstration
    job_id = list(jobs_data['jobs'].keys())[0]
    
    print_section(f"Job Details: {job_id}")
    get_job_details(job_id)
    
    print_section("Pause Job")
    pause_job(job_id)
    time.sleep(1)
    
    print("\nJob status after pausing:")
    job = get_job_details(job_id)
    
    print_section("Resume Job")
    resume_job(job_id)
    time.sleep(1)
    
    print("\nJob status after resuming:")
    get_job_details(job_id)
    
    print_section("Manually Trigger Job")
    if trigger_job(job_id):
        print("\nWaiting for execution to complete...")
        time.sleep(3)
        print("\nJob status after manual trigger:")
        get_job_details(job_id)
    
    print_section("Final Health Check")
    check_health()
    
    print("\n" + "=" * 60)
    print("Demo completed!")
    print("Visit http://localhost:8000/docs for interactive API documentation")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    demo_runtime_control()
