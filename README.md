# Espresso Job Scheduler

This project consists of asynchronous job scheduling which can be customized inside a Python script
or loaded from a YAML File.

## Features

- **Flexible Scheduling**: Support for cron, interval, one-off, and on-demand job execution
- **Multiple Input Sources**: List-based and RabbitMQ inputs (extensible)
- **Runtime Control**: REST API for managing jobs at runtime (pause, resume, trigger, etc.)
- **Execution Tracking**: Built-in metrics for job execution statistics
- **Async-First**: Built on asyncio for high-performance concurrent execution
- **Retry Logic**: Automatic retry with configurable delays
- **Worker Pool**: Configurable concurrent worker execution
- **🌐 Distributed Mode**: Run on multiple servers with Redis coordination (no duplicate jobs!)

## Quick Start

Install dependencies:
```bash
pip install -r requirements.txt
```

Run the scheduler with API:
```bash
python server.py
```

The scheduler will start along with a REST API on http://localhost:8000. 
Visit http://localhost:8000/docs for interactive API documentation.

## Basic Usage

For Example:

Creating a YAML configuration containing a simple RabbitMQ queue with a job definition to act on it will look
as follows

```yaml
# jobs_definitions/rabbit_mq_jobs.yaml
inputs:
    -   id: rabbit_orders
        type: rabbitmq
        url: amqp://guest:guest@rabbitmq:5672/
        queue: orders_queue
        prefetch_count: 10

jobs:
    -   id: process_orders_job
        type: espresso_job
        module: testing.test
        function: process_order
        trigger:
            kind: input
            input_id: rabbit_orders
        schedule:
            kind: on_demand
```

And then load and run the jobs.

```python 
# main.py
async def main():
    inputs, jobs = yaml_loader.load_jobs_from_yaml(
        "jobs_definitions/rabbit_mq_jobs.yaml"
    )

    sched = EspressoScheduler(jobs, inputs)
    await sched.run_forever()
```

The above could basically be written as follows (if we decide to manually write the inputs in python)

```python 
# main.py
async def main():
    inputs = EspressoRabbitMQInputDefinition(
        id="rabbit_orders",
        type="rabbitmq",
        url="amqp://guest:guest@rabbitmq:5672/",
        prefetch_count=10
    )

    jobs = EspressoJobDefinition(
        id="process_orders_job",
        type="espresso_job",
        module="testing.test",
        function="process_order",
        trigger=EspressoTrigger(
            kind="input",
            input_id="rabbit_orders" # note: this has to be equal to the espresso input's id or else it won't be matched
        ),
        schedule="on_demand"
    )

    sched = EspressoScheduler(jobs, inputs)
    await sched.run_forever()
```

## 🌐 Distributed Mode (NEW!)

**Run Espresso on multiple servers with automatic load distribution!**

Enable distributed coordination with just one parameter:

```python
sched = EspressoScheduler(
    jobs, 
    inputs, 
    redis_url="redis://localhost:6379"  # 👈 Add this!
)
```

Now you can run the same scheduler on multiple servers:
- ✅ Jobs execute exactly once (no duplicates)
- ✅ Automatic failover if a server crashes
- ✅ Load distributes across all servers
- ✅ Shared state via Redis

**📖 Full guide:** [DISTRIBUTED_SETUP.md](DISTRIBUTED_SETUP.md)

**🧪 Quick test:**
```bash
python examples/test_distributed.py
```

---

## Runtime Control

The scheduler includes a REST API for runtime job management. See [RUNTIME_CONTROL.md](RUNTIME_CONTROL.md) for detailed documentation.

### Available API Endpoints

- `GET /jobs` - List all jobs with status and metrics
- `GET /jobs/{job_id}` - Get specific job details
- `POST /jobs/{job_id}/pause` - Pause a job
- `POST /jobs/{job_id}/resume` - Resume a paused job
- `POST /jobs/{job_id}/stop` - Stop a job
- `POST /jobs/{job_id}/enable` - Enable a disabled job
- `POST /jobs/{job_id}/trigger` - Manually trigger job execution
- `GET /health` - Scheduler health check

### Example: Control Jobs via API

```python
import requests

# Pause a job
requests.post("http://localhost:8000/jobs/process_orders_job/pause")

# Manually trigger a job
requests.post("http://localhost:8000/jobs/process_orders_job/trigger")

# Check job status
response = requests.get("http://localhost:8000/jobs/process_orders_job")
job = response.json()
print(f"Executions: {job['execution_count']}, Status: {job['status']}")
```

Run the demo:
```bash
python examples/runtime_control_demo.py
```

## The different types of jobs and inputs currently supported

### Job Types
-   **on_demand** - Will act on input the moment it sees it. For queues, think of it as popping the moment it pushes
-   **interval** - Every x seconds, will grab a batch and act upon it
-   **cron** - Same as cron jobs. Run the function at a specific moment (9:00AM every weekday = 0 9 * * 1-5)
    - Format: minute - hour - day of the month - month - weekday
-   **one_off** - Run once at a specific datetime

### Input Types
-   **Lists** (EspressoListInputDefinition) - Process items from a static list
-   **RabbitMQ** (EspressoRabbitMQInputDefinition) - Consume messages from RabbitMQ queues

### Job States
-   **active** - Job is running normally according to schedule
-   **paused** - Job is temporarily paused (can be resumed)
-   **stopped** - Job is stopped
-   **disabled** - Job is disabled (typically after exceeding max retries)