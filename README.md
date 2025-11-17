# Espresso Job Scheduler

This project consists of asynchronous job scheduling which can be customized inside a Python script
or loaded from a YAML File.


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

## The different types of jobs and inputs currently supported

### Job Types
-   on_demand -> will act on input the moment it sees it. for queues, think of it as popping the moment it pushes
-   interval -> every x seconds, will grab a batch and act upon it
-   cron -> same as cron jobs. run the function at a specific moment (9:00AM every weekday = 0 9 * * 1-5) # minute - hour - day of the month - month - weekday

### Input Types
-   Lists (EspressoListInputDefinition)
-   RabbitMQ (EspressoRabbitMQInputDefinition)