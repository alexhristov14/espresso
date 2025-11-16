import pytest
from pathlib import Path
from datetime import datetime
import tempfile
import yaml
from scheduler.yaml_loader import load_jobs_from_yaml
from scheduler.models import (
    EspressoJobDefinition,
    EspressoInputDefinition,
    EspressoListInputDefinition,
)


@pytest.fixture
def temp_yaml_file():
    """Fixture to create a temporary YAML file for testing."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yield temp_file
    temp_file.close()
    Path(temp_file.name).unlink()


@pytest.fixture
def basic_job_yaml():
    """Fixture providing a basic job definition."""
    return {
        "jobs": [
            {
                "id": "test_job",
                "type": "espresso_job",
                "module": "testing.test",
                "function": "test_function",
                "schedule": {"kind": "cron", "cron": "0 9 * * *"},
                "max_retries": 3,
                "retry_delay_seconds": 60,
                "timeout_seconds": 300,
                "enabled": True,
            }
        ]
    }


@pytest.fixture
def job_with_inputs_yaml():
    """Fixture providing a job definition with inputs."""
    return {
        "inputs": [
            {
                "id": "list_input_1",
                "type": "list",
                "items": ["item1", "item2", "item3"],
            },
            {"id": "rabbitmq_input_1", "type": "rabbitmq"},
        ],
        "jobs": [
            {
                "id": "triggered_job",
                "type": "espresso_job",
                "module": "testing.test",
                "function": "process_data",
                "schedule": {"kind": "interval", "every_seconds": 30},
                "trigger": {"kind": "input", "input_id": "list_input_1"},
                "args": ["arg1"],
                "kwargs": {"key1": "value1"},
            }
        ],
    }


class TestLoadJobsFromYaml:
    """Test suite for load_jobs_from_yaml function."""

    def test_load_basic_cron_job(self, temp_yaml_file, basic_job_yaml):
        """Test loading a basic job with cron schedule."""
        yaml.dump(basic_job_yaml, temp_yaml_file)
        temp_yaml_file.flush()

        inputs, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(inputs) == 0
        assert len(jobs) == 1

        job = jobs[0]
        assert job.id == "test_job"
        assert job.type == "espresso_job"
        assert job.module == "testing.test"
        assert job.function == "test_function"
        assert job.schedule.kind == "cron"
        assert job.schedule.cron == "0 9 * * *"
        assert job.max_retries == 3
        assert job.retry_delay_seconds == 60
        assert job.timeout_seconds == 300
        assert job.enabled is True
        assert job.trigger is None

    def test_load_interval_schedule(self, temp_yaml_file):
        """Test loading a job with interval schedule."""
        data = {
            "jobs": [
                {
                    "id": "interval_job",
                    "type": "espresso_job",
                    "module": "test.module",
                    "function": "test_func",
                    "schedule": {"kind": "interval", "every_seconds": 60},
                }
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        _, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 1
        assert jobs[0].schedule.kind == "interval"
        assert jobs[0].schedule.every_seconds == 60
        assert jobs[0].schedule.cron is None

    def test_load_one_off_schedule(self, temp_yaml_file):
        """Test loading a job with one-off schedule."""
        run_at = "2025-12-01T15:30:00"
        data = {
            "jobs": [
                {
                    "id": "one_off_job",
                    "type": "espresso_job",
                    "module": "test.module",
                    "function": "test_func",
                    "schedule": {"kind": "one_off", "run_at": run_at},
                }
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        _, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 1
        assert jobs[0].schedule.kind == "one_off"
        assert jobs[0].schedule.run_at == datetime.fromisoformat(run_at)

    def test_load_job_with_args_and_kwargs(self, temp_yaml_file):
        """Test loading a job with arguments and keyword arguments."""
        data = {
            "jobs": [
                {
                    "id": "job_with_params",
                    "type": "espresso_job",
                    "module": "test.module",
                    "function": "test_func",
                    "schedule": {"kind": "cron", "cron": "0 * * * *"},
                    "args": ["arg1", "arg2", 123],
                    "kwargs": {"key1": "value1", "key2": 456, "key3": True},
                }
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        _, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 1
        assert jobs[0].args == ["arg1", "arg2", 123]
        assert jobs[0].kwargs == {"key1": "value1", "key2": 456, "key3": True}

    def test_load_job_with_trigger(self, temp_yaml_file, job_with_inputs_yaml):
        """Test loading a job with trigger configuration."""
        yaml.dump(job_with_inputs_yaml, temp_yaml_file)
        temp_yaml_file.flush()

        _, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 1
        job = jobs[0]
        assert job.trigger is not None
        assert job.trigger.kind == "input"
        assert job.trigger.input_id == "list_input_1"

    def test_load_inputs(self, temp_yaml_file, job_with_inputs_yaml):
        """Test loading inputs definition."""
        yaml.dump(job_with_inputs_yaml, temp_yaml_file)
        temp_yaml_file.flush()

        inputs, _ = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(inputs) == 2
        list_input = inputs[0]
        generic_input = inputs[1]
        assert isinstance(list_input, EspressoListInputDefinition)
        assert isinstance(generic_input, EspressoInputDefinition)
        assert list_input.id == "list_input_1"
        assert list_input.type == "list"
        assert list_input.items == ["item1", "item2", "item3"]

    def test_load_multiple_jobs(self, temp_yaml_file):
        """Test loading multiple job definitions."""
        data = {
            "jobs": [
                {
                    "id": "job1",
                    "type": "espresso_job",
                    "module": "test.module1",
                    "function": "func1",
                    "schedule": {"kind": "cron", "cron": "0 9 * * *"},
                },
                {
                    "id": "job2",
                    "type": "espresso_job",
                    "module": "test.module2",
                    "function": "func2",
                    "schedule": {"kind": "interval", "every_seconds": 120},
                },
                {
                    "id": "job3",
                    "type": "espresso_job",
                    "module": "test.module3",
                    "function": "func3",
                    "schedule": {"kind": "cron", "cron": "*/15 * * * *"},
                },
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        _, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 3
        assert jobs[0].id == "job1"
        assert jobs[1].id == "job2"
        assert jobs[2].id == "job3"

    def test_load_disabled_job(self, temp_yaml_file):
        """Test loading a disabled job."""
        data = {
            "jobs": [
                {
                    "id": "disabled_job",
                    "type": "espresso_job",
                    "module": "test.module",
                    "function": "test_func",
                    "schedule": {"kind": "cron", "cron": "0 * * * *"},
                    "enabled": False,
                }
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        inputs, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 1
        assert jobs[0].enabled is False

    def test_load_empty_yaml(self, temp_yaml_file):
        """Test loading YAML with no jobs or inputs."""
        data = {}
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        inputs, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(inputs) == 0
        assert len(jobs) == 0

    def test_load_with_path_object(self, temp_yaml_file, basic_job_yaml):
        """Test that function accepts Path objects."""
        yaml.dump(basic_job_yaml, temp_yaml_file)
        temp_yaml_file.flush()

        path_obj = Path(temp_yaml_file.name)
        _, jobs = load_jobs_from_yaml(path_obj)

        assert len(jobs) == 1
        assert jobs[0].id == "test_job"

    @pytest.mark.parametrize(
        "schedule_kind,schedule_params",
        [
            ("cron", {"cron": "0 0 * * *"}),
            ("interval", {"every_seconds": 300}),
            ("one_off", {"run_at": "2025-12-25T00:00:00"}),
        ],
    )
    def test_schedule_types_parametrized(
        self, temp_yaml_file, schedule_kind, schedule_params
    ):
        """Parametrized test for different schedule types."""
        data = {
            "jobs": [
                {
                    "id": f"{schedule_kind}_job",
                    "type": "espresso_job",
                    "module": "test.module",
                    "function": "test_func",
                    "schedule": {"kind": schedule_kind, **schedule_params},
                }
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        _, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        assert len(jobs) == 1
        assert jobs[0].schedule.kind == schedule_kind

    def test_default_values(self, temp_yaml_file):
        """Test that default values are applied correctly."""
        data = {
            "jobs": [
                {
                    "id": "minimal_job",
                    "type": "espresso_job",
                    "module": "test.module",
                    "function": "test_func",
                    "schedule": {"kind": "cron", "cron": "0 * * * *"},
                }
            ]
        }
        yaml.dump(data, temp_yaml_file)
        temp_yaml_file.flush()

        inputs, jobs = load_jobs_from_yaml(temp_yaml_file.name)

        job = jobs[0]
        assert job.args == []
        assert job.kwargs == {}
        assert job.max_retries == 3
        assert job.retry_delay_seconds == 60
        assert job.timeout_seconds == 300
        assert job.enabled is True
        assert job.trigger is None


class TestIntegrationWithRealFiles:
    """Integration tests using actual job definition files."""

    def test_load_real_jobs_yaml(self):
        """Test loading the actual jobs.yaml file from the project."""
        jobs_file = Path("/Users/alex.hristov/Dev/espresso/jobs_definitions/jobs.yaml")

        if jobs_file.exists():
            inputs, jobs = load_jobs_from_yaml(jobs_file)

            # Basic assertions about the structure
            assert isinstance(inputs, list)
            assert isinstance(jobs, list)

            # Each job should be properly formed
            for job in jobs:
                assert isinstance(job, EspressoJobDefinition)
                assert job.id is not None
                assert job.module is not None
                assert job.function is not None
                assert job.schedule is not None
