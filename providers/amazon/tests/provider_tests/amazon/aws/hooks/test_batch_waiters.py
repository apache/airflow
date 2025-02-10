#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import inspect
import itertools
import time
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError, WaiterError
from botocore.waiter import SingleWaiterConfig, WaiterModel
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_waiters import BatchWaitersHook
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher

INTERMEDIATE_STATES = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING")
RUNNING_STATE = "RUNNING"
SUCCESS_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"
ALL_STATES = {*INTERMEDIATE_STATES, RUNNING_STATE, SUCCESS_STATE, FAILED_STATE}
AWS_REGION = "eu-west-1"


@pytest.fixture(scope="module")
def aws_region():
    return AWS_REGION


@mock_aws
@pytest.fixture
def patch_hook(monkeypatch, aws_region):
    """Patch hook object by dummy boto3 Batch client."""
    batch_client = boto3.client("batch", region_name=aws_region)
    monkeypatch.setattr(BatchWaitersHook, "conn", batch_client)


def test_batch_waiters(aws_region):
    assert inspect.isclass(BatchWaitersHook)
    batch_waiters = BatchWaitersHook(region_name=aws_region)
    assert isinstance(batch_waiters, BatchWaitersHook)


class TestBatchWaiters:
    @pytest.fixture(autouse=True)
    def setup_tests(self, patch_hook):
        self.job_id = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
        self.region_name = AWS_REGION

        self.batch_waiters = BatchWaitersHook(region_name=self.region_name)
        assert self.batch_waiters.aws_conn_id == "aws_default"
        assert self.batch_waiters.region_name == self.region_name

        # don't pause in these unit tests
        self.mock_delay = mock.Mock(return_value=None)
        self.batch_waiters.delay = self.mock_delay
        self.mock_exponential_delay = mock.Mock(return_value=0)
        self.batch_waiters.exponential_delay = self.mock_exponential_delay

    def test_default_config(self):
        # the default config is used when no custom config is provided
        config = self.batch_waiters.default_config
        assert config == self.batch_waiters.waiter_config

        assert isinstance(config, dict)
        assert config["version"] == 2
        assert isinstance(config["waiters"], dict)

        waiters = list(sorted(config["waiters"].keys()))
        assert waiters == ["JobComplete", "JobExists", "JobRunning"]

    def test_list_waiters(self):
        # the default config is used when no custom config is provided
        config = self.batch_waiters.waiter_config

        assert isinstance(config["waiters"], dict)
        waiters = list(sorted(config["waiters"].keys()))
        assert waiters == ["JobComplete", "JobExists", "JobRunning"]
        assert waiters == self.batch_waiters.list_waiters()

    def test_waiter_model(self):
        model = self.batch_waiters.waiter_model
        assert isinstance(model, WaiterModel)

        # test some of the default config
        assert model.version == 2
        waiters = sorted(model.waiter_names)
        assert waiters == ["JobComplete", "JobExists", "JobRunning"]

        # test errors when requesting a waiter with the wrong name
        with pytest.raises(ValueError) as ctx:
            model.get_waiter("JobExist")
        assert "Waiter does not exist: JobExist" in str(ctx.value)

        # test some default waiter properties
        waiter = model.get_waiter("JobExists")
        assert isinstance(waiter, SingleWaiterConfig)
        assert waiter.max_attempts == 100
        waiter.max_attempts = 200
        assert waiter.max_attempts == 200
        assert waiter.delay == 2
        waiter.delay = 10
        assert waiter.delay == 10
        assert waiter.operation == "DescribeJobs"

    def test_wait_for_job(self):
        import sys

        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        with mock.patch.object(self.batch_waiters, "get_waiter") as get_waiter:
            self.batch_waiters.wait_for_job(self.job_id)

            assert get_waiter.call_args_list == [
                mock.call("JobExists"),
                mock.call("JobRunning"),
                mock.call("JobComplete"),
            ]

            mock_waiter = get_waiter.return_value
            mock_waiter.wait.assert_called_with(jobs=[self.job_id])
            assert mock_waiter.wait.call_count == 3

            mock_config = mock_waiter.config
            assert mock_config.delay == 0
            assert mock_config.max_attempts == sys.maxsize

    def test_wait_for_job_with_cloudwatch_logs(self):
        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        batch_log_fetcher = mock.Mock(spec=AwsTaskLogFetcher)
        mock_get_batch_log_fetcher = mock.Mock(return_value=batch_log_fetcher)

        thread_start = mock.Mock(side_effect=lambda: time.sleep(2))
        thread_stop = mock.Mock(side_effect=lambda: time.sleep(2))
        thread_join = mock.Mock(side_effect=lambda: time.sleep(2))

        with (
            mock.patch.object(self.batch_waiters, "get_waiter") as mock_get_waiter,
            mock.patch.object(batch_log_fetcher, "start", thread_start) as mock_fetcher_start,
            mock.patch.object(batch_log_fetcher, "stop", thread_stop) as mock_fetcher_stop,
            mock.patch.object(batch_log_fetcher, "join", thread_join) as mock_fetcher_join,
        ):
            # Run the wait_for_job method
            self.batch_waiters.wait_for_job(self.job_id, get_batch_log_fetcher=mock_get_batch_log_fetcher)

            # Assertions
            assert mock_get_waiter.call_args_list == [
                mock.call("JobExists"),
                mock.call("JobRunning"),
                mock.call("JobComplete"),
            ]

            mock_get_waiter.return_value.wait.assert_called_with(jobs=[self.job_id])
            mock_get_batch_log_fetcher.assert_called_with(self.job_id)
            mock_fetcher_start.assert_called_once()
            mock_fetcher_stop.assert_called_once()
            mock_fetcher_join.assert_called_once()

    def test_wait_for_job_raises_for_client_error(self):
        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        with mock.patch.object(self.batch_waiters, "get_waiter") as get_waiter:
            mock_waiter = get_waiter.return_value
            mock_waiter.wait.side_effect = ClientError(
                error_response={"Error": {"Code": "TooManyRequestsException"}},
                operation_name="get job description",
            )
            with pytest.raises(AirflowException):
                self.batch_waiters.wait_for_job(self.job_id)

            assert get_waiter.call_args_list == [mock.call("JobExists")]
            mock_waiter.wait.assert_called_with(jobs=[self.job_id])
            assert mock_waiter.wait.call_count == 1

    def test_wait_for_job_raises_for_waiter_error(self):
        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        with mock.patch.object(self.batch_waiters, "get_waiter") as get_waiter:
            mock_waiter = get_waiter.return_value
            mock_waiter.wait.side_effect = WaiterError(
                name="JobExists", reason="unit test error", last_response={}
            )
            with pytest.raises(AirflowException):
                self.batch_waiters.wait_for_job(self.job_id)

            assert get_waiter.call_args_list == [mock.call("JobExists")]
            mock_waiter.wait.assert_called_with(jobs=[self.job_id])
            assert mock_waiter.wait.call_count == 1


class TestBatchJobWaiters:
    """Test default waiters."""

    @pytest.fixture(autouse=True)
    def setup_tests(self, patch_hook):
        """Mock `describe_jobs` method before each test run."""
        self.batch_waiters = BatchWaitersHook(region_name=AWS_REGION)
        self.client = self.batch_waiters.client

        with mock.patch.object(self.client, "describe_jobs") as m:
            self.mock_describe_jobs = m
            yield

    @staticmethod
    def describe_jobs_response(job_id: str = "mock-job-id", status: str = INTERMEDIATE_STATES[0]):
        """
        Helper function for generate minimal DescribeJobs response for single job.
        https://docs.aws.amazon.com/batch/latest/APIReference/API_DescribeJobs.html
        """
        assert job_id
        assert status in ALL_STATES

        return {"jobs": [{"jobId": job_id, "status": status}]}

    @pytest.mark.parametrize("status", sorted(ALL_STATES))
    def test_job_exists_waiter_exists(self, status: str):
        """Test `JobExists` when response return dictionary regardless state."""
        self.mock_describe_jobs.return_value = self.describe_jobs_response(
            job_id="job-exist-success", status=status
        )
        job_exists_waiter = self.batch_waiters.get_waiter("JobExists")
        job_exists_waiter.config.delay = 0.01
        job_exists_waiter.config.max_attempts = 5
        job_exists_waiter.wait(jobs=["job-exist-success"])
        assert self.mock_describe_jobs.called

    def test_job_exists_waiter_missing(self):
        """Test `JobExists` waiter when response return empty dictionary."""
        self.mock_describe_jobs.return_value = {"jobs": []}

        job_exists_waiter = self.batch_waiters.get_waiter("JobExists")
        job_exists_waiter.config.delay = 0.01
        job_exists_waiter.config.max_attempts = 20
        with pytest.raises(WaiterError, match="Waiter JobExists failed"):
            job_exists_waiter.wait(jobs=["job-missing"])
        assert self.mock_describe_jobs.called

    @pytest.mark.parametrize("status", [RUNNING_STATE, SUCCESS_STATE, FAILED_STATE])
    def test_job_running_waiter_change_to_waited_state(self, status):
        """Test `JobRunning` waiter reach expected state."""
        job_id = "job-running"
        self.mock_describe_jobs.side_effect = [
            # Emulate change job status before one of expected states.
            # SUBMITTED -> PENDING -> RUNNABLE -> STARTING
            *itertools.chain.from_iterable(
                itertools.repeat(self.describe_jobs_response(job_id=job_id, status=inter_status), 3)
                for inter_status in INTERMEDIATE_STATES
            ),
            # Expected status
            self.describe_jobs_response(job_id=job_id, status=status),
            RuntimeError("This should not raise"),
        ]

        job_running_waiter = self.batch_waiters.get_waiter("JobRunning")
        job_running_waiter.config.delay = 0.01
        job_running_waiter.config.max_attempts = 20
        job_running_waiter.wait(jobs=[job_id])
        assert self.mock_describe_jobs.called

    @pytest.mark.parametrize("status", INTERMEDIATE_STATES)
    def test_job_running_waiter_max_attempt_exceeded(self, status):
        """Test `JobRunning` waiter run out of attempts."""
        job_id = "job-running-inf"
        self.mock_describe_jobs.side_effect = itertools.repeat(
            self.describe_jobs_response(job_id=job_id, status=status)
        )
        job_running_waiter = self.batch_waiters.get_waiter("JobRunning")
        job_running_waiter.config.delay = 0.01
        job_running_waiter.config.max_attempts = 20
        with pytest.raises(WaiterError, match="Waiter JobRunning failed: Max attempts exceeded"):
            job_running_waiter.wait(jobs=[job_id])
        assert self.mock_describe_jobs.called

    def test_job_complete_waiter_succeeded(self):
        """Test `JobComplete` waiter reach `SUCCEEDED` status."""
        job_id = "job-succeeded"
        self.mock_describe_jobs.side_effect = [
            *itertools.repeat(self.describe_jobs_response(job_id=job_id, status=RUNNING_STATE), 10),
            self.describe_jobs_response(job_id=job_id, status=SUCCESS_STATE),
            RuntimeError("This should not raise"),
        ]

        job_complete_waiter = self.batch_waiters.get_waiter("JobComplete")
        job_complete_waiter.config.delay = 0.01
        job_complete_waiter.config.max_attempts = 20
        job_complete_waiter.wait(jobs=[job_id])
        assert self.mock_describe_jobs.called

    def test_job_complete_waiter_failed(self):
        """Test `JobComplete` waiter reach `FAILED` status."""
        job_id = "job-failed"
        self.mock_describe_jobs.side_effect = [
            *itertools.repeat(self.describe_jobs_response(job_id=job_id, status=RUNNING_STATE), 10),
            self.describe_jobs_response(job_id=job_id, status=FAILED_STATE),
            RuntimeError("This should not raise"),
        ]

        job_complete_waiter = self.batch_waiters.get_waiter("JobComplete")
        job_complete_waiter.config.delay = 0.01
        job_complete_waiter.config.max_attempts = 20
        with pytest.raises(
            WaiterError, match="Waiter JobComplete failed: Waiter encountered a terminal failure state"
        ):
            job_complete_waiter.wait(jobs=[job_id])
        assert self.mock_describe_jobs.called

    def test_job_complete_waiter_max_attempt_exceeded(self):
        """Test `JobComplete` waiter run out of attempts."""
        job_id = "job-running-inf"
        self.mock_describe_jobs.side_effect = itertools.repeat(
            self.describe_jobs_response(job_id=job_id, status=RUNNING_STATE)
        )
        job_running_waiter = self.batch_waiters.get_waiter("JobComplete")
        job_running_waiter.config.delay = 0.01
        job_running_waiter.config.max_attempts = 20
        with pytest.raises(WaiterError, match="Waiter JobComplete failed: Max attempts exceeded"):
            job_running_waiter.wait(jobs=[job_id])
        assert self.mock_describe_jobs.called
