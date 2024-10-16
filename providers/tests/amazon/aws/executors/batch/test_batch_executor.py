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

import datetime as dt
import json
import logging
import os
from unittest import mock

import pytest
import yaml
from botocore.exceptions import ClientError, NoCredentialsError
from semver import VersionInfo

from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.batch import batch_executor, batch_executor_config
from airflow.providers.amazon.aws.executors.batch.batch_executor import (
    AwsBatchExecutor,
    BatchJob,
    BatchJobCollection,
)
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
)
from airflow.utils.helpers import convert_camel_to_snake
from airflow.utils.state import State
from airflow.version import version as airflow_version_str

from tests_common import RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES
from tests_common.test_utils.config import conf_vars

airflow_version = VersionInfo(*map(int, airflow_version_str.split(".")[:3]))
ARN1 = "arn1"

MOCK_JOB_ID = "batch-job-id"


@pytest.fixture
def set_env_vars():
    overrides: dict[tuple[str, str], str] = {
        (CONFIG_GROUP_NAME, AllBatchConfigKeys.REGION_NAME): "us-east-1",
        (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "some-job-name",
        (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "some-job-queue",
        (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "some-job-def",
        (CONFIG_GROUP_NAME, AllBatchConfigKeys.MAX_SUBMIT_JOB_ATTEMPTS): "3",
        (CONFIG_GROUP_NAME, AllBatchConfigKeys.CHECK_HEALTH_ON_STARTUP): "True",
    }
    with conf_vars(overrides):
        yield


@pytest.fixture
def mock_executor(set_env_vars) -> AwsBatchExecutor:
    """Mock Batch Executor to a repeatable starting state."""
    executor = AwsBatchExecutor()
    executor.IS_BOTO_CONNECTION_HEALTHY = True

    # Replace boto3 Batch client with mock.
    batch_mock = mock.Mock(spec=executor.batch)
    submit_job_ret_val = {"tasks": [{"taskArn": ARN1}], "failures": []}
    batch_mock.submit_job.return_value = submit_job_ret_val
    executor.batch = batch_mock

    return executor


@pytest.fixture(autouse=True)
def mock_airflow_key():
    return mock.Mock(spec=list)


@pytest.fixture(autouse=True)
def mock_cmd():
    return mock.Mock(spec=list)


class TestBatchJobCollection:
    """Tests BatchJobCollection Class"""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """
        Create a BatchJobCollection object and add 2 airflow tasks. Populates self.collection,
        self.first/second_task, self.first/second_airflow_key, and self.first/second_airflow_cmd.
        """
        self.collection = BatchJobCollection()
        # Add first task
        self.first_job_id = "001"
        self.first_airflow_key = mock.Mock(spec=tuple)
        self.collection.add_job(
            job_id=self.first_job_id,
            airflow_task_key=self.first_airflow_key,
            airflow_cmd="command1",
            queue="queue1",
            exec_config={},
            attempt_number=1,
        )
        # Add second task
        self.second_job_id = "002"
        self.second_airflow_key = mock.Mock(spec=tuple)
        self.collection.add_job(
            job_id=self.second_job_id,
            airflow_task_key=self.second_airflow_key,
            airflow_cmd="command2",
            queue="queue2",
            exec_config={},
            attempt_number=1,
        )

    def test_get_and_add(self):
        """Test add_task, task_by_arn, cmd_by_key"""
        assert len(self.collection) == 2

    def test_list(self):
        """Test get_all_arns() and get_all_task_keys()"""
        # Check basic list by ARNs & airflow-task-keys
        assert self.collection.get_all_jobs() == [self.first_job_id, self.second_job_id]

    def test_pop(self):
        """Test pop_by_key()"""
        # pop first task & ensure that it's removed
        self.collection.pop_by_id(self.first_job_id)
        assert len(self.collection) == 1
        assert self.collection.get_all_jobs() == [self.second_job_id]


class TestBatchJob:
    """Tests the BatchJob DTO"""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.all_statuses = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED"]
        self.running = "RUNNING"
        self.success = "SUCCEEDED"
        self.failed = "FAILED"

    def test_queued_jobs(self):
        """Jobs that are pending launch identified as 'queued'"""
        for status in self.all_statuses:
            if status not in (self.success, self.failed, self.running):
                job = BatchJob("id", status)
                job_state = job.get_job_state()
                assert job_state not in (State.RUNNING, State.FAILED, State.SUCCESS)
                assert job_state == State.QUEUED

    def test_running_jobs(self):
        """Jobs that have been launched are identified as 'running'"""
        assert self.running in self.all_statuses
        running_job = BatchJob("AAA", self.running)
        assert running_job.get_job_state() == State.RUNNING

    def test_success_jobs(self):
        """Jobs that have been launched are identified as 'SUCCEEDED'"""
        assert self.success in self.all_statuses

        success_job = BatchJob("BBB", self.success)
        assert success_job.get_job_state() == State.SUCCESS

    def test_failed_jobs(self):
        """Jobs that have been launched are identified as 'FAILED'"""
        assert self.failed in self.all_statuses
        running_job = BatchJob("CCC", self.failed)
        assert running_job.get_job_state() == State.FAILED


class TestAwsBatchExecutor:
    """Tests the AWS Batch Executor itself"""

    def test_execute(self, mock_executor):
        """Test execution from end-to-end"""
        airflow_key = mock.Mock(spec=tuple)
        airflow_cmd = mock.Mock(spec=list)

        mock_executor.batch.submit_job.return_value = {"jobId": MOCK_JOB_ID, "jobName": "some-job-name"}

        mock_executor.execute_async(airflow_key, airflow_cmd)
        assert len(mock_executor.pending_jobs) == 1
        mock_executor.attempt_submit_jobs()
        mock_executor.batch.submit_job.assert_called_once()
        assert len(mock_executor.active_workers) == 1

    @mock.patch.object(batch_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_attempt_all_jobs_when_some_jobs_fail(self, _, mock_executor):
        """
        Test how jobs are tried when one job fails, but others pass.

        The expected behaviour is that in one sync() iteration, all the jobs are attempted
        exactly once. Successful jobs are removed from pending_jobs to active_workers, and
        failed jobs are added back to the pending_jobs queue to be run in the next iteration.
        """
        airflow_key = TaskInstanceKey("a", "b", "c", 1, -1)
        airflow_cmd1 = mock.Mock(spec=list)
        airflow_cmd2 = mock.Mock(spec=list)
        airflow_commands = [airflow_cmd1, airflow_cmd2]
        responses = [Exception("Failure 1"), {"jobId": "job-2"}]

        submit_job_args = {
            "jobDefinition": "some-job-def",
            "jobName": "some-job-name",
            "jobQueue": "some-job-queue",
            "containerOverrides": {
                "command": ["command"],
                "environment": [{"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}],
            },
        }
        mock_executor.execute_async(airflow_key, airflow_cmd1)
        mock_executor.execute_async(airflow_key, airflow_cmd2)
        assert len(mock_executor.pending_jobs) == 2

        mock_executor.batch.submit_job.side_effect = responses
        mock_executor.attempt_submit_jobs()

        for i in range(2):
            submit_job_args["containerOverrides"]["command"] = airflow_commands[i]
            assert mock_executor.batch.submit_job.call_args_list[i].kwargs == submit_job_args
        assert len(mock_executor.pending_jobs) == 1
        mock_executor.pending_jobs[0].command == airflow_cmd1
        assert len(mock_executor.active_workers.get_all_jobs()) == 1

        # Add more tasks to pending_jobs. This simulates tasks being scheduled by Airflow
        airflow_cmd3 = mock.Mock(spec=list)
        airflow_cmd4 = mock.Mock(spec=list)
        airflow_commands.extend([airflow_cmd1, airflow_cmd3, airflow_cmd4])
        responses.extend([Exception("Failure 1"), {"jobId": "job-3"}, {"jobId": "job-4"}])
        mock_executor.execute_async(airflow_key, airflow_cmd3)
        mock_executor.execute_async(airflow_key, airflow_cmd4)
        assert len(mock_executor.pending_jobs) == 3

        mock_executor.attempt_submit_jobs()
        assert len(mock_executor.pending_jobs) == 1
        assert len(mock_executor.active_workers.get_all_jobs()) == 3

        for i in range(2, 5):
            submit_job_args["containerOverrides"]["command"] = airflow_commands[i]
            assert mock_executor.batch.submit_job.call_args_list[i].kwargs == submit_job_args
        assert len(mock_executor.pending_jobs) == 1
        mock_executor.pending_jobs[0].command == airflow_cmd1
        assert len(mock_executor.active_workers.get_all_jobs()) == 3

        airflow_commands.append(airflow_cmd1)
        responses.append(Exception("Failure 1"))

        mock_executor.attempt_submit_jobs()
        submit_job_args["containerOverrides"]["command"] = airflow_commands[0]
        assert mock_executor.batch.submit_job.call_args_list[5].kwargs == submit_job_args
        if airflow_version >= (2, 10, 0):
            log_record = mock_executor._task_event_logs[0]
            assert log_record.event == "batch job submit failure"

    @mock.patch.object(batch_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_attempt_all_jobs_when_jobs_fail(self, _, mock_executor):
        """
        Test job retry behaviour when jobs fail validation.

        Test that when a job fails with a client sided exception, all the jobs are
        attempted once. If all jobs fail, then the length of pending tasks should not change,
        until all the tasks have been attempted the maximum number of times.
        """
        airflow_key = TaskInstanceKey("a", "b", "c", 1, -1)
        airflow_cmd1 = mock.Mock(spec=list)
        airflow_cmd2 = mock.Mock(spec=list)
        commands = [airflow_cmd1, airflow_cmd2]
        failures = [Exception("Failure 1"), Exception("Failure 2")]
        submit_job_args = {
            "containerOverrides": {
                "command": ["command"],
                "environment": [{"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}],
            },
            "jobDefinition": "some-job-def",
            "jobName": "some-job-name",
            "jobQueue": "some-job-queue",
        }
        mock_executor.execute_async(airflow_key, airflow_cmd1)
        mock_executor.execute_async(airflow_key, airflow_cmd2)
        assert len(mock_executor.pending_jobs) == 2

        mock_executor.batch.submit_job.side_effect = failures
        mock_executor.attempt_submit_jobs()

        for i in range(2):
            submit_job_args["containerOverrides"]["command"] = commands[i]
            assert mock_executor.batch.submit_job.call_args_list[i].kwargs == submit_job_args
        assert len(mock_executor.pending_jobs) == 2

        mock_executor.batch.submit_job.side_effect = failures
        mock_executor.attempt_submit_jobs()
        for i in range(2):
            submit_job_args["containerOverrides"]["command"] = commands[i]
            assert mock_executor.batch.submit_job.call_args_list[i].kwargs == submit_job_args
        assert len(mock_executor.pending_jobs) == 2

        mock_executor.batch.submit_job.side_effect = failures
        mock_executor.attempt_submit_jobs()
        if VersionInfo.parse(str(airflow_version)) >= (2, 10, 0):
            events = [(x.event, x.task_id, x.try_number) for x in mock_executor._task_event_logs]
            assert events == [("batch job submit failure", "b", 1)] * 2

    def test_attempt_submit_jobs_failure(self, mock_executor):
        mock_executor.batch.submit_job.side_effect = NoCredentialsError()
        mock_executor.execute_async("airflow_key", "airflow_cmd")
        assert len(mock_executor.pending_jobs) == 1
        with pytest.raises(NoCredentialsError, match="Unable to locate credentials"):
            mock_executor.attempt_submit_jobs()
        assert len(mock_executor.active_workers.get_all_jobs()) == 0
        assert len(mock_executor.pending_jobs) == 1
        mock_resp = {
            "Error": {
                "Code": "ExpiredTokenException",
                "Message": "Expired token error",
            },
        }
        mock_executor.batch.submit_job.side_effect = ClientError(mock_resp, "test_submit_jobs")
        with pytest.raises(ClientError, match="Expired token error"):
            mock_executor.attempt_submit_jobs()
        assert len(mock_executor.active_workers.get_all_jobs()) == 0
        assert len(mock_executor.pending_jobs) == 1

    @mock.patch.object(batch_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_task_retry_on_api_failure(self, _, mock_executor, caplog):
        """Test API failure retries"""
        airflow_keys = ["TaskInstanceKey1", "TaskInstanceKey2"]
        airflow_cmds = [mock.Mock(spec=list), mock.Mock(spec=list)]

        mock_executor.execute_async(airflow_keys[0], airflow_cmds[0])
        mock_executor.execute_async(airflow_keys[1], airflow_cmds[1])
        assert len(mock_executor.pending_jobs) == 2
        jobs = [
            {
                "jobName": "job-1",
                "jobId": "job-1",
                "status": "FAILED",
                "statusReason": "Test Failure for job1",
            },
            {
                "jobName": "job-2",
                "jobId": "job-2",
                "status": "FAILED",
                "statusReason": "Test Failure for job2",
            },
        ]
        mock_executor.batch.describe_jobs.return_value = {"jobs": jobs}
        mock_executor.batch.submit_job.side_effect = [
            {"jobId": "job-1"},
            {"jobId": "job-2"},
            {"jobId": "job-1"},
            {"jobId": "job-2"},
            {"jobId": "job-1"},
            {"jobId": "job-2"},
        ]
        mock_executor.attempt_submit_jobs()

        assert len(mock_executor.active_workers.get_all_jobs()) == 2
        assert len(mock_executor.pending_jobs) == 0

        mock_executor.sync_running_jobs()
        for i in range(2):
            assert (
                f'Airflow task {airflow_keys[i]} failed due to {jobs[i]["statusReason"]}. Failure 1 out of {mock_executor.MAX_SUBMIT_JOB_ATTEMPTS} occurred on {jobs[i]["jobId"]}. Rescheduling.'
                in caplog.messages[i]
            )

        caplog.clear()
        mock_executor.attempt_submit_jobs()
        mock_executor.sync_running_jobs()
        for i in range(2):
            assert (
                f'Airflow task {airflow_keys[i]} failed due to {jobs[i]["statusReason"]}. Failure 2 out of {mock_executor.MAX_SUBMIT_JOB_ATTEMPTS} occurred on {jobs[i]["jobId"]}. Rescheduling.'
                in caplog.messages[i]
            )

        caplog.clear()
        mock_executor.attempt_submit_jobs()
        mock_executor.sync_running_jobs()
        for i in range(2):
            assert f"Airflow task {airflow_keys[i]} has failed a maximum of {mock_executor.MAX_SUBMIT_JOB_ATTEMPTS} times. Marking as failed"

    @mock.patch("airflow.providers.amazon.aws.executors.batch.batch_executor.exponential_backoff_retry")
    def test_sync_unhealthy_boto_connection(self, mock_exponentional_backoff_retry, mock_executor):
        mock_exponentional_backoff_retry.return_value = None
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor.sync()
        mock_exponentional_backoff_retry.assert_called_once()
        assert mock_executor.IS_BOTO_CONNECTION_HEALTHY is False

    def test_sync_running_jobs_no_jobs(self, mock_executor, caplog):
        caplog.set_level("DEBUG")
        assert len(mock_executor.active_workers.get_all_jobs()) == 0
        mock_executor.sync_running_jobs()
        assert "No active Airflow tasks, skipping sync" in caplog.messages[0]

    def test_sync_client_error(self, mock_executor, caplog):
        mock_executor.execute_async("airflow_key", "airflow_cmd")
        assert len(mock_executor.pending_jobs) == 1
        mock_resp = {
            "Error": {
                "Code": "ExpiredTokenException",
                "Message": "Expired token error",
            },
        }
        caplog.set_level("WARNING")
        mock_executor.batch.submit_job.side_effect = ClientError(mock_resp, "test-sync")
        mock_executor.sync()
        assert "AWS credentials are either missing or expired" in caplog.messages[0]

    def test_sync_exception(self, mock_executor, caplog):
        mock_executor.active_workers.add_job(
            job_id="job_id",
            airflow_task_key="airflow_key",
            airflow_cmd="command",
            queue="queue",
            exec_config={},
            attempt_number=1,
        )
        assert len(mock_executor.active_workers.get_all_jobs()) == 1
        caplog.set_level("ERROR")
        mock_executor.batch.describe_jobs.side_effect = Exception("test-exception")
        mock_executor.sync()
        assert f"Failed to sync {mock_executor.__class__.__name__}" in caplog.messages[0]

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync(self, success_mock, fail_mock, mock_airflow_key, mock_executor):
        """Test sync from end-to-end. Mocks a successful job & makes sure it's removed"""
        self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key())
        mock_executor.sync()

        mock_executor.batch.describe_jobs.assert_called_once()

        # task is not stored in active workers
        assert len(mock_executor.active_workers) == 0
        # Task is immediately succeeded
        success_mock.assert_called_once()
        assert fail_mock.call_count == 0

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    @mock.patch.object(batch_executor, "calculate_next_attempt_delay", return_value=dt.timedelta(seconds=0))
    def test_failed_sync(self, _, success_mock, fail_mock, mock_airflow_key, mock_executor):
        """Test failure states"""
        self._mock_sync(
            executor=mock_executor,
            airflow_key=mock_airflow_key(),
            status="FAILED",
            attempt_number=2,
        )

        mock_executor.sync()

        mock_executor.batch.describe_jobs.assert_called_once()

        # task is not stored in active workers
        assert len(mock_executor.active_workers) == 0
        # Task is immediately failed
        fail_mock.assert_called_once()
        assert success_mock.call_count == 0

    def test_start_failure_with_invalid_permissions(self, set_env_vars):
        executor = AwsBatchExecutor()

        # Replace boto3 Batch client with mock.
        batch_mock = mock.Mock(spec=executor.batch)
        batch_mock.describe_jobs.return_value = {}
        executor.batch = batch_mock

        mock_resp = {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "is not authorized to perform: batch:DescribeJobs on resource:",
            }
        }
        batch_mock.describe_jobs.side_effect = ClientError(mock_resp, "DescribeJobs")

        executor.batch = batch_mock

        with pytest.raises(AirflowException, match=mock_resp["Error"]["Message"]):
            executor.start()

    def test_start_success(self, set_env_vars, caplog):
        executor = AwsBatchExecutor()

        # Replace boto3 Batch client with mock.
        batch_mock = mock.Mock(spec=executor.batch)
        batch_mock.describe_jobs.return_value = {}
        executor.batch = batch_mock

        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            executor.start()

        assert "succeeded" in caplog.text

    def test_health_check_failure(self, mock_executor, set_env_vars):
        mock_executor.batch.describe_jobs.side_effect = Exception("Test_failure")
        executor = AwsBatchExecutor()
        batch_mock = mock.Mock(spec=executor.batch)
        batch_mock.describe_jobs.side_effect = Exception("Test_failure")
        executor.batch = batch_mock

        with pytest.raises(Exception, match="Test_failure"):
            executor.start()

    def test_start_health_check_config(self, set_env_vars):
        executor = AwsBatchExecutor()

        # Replace boto3 Batch client with mock.
        batch_mock = mock.Mock(spec=executor.batch)
        batch_mock.describe_jobs.side_effect = {}
        executor.batch = batch_mock

        env_var_key = f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.CHECK_HEALTH_ON_STARTUP}".upper()
        with mock.patch.dict(os.environ, {env_var_key: "False"}):
            executor.start()

        batch_mock.describe_jobs.assert_not_called()

    def test_terminate(self, mock_airflow_key, mock_executor):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit"""
        self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key(), status="FAILED")

        mock_executor.terminate()
        mock_executor.batch.terminate_job.assert_called_once()

    def test_terminate_failure(self, mock_executor, caplog):
        mock_executor.active_workers.add_job(
            job_id="job_id",
            airflow_task_key="airflow_key",
            airflow_cmd="command",
            queue="queue",
            exec_config={},
            attempt_number=1,
        )
        assert len(mock_executor.active_workers.get_all_jobs()) == 1
        caplog.set_level("ERROR")
        mock_executor.batch.terminate_job.side_effect = Exception("test-exception")
        mock_executor.terminate()
        assert f"Failed to terminate {mock_executor.__class__.__name__}" in caplog.messages[0]

    def test_end(self, mock_airflow_key, mock_executor):
        """The end() function should call sync 3 times, and the task should fail on the 3rd call"""
        self.sync_call_count = 0
        sync_func = mock_executor.sync
        self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key(), status="RUNNING")

        def sync_mock():
            """This is to count the number of times sync is called. On the 3rd time, mock the job to fail"""
            if self.sync_call_count >= 2:
                self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key(), status="FAILED")
            sync_func()
            self.sync_call_count += 1

        mock_executor.sync = sync_mock
        self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key(), status="RUNNING")
        mock_executor.end(heartbeat_interval=0)
        assert self.sync_call_count == 3
        mock_executor.sync = sync_func

    def _mock_sync(
        self,
        executor: AwsBatchExecutor,
        airflow_key: str,
        job_id: str = MOCK_JOB_ID,
        status: str = "SUCCEEDED",
        status_reason: str = "",
        attempt_number: int = 0,
    ):
        """
        This function is not mocking sync so much as it is preparing for
        sync to be called. It adds a job to active_workers and mocks the describe_jobs API call.
        """
        executor.active_workers.add_job(
            job_id=job_id,
            airflow_task_key=airflow_key,
            airflow_cmd="airflow_cmd",
            queue="queue",
            exec_config={},
            attempt_number=attempt_number,
        )

        after_batch_job = {
            "jobName": "some-job-queue",
            "jobId": job_id,
            "jobQueue": "some-job-queue",
            "status": status,
            "statusReason": status_reason,
            "createdAt": dt.datetime.now().timestamp(),
            "jobDefinition": "some-job-def",
        }
        executor.batch.describe_jobs.return_value = {"jobs": [after_batch_job]}

    def test_try_adopt_task_instances(self, mock_executor):
        """Test that executor can adopt orphaned task instances from a SchedulerJob shutdown event."""
        mock_executor.batch.describe_jobs.return_value = {
            "jobs": [
                {"jobId": "001", "status": "SUCCEEDED"},
                {"jobId": "002", "status": "SUCCEEDED"},
            ],
        }

        orphaned_tasks = [
            mock.Mock(spec=TaskInstance),
            mock.Mock(spec=TaskInstance),
            mock.Mock(spec=TaskInstance),
        ]
        orphaned_tasks[0].external_executor_id = "001"  # Matches a running task_arn
        orphaned_tasks[1].external_executor_id = "002"  # Matches a running task_arn
        orphaned_tasks[2].external_executor_id = None  # One orphaned task has no external_executor_id
        for task in orphaned_tasks:
            task.try_number = 1

        not_adopted_tasks = mock_executor.try_adopt_task_instances(orphaned_tasks)

        mock_executor.batch.describe_jobs.assert_called_once()
        # Two of the three tasks should be adopted.
        assert len(orphaned_tasks) - 1 == len(mock_executor.active_workers)
        # The remaining one task is unable to be adopted.
        assert 1 == len(not_adopted_tasks)


class TestBatchExecutorConfig:
    @staticmethod
    def _unset_conf():
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def teardown_method(self) -> None:
        self._unset_conf()

    @pytest.mark.skipif(
        RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES,
        reason="Config defaults are validated against provider.yaml so this test "
        "should only run when tests are run from sources",
    )
    def test_validate_config_defaults(self):
        """Assert that the defaults stated in the config.yml file match those in utils.CONFIG_DEFAULTS."""
        from airflow.providers.amazon import __file__ as provider_path

        config_filename = os.path.join(os.path.dirname(provider_path), "provider.yaml")

        with open(config_filename) as config:
            options = yaml.safe_load(config)["config"][CONFIG_GROUP_NAME]["options"]
            file_defaults = {
                option: default for (option, value) in options.items() if (default := value.get("default"))
            }

        assert len(file_defaults) == len(CONFIG_DEFAULTS)
        for key in file_defaults.keys():
            assert file_defaults[key] == CONFIG_DEFAULTS[key]

    @pytest.mark.parametrize(
        "bad_config",
        [
            pytest.param({"command": "bad_robot"}, id="executor_config_can_not_overwrite_command"),
        ],
    )
    def test_executor_config_exceptions(self, bad_config, mock_executor):
        with pytest.raises(ValueError) as raised:
            mock_executor.execute_async(mock_airflow_key, mock_cmd, executor_config=bad_config)

        assert raised.match('Executor Config should never override "command')

    def test_config_defaults_are_applied(self):
        submit_kwargs = batch_executor_config.build_submit_kwargs()
        found_keys = {convert_camel_to_snake(key): key for key in submit_kwargs.keys()}

        for expected_key, expected_value in CONFIG_DEFAULTS.items():
            # these config options are used by the executor,
            # but are not expected to appear in the submit_kwargs.
            if expected_key in [
                AllBatchConfigKeys.AWS_CONN_ID,
                AllBatchConfigKeys.CHECK_HEALTH_ON_STARTUP,
                AllBatchConfigKeys.MAX_SUBMIT_JOB_ATTEMPTS,
                AllBatchConfigKeys.SUBMIT_JOB_KWARGS,
                AllBatchConfigKeys.REGION_NAME,
            ]:
                assert expected_key not in found_keys.keys()
            else:
                assert expected_key in found_keys.keys()
                assert expected_value == submit_kwargs[found_keys[expected_key]]

    def test_verify_tags_are_used_as_provided(self):
        """Confirm that the ``tags`` provided are not converted to camelCase."""
        templated_tags = {"Apache": "Airflow"}

        provided_run_submit_kwargs = {
            "tags": templated_tags,  # The user should be allowed to pass arbitrary submit_job args
        }

        run_submit_kwargs_env_key = (
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.SUBMIT_JOB_KWARGS}".upper()
        )
        os.environ[run_submit_kwargs_env_key] = json.dumps(provided_run_submit_kwargs)
        submit_kwargs = batch_executor_config.build_submit_kwargs()

        # Verify that tag names are exempt from the camel-case conversion.
        assert submit_kwargs["tags"] == templated_tags

    @pytest.mark.parametrize(
        "submit_job_kwargs, exec_config, expected_result",
        [
            # No input submit_job_kwargs or executor overrides
            (
                {},
                {},
                {
                    "jobDefinition": "some-job-def",
                    "jobQueue": "some-job-queue",
                    "jobName": "some-job-name",
                    "containerOverrides": {
                        "command": ["command"],
                        "environment": [{"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}],
                    },
                },
            ),
            # submit_job_kwargs provided, not exec_config
            (
                {
                    "shareIdentifier": "Banana",
                    "tags": [{"key": "FOO", "value": "BAR"}],
                    "containerOverrides": {
                        "memory": 500,
                        "vcpus": 10,
                        "environment": [{"name": "X", "value": "Y"}],
                    },
                },
                {},
                {
                    "shareIdentifier": "Banana",
                    "tags": [{"key": "FOO", "value": "BAR"}],
                    "jobDefinition": "some-job-def",
                    "jobQueue": "some-job-queue",
                    "jobName": "some-job-name",
                    "containerOverrides": {
                        "command": ["command"],
                        "memory": 500,
                        "vcpus": 10,
                        "environment": [
                            {"name": "X", "value": "Y"},
                            # Added by the batch executor
                            {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"},
                        ],
                    },
                },
            ),
            # exec_config provided, no submit_job_kwargs
            (
                {},
                {
                    "shareIdentifier": "Banana",
                    "tags": [{"key": "FOO", "value": "BAR"}],
                    "containerOverrides": {
                        "memory": 500,
                        "vcpus": 10,
                        "environment": [{"name": "X", "value": "Y"}],
                    },
                },
                {
                    "shareIdentifier": "Banana",
                    "tags": [{"key": "FOO", "value": "BAR"}],
                    "jobDefinition": "some-job-def",
                    "jobQueue": "some-job-queue",
                    "jobName": "some-job-name",
                    "containerOverrides": {
                        "command": ["command"],
                        "memory": 500,
                        "vcpus": 10,
                        "environment": [
                            {"name": "X", "value": "Y"},
                            # Added by the batch executor
                            {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"},
                        ],
                    },
                },
            ),
            # Both submit_job_kwargs and executor_config provided. The latter should override the former,
            # following a recursive python dict update strategy
            (
                {
                    "shareIdentifier": "Banana",
                    "tags": [{"key": "FOO", "value": "BAR"}],
                    "propagateTags": True,
                    "containerOverrides": {
                        "memory": 500,
                        "vcpus": 10,
                        "environment": [{"name": "X", "value": "Y"}],
                    },
                },
                {
                    "shareIdentifier": "Fish",
                    "tags": [{"key": "X", "value": "Y"}, {"key": "W", "value": "Z"}],
                    "containerOverrides": {
                        "memory": 300,
                        "environment": [{"name": "W", "value": "Z"}],
                    },
                },
                {
                    # tags and shareIdentifier are overridden by exec_config
                    "shareIdentifier": "Fish",
                    # List types overwrite entirely, as python dict update would do
                    "tags": [{"key": "X", "value": "Y"}, {"key": "W", "value": "Z"}],
                    # propagateTags remains since it is not a list type and not overridden by exec config
                    "propagateTags": True,
                    "jobDefinition": "some-job-def",
                    "jobQueue": "some-job-queue",
                    "jobName": "some-job-name",
                    "containerOverrides": {
                        "command": ["command"],
                        "memory": 300,
                        # vcpus is present because it is missing from the exec config
                        "vcpus": 10,
                        "environment": [
                            # Overridden list type
                            {"name": "W", "value": "Z"},  # Only new env vars present, overwritten
                            # Added by the batch executor
                            {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"},
                        ],
                    },
                },
            ),
        ],
    )
    def test_submit_job_kwargs_exec_config_overrides(
        self, set_env_vars, submit_job_kwargs, exec_config, expected_result
    ):
        submit_job_kwargs_env_key = (
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.SUBMIT_JOB_KWARGS}".upper()
        )
        os.environ[submit_job_kwargs_env_key] = json.dumps(submit_job_kwargs)

        mock_ti_key = mock.Mock(spec=tuple)
        command = ["command"]

        executor = AwsBatchExecutor()

        final_run_task_kwargs = executor._submit_job_kwargs(mock_ti_key, command, "queue", exec_config)

        assert final_run_task_kwargs == expected_result

    def test_short_import_path(self):
        from airflow.providers.amazon.aws.executors.batch import AwsBatchExecutor as AwsBatchExecutorShortPath

        assert AwsBatchExecutor is AwsBatchExecutorShortPath
