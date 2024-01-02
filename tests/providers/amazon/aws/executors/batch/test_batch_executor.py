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
import os
from unittest import mock

import pytest
import yaml

from airflow.executors.base_executor import BaseExecutor
from airflow.providers.amazon.aws.executors.batch import batch_executor_config
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

ARN1 = "arn1"

MOCK_JOB_ID = "batch-job-id"


@pytest.fixture
def set_env_vars():
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.REGION_NAME}".upper()] = "us-west-1"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.JOB_NAME}".upper()] = "some-job-name"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.JOB_QUEUE}".upper()] = "some-job-queue"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.JOB_DEFINITION}".upper()] = "some-job-def"


@pytest.fixture
def mock_executor(set_env_vars) -> AwsBatchExecutor:
    """Mock Batch Executor to a repeatable starting state."""
    executor = AwsBatchExecutor()

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
        self.collection.add_job(self.first_job_id, self.first_airflow_key)
        # Add second task
        self.second_job_id = "002"
        self.second_airflow_key = mock.Mock(spec=tuple)
        self.collection.add_job(self.second_job_id, self.second_airflow_key)

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
        """Jobs that have been launched are identified as 'running'"""
        assert self.success in self.all_statuses

        success_job = BatchJob("BBB", self.success)
        assert success_job.get_job_state() == State.SUCCESS

    def test_failed_jobs(self):
        """Jobs that have been launched are identified as 'running'"""
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

        mock_executor.batch.submit_job.assert_called_once()
        assert len(mock_executor.active_workers) == 1

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
    def test_failed_sync(self, success_mock, fail_mock, mock_airflow_key, mock_executor):
        """Test failure states"""
        self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key(), status="FAILED")

        mock_executor.sync()

        mock_executor.batch.describe_jobs.assert_called_once()

        # task is not stored in active workers
        assert len(mock_executor.active_workers) == 0
        # Task is immediately failed
        fail_mock.assert_called_once()
        assert success_mock.call_count == 0

    def test_terminate(self, mock_airflow_key, mock_executor):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit"""
        self._mock_sync(executor=mock_executor, airflow_key=mock_airflow_key(), status="FAILED")

        mock_executor.terminate()
        mock_executor.batch.terminate_job.assert_called_once()

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
    ):
        """
        This function is not mocking sync so much as it is preparing for
        sync to be called. It adds a job to active_workers and mocks the describe_jobs API call.
        """
        executor.active_workers.add_job(job_id, airflow_key)

        after_batch_job = {
            "jobName": "some-job-queue",
            "jobId": job_id,
            "jobQueue": "some-job-queue",
            "status": status,
            "createdAt": dt.datetime.now().timestamp(),
            "jobDefinition": "some-job-def",
        }
        executor.batch.describe_jobs.return_value = {"jobs": [after_batch_job]}


class TestBatchExecutorConfig:
    @staticmethod
    def _unset_conf():
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def teardown_method(self) -> None:
        self._unset_conf()

    def test_validate_config_defaults(self):
        """Assert that the defaults stated in the config.yml file match those in utils.CONFIG_DEFAULTS."""
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        executor_path = "aws/executors/batch"
        config_filename = curr_dir.replace("tests", "airflow").replace(executor_path, "provider.yaml")

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
