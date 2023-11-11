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
import re
import time
from functools import partial
from typing import Callable
from unittest import mock

import pytest
import yaml
from botocore.exceptions import ClientError
from inflection import camelize

from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.providers.amazon.aws.executors.ecs import ecs_executor_config
from airflow.providers.amazon.aws.executors.ecs.boto_schema import BotoTaskSchema
from airflow.providers.amazon.aws.executors.ecs.ecs_executor import (
    CONFIG_GROUP_NAME,
    AllEcsConfigKeys,
    AwsEcsExecutor,
    EcsTaskCollection,
)
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_DEFAULTS,
    EcsExecutorTask,
    _recursive_flatten_dict,
    parse_assign_public_ip,
)
from airflow.utils.helpers import convert_camel_to_snake
from airflow.utils.state import State, TaskInstanceState

pytestmark = pytest.mark.db_test

ARN1 = "arn1"
ARN2 = "arn2"
ARN3 = "arn3"


def mock_task(arn=ARN1, state=State.RUNNING):
    task = mock.Mock(spec=EcsExecutorTask, task_arn=arn)
    task.api_failure_count = 0
    task.get_task_state.return_value = state

    return task


# These first two fixtures look unusual.  For tests which do not care if the object
# returned by the fixture is unique, use it like a normal fixture.  If your test
# requires a unique value, then call it like a method.
#
# See `test_info_by_key` for an example of a test that requires two unique mocked queues.


@pytest.fixture(autouse=True)
def mock_airflow_key():
    def _key():
        return mock.Mock(spec=tuple)

    return _key


@pytest.fixture(autouse=True)
def mock_queue():
    def _queue():
        return mock.Mock(spec=str)

    return _queue


# The following two fixtures look different because no existing test
# cares if they have unique values, so the same value is always used.
@pytest.fixture(autouse=True)
def mock_cmd():
    return mock.Mock(spec=list)


@pytest.fixture(autouse=True)
def mock_config():
    return mock.Mock(spec=dict)


@pytest.fixture
def set_env_vars():
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.REGION_NAME}".upper()] = "us-west-1"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CLUSTER}".upper()] = "some-cluster"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CONTAINER_NAME}".upper()] = "container-name"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.TASK_DEFINITION}".upper()] = "some-task-def"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.LAUNCH_TYPE}".upper()] = "FARGATE"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.PLATFORM_VERSION}".upper()] = "LATEST"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.ASSIGN_PUBLIC_IP}".upper()] = "False"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.SECURITY_GROUPS}".upper()] = "sg1,sg2"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.SUBNETS}".upper()] = "sub1,sub2"
    os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS}".upper()] = "3"


@pytest.fixture
def mock_executor(set_env_vars) -> AwsEcsExecutor:
    """Mock ECS to a repeatable starting state.."""
    executor = AwsEcsExecutor()

    # Replace boto3 ECS client with mock.
    ecs_mock = mock.Mock(spec=executor.ecs)
    run_task_ret_val = {"tasks": [{"taskArn": ARN1}], "failures": []}
    ecs_mock.run_task.return_value = run_task_ret_val
    executor.ecs = ecs_mock

    return executor


class TestEcsTaskCollection:
    """Tests EcsTaskCollection Class."""

    # You can't use a fixture in setup_method unless you declare setup_method to be a fixture itself.
    @pytest.fixture(autouse=True)
    def setup_method(self, mock_airflow_key):
        # Create a new Collection and verify it is empty.
        self.collection = EcsTaskCollection()
        assert len(self.collection) == 0

        # Generate two mock keys and assert they are different.  If the value
        # of the key does not matter for a test, let it use the auto-fixture.
        self.key1 = mock_airflow_key()
        self.key2 = mock_airflow_key()
        assert self.key1 != self.key2

    def test_add_task(self):
        # Add a task, verify that the collection has grown and the task arn matches.
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config, 1)
        assert len(self.collection) == 1
        assert self.collection.tasks[ARN1].task_arn == ARN1

        # Add a task, verify that the collection has grown and the task arn is not the same as the first.
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config, 1)
        assert len(self.collection) == 2
        assert self.collection.tasks[ARN2].task_arn == ARN2
        assert self.collection.tasks[ARN2].task_arn != self.collection.tasks[ARN1].task_arn

    def test_task_by_key(self):
        self.collection.add_task(mock_task(), mock_airflow_key, mock_queue, mock_cmd, mock_config, 1)

        task = self.collection.task_by_key(mock_airflow_key)

        assert task == self.collection.tasks[ARN1]

    def test_task_by_arn(self):
        self.collection.add_task(mock_task(), mock_airflow_key, mock_queue, mock_cmd, mock_config, 1)

        task = self.collection.task_by_arn(ARN1)

        assert task == self.collection.tasks[ARN1]

    def test_info_by_key(self, mock_queue):
        self.collection.add_task(mock_task(ARN1), self.key1, queue1 := mock_queue(), mock_cmd, mock_config, 1)
        self.collection.add_task(mock_task(ARN2), self.key2, queue2 := mock_queue(), mock_cmd, mock_config, 1)
        assert queue1 != queue2

        task1_info = self.collection.info_by_key(self.key1)
        assert task1_info.queue == queue1
        assert task1_info.cmd == mock_cmd
        assert task1_info.config == mock_config

        task2_info = self.collection.info_by_key(self.key2)
        assert task2_info.queue == queue2
        assert task2_info.cmd == mock_cmd
        assert task2_info.config == mock_config

        assert task1_info != task2_info

    def test_get_all_arns(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config, 1)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config, 1)

        assert self.collection.get_all_arns() == [ARN1, ARN2]

    def test_get_all_task_keys(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config, 1)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config, 1)

        assert self.collection.get_all_task_keys() == [self.key1, self.key2]

    def test_pop_by_key(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config, 1)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config, 1)
        task1_as_saved = self.collection.tasks[ARN1]

        assert len(self.collection) == 2
        task1_as_popped = self.collection.pop_by_key(self.key1)
        assert len(self.collection) == 1
        # Assert it returns the same task.
        assert task1_as_popped == task1_as_saved
        # Assert the popped task is removed.
        with pytest.raises(KeyError):
            assert self.collection.task_by_key(self.key1)
        # Assert the remaining task is task2.
        assert self.collection.task_by_key(self.key2)

    def test_update_task(self):
        self.collection.add_task(
            initial_task := mock_task(), mock_airflow_key, mock_queue, mock_cmd, mock_config, 1
        )
        assert self.collection[ARN1] == initial_task
        self.collection.update_task(updated_task := mock_task())

        assert self.collection[ARN1] == updated_task
        assert initial_task != updated_task

    def test_failure_count(self):
        # Create a new Collection and add a two tasks.
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config, 1)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config, 1)

        # failure_count is set to attempt number, which is initialized as 1.
        assert self.collection.failure_count_by_key(self.key1) == 1
        for i in range(1, 5):
            self.collection.increment_failure_count(self.key1)
            assert self.collection.failure_count_by_key(self.key1) == i + 1
        assert self.collection.failure_count_by_key(self.key2) == 1


class TestEcsExecutorTask:
    """Tests the EcsExecutorTask DTO."""

    def test_repr(self):
        last_status = "QUEUED"
        desired_status = "SUCCESS"
        running_task = EcsExecutorTask(
            task_arn=ARN1, last_status=last_status, desired_status=desired_status, containers=[{}]
        )
        assert f"({ARN1}, {last_status}->{desired_status}, {running_task.get_task_state()})" == repr(
            running_task
        )

    def test_queued_tasks(self):
        """Tasks that are pending launch identified as 'queued'."""
        queued_tasks = [
            EcsExecutorTask(
                task_arn=ARN1, last_status="PROVISIONING", desired_status="RUNNING", containers=[{}]
            ),
            EcsExecutorTask(task_arn=ARN2, last_status="PENDING", desired_status="RUNNING", containers=[{}]),
            EcsExecutorTask(
                task_arn=ARN3, last_status="ACTIVATING", desired_status="RUNNING", containers=[{}]
            ),
        ]
        for task in queued_tasks:
            assert State.QUEUED == task.get_task_state()

    def test_running_tasks(self):
        """Tasks that have been launched are identified as 'running'."""
        running_task = EcsExecutorTask(
            task_arn=ARN1, last_status="RUNNING", desired_status="RUNNING", containers=[{}]
        )
        assert State.RUNNING == running_task.get_task_state()

    def test_running_tasks_edge_cases(self):
        """Tasks that are not finished have been launched are identified as 'running'."""
        running_task = EcsExecutorTask(
            task_arn=ARN1, last_status="QUEUED", desired_status="SUCCESS", containers=[{}]
        )
        assert State.RUNNING == running_task.get_task_state()

    def test_removed_tasks(self):
        """Tasks that failed to launch are identified as 'removed'."""
        deprovisioning_tasks = [
            EcsExecutorTask(
                task_arn=ARN1, last_status="DEACTIVATING", desired_status="STOPPED", containers=[{}]
            ),
            EcsExecutorTask(task_arn=ARN2, last_status="STOPPING", desired_status="STOPPED", containers=[{}]),
            EcsExecutorTask(
                task_arn=ARN3, last_status="DEPROVISIONING", desired_status="STOPPED", containers=[{}]
            ),
        ]
        for task in deprovisioning_tasks:
            assert State.REMOVED == task.get_task_state()

        removed_task = EcsExecutorTask(
            task_arn="DEAD",
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[{}],
            stopped_reason="Timeout waiting for network interface provisioning to complete.",
        )
        assert State.REMOVED == removed_task.get_task_state()

    def test_stopped_tasks(self):
        """Tasks that have terminated are identified as either 'success' or 'failure'."""
        successful_container = {"exit_code": 0, "last_status": "STOPPED"}
        error_container = {"exit_code": 100, "last_status": "STOPPED"}

        for status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
            success_task = EcsExecutorTask(
                task_arn="GOOD",
                last_status=status,
                desired_status="STOPPED",
                stopped_reason="Essential container in task exited",
                started_at=dt.datetime.now(),
                containers=[successful_container],
            )
            assert State.SUCCESS == success_task.get_task_state()

        for status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
            failed_task = EcsExecutorTask(
                task_arn="FAIL",
                last_status=status,
                desired_status="STOPPED",
                stopped_reason="Essential container in task exited",
                started_at=dt.datetime.now(),
                containers=[successful_container, successful_container, error_container],
            )
            assert State.FAILED == failed_task.get_task_state()


class TestAwsEcsExecutor:
    """Tests the AWS ECS Executor."""

    def teardown_method(self) -> None:
        self._unset_conf()

    def test_execute(self, mock_airflow_key, mock_executor):
        """Test execution from end-to-end."""
        airflow_key = mock_airflow_key()

        mock_executor.ecs.run_task.return_value = {
            "tasks": [
                {
                    "taskArn": ARN1,
                    "lastStatus": "",
                    "desiredStatus": "",
                    "containers": [{"name": "some-ecs-container"}],
                }
            ],
            "failures": [],
        }

        assert 0 == len(mock_executor.pending_tasks)
        mock_executor.execute_async(airflow_key, mock_cmd)
        assert 1 == len(mock_executor.pending_tasks)

        mock_executor.attempt_task_runs()
        mock_executor.ecs.run_task.assert_called_once()

        # Task is stored in active worker.
        assert 1 == len(mock_executor.active_workers)
        assert ARN1 in mock_executor.active_workers.task_by_key(airflow_key).task_arn

    def test_success_execute_api_exception(self, mock_executor):
        """Test what happens when ECS throws an exception, but ultimately runs the task."""
        run_task_exception = Exception("Test exception")
        run_task_success = {
            "tasks": [
                {
                    "taskArn": ARN1,
                    "lastStatus": "",
                    "desiredStatus": "",
                    "containers": [{"name": "some-ecs-container"}],
                }
            ],
            "failures": [],
        }
        mock_executor.ecs.run_task.side_effect = [run_task_exception, run_task_exception, run_task_success]
        mock_executor.execute_async(mock_airflow_key, mock_cmd)

        # Fail 2 times
        for _ in range(2):
            mock_executor.attempt_task_runs()
            # Task is not stored in active workers.
            assert len(mock_executor.active_workers) == 0

        # Pass in last attempt
        mock_executor.attempt_task_runs()
        assert len(mock_executor.pending_tasks) == 0
        assert ARN1 in mock_executor.active_workers.get_all_arns()

    def test_failed_execute_api_exception(self, mock_executor):
        """Test what happens when ECS refuses to execute a task and throws an exception"""
        mock_executor.ecs.run_task.side_effect = Exception("Test exception")
        mock_executor.execute_async(mock_airflow_key, mock_cmd)

        # No matter what, don't schedule until run_task becomes successful.
        for _ in range(int(mock_executor.MAX_RUN_TASK_ATTEMPTS) * 2):
            mock_executor.attempt_task_runs()
            # Task is not stored in active workers.
            assert len(mock_executor.active_workers) == 0

    def test_failed_execute_api(self, mock_executor):
        """Test what happens when ECS refuses to execute a task."""
        mock_executor.ecs.run_task.return_value = {
            "tasks": [],
            "failures": [
                {"arn": ARN1, "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        mock_executor.execute_async(mock_airflow_key, mock_cmd)

        # No matter what, don't schedule until run_task becomes successful.
        for _ in range(int(mock_executor.MAX_RUN_TASK_ATTEMPTS) * 2):
            mock_executor.attempt_task_runs()
            # Task is not stored in active workers.
            assert len(mock_executor.active_workers) == 0

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync(self, success_mock, fail_mock, mock_executor):
        """Test sync from end-to-end."""
        self._mock_sync(mock_executor)

        mock_executor.sync_running_tasks()
        mock_executor.ecs.describe_tasks.assert_called_once()

        # Task is not stored in active workers.
        assert len(mock_executor.active_workers) == 0
        # Task is immediately succeeded.
        success_mock.assert_called_once()
        fail_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    @mock.patch.object(EcsTaskCollection, "get_all_arns", return_value=[])
    def test_sync_short_circuits_with_no_arns(self, _, success_mock, fail_mock, mock_executor):
        self._mock_sync(mock_executor)

        mock_executor.sync_running_tasks()

        mock_executor.ecs.describe_tasks.assert_not_called()
        fail_mock.assert_not_called()
        success_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_failed_sync(self, success_mock, fail_mock, mock_executor):
        """Test success and failure states."""
        self._mock_sync(mock_executor, State.FAILED)

        mock_executor.sync()
        mock_executor.ecs.describe_tasks.assert_called_once()

        # Task is not stored in active workers.
        assert len(mock_executor.active_workers) == 0
        # Task is immediately succeeded.
        fail_mock.assert_called_once()
        success_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_removed_sync(self, fail_mock, success_mock, mock_executor):
        """A removed task will increment failure count but call neither fail() nor success()."""
        self._mock_sync(mock_executor, expected_state=State.REMOVED, set_task_state=State.REMOVED)
        task_instance_key = mock_executor.active_workers.arn_to_key[ARN1]

        mock_executor.sync_running_tasks()

        assert ARN1 in mock_executor.active_workers.get_all_arns()
        assert mock_executor.active_workers.key_to_failure_counts[task_instance_key] == 2
        fail_mock.assert_not_called()
        success_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_failed_sync_cumulative_fail(self, success_mock, fail_mock, mock_airflow_key, mock_executor):
        """Test that failure_count/attempt_number is cumulative for pending tasks and active workers."""
        AwsEcsExecutor.MAX_RUN_TASK_ATTEMPTS = "5"
        mock_executor.ecs.run_task.return_value = {
            "tasks": [],
            "failures": [
                {"arn": ARN1, "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }
        task_key = mock_airflow_key()
        mock_executor.execute_async(task_key, mock_cmd)
        for _ in range(2):
            assert len(mock_executor.pending_tasks) == 1
            keys = [task.key for task in mock_executor.pending_tasks]
            assert task_key in keys
            mock_executor.attempt_task_runs()
            assert len(mock_executor.pending_tasks) == 1

        mock_executor.ecs.run_task.return_value = {
            "tasks": [
                {
                    "taskArn": ARN1,
                    "lastStatus": "",
                    "desiredStatus": "",
                    "containers": [{"name": "some-ecs-container"}],
                }
            ],
            "failures": [],
        }
        mock_executor.attempt_task_runs()
        assert len(mock_executor.pending_tasks) == 0
        assert ARN1 in mock_executor.active_workers.get_all_arns()

        mock_executor.ecs.describe_tasks.return_value = {
            "tasks": [],
            "failures": [
                {"arn": ARN1, "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        # Call sync_running_tasks 2 times with failures.
        for _ in range(2):
            mock_executor.sync_running_tasks()

            # Ensure task arn is not removed from active.
            assert ARN1 in mock_executor.active_workers.get_all_arns()

            # Task is neither failed nor succeeded.
            fail_mock.assert_not_called()
            success_mock.assert_not_called()

        # run_task failed twice, and passed once
        assert mock_executor.ecs.run_task.call_count == 3
        # describe_tasks failed 2 times so far
        assert mock_executor.ecs.describe_tasks.call_count == 2

        # 2 run_task failures + 2 describe_task failures = 4 failures
        # Last call should fail the task.
        mock_executor.sync_running_tasks()
        assert ARN1 not in mock_executor.active_workers.get_all_arns()
        fail_mock.assert_called()
        success_mock.assert_not_called()

    def test_failed_sync_api_exception(self, mock_executor, caplog):
        """Test what happens when ECS sync fails for certain tasks repeatedly."""
        self._mock_sync(mock_executor)
        mock_executor.ecs.describe_tasks.side_effect = Exception("Test Exception")

        mock_executor.sync()
        assert "Failed to sync" in caplog.messages[0]

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_failed_sync_api(self, success_mock, fail_mock, mock_executor):
        """Test what happens when ECS sync fails for certain tasks repeatedly."""
        self._mock_sync(mock_executor)
        mock_executor.ecs.describe_tasks.return_value = {
            "tasks": [],
            "failures": [
                {"arn": ARN1, "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        # Call Sync 2 times with failures. The task can only fail MAX_RUN_TASK_ATTEMPTS times.
        for check_count in range(1, int(AwsEcsExecutor.MAX_RUN_TASK_ATTEMPTS)):
            mock_executor.sync_running_tasks()
            assert mock_executor.ecs.describe_tasks.call_count == check_count

            # Ensure task arn is not removed from active.
            assert ARN1 in mock_executor.active_workers.get_all_arns()

            # Task is neither failed nor succeeded.
            fail_mock.assert_not_called()
            success_mock.assert_not_called()

        # Last call should fail the task.
        mock_executor.sync_running_tasks()
        assert ARN1 not in mock_executor.active_workers.get_all_arns()
        fail_mock.assert_called()
        success_mock.assert_not_called()

    def test_terminate(self, mock_executor):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit."""
        self._mock_sync(mock_executor, State.FAILED)

        mock_executor.terminate()

        mock_executor.ecs.stop_task.assert_called()

    def test_end(self, mock_executor):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        mock_executor.sync = partial(self._sync_mock_with_call_counts, mock_executor.sync)

        self._mock_sync(mock_executor, State.FAILED)

        mock_executor.end(heartbeat_interval=0)

    @mock.patch.object(time, "sleep", return_value=None)
    def test_end_with_queued_tasks_will_wait(self, _, mock_executor):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        sync_call_count = 0
        sync_func = mock_executor.sync

        def sync_mock():
            """Mock won't work here, because we actually want to call the 'sync' func."""
            nonlocal sync_call_count
            sync_func()
            sync_call_count += 1

            if sync_call_count == 1:
                # On the second pass, remove the pending task. This is the equivalent of using
                # mock side_effects to simulate a pending task the first time (triggering the
                # sleep()) and no pending task the second pass, triggering the break and allowing
                # the executor to shut down.
                mock_executor.active_workers.update_task(
                    EcsExecutorTask(
                        ARN2,
                        "STOPPED",
                        "STOPPED",
                        {"exit_code": 0, "name": "some-ecs-container", "last_status": "STOPPED"},
                    )
                )
                self.response_task2_json.update({"desiredStatus": "STOPPED", "lastStatus": "STOPPED"})
                mock_executor.ecs.describe_tasks.return_value = {
                    "tasks": [self.response_task2_json],
                    "failures": [],
                }

        mock_executor.sync = sync_mock

        self._add_mock_task(mock_executor, ARN1)
        self._add_mock_task(mock_executor, ARN2)

        base_response_task_json = {
            "startedAt": dt.datetime.now(),
            "containers": [{"name": "some-ecs-container", "lastStatus": "STOPPED", "exitCode": 0}],
        }
        self.response_task1_json = {
            "taskArn": ARN1,
            "desiredStatus": "STOPPED",
            "lastStatus": "SUCCESS",
            **base_response_task_json,
        }
        self.response_task2_json = {
            "taskArn": ARN2,
            "desiredStatus": "QUEUED",
            "lastStatus": "QUEUED",
            **base_response_task_json,
        }

        mock_executor.ecs.describe_tasks.return_value = {
            "tasks": [self.response_task1_json, self.response_task2_json],
            "failures": [],
        }

        mock_executor.end(heartbeat_interval=0)

        assert sync_call_count == 2

    @pytest.mark.parametrize(
        "bad_config",
        [
            pytest.param({"name": "bad_robot"}, id="executor_config_can_not_overwrite_name"),
            pytest.param({"command": "bad_robot"}, id="executor_config_can_not_overwrite_command"),
        ],
    )
    def test_executor_config_exceptions(self, bad_config, mock_executor):
        with pytest.raises(ValueError) as raised:
            mock_executor.execute_async(mock_airflow_key, mock_cmd, executor_config=bad_config)

        assert raised.match('Executor Config should never override "name" or "command"')
        assert 0 == len(mock_executor.pending_tasks)

    @mock.patch.object(ecs_executor_config, "build_task_kwargs")
    def test_container_not_found(self, mock_build_task_kwargs, mock_executor):
        mock_build_task_kwargs.return_value({"overrides": {"containerOverrides": [{"name": "foo"}]}})

        with pytest.raises(KeyError) as raised:
            AwsEcsExecutor()
        assert raised.match(
            re.escape(
                "Rendered JSON template does not contain key "
                '"overrides[containerOverrides][containers][x][command]"'
            )
        )
        assert 0 == len(mock_executor.pending_tasks)

    @staticmethod
    def _unset_conf():
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def _mock_sync(
        self,
        executor: AwsEcsExecutor,
        expected_state=TaskInstanceState.SUCCESS,
        set_task_state=TaskInstanceState.RUNNING,
    ) -> None:
        """Mock ECS to the expected state."""
        self._add_mock_task(executor, ARN1, set_task_state)

        response_task_json = {
            "taskArn": ARN1,
            "desiredStatus": "STOPPED",
            "lastStatus": set_task_state,
            "containers": [
                {
                    "name": "some-ecs-container",
                    "lastStatus": "STOPPED",
                    "exitCode": 100 if expected_state in [State.FAILED, State.QUEUED] else 0,
                }
            ],
        }
        if not set_task_state == State.REMOVED:
            response_task_json["startedAt"] = dt.datetime.now()
        assert expected_state == BotoTaskSchema().load(response_task_json).get_task_state()

        executor.ecs.describe_tasks.return_value = {"tasks": [response_task_json], "failures": []}

    @staticmethod
    def _add_mock_task(executor: AwsEcsExecutor, arn: str, state=TaskInstanceState.RUNNING):
        task = mock_task(arn, state)
        executor.active_workers.add_task(task, mock.Mock(spec=tuple), mock_queue, mock_cmd, mock_config, 1)  # type:ignore[arg-type]

    def _sync_mock_with_call_counts(self, sync_func: Callable):
        """Mock won't work here, because we actually want to call the 'sync' func."""
        # If we call `mock_executor.sync()` here directly we get endless recursion below
        # because we are assigning it to itself with `mock_executor.sync = sync_mock`.
        self.sync_call_count = 0

        sync_func()
        self.sync_call_count += 1

    @pytest.mark.parametrize(
        "desired_status, last_status, exit_code, expected_status",
        [
            ("RUNNING", "QUEUED", 0, State.QUEUED),
            ("STOPPED", "RUNNING", 0, State.RUNNING),
            ("STOPPED", "QUEUED", 0, State.REMOVED),
        ],
    )
    def test_update_running_tasks(
        self, mock_executor, desired_status, last_status, exit_code, expected_status
    ):
        self._add_mock_task(mock_executor, ARN1)
        test_response_task_json = {
            "taskArn": ARN1,
            "desiredStatus": desired_status,
            "lastStatus": last_status,
            "containers": [
                {
                    "name": "test_container",
                    "lastStatus": "QUEUED",
                    "exitCode": exit_code,
                }
            ],
        }
        mock_executor.ecs.describe_tasks.return_value = {"tasks": [test_response_task_json], "failures": []}
        mock_executor.sync_running_tasks()
        assert mock_executor.active_workers.tasks["arn1"].get_task_state() == expected_status
        # The task is not removed from active_workers in these states
        assert len(mock_executor.active_workers) == 1

    def test_update_running_tasks_success(self, mock_executor):
        self._add_mock_task(mock_executor, ARN1)
        test_response_task_json = {
            "taskArn": ARN1,
            "desiredStatus": "STOPPED",
            "lastStatus": "STOPPED",
            "startedAt": dt.datetime.now(),
            "containers": [
                {
                    "name": "test_container",
                    "lastStatus": "STOPPED",
                    "exitCode": 0,
                }
            ],
        }
        patcher = mock.patch(
            "airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor.success", auth_spec=True
        )
        mock_success_function = patcher.start()
        mock_executor.ecs.describe_tasks.return_value = {"tasks": [test_response_task_json], "failures": []}
        mock_executor.sync_running_tasks()
        assert len(mock_executor.active_workers) == 0
        mock_success_function.assert_called_once()

    def test_update_running_tasks_failed(self, mock_executor, caplog):
        caplog.set_level(logging.WARNING)
        self._add_mock_task(mock_executor, ARN1)
        test_response_task_json = {
            "taskArn": ARN1,
            "desiredStatus": "STOPPED",
            "lastStatus": "STOPPED",
            "startedAt": dt.datetime.now(),
            "containers": [
                {
                    "containerArn": "test-container-arn1",
                    "name": "test_container",
                    "lastStatus": "STOPPED",
                    "exitCode": 30,
                    "reason": "test failure",
                }
            ],
        }

        patcher = mock.patch(
            "airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor.fail", auth_spec=True
        )
        mock_failed_function = patcher.start()
        mock_executor.ecs.describe_tasks.return_value = {"tasks": [test_response_task_json], "failures": []}
        mock_executor.sync_running_tasks()
        assert len(mock_executor.active_workers) == 0
        mock_failed_function.assert_called_once()
        assert (
            "The ECS task failed due to the following containers failing: \ntest-container-arn1 - "
            "test failure" in caplog.messages[0]
        )


class TestEcsExecutorConfig:
    @pytest.fixture()
    def assign_subnets(self):
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.SUBNETS}".upper()] = "sub1,sub2"

    @staticmethod
    def teardown_method() -> None:
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME}__".upper()):
                os.environ.pop(env)

    def test_flatten_dict(self):
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.SUBNETS}".upper()] = "sub1,sub2"
        nested_dict = {"a": "a", "b": "b", "c": {"d": "d"}}

        assert _recursive_flatten_dict(nested_dict) == {"a": "a", "b": "b", "d": "d"}

    def test_validate_config_defaults(self):
        """Assert that the defaults stated in the config.yml file match those in utils.CONFIG_DEFAULTS."""
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        executor_path = "aws/executors/ecs"
        config_filename = curr_dir.replace("tests", "airflow").replace(executor_path, "provider.yaml")

        with open(config_filename) as config:
            options = yaml.safe_load(config)["config"][CONFIG_GROUP_NAME]["options"]
            file_defaults = {
                option: default for (option, value) in options.items() if (default := value.get("default"))
            }

        assert len(file_defaults) == len(CONFIG_DEFAULTS)
        for key in file_defaults.keys():
            assert file_defaults[key] == CONFIG_DEFAULTS[key]

    def test_subnets_required(self):
        assert f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.SUBNETS}".upper() not in os.environ

        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.REGION_NAME}".upper()] = "us-west-1"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CLUSTER}".upper()] = "some-cluster"
        os.environ[
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CONTAINER_NAME}".upper()
        ] = "container-name"
        os.environ[
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.TASK_DEFINITION}".upper()
        ] = "some-task-def"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.LAUNCH_TYPE}".upper()] = "FARGATE"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.PLATFORM_VERSION}".upper()] = "LATEST"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.ASSIGN_PUBLIC_IP}".upper()] = "False"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.SECURITY_GROUPS}".upper()] = "sg1,sg2"

        with pytest.raises(ValueError) as raised:
            ecs_executor_config.build_task_kwargs()
        assert raised.match("At least one subnet is required to run a task.")

    def test_config_defaults_are_applied(self, assign_subnets):
        os.environ[
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CONTAINER_NAME}".upper()
        ] = "container-name"
        from airflow.providers.amazon.aws.executors.ecs import ecs_executor_config

        task_kwargs = _recursive_flatten_dict(ecs_executor_config.build_task_kwargs())
        found_keys = {convert_camel_to_snake(key): key for key in task_kwargs.keys()}

        for expected_key, expected_value in CONFIG_DEFAULTS.items():
            # conn_id, max_run_task_attempts, and check_health_on_startup are used by the executor,
            # but are not expected to appear in the task_kwargs.
            if expected_key in [
                AllEcsConfigKeys.AWS_CONN_ID,
                AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS,
                AllEcsConfigKeys.CHECK_HEALTH_ON_STARTUP,
            ]:
                assert expected_key not in found_keys.keys()
            else:
                assert expected_key in found_keys.keys()
                # Make sure to convert "assign_public_ip" from True/False to ENABLE/DISABLE.
                if expected_key is AllEcsConfigKeys.ASSIGN_PUBLIC_IP:
                    expected_value = parse_assign_public_ip(expected_value)
                assert expected_value == task_kwargs[found_keys[expected_key]]

    def test_provided_values_override_defaults(self, assign_subnets):
        """
        Expected precedence is default values are overwritten by values provided explicitly,
        and those values are overwritten by those provided in run_task_kwargs.
        """
        default_version = CONFIG_DEFAULTS[AllEcsConfigKeys.PLATFORM_VERSION]
        templated_version = "1"
        first_explicit_version = "2"
        second_explicit_version = "3"

        run_task_kwargs_env_key = f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.RUN_TASK_KWARGS}".upper()
        platform_version_env_key = (
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.PLATFORM_VERSION}".upper()
        )
        # Required param which doesn't have a default
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CONTAINER_NAME}".upper()] = "foobar"

        # Confirm the default value is applied when no value is provided.
        assert run_task_kwargs_env_key not in os.environ
        assert platform_version_env_key not in os.environ
        from airflow.providers.amazon.aws.executors.ecs import ecs_executor_config

        task_kwargs = ecs_executor_config.build_task_kwargs()

        assert task_kwargs["platformVersion"] == default_version

        # Provide a new value explicitly and assert that it is applied over the default.
        os.environ[platform_version_env_key] = first_explicit_version
        task_kwargs = ecs_executor_config.build_task_kwargs()

        assert task_kwargs["platformVersion"] == first_explicit_version

        # Provide a value via template and assert that it is applied over the explicit value.
        os.environ[run_task_kwargs_env_key] = json.dumps(
            {AllEcsConfigKeys.PLATFORM_VERSION: templated_version}
        )
        task_kwargs = ecs_executor_config.build_task_kwargs()

        assert task_kwargs["platformVersion"] == templated_version

        # Provide a new value explicitly and assert it is not applied over the templated values.
        os.environ[platform_version_env_key] = second_explicit_version
        task_kwargs = ecs_executor_config.build_task_kwargs()

        assert task_kwargs["platformVersion"] == templated_version

    def test_count_can_not_be_modified_by_the_user(self, assign_subnets):
        """The ``count`` parameter must always be 1; verify that the user can not override this value."""

        templated_version = "1"
        templated_cluster = "templated_cluster_name"
        provided_run_task_kwargs = {
            AllEcsConfigKeys.PLATFORM_VERSION: templated_version,
            AllEcsConfigKeys.CLUSTER: templated_cluster,
            "count": 2,  # The user should not be allowed to overwrite count, it must be value of 1
        }

        run_task_kwargs_env_key = f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.RUN_TASK_KWARGS}".upper()
        # Required param which doesn't have a default
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CONTAINER_NAME}".upper()] = "foobar"

        # Provide values via task run kwargs template and assert that they are applied,
        # which verifies that the OTHER values were changed.
        os.environ[run_task_kwargs_env_key] = json.dumps(provided_run_task_kwargs)
        task_kwargs = ecs_executor_config.build_task_kwargs()
        assert task_kwargs["platformVersion"] == templated_version
        assert task_kwargs["cluster"] == templated_cluster

        # Assert that count was NOT overridden when the others were applied.
        assert task_kwargs["count"] == 1

    def test_verify_tags_are_used_as_provided(self, assign_subnets):
        """Confirm that the ``tags`` provided are not converted to camelCase."""
        templated_tags = {"Apache": "Airflow"}

        provided_run_task_kwargs = {
            "tags": templated_tags,  # The user should be allowed to pass arbitrary run task args
        }

        run_task_kwargs_env_key = f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.RUN_TASK_KWARGS}".upper()
        # Required param which doesn't have a default
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CONTAINER_NAME}".upper()] = "foobar"

        os.environ[run_task_kwargs_env_key] = json.dumps(provided_run_task_kwargs)
        task_kwargs = ecs_executor_config.build_task_kwargs()

        # Verify that tag names are exempt from the camel-case conversion.
        assert task_kwargs["tags"] == templated_tags

    def test_that_provided_kwargs_are_moved_to_correct_nesting(self, assign_subnets):
        """
        kwargs such as subnets, security groups,  public ip, and container name are valid run task kwargs,
        but they are not placed at the root of the kwargs dict, they should be nested in various sub dicts.
        Ensure we don't leave any behind in the wrong location.
        """
        kwargs_to_test = {
            AllEcsConfigKeys.CONTAINER_NAME: "foobar",
            AllEcsConfigKeys.ASSIGN_PUBLIC_IP: "True",
            AllEcsConfigKeys.SECURITY_GROUPS: "sg1,sg2",
            AllEcsConfigKeys.SUBNETS: "sub1,sub2",
        }
        for key, value in kwargs_to_test.items():
            os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{key}".upper()] = value

        run_task_kwargs = ecs_executor_config.build_task_kwargs()
        run_task_kwargs_network_config = run_task_kwargs["networkConfiguration"]["awsvpcConfiguration"]
        for key, value in kwargs_to_test.items():
            # Assert that the values are not at the root of the kwargs
            camelized_key = camelize(key, uppercase_first_letter=False)

            assert key not in run_task_kwargs
            assert camelized_key not in run_task_kwargs
            if key == AllEcsConfigKeys.CONTAINER_NAME:
                # The actual ECS run_task_kwarg is "name" not "containerName"
                assert run_task_kwargs["overrides"]["containerOverrides"][0]["name"] == value
            elif key == AllEcsConfigKeys.ASSIGN_PUBLIC_IP:
                # The value for this kwarg is cast from bool to enabled/disabled
                assert run_task_kwargs_network_config[camelized_key] == "ENABLED"
            else:
                assert run_task_kwargs_network_config[camelized_key] == value.split(",")

    def test_start_failure_with_invalid_permissions(self, set_env_vars):
        executor = AwsEcsExecutor()

        # Replace boto3 ECS client with mock.
        ecs_mock = mock.Mock(spec=executor.ecs)
        mock_resp = {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "no identity-based policy allows the ecs:StopTask action",
            }
        }
        ecs_mock.stop_task.side_effect = ClientError(mock_resp, "StopTask")

        executor.ecs = ecs_mock

        with pytest.raises(AirflowException, match=mock_resp["Error"]["Message"]):
            executor.start()

    def test_start_failure_with_invalid_cluster_name(self, set_env_vars):
        executor = AwsEcsExecutor()

        # Replace boto3 ECS client with mock.
        ecs_mock = mock.Mock(spec=executor.ecs)
        mock_resp = {"Error": {"Code": "ClusterNotFoundException", "Message": "Cluster not found."}}
        ecs_mock.stop_task.side_effect = ClientError(mock_resp, "StopTask")

        executor.ecs = ecs_mock

        with pytest.raises(AirflowException, match=mock_resp["Error"]["Message"]):
            executor.start()

    def test_start_success(self, set_env_vars, caplog):
        executor = AwsEcsExecutor()

        # Replace boto3 ECS client with mock.
        ecs_mock = mock.Mock(spec=executor.ecs)
        mock_resp = {
            "Error": {"Code": "InvalidParameterException", "Message": "The referenced task was not found."}
        }
        ecs_mock.stop_task.side_effect = ClientError(mock_resp, "StopTask")

        executor.ecs = ecs_mock

        caplog.set_level(logging.DEBUG)

        executor.start()

        assert "succeeded" in caplog.text

    def test_start_health_check_config(self, set_env_vars):
        executor = AwsEcsExecutor()

        # Replace boto3 ECS client with mock.
        ecs_mock = mock.Mock(spec=executor.ecs)
        mock_resp = {
            "Error": {"Code": "InvalidParameterException", "Message": "The referenced task was not found."}
        }
        ecs_mock.stop_task.side_effect = ClientError(mock_resp, "StopTask")

        executor.ecs = ecs_mock

        os.environ[
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllEcsConfigKeys.CHECK_HEALTH_ON_STARTUP}".upper()
        ] = "False"

        executor.start()

        ecs_mock.stop_task.assert_not_called()
