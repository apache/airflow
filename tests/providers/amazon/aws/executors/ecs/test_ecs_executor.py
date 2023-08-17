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
import os
import re
import time
from importlib import reload
from unittest import mock

import pytest
import yaml

from airflow.executors.base_executor import BaseExecutor
from airflow.providers.amazon.aws.executors.ecs import (
    CONFIG_GROUP_NAME,
    AwsEcsExecutor,
    EcsConfigKeys,
    EcsTaskCollection,
)
from airflow.providers.amazon.aws.executors.ecs.boto_schema import BotoTaskSchema
from airflow.providers.amazon.aws.executors.ecs.utils import CONFIG_DEFAULTS, EcsExecutorTask
from airflow.utils.state import State

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
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config)
        assert len(self.collection) == 1
        assert self.collection.tasks[ARN1].task_arn == ARN1

        # Add a task, verify that the collection has grown and the task arn is not the same as the first.
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config)
        assert len(self.collection) == 2
        assert self.collection.tasks[ARN2].task_arn == ARN2
        assert self.collection.tasks[ARN2].task_arn != self.collection.tasks[ARN1].task_arn

    def test_task_by_key(self):
        self.collection.add_task(mock_task(), mock_airflow_key, mock_queue, mock_cmd, mock_config)

        task = self.collection.task_by_key(mock_airflow_key)

        assert task == self.collection.tasks[ARN1]

    def test_task_by_arn(self):
        self.collection.add_task(mock_task(), mock_airflow_key, mock_queue, mock_cmd, mock_config)

        task = self.collection.task_by_arn(ARN1)

        assert task == self.collection.tasks[ARN1]

    def test_info_by_key(self, mock_queue):
        self.collection.add_task(mock_task(ARN1), self.key1, queue1 := mock_queue(), mock_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, queue2 := mock_queue(), mock_cmd, mock_config)
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
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config)

        assert self.collection.get_all_arns() == [ARN1, ARN2]

    def test_get_all_task_keys(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config)

        assert self.collection.get_all_task_keys() == [self.key1, self.key2]

    def test_pop_by_key(self):
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config)
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
            initial_task := mock_task(), mock_airflow_key, mock_queue, mock_cmd, mock_config
        )
        assert self.collection[ARN1] == initial_task
        self.collection.update_task(updated_task := mock_task())

        assert self.collection[ARN1] == updated_task
        assert initial_task != updated_task

    def test_failure_count(self):
        # Create a new Collection and add a two tasks.
        self.collection.add_task(mock_task(ARN1), self.key1, mock_queue, mock_cmd, mock_config)
        self.collection.add_task(mock_task(ARN2), self.key2, mock_queue, mock_cmd, mock_config)

        assert self.collection.failure_count_by_key(self.key1) == 0
        for i in range(5):
            self.collection.increment_failure_count(self.key1)
            assert self.collection.failure_count_by_key(self.key1) == i + 1
        assert self.collection.failure_count_by_key(self.key2) == 0


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

    def setup_method(self) -> None:
        self._set_conf()
        self._mock_executor()

    def teardown_method(self) -> None:
        self._unset_conf()

    def test_validate_config_defaults(self):
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        config_filename = curr_dir.replace("tests", "airflow").replace(
            "executors/ecs", "config_templates/config.yml"
        )

        with open(config_filename) as config:
            options = yaml.safe_load(config)[CONFIG_GROUP_NAME]["options"]
            file_defaults = {
                option: default for (option, value) in options.items() if (default := value.get("default"))
            }

        assert len(file_defaults) == len(CONFIG_DEFAULTS)
        for key in file_defaults.keys():
            assert file_defaults[key] == CONFIG_DEFAULTS[key]

    def test_execute(self, mock_airflow_key):
        """Test execution from end-to-end."""
        airflow_key = mock_airflow_key()

        self.executor.ecs.run_task.return_value = {
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

        assert 0 == len(self.executor.pending_tasks)
        self.executor.execute_async(airflow_key, mock_cmd)
        assert 1 == len(self.executor.pending_tasks)

        self.executor.attempt_task_runs()
        self.executor.ecs.run_task.assert_called_once()

        # Task is stored in active worker.
        assert 1 == len(self.executor.active_workers)
        assert ARN1 in self.executor.active_workers.task_by_key(airflow_key).task_arn

    def test_failed_execute_api(self):
        """Test what happens when ECS refuses to execute a task."""
        self.executor.ecs.run_task.return_value = {
            "tasks": [],
            "failures": [
                {"arn": ARN1, "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        self.executor.execute_async(mock_airflow_key, mock_cmd)

        # No matter what, don't schedule until run_task becomes successful.
        for _ in range(self.executor.MAX_FAILURE_CHECKS * 2):
            self.executor.attempt_task_runs()
            # Task is not stored in active workers.
            assert len(self.executor.active_workers) == 0

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_sync(self, success_mock, fail_mock):
        """Test synch from end-to-end."""
        self._mock_sync()

        self.executor.sync_running_tasks()
        self.executor.ecs.describe_tasks.assert_called_once()

        # Task is not stored in active workers.
        assert len(self.executor.active_workers) == 0
        # Task is immediately succeeded.
        success_mock.assert_called_once()
        fail_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    @mock.patch.object(EcsTaskCollection, "get_all_arns", return_value=[])
    def test_sync_short_circuits_with_no_arns(self, _, success_mock, fail_mock):
        self._mock_sync()

        self.executor.sync_running_tasks()

        self.executor.ecs.describe_tasks.assert_not_called()
        fail_mock.assert_not_called()
        success_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_failed_sync(self, success_mock, fail_mock):
        """Test success and failure states."""
        self._mock_sync(State.FAILED)

        self.executor.sync()
        self.executor.ecs.describe_tasks.assert_called_once()

        # Task is not stored in active workers.
        assert len(self.executor.active_workers) == 0
        # Task is immediately succeeded.
        fail_mock.assert_called_once()
        success_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_removed_sync(self, fail_mock, success_mock):
        """A removed task will increment failure count but call neither fail() nor success()."""
        self._mock_sync(expected_state=State.REMOVED, set_task_state=State.REMOVED)
        task_instance_key = self.executor.active_workers.arn_to_key[ARN1]

        self.executor.sync_running_tasks()

        assert ARN1 in self.executor.active_workers.get_all_arns()
        assert self.executor.active_workers.key_to_failure_counts[task_instance_key] == 1
        fail_mock.assert_not_called()
        success_mock.assert_not_called()

    @mock.patch.object(BaseExecutor, "fail")
    @mock.patch.object(BaseExecutor, "success")
    def test_failed_sync_api(self, success_mock, fail_mock):
        """Test what happens when ECS sync fails for certain tasks repeatedly."""
        self._mock_sync()
        self.executor.ecs.describe_tasks.return_value = {
            "tasks": [],
            "failures": [
                {"arn": ARN1, "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        # Call Sync 3 times with failures.
        for check_count in range(AwsEcsExecutor.MAX_FAILURE_CHECKS):
            self.executor.sync_running_tasks()
            assert self.executor.ecs.describe_tasks.call_count == check_count + 1

            # Ensure task arn is not removed from active.
            assert ARN1 in self.executor.active_workers.get_all_arns()

            # Task is neither failed nor succeeded.
            fail_mock.assert_not_called()
            success_mock.assert_not_called()

        # Last call should fail the task.
        self.executor.sync_running_tasks()
        assert ARN1 not in self.executor.active_workers.get_all_arns()
        fail_mock.assert_called()
        success_mock.assert_not_called()

    def test_terminate(self):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit."""
        self._mock_sync(State.FAILED)

        self.executor.terminate()

        self.executor.ecs.stop_task.assert_called()

    def test_end(self):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        sync_call_count = 0
        sync_func = self.executor.sync

        def sync_mock():
            """Mock won't work here, because we actually want to call the 'sync' func."""
            nonlocal sync_call_count
            sync_func()
            sync_call_count += 1

        self.executor.sync = sync_mock
        self._mock_sync(State.FAILED)

        self.executor.end(heartbeat_interval=0)

        self.executor.sync = sync_func

    @mock.patch.object(time, "sleep", return_value=None)
    def test_end_with_queued_tasks_will_wait(self, mock_sleep):
        """Test that executor can end successfully; waiting for all tasks to naturally exit."""
        sync_call_count = 0
        sync_func = self.executor.sync

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
                self.executor.active_workers.update_task(
                    EcsExecutorTask(
                        ARN2,
                        "STOPPED",
                        "STOPPED",
                        {"exit_code": 0, "name": "some-ecs-container", "last_status": "STOPPED"},
                    )
                )
                self.response_task2_json.update({"desiredStatus": "STOPPED", "lastStatus": "STOPPED"})
                self.executor.ecs.describe_tasks.return_value = {
                    "tasks": [self.response_task2_json],
                    "failures": [],
                }

        self.executor.sync = sync_mock

        self._add_mock_task(ARN1)
        self._add_mock_task(ARN2)

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

        self.executor.ecs.describe_tasks.return_value = {
            "tasks": [self.response_task1_json, self.response_task2_json],
            "failures": [],
        }

        self.executor.end(heartbeat_interval=0)

        self.executor.sync = sync_func

        assert sync_call_count == 2

    @pytest.mark.parametrize(
        "bad_config",
        [
            pytest.param({"name": "bad_robot"}, id="executor_config_can_not_overwrite_name"),
            pytest.param({"command": "bad_robot"}, id="executor_config_can_not_overwrite_command"),
        ],
    )
    def test_executor_config_exceptions(self, bad_config):
        with pytest.raises(ValueError) as raised:
            self.executor.execute_async(mock_airflow_key, mock_cmd, executor_config=bad_config)

        assert raised.match('Executor Config should never override "name" or "command"')
        assert 0 == len(self.executor.pending_tasks)

    def test_container_not_found(self):
        # Force a container name mismatch
        os.environ[
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.CONTAINER_NAME}".upper()
        ] = "bad-container-name"

        with pytest.raises(KeyError) as raised:
            AwsEcsExecutor()
        assert raised.match(
            re.escape(
                "Rendered JSON template does not contain key "
                '"overrides[containerOverrides][containers][x][command]"'
            )
        )
        assert 0 == len(self.executor.pending_tasks)

    def _mock_executor(self) -> None:
        """Mock ECS such that there's nothing wrong."""
        executor = AwsEcsExecutor()
        executor.start()

        # Replace boto3 ECS client with mock.
        ecs_mock = mock.Mock(spec=executor.ecs)
        run_task_ret_val = {"tasks": [{"taskArn": ARN1}], "failures": []}
        ecs_mock.run_task.return_value = run_task_ret_val
        executor.ecs = ecs_mock

        self.executor = executor

    @staticmethod
    def _set_conf():
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.REGION}".upper()] = "us-west-1"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.CLUSTER}".upper()] = "some-cluster"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.CONTAINER_NAME}".upper()] = "container-name"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.TASK_DEFINITION}".upper()] = "some-task-def"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.LAUNCH_TYPE}".upper()] = "FARGATE"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.PLATFORM_VERSION}".upper()] = "LATEST"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.ASSIGN_PUBLIC_IP}".upper()] = "False"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.SECURITY_GROUPS}".upper()] = "sg1,sg2"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.SUBNETS}".upper()] = "sub1,sub2"

    @staticmethod
    def _unset_conf():
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def _mock_sync(
        self, expected_state: State = State.SUCCESS, set_task_state: State = State.RUNNING
    ) -> None:
        """Mock ECS to the expected state."""
        self._add_mock_task(ARN1, set_task_state)

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

        self.executor.ecs.describe_tasks.return_value = {"tasks": [response_task_json], "failures": []}

    def _add_mock_task(self, arn, state: State = State.RUNNING):
        task = mock_task(arn, state)
        self.executor.active_workers.add_task(task, mock.Mock(spec=tuple), mock_queue, mock_cmd, mock_config)


class TestEcsExecutorConfig:
    def teardown_method(self) -> None:
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def test_subnets_required(self):
        assert f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.SUBNETS}".upper() not in os.environ

        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.REGION}".upper()] = "us-west-1"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.CLUSTER}".upper()] = "some-cluster"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.CONTAINER_NAME}".upper()] = "container-name"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.TASK_DEFINITION}".upper()] = "some-task-def"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.LAUNCH_TYPE}".upper()] = "FARGATE"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.PLATFORM_VERSION}".upper()] = "LATEST"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.ASSIGN_PUBLIC_IP}".upper()] = "False"
        os.environ[f"AIRFLOW__{CONFIG_GROUP_NAME}__{EcsConfigKeys.SECURITY_GROUPS}".upper()] = "sg1,sg2"
        from airflow.providers.amazon.aws.executors.ecs import ecs_executor_config

        with pytest.raises(ValueError) as raised:
            # The executor config module runs code and sets values at the
            # module level. So to trigger any changes, the module must be
            # reloaded
            reload(ecs_executor_config)
        assert raised.match("At least one subnet is required to run a task.")
