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
from unittest import mock

from airflow.providers.amazon.aws.executors.ecs_fargate_executor import (
    AwsEcsFargateExecutor,
    BotoTaskSchema,
    EcsFargateTask,
    EcsFargateTaskCollection,
)
from airflow.utils.state import State

from .botocore_helper import assert_botocore_call, get_botocore_model


def set_conf():
    os.environ["AIRFLOW__ECS_FARGATE__REGION"] = "us-west-1"
    os.environ["AIRFLOW__ECS_FARGATE__CLUSTER"] = "some-cluster"
    os.environ["AIRFLOW__ECS_FARGATE__CONTAINER_NAME"] = "some-container-name"
    os.environ["AIRFLOW__ECS_FARGATE__TASK_DEFINITION"] = "some-task-def"
    os.environ["AIRFLOW__ECS_FARGATE__LAUNCH_TYPE"] = "FARGATE"
    os.environ["AIRFLOW__ECS_FARGATE__PLATFORM_VERSION"] = "LATEST"
    os.environ["AIRFLOW__ECS_FARGATE__ASSIGN_PUBLIC_IP"] = "DISABLED"
    os.environ["AIRFLOW__ECS_FARGATE__SECURITY_GROUPS"] = "sg1,sg2"
    os.environ["AIRFLOW__ECS_FARGATE__SUBNETS"] = "sub1,sub2"


def unset_conf():
    for env in os.environ:
        if env.startswith("AIRFLOW__ECS_FARGATE__"):
            os.environ.pop(env)


class TestEcsTaskCollection:
    """Tests EcsTaskCollection Class"""

    def setup_method(self):
        """
        Create an ECS Task Collection and add 2 Airflow tasks. Populates self.collection,
        self.first/second_task, self.first/second_airflow_key, and self.first/second_airflow_cmd.
        """
        self.collection = EcsFargateTaskCollection()
        # Add first task
        self.first_task = mock.Mock(spec=EcsFargateTask)
        self.first_task.task_arn = "001"
        self.first_airflow_key = mock.Mock(spec=tuple)
        self.first_airflow_cmd = mock.Mock(spec=list)
        self.first_airflow_queue = mock.Mock(spec=str)
        self.first_airflow_exec_config = mock.Mock(spec=dict)
        self.collection.add_task(
            self.first_task,
            self.first_airflow_key,
            self.first_airflow_queue,
            self.first_airflow_cmd,
            self.first_airflow_exec_config,
        )
        # Add second task
        self.second_task = mock.Mock(spec=EcsFargateTask)
        self.second_task.task_arn = "002"
        self.second_airflow_key = mock.Mock(spec=tuple)
        self.second_airflow_cmd = mock.Mock(spec=list)
        self.second_airflow_queue = mock.Mock(spec=str)
        self.second_airflow_exec_config = mock.Mock(spec=dict)
        self.collection.add_task(
            self.second_task,
            self.second_airflow_key,
            self.second_airflow_queue,
            self.second_airflow_cmd,
            self.second_airflow_exec_config,
        )

    def test_get_and_add(self):
        """Test add_task, task_by_arn, cmd_by_key"""
        assert len(self.collection) == 2

        # Check basic get for first task
        assert self.collection.task_by_arn("001") == self.first_task
        assert self.collection["001"] == self.first_task
        assert self.collection.task_by_key(self.first_airflow_key) == self.first_task
        assert self.collection.info_by_key(self.first_airflow_key).cmd == self.first_airflow_cmd
        assert self.collection.info_by_key(self.first_airflow_key).queue == self.first_airflow_queue
        assert self.collection.info_by_key(self.first_airflow_key).config == self.first_airflow_exec_config

        # Check basic get for second task
        assert self.collection.task_by_arn("002") == self.second_task
        assert self.collection["002"] == self.second_task
        assert self.collection.task_by_key(self.second_airflow_key) == self.second_task
        assert self.collection.info_by_key(self.second_airflow_key).cmd == self.second_airflow_cmd
        assert self.collection.info_by_key(self.second_airflow_key).queue == self.second_airflow_queue
        assert self.collection.info_by_key(self.second_airflow_key).config == self.second_airflow_exec_config

    def test_list(self):
        """Test get_all_arns() and get_all_task_keys()"""
        # Check basic list by ARNs & airflow-task-keys
        assert self.collection.get_all_arns() == ["001", "002"]
        assert self.collection.get_all_task_keys() == [self.first_airflow_key, self.second_airflow_key]

    def test_pop(self):
        """Test pop_by_key()"""
        # pop first task & ensure that it's removed
        assert self.collection.pop_by_key(self.first_airflow_key) == self.first_task
        assert "001" not in self.collection.get_all_arns()

    def test_update(self):
        """Test update_task"""
        # update arn with new task object
        assert self.collection["001"] == self.first_task
        updated_task = mock.Mock(spec=EcsFargateTask)
        updated_task.task_arn = "001"
        self.collection.update_task(updated_task)
        assert self.collection["001"] == updated_task

    def test_failure(self):
        """Test collection failure increments and counts"""
        assert 0 == self.collection.failure_count_by_key(self.first_airflow_key)
        for i in range(5):
            self.collection.increment_failure_count(self.first_airflow_key)
            assert i + 1 == self.collection.failure_count_by_key(self.first_airflow_key)
        assert 0 == self.collection.failure_count_by_key(self.second_airflow_key)


class TestEcsFargateTask:
    """Tests the EcsFargateTask DTO"""

    def test_queued_tasks(self):
        """Tasks that are pending launch identified as 'queued'"""
        queued_tasks = [
            EcsFargateTask(
                task_arn="AAA", last_status="PROVISIONING", desired_status="RUNNING", containers=[{}]
            ),
            EcsFargateTask(task_arn="BBB", last_status="PENDING", desired_status="RUNNING", containers=[{}]),
            EcsFargateTask(
                task_arn="CCC", last_status="ACTIVATING", desired_status="RUNNING", containers=[{}]
            ),
        ]
        for task in queued_tasks:
            assert State.QUEUED == task.get_task_state()

    def test_running_tasks(self):
        """Tasks that have been launched are identified as 'running'"""
        running_task = EcsFargateTask(
            task_arn="AAA", last_status="RUNNING", desired_status="RUNNING", containers=[{}]
        )
        assert State.RUNNING == running_task.get_task_state()

    def test_removed_tasks(self):
        """Tasks that failed to launch are identified as 'removed'"""
        deprovisioning_tasks = [
            EcsFargateTask(
                task_arn="DDD", last_status="DEACTIVATING", desired_status="STOPPED", containers=[{}]
            ),
            EcsFargateTask(task_arn="EEE", last_status="STOPPING", desired_status="STOPPED", containers=[{}]),
            EcsFargateTask(
                task_arn="FFF", last_status="DEPROVISIONING", desired_status="STOPPED", containers=[{}]
            ),
        ]
        for task in deprovisioning_tasks:
            assert State.REMOVED == task.get_task_state()

        removed_task = EcsFargateTask(
            task_arn="DEAD",
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[{}],
            stopped_reason="Timeout waiting for network interface provisioning to complete.",
        )
        assert State.REMOVED == removed_task.get_task_state()

    def test_stopped_tasks(self):
        """Tasks that have terminated are identified as either 'success' or 'failure'"""
        successful_container = {"exit_code": 0, "last_status": "STOPPED"}
        error_container = {"exit_code": 100, "last_status": "STOPPED"}

        for status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
            success_task = EcsFargateTask(
                task_arn="GOOD",
                last_status=status,
                desired_status="STOPPED",
                stopped_reason="Essential container in task exited",
                started_at=dt.datetime.now(),
                containers=[successful_container],
            )
            assert State.SUCCESS == success_task.get_task_state()

        for status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
            failed_task = EcsFargateTask(
                task_arn="FAIL",
                last_status=status,
                desired_status="STOPPED",
                stopped_reason="Essential container in task exited",
                started_at=dt.datetime.now(),
                containers=[successful_container, successful_container, error_container],
            )
            assert State.FAILED == failed_task.get_task_state()


class TestAwsEcsFargateExecutor:
    """Tests the AWS ECS Executor itself"""

    def setup_method(self) -> None:
        """Creates Botocore Loader (used for asserting botocore calls) and a mocked ecs client"""
        set_conf()
        self.ecs_model = get_botocore_model("ecs")
        self.__set_mocked_executor()

    @classmethod
    def teardown_method(cls) -> None:
        unset_conf()

    def test_execute(self):
        """Test execution from end-to-end"""
        airflow_key = mock.Mock(spec=tuple)
        airflow_cmd = mock.Mock(spec=list)

        self.executor.ecs.run_task.return_value = {
            "tasks": [
                {
                    "taskArn": "001",
                    "lastStatus": "",
                    "desiredStatus": "",
                    "containers": [{"name": "some-ecs-container"}],
                }
            ],
            "failures": [],
        }

        assert 0 == len(self.executor.pending_tasks)
        self.executor.execute_async(airflow_key, airflow_cmd)
        assert 1 == len(self.executor.pending_tasks)

        self.executor.attempt_task_runs()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.run_task.assert_called_once()
        self.assert_botocore_call("RunTask", *self.executor.ecs.run_task.call_args)

        # task is stored in active worker
        assert 1 == len(self.executor.active_workers)
        assert "001" in self.executor.active_workers.task_by_key(airflow_key).task_arn

    def test_failed_execute_api(self):
        """Test what happens when FARGATE refuses to execute a task"""
        self.executor.ecs.run_task.return_value = {
            "tasks": [],
            "failures": [
                {"arn": "001", "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        airflow_key = mock.Mock(spec=tuple)
        airflow_cmd = mock.Mock(spec=list)
        self.executor.execute_async(airflow_key, airflow_cmd)

        # no matter what, don't schedule until run_task becomes successful
        for _ in range(self.executor.MAX_FAILURE_CHECKS * 2):
            self.executor.attempt_task_runs()
            # task is not stored in active workers
            assert len(self.executor.active_workers) == 0

    @mock.patch("airflow.executors.base_executor.BaseExecutor.fail")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.success")
    def test_sync(self, success_mock, fail_mock):
        """Test synch from end-to-end"""
        after_fargate_json = self.__mock_sync()
        loaded_fargate_json = BotoTaskSchema().load(after_fargate_json)
        assert State.SUCCESS == loaded_fargate_json.get_task_state()

        self.executor.sync_running_tasks()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.describe_tasks.assert_called_once()
        self.assert_botocore_call("DescribeTasks", *self.executor.ecs.describe_tasks.call_args)

        # task is not stored in active workers
        assert len(self.executor.active_workers) == 0
        # Task is immediately succeeded
        success_mock.assert_called_once()
        fail_mock.assert_not_called()

    @mock.patch("airflow.executors.base_executor.BaseExecutor.fail")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.success")
    def test_failed_sync(self, success_mock, fail_mock):
        """Test success and failure states"""
        after_fargate_json = self.__mock_sync()

        # set container's exit code to failure
        after_fargate_json["containers"][0]["exitCode"] = 100
        assert State.FAILED == BotoTaskSchema().load(after_fargate_json).get_task_state()
        self.executor.sync()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.describe_tasks.assert_called_once()
        self.assert_botocore_call("DescribeTasks", *self.executor.ecs.describe_tasks.call_args)

        # task is not stored in active workers
        assert len(self.executor.active_workers) == 0
        # Task is immediately succeeded
        fail_mock.assert_called_once()
        success_mock.assert_not_called()

    @mock.patch("airflow.executors.base_executor.BaseExecutor.fail")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.success")
    def test_failed_sync_api(self, success_mock, fail_mock):
        """Test what happens when ECS sync fails for certain tasks repeatedly"""
        self.__mock_sync()
        self.executor.ecs.describe_tasks.return_value = {
            "tasks": [],
            "failures": [
                {"arn": "ABC", "reason": "Sample Failure", "detail": "UnitTest Failure - Please ignore"}
            ],
        }

        # Call Sync 3 times with failures
        for check_count in range(AwsEcsFargateExecutor.MAX_FAILURE_CHECKS):
            self.executor.sync_running_tasks()
            # ensure that run_task is called correctly as defined by Botocore docs
            assert self.executor.ecs.describe_tasks.call_count == check_count + 1
            self.assert_botocore_call("DescribeTasks", *self.executor.ecs.describe_tasks.call_args)

            # Ensure task arn is not removed from active
            assert "ABC" in self.executor.active_workers.get_all_arns()

            # Task is not failed or succeeded
            fail_mock.assert_not_called()
            success_mock.assert_not_called()

        # Last call should fail the task
        self.executor.sync_running_tasks()
        assert "ABC" not in self.executor.active_workers.get_all_arns()
        fail_mock.assert_called()
        success_mock.asswer_not_called()

    def test_terminate(self):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit"""
        after_fargate_task = self.__mock_sync()
        after_fargate_task["containers"][0]["exitCode"] = 100
        assert State.FAILED == BotoTaskSchema().load(after_fargate_task).get_task_state()

        self.executor.terminate()

        self.executor.ecs.stop_task.assert_called()
        self.assert_botocore_call("StopTask", *self.executor.ecs.stop_task.call_args)

    def assert_botocore_call(self, method_name, args, kwargs):
        assert_botocore_call(self.ecs_model, method_name, args, kwargs)

    def test_end(self):
        """Test that executor can end successfully; awaiting for all tasks to naturally exit"""
        sync_call_count = 0
        sync_func = self.executor.sync

        def sync_mock():
            """Mock won't work here, because we actually want to call the 'sync' func"""
            nonlocal sync_call_count
            sync_func()
            sync_call_count += 1

        self.executor.sync = sync_mock
        after_fargate_task = self.__mock_sync()
        after_fargate_task["containers"][0]["exitCode"] = 100
        self.executor.end(heartbeat_interval=0)

        self.executor.sync = sync_func

    def __set_mocked_executor(self):
        """Mock ECS such that there's nothing wrong with anything"""
        executor = AwsEcsFargateExecutor()
        executor.start()

        # replace boto3 ecs client with mock
        ecs_mock = mock.Mock(spec=executor.ecs)
        run_task_ret_val = {"tasks": [{"taskArn": "001"}], "failures": []}
        ecs_mock.run_task.return_value = run_task_ret_val
        executor.ecs = ecs_mock

        self.executor = executor

    def __mock_sync(self):
        """Mock ECS such that there's nothing wrong with anything"""

        # create running fargate instance
        before_fargate_task = mock.Mock(spec=EcsFargateTask)
        before_fargate_task.task_arn = "ABC"
        before_fargate_task.api_failure_count = 0
        before_fargate_task.get_task_state.return_value = State.RUNNING

        airflow_cmd = mock.Mock(spec=list)
        airflow_key = mock.Mock(spec=tuple)
        airflow_queue = mock.Mock(spec=str)
        airflow_exec_conf = mock.Mock(spec=dict)
        self.executor.active_workers.add_task(
            before_fargate_task, airflow_key, airflow_queue, airflow_cmd, airflow_exec_conf
        )

        after_task_json = {
            "taskArn": "ABC",
            "desiredStatus": "STOPPED",
            "lastStatus": "STOPPED",
            "startedAt": dt.datetime.now(),
            "containers": [{"name": "some-ecs-container", "lastStatus": "STOPPED", "exitCode": 0}],
        }
        self.executor.ecs.describe_tasks.return_value = {"tasks": [after_task_json], "failures": []}
        return after_task_json
