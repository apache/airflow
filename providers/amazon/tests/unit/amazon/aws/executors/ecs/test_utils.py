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

"""Unit tests for AWS ECS Executor Utilities."""

from __future__ import annotations

import datetime
from unittest import mock

from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.amazon.aws.executors.ecs.utils import (
    AllEcsConfigKeys,
    EcsExecutorException,
    EcsExecutorTask,
    EcsQueuedTask,
    EcsTaskCollection,
    EcsTaskInfo,
    RunTaskKwargsConfigKeys,
    _recursive_flatten_dict,
    camelize_dict_keys,
    parse_assign_public_ip,
)
from airflow.utils.state import State


class TestEcsQueuedTask:
    """Test EcsQueuedTask dataclass."""

    def test_ecs_queued_task_creation(self):
        """Test EcsQueuedTask object creation."""
        key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)
        command = ["echo", "hello"]
        queue = "default"
        executor_config = {"key": "value"}
        attempt_number = 1
        next_attempt_time = datetime.datetime.now()

        queued_task = EcsQueuedTask(
            key=key,
            command=command,
            queue=queue,
            executor_config=executor_config,
            attempt_number=attempt_number,
            next_attempt_time=next_attempt_time,
        )

        assert queued_task.key == key
        assert queued_task.command == command
        assert queued_task.queue == queue
        assert queued_task.executor_config == executor_config
        assert queued_task.attempt_number == attempt_number
        assert queued_task.next_attempt_time == next_attempt_time


class TestEcsTaskInfo:
    """Test EcsTaskInfo dataclass."""

    def test_ecs_task_info_creation(self):
        """Test EcsTaskInfo object creation."""
        cmd = ["echo", "hello"]
        queue = "default"
        config = {"key": "value"}

        task_info = EcsTaskInfo(cmd=cmd, queue=queue, config=config)

        assert task_info.cmd == cmd
        assert task_info.queue == queue
        assert task_info.config == config


class TestRunTaskKwargsConfigKeys:
    """Test RunTaskKwargsConfigKeys class."""

    def test_config_keys_values(self):
        """Test that config keys have correct values."""
        assert RunTaskKwargsConfigKeys.ASSIGN_PUBLIC_IP == "assign_public_ip"
        assert RunTaskKwargsConfigKeys.CAPACITY_PROVIDER_STRATEGY == "capacity_provider_strategy"
        assert RunTaskKwargsConfigKeys.CLUSTER == "cluster"
        assert RunTaskKwargsConfigKeys.CONTAINER_NAME == "container_name"
        assert RunTaskKwargsConfigKeys.LAUNCH_TYPE == "launch_type"
        assert RunTaskKwargsConfigKeys.PLATFORM_VERSION == "platform_version"
        assert RunTaskKwargsConfigKeys.SECURITY_GROUPS == "security_groups"
        assert RunTaskKwargsConfigKeys.SUBNETS == "subnets"
        assert RunTaskKwargsConfigKeys.TASK_DEFINITION == "task_definition"


class TestAllEcsConfigKeys:
    """Test AllEcsConfigKeys class."""

    def test_all_config_keys_values(self):
        """Test that all config keys have correct values."""
        # Test inherited keys
        assert AllEcsConfigKeys.ASSIGN_PUBLIC_IP == "assign_public_ip"
        assert AllEcsConfigKeys.CLUSTER == "cluster"

        # Test additional keys
        assert AllEcsConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllEcsConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"
        assert AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS == "max_run_task_attempts"
        assert AllEcsConfigKeys.REGION_NAME == "region_name"
        assert AllEcsConfigKeys.RUN_TASK_KWARGS == "run_task_kwargs"


class TestEcsExecutorException:
    """Test EcsExecutorException class."""

    def test_ecs_executor_exception_creation(self):
        """Test EcsExecutorException creation."""
        exception = EcsExecutorException("Test error message")
        assert str(exception) == "Test error message"
        assert isinstance(exception, Exception)


class TestEcsExecutorTask:
    """Test EcsExecutorTask class."""

    def test_ecs_executor_task_creation(self):
        """Test EcsExecutorTask object creation."""
        task_arn = "arn:aws:ecs:us-east-1:123456789012:task/test-task"
        last_status = "RUNNING"
        desired_status = "RUNNING"
        containers = [{"name": "container1", "exit_code": 0}]
        started_at = datetime.datetime.now()
        stopped_reason = None
        external_executor_id = "test-executor-id"

        task = EcsExecutorTask(
            task_arn=task_arn,
            last_status=last_status,
            desired_status=desired_status,
            containers=containers,
            started_at=started_at,
            stopped_reason=stopped_reason,
            external_executor_id=external_executor_id,
        )

        assert task.task_arn == task_arn
        assert task.last_status == last_status
        assert task.desired_status == desired_status
        assert task.containers == containers
        assert task.started_at == started_at
        assert task.stopped_reason == stopped_reason
        assert task.external_executor_id == external_executor_id

    def test_get_task_state_running(self):
        """Test get_task_state returns RUNNING when last_status is RUNNING."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="RUNNING",
            desired_status="RUNNING",
            containers=[{"name": "container1", "exit_code": 0}],
        )
        assert task.get_task_state() == State.RUNNING

    def test_get_task_state_queued(self):
        """Test get_task_state returns QUEUED when desired_status is RUNNING but last_status is not RUNNING."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="PENDING",
            desired_status="RUNNING",
            containers=[{"name": "container1", "exit_code": 0}],
        )
        assert task.get_task_state() == State.QUEUED

    def test_get_task_state_removed_timeout(self):
        """Test get_task_state returns REMOVED when task timed out."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[{"name": "container1", "exit_code": 0}],
            started_at=None,
        )
        assert task.get_task_state() == State.REMOVED

    def test_get_task_state_running_not_finished(self):
        """Test get_task_state returns RUNNING when task is not finished."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="RUNNING",
            desired_status="RUNNING",
            containers=[{"name": "container1"}],  # No exit_code
        )
        assert task.get_task_state() == State.RUNNING

    def test_get_task_state_success(self):
        """Test get_task_state returns SUCCESS when all containers succeeded."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[
                {"name": "container1", "exit_code": 0},
                {"name": "container2", "exit_code": 0},
            ],
            started_at=datetime.datetime.now(),
        )
        assert task.get_task_state() == State.SUCCESS

    def test_get_task_state_failed(self):
        """Test get_task_state returns FAILED when at least one container failed."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[
                {"name": "container1", "exit_code": 0},
                {"name": "container2", "exit_code": 1},
            ],
            started_at=datetime.datetime.now(),
        )
        assert task.get_task_state() == State.FAILED

    def test_repr(self):
        """Test __repr__ method."""
        task = EcsExecutorTask(
            task_arn="arn:aws:ecs:us-east-1:123456789012:task/test-task",
            last_status="RUNNING",
            desired_status="RUNNING",
            containers=[{"name": "container1", "exit_code": 0}],
        )
        expected = "(arn:aws:ecs:us-east-1:123456789012:task/test-task, RUNNING->RUNNING, running)"
        assert repr(task) == expected


class TestEcsTaskCollection:
    """Test EcsTaskCollection class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.collection = EcsTaskCollection()
        self.task_key = TaskInstanceKey(
            dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1
        )
        self.task_arn = "arn:aws:ecs:us-east-1:123456789012:task/test-task"
        self.task = EcsExecutorTask(
            task_arn=self.task_arn,
            last_status="RUNNING",
            desired_status="RUNNING",
            containers=[{"name": "container1", "exit_code": 0}],
        )
        self.cmd = ["echo", "hello"]
        self.queue = "default"
        self.exec_config = {"key": "value"}

    def test_init(self):
        """Test EcsTaskCollection initialization."""
        collection = EcsTaskCollection()
        assert collection.key_to_arn == {}
        assert collection.arn_to_key == {}
        assert collection.tasks == {}
        assert collection.key_to_failure_counts == {}
        assert collection.key_to_task_info == {}

    def test_add_task(self):
        """Test adding a task to the collection."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        assert self.collection.key_to_arn[self.task_key] == self.task_arn
        assert self.collection.arn_to_key[self.task_arn] == self.task_key
        assert self.collection.tasks[self.task_arn] == self.task
        assert self.collection.key_to_failure_counts[self.task_key] == 1
        assert self.collection.key_to_task_info[self.task_key].cmd == self.cmd
        assert self.collection.key_to_task_info[self.task_key].queue == self.queue
        assert self.collection.key_to_task_info[self.task_key].config == self.exec_config

    def test_update_task(self):
        """Test updating a task in the collection."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        updated_task = EcsExecutorTask(
            task_arn=self.task_arn,
            last_status="STOPPED",
            desired_status="STOPPED",
            containers=[{"name": "container1", "exit_code": 0}],
        )
        self.collection.update_task(updated_task)

        assert self.collection.tasks[self.task_arn].last_status == "STOPPED"
        assert self.collection.tasks[self.task_arn].desired_status == "STOPPED"

    def test_task_by_key(self):
        """Test getting a task by Airflow task key."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        retrieved_task = self.collection.task_by_key(self.task_key)
        assert retrieved_task == self.task

    def test_task_by_arn(self):
        """Test getting a task by ARN."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        retrieved_task = self.collection.task_by_arn(self.task_arn)
        assert retrieved_task == self.task

    def test_pop_by_key(self):
        """Test removing a task by Airflow task key."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        popped_task = self.collection.pop_by_key(self.task_key)
        assert popped_task == self.task
        assert self.task_key not in self.collection.key_to_arn
        assert self.task_arn not in self.collection.arn_to_key
        assert self.task_arn not in self.collection.tasks
        assert self.task_key not in self.collection.key_to_task_info
        assert self.task_key not in self.collection.key_to_failure_counts

    def test_get_all_arns(self):
        """Test getting all ARNs from collection."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        arns = self.collection.get_all_arns()
        assert arns == [self.task_arn]

    def test_get_all_task_keys(self):
        """Test getting all task keys from collection."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        keys = self.collection.get_all_task_keys()
        assert keys == [self.task_key]

    def test_failure_count_by_key(self):
        """Test getting failure count by task key."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=3,
        )

        failure_count = self.collection.failure_count_by_key(self.task_key)
        assert failure_count == 3

    def test_increment_failure_count(self):
        """Test incrementing failure count."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        self.collection.increment_failure_count(self.task_key)
        assert self.collection.key_to_failure_counts[self.task_key] == 2

    def test_info_by_key(self):
        """Test getting task info by task key."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        task_info = self.collection.info_by_key(self.task_key)
        assert task_info.cmd == self.cmd
        assert task_info.queue == self.queue
        assert task_info.config == self.exec_config

    def test_getitem(self):
        """Test __getitem__ method."""
        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        retrieved_task = self.collection[self.task_arn]
        assert retrieved_task == self.task

    def test_len(self):
        """Test __len__ method."""
        assert len(self.collection) == 0

        self.collection.add_task(
            task=self.task,
            airflow_task_key=self.task_key,
            queue=self.queue,
            airflow_cmd=self.cmd,
            exec_config=self.exec_config,
            attempt_number=1,
        )

        assert len(self.collection) == 1


class TestRecursiveFlattenDict:
    """Test _recursive_flatten_dict function."""

    def test_flat_dict(self):
        """Test flattening a flat dictionary."""
        input_dict = {"a": "value1", "b": "value2"}
        expected = {"a": "value1", "b": "value2"}
        assert _recursive_flatten_dict(input_dict) == expected

    def test_nested_dict(self):
        """Test flattening a nested dictionary."""
        input_dict = {"a": "value1", "b": {"c": "value2", "d": "value3"}}
        expected = {"a": "value1", "c": "value2", "d": "value3"}
        assert _recursive_flatten_dict(input_dict) == expected

    def test_deeply_nested_dict(self):
        """Test flattening a deeply nested dictionary."""
        input_dict = {"a": {"b": {"c": {"d": "value"}}}}
        expected = {"d": "value"}
        assert _recursive_flatten_dict(input_dict) == expected

    def test_mixed_dict(self):
        """Test flattening a dictionary with mixed nested and flat values."""
        input_dict = {"a": "value1", "b": {"c": "value2"}, "d": "value3"}
        expected = {"a": "value1", "c": "value2", "d": "value3"}
        assert _recursive_flatten_dict(input_dict) == expected

    def test_empty_dict(self):
        """Test flattening an empty dictionary."""
        assert _recursive_flatten_dict({}) == {}

    def test_dict_with_empty_nested_dict(self):
        """Test flattening a dictionary with empty nested dictionaries."""
        input_dict = {"a": "value1", "b": {}}
        expected = {"a": "value1"}
        assert _recursive_flatten_dict(input_dict) == expected


class TestParseAssignPublicIp:
    """Test parse_assign_public_ip function."""

    def test_parse_assign_public_ip_true_fargate(self):
        """Test parsing assign_public_ip=True for Fargate launch type."""
        result = parse_assign_public_ip("True", is_launch_type_ec2=False)
        assert result == "ENABLED"

    def test_parse_assign_public_ip_false_fargate(self):
        """Test parsing assign_public_ip=False for Fargate launch type."""
        result = parse_assign_public_ip("False", is_launch_type_ec2=False)
        assert result == "DISABLED"

    def test_parse_assign_public_ip_true_ec2(self):
        """Test parsing assign_public_ip=True for EC2 launch type."""
        result = parse_assign_public_ip("True", is_launch_type_ec2=True)
        assert result is None

    def test_parse_assign_public_ip_false_ec2(self):
        """Test parsing assign_public_ip=False for EC2 launch type."""
        result = parse_assign_public_ip("False", is_launch_type_ec2=True)
        assert result is None

    def test_parse_assign_public_ip_default_fargate(self):
        """Test parsing assign_public_ip with default for Fargate launch type."""
        result = parse_assign_public_ip("False", is_launch_type_ec2=False)
        assert result == "DISABLED"


class TestCamelizeDictKeys:
    """Test camelize_dict_keys function."""

    def test_camelize_flat_dict(self):
        """Test camelizing keys in a flat dictionary."""
        input_dict = {"test_key": "value", "another_key": "value2"}
        expected = {"testKey": "value", "anotherKey": "value2"}
        assert camelize_dict_keys(input_dict) == expected

    def test_camelize_nested_dict(self):
        """Test camelizing keys in a nested dictionary."""
        input_dict = {"test_key": {"nested_key": "value"}}
        expected = {"testKey": {"nestedKey": "value"}}
        assert camelize_dict_keys(input_dict) == expected

    def test_camelize_dict_with_tags(self):
        """Test that tags key is not camelized."""
        input_dict = {"test_key": "value", "tags": {"custom_key": "custom_value"}}
        expected = {"testKey": "value", "tags": {"custom_key": "custom_value"}}
        assert camelize_dict_keys(input_dict) == expected

    def test_camelize_dict_with_tags_uppercase(self):
        """Test that TAGS key (uppercase) gets camelized to tAGS."""
        input_dict = {"test_key": "value", "TAGS": {"custom_key": "custom_value"}}
        expected = {"testKey": "value", "tAGS": {"custom_key": "custom_value"}}
        assert camelize_dict_keys(input_dict) == expected

    def test_camelize_dict_with_mixed_case_tags(self):
        """Test that mixed case 'Tags' key gets camelized to tags."""
        input_dict = {"test_key": "value", "Tags": {"custom_key": "custom_value"}}
        expected = {"testKey": "value", "tags": {"custom_key": "custom_value"}}
        assert camelize_dict_keys(input_dict) == expected

    def test_camelize_empty_dict(self):
        """Test camelizing an empty dictionary."""
        assert camelize_dict_keys({}) == {}

    def test_camelize_dict_with_non_dict_values(self):
        """Test camelizing dictionary with non-dict values."""
        input_dict = {"test_key": ["list", "values"], "another_key": 123}
        expected = {"testKey": ["list", "values"], "anotherKey": 123}
        assert camelize_dict_keys(input_dict) == expected

    @mock.patch("airflow.providers.amazon.aws.executors.ecs.utils.camelize")
    def test_camelize_dict_keys_with_mock(self, mock_camelize):
        """Test camelize_dict_keys with mocked camelize function."""
        mock_camelize.side_effect = lambda x, uppercase_first_letter=False: f"camelized_{x}"

        input_dict = {"test_key": {"nested_key": "value"}}
        result = camelize_dict_keys(input_dict)

        expected = {"camelized_test_key": {"camelized_nested_key": "value"}}
        assert result == expected
        mock_camelize.assert_called()
