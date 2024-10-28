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

"""
AWS ECS Executor Utilities.

Data classes and utility functions used by the ECS executor.
"""

from __future__ import annotations

import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List

from inflection import camelize

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey

CommandType = List[str]
ExecutorConfigFunctionType = Callable[[CommandType], dict]
ExecutorConfigType = Dict[str, Any]

ECS_LAUNCH_TYPE_EC2 = "EC2"
ECS_LAUNCH_TYPE_FARGATE = "FARGATE"

CONFIG_GROUP_NAME = "aws_ecs_executor"

CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "max_run_task_attempts": "3",
    "assign_public_ip": "False",
    "platform_version": "LATEST",
    "check_health_on_startup": "True",
}


@dataclass
class EcsQueuedTask:
    """Represents an ECS task that is queued. The task will be run in the next heartbeat."""

    key: TaskInstanceKey
    command: CommandType
    queue: str
    executor_config: ExecutorConfigType
    attempt_number: int
    next_attempt_time: datetime.datetime


@dataclass
class EcsTaskInfo:
    """Contains information about a currently running ECS task."""

    cmd: CommandType
    queue: str
    config: ExecutorConfigType


class RunTaskKwargsConfigKeys(BaseConfigKeys):
    """Keys loaded into the config which are valid ECS run_task kwargs."""

    ASSIGN_PUBLIC_IP = "assign_public_ip"
    CAPACITY_PROVIDER_STRATEGY = "capacity_provider_strategy"
    CLUSTER = "cluster"
    CONTAINER_NAME = "container_name"
    LAUNCH_TYPE = "launch_type"
    PLATFORM_VERSION = "platform_version"
    SECURITY_GROUPS = "security_groups"
    SUBNETS = "subnets"
    TASK_DEFINITION = "task_definition"


class AllEcsConfigKeys(RunTaskKwargsConfigKeys):
    """All keys loaded into the config which are related to the ECS Executor."""

    AWS_CONN_ID = "conn_id"
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
    MAX_RUN_TASK_ATTEMPTS = "max_run_task_attempts"
    REGION_NAME = "region_name"
    RUN_TASK_KWARGS = "run_task_kwargs"


class EcsExecutorException(Exception):
    """Thrown when something unexpected has occurred within the ECS ecosystem."""


class EcsExecutorTask:
    """Data Transfer Object for an ECS Task."""

    def __init__(
        self,
        task_arn: str,
        last_status: str,
        desired_status: str,
        containers: list[dict[str, Any]],
        started_at: Any | None = None,
        stopped_reason: str | None = None,
        external_executor_id: str | None = None,
    ):
        self.task_arn = task_arn
        self.last_status = last_status
        self.desired_status = desired_status
        self.containers = containers
        self.started_at = started_at
        self.stopped_reason = stopped_reason
        self.external_executor_id = external_executor_id

    def get_task_state(self) -> str:
        """
        Determine the state of an ECS task based on its status and other relevant attributes.

        It can return one of the following statuses:
            QUEUED - Task is being provisioned.
            RUNNING - Task is launched on ECS.
            REMOVED - Task provisioning has failed for some reason. See `stopped_reason`.
            FAILED - Task is completed and at least one container has failed.
            SUCCESS - Task is completed and all containers have succeeded.
        """
        if self.last_status == "RUNNING":
            return State.RUNNING
        elif self.desired_status == "RUNNING":
            return State.QUEUED
        is_finished = self.desired_status == "STOPPED"
        has_exit_codes = all(["exit_code" in x for x in self.containers])
        # Sometimes ECS tasks may time out.
        if not self.started_at and is_finished:
            return State.REMOVED
        if not is_finished or not has_exit_codes:
            return State.RUNNING
        all_containers_succeeded = all([x["exit_code"] == 0 for x in self.containers])
        return State.SUCCESS if all_containers_succeeded else State.FAILED

    def __repr__(self):
        """Return a string representation of the ECS task."""
        return f"({self.task_arn}, {self.last_status}->{self.desired_status}, {self.get_task_state()})"


class EcsTaskCollection:
    """A five-way dictionary between Airflow task ids, Airflow cmds, ECS ARNs, and ECS task objects."""

    def __init__(self):
        self.key_to_arn: dict[TaskInstanceKey, str] = {}
        self.arn_to_key: dict[str, TaskInstanceKey] = {}
        self.tasks: dict[str, EcsExecutorTask] = {}
        self.key_to_failure_counts: dict[TaskInstanceKey, int] = defaultdict(int)
        self.key_to_task_info: dict[TaskInstanceKey, EcsTaskInfo] = {}

    def add_task(
        self,
        task: EcsExecutorTask,
        airflow_task_key: TaskInstanceKey,
        queue: str,
        airflow_cmd: CommandType,
        exec_config: ExecutorConfigType,
        attempt_number: int,
    ):
        """Add a task to the collection."""
        arn = task.task_arn
        self.tasks[arn] = task
        self.key_to_arn[airflow_task_key] = arn
        self.arn_to_key[arn] = airflow_task_key
        self.key_to_task_info[airflow_task_key] = EcsTaskInfo(
            airflow_cmd, queue, exec_config
        )
        self.key_to_failure_counts[airflow_task_key] = attempt_number

    def update_task(self, task: EcsExecutorTask):
        """Update the state of the given task based on task ARN."""
        self.tasks[task.task_arn] = task

    def task_by_key(self, task_key: TaskInstanceKey) -> EcsExecutorTask:
        """Get a task by Airflow Instance Key."""
        arn = self.key_to_arn[task_key]
        return self.task_by_arn(arn)

    def task_by_arn(self, arn) -> EcsExecutorTask:
        """Get a task by AWS ARN."""
        return self.tasks[arn]

    def pop_by_key(self, task_key: TaskInstanceKey) -> EcsExecutorTask:
        """Delete task from collection based off of Airflow Task Instance Key."""
        arn = self.key_to_arn[task_key]
        task = self.tasks[arn]
        del self.key_to_arn[task_key]
        del self.key_to_task_info[task_key]
        del self.arn_to_key[arn]
        del self.tasks[arn]
        if task_key in self.key_to_failure_counts:
            del self.key_to_failure_counts[task_key]
        return task

    def get_all_arns(self) -> list[str]:
        """Get all AWS ARNs in collection."""
        return list(self.key_to_arn.values())

    def get_all_task_keys(self) -> list[TaskInstanceKey]:
        """Get all Airflow Task Keys in collection."""
        return list(self.key_to_arn.keys())

    def failure_count_by_key(self, task_key: TaskInstanceKey) -> int:
        """Get the number of times a task has failed given an Airflow Task Key."""
        return self.key_to_failure_counts[task_key]

    def increment_failure_count(self, task_key: TaskInstanceKey):
        """Increment the failure counter given an Airflow Task Key."""
        self.key_to_failure_counts[task_key] += 1

    def info_by_key(self, task_key: TaskInstanceKey) -> EcsTaskInfo:
        """Get the Airflow Command given an Airflow task key."""
        return self.key_to_task_info[task_key]

    def __getitem__(self, value):
        """Get a task by AWS ARN."""
        return self.task_by_arn(value)

    def __len__(self):
        """Determine the number of tasks in collection."""
        return len(self.tasks)


def _recursive_flatten_dict(nested_dict):
    """
    Recursively unpack a nested dict and return it as a flat dict.

    For example, _flatten_dict({'a': 'a', 'b': 'b', 'c': {'d': 'd'}}) returns {'a': 'a', 'b': 'b', 'd': 'd'}.
    """
    items = []
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            items.extend(_recursive_flatten_dict(value).items())
        else:
            items.append((key, value))
    return dict(items)


def parse_assign_public_ip(assign_public_ip, is_launch_type_ec2=False):
    """Convert "assign_public_ip" from True/False to ENABLE/DISABLE."""
    # If the launch type is EC2, you cannot/should not provide the assignPublicIp parameter (which is
    # specific to Fargate)
    if not is_launch_type_ec2:
        return "ENABLED" if assign_public_ip == "True" else "DISABLED"


def camelize_dict_keys(nested_dict) -> dict:
    """Accept a potentially nested dictionary and recursively convert all keys into camelCase."""
    result = {}
    for key, value in nested_dict.items():
        new_key = camelize(key, uppercase_first_letter=False)
        if isinstance(value, dict) and (key.lower() != "tags"):
            # The key name on tags can be whatever the user wants, and we should not mess with them.
            result[new_key] = camelize_dict_keys(value)
        else:
            result[new_key] = nested_dict[key]
    return result
