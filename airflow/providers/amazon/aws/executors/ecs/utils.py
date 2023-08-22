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

from collections import defaultdict, namedtuple
from typing import TYPE_CHECKING, Any, Callable, Dict, List

from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey

CommandType = List[str]
ExecutorConfigFunctionType = Callable[[CommandType], dict]
EcsQueuedTask = namedtuple("EcsQueuedTask", ("key", "command", "queue", "executor_config"))
ExecutorConfigType = Dict[str, Any]
EcsTaskInfo = namedtuple("EcsTaskInfo", ("cmd", "queue", "config"))

CONFIG_GROUP_NAME = "aws_ecs_executor"

RUN_TASK_KWARG_DEFAULTS = {
    "assign_public_ip": "False",
    "launch_type": "FARGATE",
    "platform_version": "LATEST",
}

CONFIG_DEFAULTS = {"conn_id": "aws_default", **RUN_TASK_KWARG_DEFAULTS}


class EcsConfigKeys:
    """Keys loaded into the config which are related to the ECS Executor."""

    ASSIGN_PUBLIC_IP = "assign_public_ip"
    AWS_CONN_ID = "conn_id"
    CLUSTER = "cluster"
    CONTAINER_NAME = "container_name"
    LAUNCH_TYPE = "launch_type"
    PLATFORM_VERSION = "platform_version"
    REGION = "region"
    RUN_TASK_KWARGS = "run_task_kwargs"
    SECURITY_GROUPS = "security_groups"
    SUBNETS = "subnets"
    TASK_DEFINITION = "task_definition"

    def __iter__(self):
        return iter({value for (key, value) in EcsConfigKeys.__dict__.items() if not key.startswith("__")})


class EcsExecutorException(Exception):
    """Thrown when something unexpected has occurred within the ECS ecosystem."""


class EcsExecutorTask:
    """Data Transfer Object for an ECS Fargate Task."""

    def __init__(
        self,
        task_arn: str,
        last_status: str,
        desired_status: str,
        containers: list[dict[str, Any]],
        started_at: Any | None = None,
        stopped_reason: str | None = None,
    ):
        self.task_arn = task_arn
        self.last_status = last_status
        self.desired_status = desired_status
        self.containers = containers
        self.started_at = started_at
        self.stopped_reason = stopped_reason

    def get_task_state(self) -> str:
        """
        This is the primary logic that handles state in an ECS task.

        It will determine if a status is:
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
    ):
        """Adds a task to the collection."""
        arn = task.task_arn
        self.tasks[arn] = task
        self.key_to_arn[airflow_task_key] = arn
        self.arn_to_key[arn] = airflow_task_key
        self.key_to_task_info[airflow_task_key] = EcsTaskInfo(airflow_cmd, queue, exec_config)

    def update_task(self, task: EcsExecutorTask):
        """Updates the state of the given task based on task ARN."""
        self.tasks[task.task_arn] = task

    def task_by_key(self, task_key: TaskInstanceKey) -> EcsExecutorTask:
        """Get a task by Airflow Instance Key."""
        arn = self.key_to_arn[task_key]
        return self.task_by_arn(arn)

    def task_by_arn(self, arn) -> EcsExecutorTask:
        """Get a task by AWS ARN."""
        return self.tasks[arn]

    def pop_by_key(self, task_key: TaskInstanceKey) -> EcsExecutorTask:
        """Deletes task from collection based off of Airflow Task Instance Key."""
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
        """Gets a task by AWS ARN."""
        return self.task_by_arn(value)

    def __len__(self):
        """Determines the number of tasks in collection."""
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


def parse_assign_public_ip(assign_public_ip):
    """Convert "assign_public_ip" from True/False to ENABLE/DISABLE."""
    return "ENABLED" if assign_public_ip == "True" else "DISABLED"


def convert_dict_keys_camel_case(nested_dict) -> dict:
    """Accept a potentially nested dictionary and recursively convert all keys into camelCase."""

    def _snake_to_camel(_key):
        split_key = _key.split("_")
        first_word = split_key[0]
        return first_word[0].lower() + first_word[1:] + "".join(word.title() for word in split_key[1:])

    result = {}
    for (key, value) in nested_dict.items():
        new_key = _snake_to_camel(key)
        if isinstance(value, dict) and (key.lower() != "tags"):
            # The key name on tags can be whatever the user wants, and we should not mess with them.
            result[new_key] = convert_dict_keys_camel_case(value)
        else:
            result[new_key] = nested_dict[key]
    return result
