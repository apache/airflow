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

"""AWS ECS Executor. Each Airflow task gets delegated out to an AWS ECS or Fargate Task."""
from __future__ import annotations

import time
from collections import defaultdict, deque, namedtuple
from copy import deepcopy
from typing import Any, Callable, Dict, List

import boto3
from marshmallow import EXCLUDE, Schema, ValidationError, fields, post_load

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.module_loading import import_string
from airflow.utils.state import State

CommandType = List[str]
ExecutorConfigFunctionType = Callable[[CommandType], dict]
EcsFargateQueuedTask = namedtuple("EcsFargateQueuedTask", ("key", "command", "queue", "executor_config"))
ExecutorConfigType = Dict[str, Any]
EcsFargateTaskInfo = namedtuple("EcsFargateTaskInfo", ("cmd", "queue", "config"))


class EcsFargateTask:
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
        This is the primary logic that handles state in an ECS/Fargate Task.
        It will determine if a status is:
            QUEUED - Task is being provisioned.
            RUNNING - Task is launched on ECS/Fargate.
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
        # sometimes fargate tasks may time out. Whoops.
        if not self.started_at and is_finished:
            return State.REMOVED
        if not is_finished or not has_exit_codes:
            return State.RUNNING
        all_containers_succeeded = all([x["exit_code"] == 0 for x in self.containers])
        return State.SUCCESS if all_containers_succeeded else State.FAILED

    def __repr__(self):
        return f"({self.task_arn}, {self.last_status}->{self.desired_status}, {self.get_task_state()})"


class AwsEcsFargateExecutor(BaseExecutor):
    """
    The Airflow Scheduler creates a shell command, and passes it to the executor. This ECS Executor simply
    runs said airflow command on a remote AWS Fargate or AWS ECS Cluster with an task-definition configured
    with the same containers as the Scheduler. It then periodically checks in with the launched tasks
    (via task-arns) to determine the status.
    This allows individual tasks to specify CPU, memory, GPU, env variables, etc. When initializing a task,
    there's an option for "executor config" which should be a dictionary with keys that match the
    "ContainerOverride" definition per AWS' documentation (see link below).
    Prerequisite: proper configuration of Boto3 library
    .. seealso:: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for
    authentication and access-key management. You can store an environmental variable, setup aws config from
    console, or use IAM roles.
    .. seealso:: https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerOverride.html for an
     Airflow TaskInstance's executor_config.
    """

    # Number of retries in the scenario where the API cannot find a task key. We do this because sometimes
    # AWS misplaces RunTask executions; even if they have a valid ARN.
    MAX_FAILURE_CHECKS = 3
    # AWS only allows a maximum number of ARNs in the describe_tasks function
    DESCRIBE_TASKS_BATCH_SIZE = 99

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster: str | None = None
        self.container_name: str | None = None
        # TODO::  In the inherited code, these next two defaulted to None and were set in start()
        #         below but mypy didn't like that. The provided tests still pass with this change,
        #         but it is possible it might have some unanticipated consequences.
        self.active_workers: EcsFargateTaskCollection = EcsFargateTaskCollection()
        self.pending_tasks: deque = deque()
        self.ecs = None
        self.run_task_kwargs = None

    def start(self):
        """Initialize Boto3 ECS Client, and other internal variables."""
        region = conf.get("ecs_fargate", "region")
        self.cluster = conf.get("ecs_fargate", "cluster")
        self.container_name = conf.get("ecs_fargate", "container_name")
        # TODO:: Confirm that defaulting in the init is functionally identical then remove these
        #        next two commented lines.
        # self.active_workers = EcsFargateTaskCollection()
        # self.pending_tasks = deque()
        self.ecs = boto3.client("ecs", region_name=region)
        self.run_task_kwargs = self._load_run_kwargs()

    def sync(self):
        self.sync_running_tasks()
        self.attempt_task_runs()

    def sync_running_tasks(self):
        """Checks and update state on all running tasks."""
        all_task_arns = self.active_workers.get_all_arns()
        if not all_task_arns:
            self.log.debug("No active tasks, skipping sync")
            return

        describe_tasks_response = self.__describe_tasks(all_task_arns)
        self.log.debug("Active Workers: %s", describe_tasks_response)

        if describe_tasks_response["failures"]:
            for failure in describe_tasks_response["failures"]:
                self.__handle_failed_task(failure["arn"], failure["reason"])

        updated_tasks = describe_tasks_response["tasks"]
        for task in updated_tasks:
            self.__update_running_task(task)

    def __update_running_task(self, task):
        self.active_workers.update_task(task)
        # get state of current task
        task_state = task.get_task_state()
        task_key = self.active_workers.arn_to_key[task.task_arn]
        # mark finished tasks as either a success/failure
        if task_state == State.FAILED:
            self.fail(task_key)
        elif task_state == State.SUCCESS:
            self.success(task_key)
        elif task_state == State.REMOVED:
            self.__handle_failed_task(task.task_arn, task.stopped_reason)
        if task_state in (State.FAILED, State.SUCCESS):
            self.log.debug("Task %s marked as %s after running on %s", task_key, task_state, task.task_arn)
            self.active_workers.pop_by_key(task_key)

    def __describe_tasks(self, task_arns):
        all_task_descriptions = {"tasks": [], "failures": []}
        for i in range(0, len(task_arns), self.DESCRIBE_TASKS_BATCH_SIZE):
            batched_task_arns = task_arns[i : i + self.DESCRIBE_TASKS_BATCH_SIZE]
            if not batched_task_arns:
                continue
            boto_describe_tasks = self.ecs.describe_tasks(tasks=batched_task_arns, cluster=self.cluster)
            try:
                describe_tasks_response = BotoDescribeTasksSchema().load(boto_describe_tasks)
            except ValidationError as err:
                self.log.error("ECS DescribeTask Response: %s", boto_describe_tasks)
                raise EcsFargateError(
                    f"DescribeTasks API call does not match expected JSON shape. "
                    f"Are you sure that the correct version of Boto3 is installed? {err}"
                )
            all_task_descriptions["tasks"].extend(describe_tasks_response["tasks"])
            all_task_descriptions["failures"].extend(describe_tasks_response["failures"])
        return all_task_descriptions

    def __handle_failed_task(self, task_arn: str, reason: str):
        """
        AWS' APIs aren't perfect. For example, sometimes task-arns get dropped and never make it to the
        ECS/Fargate Cloud. If an API failure occurs the task is simply rescheduled.
        """
        task_key = self.active_workers.arn_to_key[task_arn]
        task_cmd, queue, exec_info = self.active_workers.info_by_key(task_key)
        failure_count = self.active_workers.failure_count_by_key(task_key)
        if failure_count < self.__class__.MAX_FAILURE_CHECKS:
            self.log.warning(
                "Task %s has failed due to %s. Failure %s out of %s occurred on %s. Rescheduling.",
                task_key,
                reason,
                failure_count,
                self.__class__.MAX_FAILURE_CHECKS,
                task_arn,
            )
            self.active_workers.increment_failure_count(task_key)
            self.pending_tasks.appendleft(EcsFargateQueuedTask(task_key, task_cmd, queue, exec_info))
        else:
            self.log.error(
                "Task %s has failed a maximum of %s times. Marking as failed", task_key, failure_count
            )
            self.active_workers.pop_by_key(task_key)
            self.fail(task_key)

    def attempt_task_runs(self):
        """
        Takes tasks from the pending_tasks queue, and attempts to find an instance to run it on. If the
        launch type is ECS, then this will attempt to place tasks on empty EC2 instances. If there are no EC2
        instances available, no task is placed and this function will be called again in the next heart-beat.
        If the launch type is FARGATE, then this will attempt to place tasks on an AWS Fargate/Fargate-spot
        instance (based off of your task definition).
        """
        queue_len = len(self.pending_tasks)
        failure_reasons = defaultdict(int)
        for _ in range(queue_len):
            ecs_task = self.pending_tasks.popleft()
            task_key, cmd, queue, exec_config = ecs_task
            run_task_response = self._run_task(task_key, cmd, queue, exec_config)
            if run_task_response["failures"]:
                for f in run_task_response["failures"]:
                    failure_reasons[f["reason"]] += 1
                self.pending_tasks.append(ecs_task)
            elif not run_task_response["tasks"]:
                self.log.error("ECS RunTask Response: %s", run_task_response)
                raise EcsFargateError(
                    "No failures and no tasks provided in response. This should never happen."
                )
            else:
                task = run_task_response["tasks"][0]
                self.active_workers.add_task(task, task_key, queue, cmd, exec_config)
        if failure_reasons:
            self.log.debug(
                "Pending tasks failed to launch for the following reasons: %s. Will retry later.",
                dict(failure_reasons),
            )

    def _run_task(
        self, task_id: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ):
        """
        This function is the actual attempt to run a queued-up airflow task. Not to
        be confused with execute_async() which inserts tasks into the queue. The
        command and executor config will be placed in the container-override section
        of the JSON request, before calling Boto3's "run_task" function.
        """
        run_task_api = self._run_task_kwargs(task_id, cmd, queue, exec_config)
        boto_run_task = self.ecs.run_task(**run_task_api)
        try:
            run_task_response = BotoRunTaskSchema().load(boto_run_task)
        except ValidationError as err:
            self.log.error("ECS RunTask Response: %s", err)
            raise EcsFargateError(
                f"RunTask API call does not match expected JSON shape. "
                f"Are you sure that the correct version of Boto3 is installed? {err}"
            )
        return run_task_response

    def _run_task_kwargs(
        self, task_id: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ) -> dict:
        """
        This modifies the standard kwargs to be specific to this task by
        overriding the airflow command and updating the container overrides.

        One last chance to modify Boto3's "run_task" kwarg params before it gets passed into the Boto3 client.
        """
        run_task_api = deepcopy(self.run_task_kwargs)
        container_override = self.get_container(run_task_api["overrides"]["containerOverrides"])
        container_override["command"] = cmd
        container_override.update(exec_config)
        return run_task_api

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue=None, executor_config=None):
        """Save the task to be executed in the next sync by inserting the commands into a queue."""
        if executor_config and ("name" in executor_config or "command" in executor_config):
            raise ValueError('Executor Config should never override "name" or "command"')
        self.pending_tasks.append(EcsFargateQueuedTask(key, command, queue, executor_config or {}))

    def end(self, heartbeat_interval=10):
        """Waits for all currently running tasks to end, and doesn't launch any tasks."""
        while True:
            self.sync()
            if not self.active_workers:
                break
            time.sleep(heartbeat_interval)

    def terminate(self):
        """Kill all ECS processes by calling Boto3's StopTask API."""
        for arn in self.active_workers.get_all_arns():
            self.ecs.stop_task(cluster=self.cluster, task=arn, reason="Airflow Executor received a SIGTERM")
        self.end()

    def _load_run_kwargs(self) -> dict:
        run_kwargs = import_string(
            conf.get(
                "ecs_fargate",
                "run_task_kwargs",
                fallback=(
                    "airflow.providers.amazon.aws.executors.ecs_fargate_conf.ECS_FARGATE_RUN_TASK_KWARGS"
                ),
            )
        )
        if not isinstance(run_kwargs, dict):
            raise ValueError(f"AWS ECS config value must be a dictionary. Got {type(run_kwargs)}")

        print(
            "NANI",
            run_kwargs,
            conf.get(
                "ecs_fargate",
                "run_task_kwargs",
                fallback="airflow.providers.amazon.aws.executors.executor_conf.ECS_FARGATE_RUN_TASK_KWARGS",
            ),
        )

        if (
            "overrides" not in run_kwargs
            or "containerOverrides" not in run_kwargs["overrides"]
            or not run_kwargs["overrides"]["containerOverrides"]
            or "command" not in self.get_container(run_kwargs["overrides"]["containerOverrides"])
        ):
            raise KeyError(
                "Rendered JSON template does not contain key "
                '"overrides[containerOverrides][containers][x][command]"'
            )
        return run_kwargs

    def get_container(self, container_list):
        """Searches task list for core Airflow container."""
        for container in container_list:
            if container["name"] == self.container_name:
                return container
        raise KeyError(f"No such container found by container name: {self.container_name}")


class EcsFargateTaskCollection:
    """A five-way dictionary between Airflow task ids, Airflow cmds, ECS ARNs, and ECS task objects."""

    def __init__(self):
        self.key_to_arn: dict[TaskInstanceKey, str] = {}
        self.arn_to_key: dict[str, TaskInstanceKey] = {}
        self.tasks: dict[str, EcsFargateTask] = {}
        self.key_to_failure_counts: dict[TaskInstanceKey, int] = defaultdict(int)
        self.key_to_task_info: dict[TaskInstanceKey, EcsFargateTaskInfo] = {}

    def add_task(
        self,
        task: EcsFargateTask,
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
        self.key_to_task_info[airflow_task_key] = EcsFargateTaskInfo(airflow_cmd, queue, exec_config)

    def update_task(self, task: EcsFargateTask):
        """Updates the state of the given task based on task ARN."""
        self.tasks[task.task_arn] = task

    def task_by_key(self, task_key: TaskInstanceKey) -> EcsFargateTask:
        """Get a task by Airflow Instance Key."""
        arn = self.key_to_arn[task_key]
        return self.task_by_arn(arn)

    def task_by_arn(self, arn) -> EcsFargateTask:
        """Get a task by AWS ARN."""
        return self.tasks[arn]

    def pop_by_key(self, task_key: TaskInstanceKey) -> EcsFargateTask:
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

    def info_by_key(self, task_key: TaskInstanceKey) -> EcsFargateTaskInfo:
        """Get the Airflow Command given an airflow task key."""
        return self.key_to_task_info[task_key]

    def __getitem__(self, value):
        """Gets a task by AWS ARN."""
        return self.task_by_arn(value)

    def __len__(self):
        """Determines the number of tasks in collection."""
        return len(self.tasks)


class BotoContainerSchema(Schema):
    """
    Botocore Serialization Object for ECS 'Container' shape.
    Note that there are many more parameters, but the executor only needs the members listed below.
    """

    exit_code = fields.Integer(data_key="exitCode")
    last_status = fields.String(data_key="lastStatus")
    name = fields.String(required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoTaskSchema(Schema):
    """
    Botocore Serialization Object for ECS 'Task' shape.
    Note that there are many more parameters, but the executor only needs the members listed below.
    """

    task_arn = fields.String(data_key="taskArn", required=True)
    last_status = fields.String(data_key="lastStatus", required=True)
    desired_status = fields.String(data_key="desiredStatus", required=True)
    containers = fields.List(fields.Nested(BotoContainerSchema), required=True)
    started_at = fields.Field(data_key="startedAt")
    stopped_reason = fields.String(data_key="stoppedReason")

    @post_load
    def make_task(self, data, **kwargs):
        """Overwrites marshmallow load() to return an instance of EcsFargateTask instead of a dictionary."""
        return EcsFargateTask(**data)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoFailureSchema(Schema):
    """Botocore Serialization Object for ECS 'Failure' Shape."""

    arn = fields.String()
    reason = fields.String()

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoRunTaskSchema(Schema):
    """Botocore Serialization Object for ECS 'RunTask' Operation output."""

    tasks = fields.List(fields.Nested(BotoTaskSchema), required=True)
    failures = fields.List(fields.Nested(BotoFailureSchema), required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoDescribeTasksSchema(Schema):
    """Botocore Serialization Object for ECS 'DescribeTask' Operation output."""

    tasks = fields.List(fields.Nested(BotoTaskSchema), required=True)
    failures = fields.List(fields.Nested(BotoFailureSchema), required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class EcsFargateError(Exception):
    """Thrown when something unexpected has occurred within the AWS ECS/Fargate ecosystem."""
