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
AWS ECS Executor.

Each Airflow task gets delegated out to an Amazon ECS Task.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from copy import deepcopy
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.providers.amazon.aws.executors.ecs.boto_schema import BotoDescribeTasksSchema, BotoRunTaskSchema
from airflow.providers.amazon.aws.executors.ecs.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllEcsConfigKeys,
    EcsExecutorException,
    EcsQueuedTask,
    EcsTaskCollection,
)
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.providers.amazon.aws.executors.ecs.utils import (
        CommandType,
        ExecutorConfigType,
    )


class AwsEcsExecutor(BaseExecutor):
    """
    Executes the provided Airflow command on an ECS instance.

    The Airflow Scheduler creates a shell command, and passes it to the executor. This ECS Executor
    runs said Airflow command on a remote Amazon ECS Cluster with a task-definition configured to
    launch the same containers as the Scheduler. It then periodically checks in with the launched
    tasks (via task ARNs) to determine the status.

    This allows individual tasks to specify CPU, memory, GPU, env variables, etc. When initializing a task,
    there's an option for "executor config" which should be a dictionary with keys that match the
    ``ContainerOverride`` definition per AWS documentation (see link below).

    Prerequisite: proper configuration of Boto3 library
    .. seealso:: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for
    authentication and access-key management. You can store an environmental variable, setup aws config from
    console, or use IAM roles.

    .. seealso:: https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerOverride.html for an
     Airflow TaskInstance's executor_config.
    """

    # Maximum number of retries to run an ECS task.
    MAX_RUN_TASK_ATTEMPTS = conf.get(
        CONFIG_GROUP_NAME,
        AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS,
        fallback=CONFIG_DEFAULTS[AllEcsConfigKeys.MAX_RUN_TASK_ATTEMPTS],
    )

    # AWS limits the maximum number of ARNs in the describe_tasks function.
    DESCRIBE_TASKS_BATCH_SIZE = 99

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_workers: EcsTaskCollection = EcsTaskCollection()
        self.pending_tasks: deque = deque()

        self.cluster = conf.get(CONFIG_GROUP_NAME, AllEcsConfigKeys.CLUSTER)
        self.container_name = conf.get(CONFIG_GROUP_NAME, AllEcsConfigKeys.CONTAINER_NAME)
        aws_conn_id = conf.get(
            CONFIG_GROUP_NAME,
            AllEcsConfigKeys.AWS_CONN_ID,
            fallback=CONFIG_DEFAULTS[AllEcsConfigKeys.AWS_CONN_ID],
        )
        region_name = conf.get(CONFIG_GROUP_NAME, AllEcsConfigKeys.REGION_NAME)
        from airflow.providers.amazon.aws.hooks.ecs import EcsHook

        self.ecs = EcsHook(aws_conn_id=aws_conn_id, region_name=region_name).conn
        self.run_task_kwargs = self._load_run_kwargs()

    def start(self):
        """
        Make a test API call to check the health of the ECS Executor.

        Deliberately use an invalid task ID, some potential outcomes in order:
          1. "AccessDeniedException" is raised if there are insufficient permissions.
          2. "ClusterNotFoundException" is raised if permissions exist but the cluster does not.
          3. The API responds with a failure message if the cluster is found and there
             are permissions, but the cluster itself has issues.
          4. "InvalidParameterException" is raised if the permissions and cluster exist but the task does not.

        The last one is considered a success state for the purposes of this check.
        """
        check_health = conf.getboolean(
            CONFIG_GROUP_NAME, AllEcsConfigKeys.CHECK_HEALTH_ON_STARTUP, fallback=False
        )

        if not check_health:
            return

        self.log.info("Starting ECS Executor and determining health...")

        success_status = "succeeded."
        status = success_status

        try:
            invalid_task_id = "a" * 32
            self.ecs.stop_task(cluster=self.cluster, task=invalid_task_id)

            # If it got this far, something is wrong.  stop_task() called with an
            # invalid taskID should have thrown a ClientError.  All known reasons are
            # covered in the ``except`` block below, and this should never be reached.
            status = "failed for an unknown reason. "
        except ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            error_message = ex.response["Error"]["Message"]

            if ("InvalidParameterException" in error_code) and ("task was not found" in error_message):
                # This failure is expected, and means we're healthy
                pass
            else:
                # Catch all for unexpected failures
                status = f"failed because: {error_message}. "
        except Exception as e:
            # Any non-ClientError exceptions. This can include Botocore exceptions for example
            status = f"failed because: {e}. "
        finally:
            msg_prefix = "ECS Executor health check has %s"
            if status == success_status:
                self.log.info(msg_prefix, status)
            else:
                msg_error_suffix = (
                    "The ECS executor will not be able to run Airflow tasks until the issue is addressed. "
                    "Stopping the Airflow Scheduler from starting until the issue is resolved."
                )
                raise AirflowException(msg_prefix % status + msg_error_suffix)

    def sync(self):
        try:
            self.sync_running_tasks()
            self.attempt_task_runs()
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process
            self.log.exception("Failed to sync %s", self.__class__.__name__)

    def sync_running_tasks(self):
        """Checks and update state on all running tasks."""
        all_task_arns = self.active_workers.get_all_arns()
        if not all_task_arns:
            self.log.debug("No active Airflow tasks, skipping sync.")
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
        # Get state of current task.
        task_state = task.get_task_state()
        task_key = self.active_workers.arn_to_key[task.task_arn]
        # Mark finished tasks as either a success/failure.
        if task_state == State.FAILED:
            self.fail(task_key)
            self.__log_container_failures(task_arn=task.task_arn)
        elif task_state == State.SUCCESS:
            self.success(task_key)
        elif task_state == State.REMOVED:
            self.__handle_failed_task(task.task_arn, task.stopped_reason)
        if task_state in (State.FAILED, State.SUCCESS):
            self.log.debug(
                "Airflow task %s marked as %s after running on ECS Task (arn) %s",
                task_key,
                task_state,
                task.task_arn,
            )
            self.active_workers.pop_by_key(task_key)

    def __describe_tasks(self, task_arns):
        all_task_descriptions = {"tasks": [], "failures": []}
        for i in range(0, len(task_arns), self.DESCRIBE_TASKS_BATCH_SIZE):
            batched_task_arns = task_arns[i : i + self.DESCRIBE_TASKS_BATCH_SIZE]
            if not batched_task_arns:
                continue
            boto_describe_tasks = self.ecs.describe_tasks(tasks=batched_task_arns, cluster=self.cluster)
            describe_tasks_response = BotoDescribeTasksSchema().load(boto_describe_tasks)

            all_task_descriptions["tasks"].extend(describe_tasks_response["tasks"])
            all_task_descriptions["failures"].extend(describe_tasks_response["failures"])
        return all_task_descriptions

    def __log_container_failures(self, task_arn: str):
        """Check if the task failed due to issues with the containers."""
        message = "The ECS task failed due to the following containers failing: \n"
        containers = self.active_workers.task_by_arn(task_arn).containers
        has_exit_codes = all(["exit_code" in x for x in containers])
        if not has_exit_codes:
            return ""
        reasons = [
            f'{container["container_arn"]} - {container["reason"]}'
            for container in containers
            if "reason" in container
        ]

        self.log.warning(message + "\n".join(reasons)) if reasons else ""

    def __handle_failed_task(self, task_arn: str, reason: str):
        """If an API failure occurs, the task is rescheduled."""
        task_key = self.active_workers.arn_to_key[task_arn]
        task_info = self.active_workers.info_by_key(task_key)
        task_cmd = task_info.cmd
        queue = task_info.queue
        exec_info = task_info.config
        failure_count = self.active_workers.failure_count_by_key(task_key)
        if int(failure_count) < int(self.__class__.MAX_RUN_TASK_ATTEMPTS):
            self.log.warning(
                "Airflow task %s failed due to %s. Failure %s out of %s occurred on %s. Rescheduling.",
                task_key,
                reason,
                failure_count,
                self.__class__.MAX_RUN_TASK_ATTEMPTS,
                task_arn,
            )
            self.active_workers.increment_failure_count(task_key)
            self.pending_tasks.appendleft(
                EcsQueuedTask(task_key, task_cmd, queue, exec_info, failure_count + 1)
            )
        else:
            self.log.error(
                "Airflow task %s has failed a maximum of %s times. Marking as failed",
                task_key,
                failure_count,
            )
            self.active_workers.pop_by_key(task_key)
            self.fail(task_key)

    def attempt_task_runs(self):
        """
        Takes tasks from the pending_tasks queue, and attempts to find an instance to run it on.

        If the launch type is EC2, this will attempt to place tasks on empty EC2 instances.  If
            there are no EC2 instances available, no task is placed and this function will be
            called again in the next heart-beat.

        If the launch type is FARGATE, this will run the tasks on new AWS Fargate instances.
        """
        queue_len = len(self.pending_tasks)
        failure_reasons = defaultdict(int)
        for _ in range(queue_len):
            ecs_task = self.pending_tasks.popleft()
            task_key = ecs_task.key
            cmd = ecs_task.command
            queue = ecs_task.queue
            exec_config = ecs_task.executor_config
            attempt_number = ecs_task.attempt_number
            _failure_reasons = []
            try:
                run_task_response = self._run_task(task_key, cmd, queue, exec_config)
            except Exception as e:
                # Failed to even get a response back from the Boto3 API or something else went
                # wrong.  For any possible failure we want to add the exception reasons to the
                # failure list so that it is logged to the user and most importantly the task is
                # added back to the pending list to be retried later.
                _failure_reasons.append(str(e))
            else:
                # We got a response back, check if there were failures. If so, add them to the
                # failures list so that it is logged to the user and most importantly the task
                # is added back to the pending list to be retried later.
                if run_task_response["failures"]:
                    _failure_reasons.extend([f["reason"] for f in run_task_response["failures"]])

            if _failure_reasons:
                for reason in _failure_reasons:
                    failure_reasons[reason] += 1
                # Make sure the number of attempts does not exceed MAX_RUN_TASK_ATTEMPTS
                if int(attempt_number) <= int(self.__class__.MAX_RUN_TASK_ATTEMPTS):
                    ecs_task.attempt_number += 1
                    self.pending_tasks.appendleft(ecs_task)
                else:
                    self.log.error(
                        "ECS task %s has failed a maximum of %s times. Marking as failed",
                        task_key,
                        attempt_number,
                    )
                    self.fail(task_key)
            elif not run_task_response["tasks"]:
                self.log.error("ECS RunTask Response: %s", run_task_response)
                raise EcsExecutorException(
                    "No failures and no ECS tasks provided in response. This should never happen."
                )
            else:
                task = run_task_response["tasks"][0]
                self.active_workers.add_task(task, task_key, queue, cmd, exec_config, attempt_number)
        if failure_reasons:
            self.log.error(
                "Pending ECS tasks failed to launch for the following reasons: %s. Retrying later.",
                dict(failure_reasons),
            )

    def _run_task(
        self, task_id: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ):
        """
        Run a queued-up Airflow task.

        Not to be confused with execute_async() which inserts tasks into the queue.
        The command and executor config will be placed in the container-override
        section of the JSON request before calling Boto3's "run_task" function.
        """
        run_task_api = self._run_task_kwargs(task_id, cmd, queue, exec_config)
        boto_run_task = self.ecs.run_task(**run_task_api)
        run_task_response = BotoRunTaskSchema().load(boto_run_task)
        return run_task_response

    def _run_task_kwargs(
        self, task_id: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ) -> dict:
        """
        Overrides the Airflow command to update the container overrides so kwargs are specific to this task.

        One last chance to modify Boto3's "run_task" kwarg params before it gets passed into the Boto3 client.
        """
        run_task_api = deepcopy(self.run_task_kwargs)
        container_override = self.get_container(run_task_api["overrides"]["containerOverrides"])
        container_override["command"] = cmd
        container_override.update(exec_config)

        # Inject the env variable to configure logging for containerized execution environment
        if "environment" not in container_override:
            container_override["environment"] = []
        container_override["environment"].append({"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"})

        return run_task_api

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue=None, executor_config=None):
        """Save the task to be executed in the next sync by inserting the commands into a queue."""
        if executor_config and ("name" in executor_config or "command" in executor_config):
            raise ValueError('Executor Config should never override "name" or "command"')
        self.pending_tasks.append(EcsQueuedTask(key, command, queue, executor_config or {}, 1))

    def end(self, heartbeat_interval=10):
        """Waits for all currently running tasks to end, and doesn't launch any tasks."""
        try:
            while True:
                self.sync()
                if not self.active_workers:
                    break
                time.sleep(heartbeat_interval)
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process.
            self.log.exception("Failed to end %s", self.__class__.__name__)

    def terminate(self):
        """Kill all ECS processes by calling Boto3's StopTask API."""
        try:
            for arn in self.active_workers.get_all_arns():
                self.ecs.stop_task(
                    cluster=self.cluster, task=arn, reason="Airflow Executor received a SIGTERM"
                )
            self.end()
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process.
            self.log.exception("Failed to terminate %s", self.__class__.__name__)

    def _load_run_kwargs(self) -> dict:
        from airflow.providers.amazon.aws.executors.ecs.ecs_executor_config import build_task_kwargs

        ecs_executor_run_task_kwargs = build_task_kwargs()

        try:
            self.get_container(ecs_executor_run_task_kwargs["overrides"]["containerOverrides"])["command"]
        except KeyError:
            raise KeyError(
                "Rendered JSON template does not contain key "
                '"overrides[containerOverrides][containers][x][command]"'
            )
        return ecs_executor_run_task_kwargs

    def get_container(self, container_list):
        """Searches task list for core Airflow container."""
        for container in container_list:
            if container["name"] == self.container_name:
                return container
        raise KeyError(f"No such container found by container name: {self.container_name}")
