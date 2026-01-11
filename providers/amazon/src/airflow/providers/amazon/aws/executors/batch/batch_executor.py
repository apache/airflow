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

"""AWS Batch Executor. Each Airflow task gets delegated out to an AWS Batch Job."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError, NoCredentialsError

from airflow.executors.base_executor import BaseExecutor
from airflow.providers.amazon.aws.executors.utils.exponential_backoff_retry import (
    calculate_next_attempt_delay,
    exponential_backoff_retry,
)
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.common.compat.sdk import AirflowException, Stats, conf, timezone
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.executors import workloads
    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.providers.amazon.aws.executors.batch.boto_schema import (
    BatchDescribeJobsResponseSchema,
    BatchSubmitJobResponseSchema,
)
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
    BatchJob,
    BatchJobCollection,
    BatchQueuedJob,
)
from airflow.utils.state import State

CommandType = Sequence[str]
ExecutorConfigType = dict[str, Any]

INVALID_CREDENTIALS_EXCEPTIONS = [
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "UnrecognizedClientException",
]


class AwsBatchExecutor(BaseExecutor):
    """
    The Airflow Scheduler creates a shell command, and passes it to the executor.

    This Batch Executor simply runs said airflow command in a resource provisioned and managed
    by AWS Batch. It then periodically checks in with the launched jobs (via job-ids) to
    determine the status.
    The `submit_job_kwargs` is a dictionary that should match the kwargs for the
    SubmitJob definition per AWS' documentation (see below).
    For maximum flexibility, individual tasks can specify `executor_config` as a dictionary, with keys that
    match the request syntax for the SubmitJob definition per AWS' documentation (see link below). The
    `executor_config` will update the `submit_job_kwargs` dictionary when calling the task. This allows
    individual jobs to specify CPU, memory, GPU, env variables, etc.
    Prerequisite: proper configuration of Boto3 library
    .. seealso:: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for
    authentication and access-key management. You can store an environmental variable, setup aws config from
    console, or use IAM roles.
    .. seealso:: https://docs.aws.amazon.com/batch/latest/APIReference/API_SubmitJob.html for an
    Airflow TaskInstance's executor_config.
    """

    # Maximum number of retries to submit a Batch Job.
    MAX_SUBMIT_JOB_ATTEMPTS = conf.get(
        CONFIG_GROUP_NAME,
        AllBatchConfigKeys.MAX_SUBMIT_JOB_ATTEMPTS,
        fallback=CONFIG_DEFAULTS[AllBatchConfigKeys.MAX_SUBMIT_JOB_ATTEMPTS],
    )

    # AWS only allows a maximum number of JOBs in the describe_jobs function
    DESCRIBE_JOBS_BATCH_SIZE = 99

    if TYPE_CHECKING and AIRFLOW_V_3_0_PLUS:
        # In the v3 path, we store workloads, not commands as strings.
        # TODO: TaskSDK: move this type change into BaseExecutor
        queued_tasks: dict[TaskInstanceKey, workloads.All]  # type: ignore[assignment]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_workers = BatchJobCollection()
        self.pending_jobs: deque = deque()
        self.attempts_since_last_successful_connection = 0
        self.load_batch_connection(check_connection=False)
        self.IS_BOTO_CONNECTION_HEALTHY = False
        self.submit_job_kwargs = self._load_submit_kwargs()

    def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
        from airflow.executors import workloads

        if not isinstance(workload, workloads.ExecuteTask):
            raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")
        ti = workload.ti
        self.queued_tasks[ti.key] = workload

    def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
        from airflow.executors.workloads import ExecuteTask

        # Airflow V3 version
        for w in workloads:
            if not isinstance(w, ExecuteTask):
                raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(w)}")
            command = [w]
            key = w.ti.key
            queue = w.ti.queue
            executor_config = w.ti.executor_config or {}

            del self.queued_tasks[key]
            self.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)  # type: ignore[arg-type]
            self.running.add(key)

    def check_health(self):
        """Make a test API call to check the health of the Batch Executor."""
        success_status = "succeeded."
        status = success_status

        try:
            invalid_job_id = "a" * 32
            self.batch.describe_jobs(jobs=[invalid_job_id])
            # If an empty response was received, then that is considered to be the success case.
        except ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            error_message = ex.response["Error"]["Message"]
            status = f"failed because: {error_code}: {error_message}. "
        except Exception as e:
            # Any non-ClientError exceptions. This can include Botocore exceptions for example
            status = f"failed because: {e}. "
        finally:
            msg_prefix = "Batch Executor health check has %s"
            if status == success_status:
                self.IS_BOTO_CONNECTION_HEALTHY = True
                self.log.info(msg_prefix, status)
            else:
                msg_error_suffix = (
                    "The Batch executor will not be able to run Airflow tasks until the issue is addressed."
                )
                raise AirflowException(msg_prefix % status + msg_error_suffix)

    def start(self):
        """Call this when the Executor is run for the first time by the scheduler."""
        check_health = conf.getboolean(
            CONFIG_GROUP_NAME, AllBatchConfigKeys.CHECK_HEALTH_ON_STARTUP, fallback=False
        )

        if not check_health:
            return

        self.log.info("Starting Batch Executor and determining health...")
        try:
            self.check_health()
        except AirflowException:
            self.log.error("Stopping the Airflow Scheduler from starting until the issue is resolved.")
            raise

    def load_batch_connection(self, check_connection: bool = True):
        self.log.info("Loading Connection information")
        aws_conn_id = conf.get(
            CONFIG_GROUP_NAME,
            AllBatchConfigKeys.AWS_CONN_ID,
            fallback=CONFIG_DEFAULTS[AllBatchConfigKeys.AWS_CONN_ID],
        )
        region_name = conf.get(CONFIG_GROUP_NAME, AllBatchConfigKeys.REGION_NAME, fallback=None)
        self.batch = BatchClientHook(aws_conn_id=aws_conn_id, region_name=region_name).conn
        self.attempts_since_last_successful_connection += 1
        self.last_connection_reload = timezone.utcnow()

        if check_connection:
            self.check_health()
            self.attempts_since_last_successful_connection = 0

    def sync(self):
        """Sync will get called periodically by the heartbeat method in the scheduler."""
        if not self.IS_BOTO_CONNECTION_HEALTHY:
            exponential_backoff_retry(
                self.last_connection_reload,
                self.attempts_since_last_successful_connection,
                self.load_batch_connection,
            )
            if not self.IS_BOTO_CONNECTION_HEALTHY:
                return
        try:
            self.sync_running_jobs()
            self.attempt_submit_jobs()
        except (ClientError, NoCredentialsError) as error:
            error_code = error.response["Error"]["Code"]
            if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                self.IS_BOTO_CONNECTION_HEALTHY = False
                self.log.warning(
                    "AWS credentials are either missing or expired: %s.\nRetrying connection", error
                )
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process
            self.log.exception("Failed to sync %s", self.__class__.__name__)

    def sync_running_jobs(self):
        all_job_ids = self.active_workers.get_all_jobs()
        if not all_job_ids:
            self.log.debug("No active Airflow tasks, skipping sync")
            return
        describe_job_response = self._describe_jobs(all_job_ids)

        self.log.debug("Active Workers: %s", describe_job_response)

        for job in describe_job_response:
            if job.get_job_state() == State.FAILED:
                self._handle_failed_job(job)
            elif job.get_job_state() == State.SUCCESS:
                task_key = self.active_workers.pop_by_id(job.job_id)
                self.success(task_key)

    def _handle_failed_job(self, job):
        """
        Handle a failed AWS Batch job.

        If an API failure occurs when running a Batch job, the job is rescheduled.
        """
        # A failed job here refers to a job that has been marked Failed by AWS Batch, which is not
        # necessarily the same as being marked Failed by Airflow. AWS Batch will mark a job Failed
        # if the job fails before the Airflow process on the container has started. These failures
        # can be caused by a Batch API failure, container misconfiguration etc.
        # If the container is able to start up and run the Airflow process, any failures after that
        # (i.e. DAG failures) will not be marked as Failed by AWS Batch, because Batch on assumes
        # responsibility for ensuring the process started. Failures in the DAG will be caught by
        # Airflow, which will be handled separately.
        job_info = self.active_workers.id_to_job_info[job.job_id]
        task_key = self.active_workers.id_to_key[job.job_id]
        task_cmd = job_info.cmd
        queue = job_info.queue
        exec_info = job_info.config
        failure_count = self.active_workers.failure_count_by_id(job_id=job.job_id)
        if int(failure_count) < int(self.__class__.MAX_SUBMIT_JOB_ATTEMPTS):
            self.log.warning(
                "Airflow task %s failed due to %s. Failure %s out of %s occurred on %s. Rescheduling.",
                task_key,
                job.status_reason,
                failure_count,
                self.__class__.MAX_SUBMIT_JOB_ATTEMPTS,
                job.job_id,
            )
            self.active_workers.increment_failure_count(job_id=job.job_id)
            self.active_workers.pop_by_id(job.job_id)
            self.pending_jobs.append(
                BatchQueuedJob(
                    task_key,
                    task_cmd,
                    queue,
                    exec_info,
                    failure_count + 1,
                    timezone.utcnow() + calculate_next_attempt_delay(failure_count),
                )
            )
        else:
            self.log.error(
                "Airflow task %s has failed a maximum of %s times. Marking as failed",
                task_key,
                failure_count,
            )
            self.active_workers.pop_by_id(job.job_id)
            self.fail(task_key)

    def attempt_submit_jobs(self):
        """
        Attempt to submit all jobs submitted to the Executor.

        For each iteration of the sync() method, every pending job is submitted to Batch.
        If a job fails validation, it will be put at the back of the queue to be reattempted
        in the next iteration of the sync() method, unless it has exceeded the maximum number of
        attempts. If a job exceeds the maximum number of attempts, it is removed from the queue.
        """
        for _ in range(len(self.pending_jobs)):
            batch_job = self.pending_jobs.popleft()
            key = batch_job.key
            cmd = batch_job.command
            queue = batch_job.queue
            exec_config = batch_job.executor_config
            attempt_number = batch_job.attempt_number
            failure_reason: str | None = None
            if timezone.utcnow() < batch_job.next_attempt_time:
                self.pending_jobs.append(batch_job)
                continue
            try:
                submit_job_response = self._submit_job(key, cmd, queue, exec_config or {})
            except NoCredentialsError:
                self.pending_jobs.append(batch_job)
                raise
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                    self.pending_jobs.append(batch_job)
                    raise
                failure_reason = str(e)
            except Exception as e:
                failure_reason = str(e)

            if failure_reason:
                if attempt_number >= int(self.__class__.MAX_SUBMIT_JOB_ATTEMPTS):
                    self.log.error(
                        (
                            "This job has been unsuccessfully attempted too many times (%s). "
                            "Dropping the task. Reason: %s"
                        ),
                        attempt_number,
                        failure_reason,
                    )
                    self.log_task_event(
                        event="batch job submit failure",
                        extra=f"This job has been unsuccessfully attempted too many times ({attempt_number}). "
                        f"Dropping the task. Reason: {failure_reason}",
                        ti_key=key,
                    )
                    self.fail(key=key)
                else:
                    batch_job.next_attempt_time = timezone.utcnow() + calculate_next_attempt_delay(
                        attempt_number
                    )
                    batch_job.attempt_number += 1
                    self.pending_jobs.append(batch_job)
            else:
                # Success case
                job_id = submit_job_response["job_id"]
                self.active_workers.add_job(
                    job_id=job_id,
                    airflow_task_key=key,
                    airflow_cmd=cmd,
                    queue=queue,
                    exec_config=exec_config,
                    attempt_number=attempt_number,
                )
                self.running_state(key, job_id)

    def _describe_jobs(self, job_ids) -> list[BatchJob]:
        all_jobs = []
        for i in range(0, len(job_ids), self.__class__.DESCRIBE_JOBS_BATCH_SIZE):
            batched_job_ids = job_ids[i : i + self.__class__.DESCRIBE_JOBS_BATCH_SIZE]
            if not batched_job_ids:
                continue
            boto_describe_tasks = self.batch.describe_jobs(jobs=batched_job_ids)

            describe_tasks_response = BatchDescribeJobsResponseSchema().load(boto_describe_tasks)
            all_jobs.extend(describe_tasks_response["jobs"])
        return all_jobs

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue=None, executor_config=None):
        """Save the task to be executed in the next sync using Boto3's RunTask API."""
        if executor_config and "command" in executor_config:
            raise ValueError('Executor Config should never override "command"')

        if len(command) == 1:
            from airflow.executors.workloads import ExecuteTask

            if isinstance(command[0], ExecuteTask):
                workload = command[0]
                ser_input = workload.model_dump_json()
                command = [
                    "python",
                    "-m",
                    "airflow.sdk.execution_time.execute_workload",
                    "--json-string",
                    ser_input,
                ]
            else:
                raise ValueError(
                    f"BatchExecutor doesn't know how to handle workload of type: {type(command[0])}"
                )

        self.pending_jobs.append(
            BatchQueuedJob(
                key=key,
                command=list(command),
                queue=queue,
                executor_config=executor_config or {},
                attempt_number=1,
                next_attempt_time=timezone.utcnow(),
            )
        )

    def _submit_job(
        self, key: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ) -> str:
        """
        Override the submit_job_kwargs, and calls the boto3 API submit_job endpoint.

        The command and executor config will be placed in the container-override section of the JSON request,
        before calling Boto3's "submit_job" function.
        """
        submit_job_api = self._submit_job_kwargs(key, cmd, queue, exec_config)

        boto_submit_job = self.batch.submit_job(**submit_job_api)
        submit_job_response = BatchSubmitJobResponseSchema().load(boto_submit_job)
        return submit_job_response

    def _submit_job_kwargs(
        self, key: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ) -> dict:
        """
        Override the Airflow command to update the container overrides so kwargs are specific to this task.

        One last chance to modify Boto3's "submit_job" kwarg params before it gets passed into the Boto3
        client. For the latest kwarg parameters:
        .. seealso:: https://docs.aws.amazon.com/batch/latest/APIReference/API_SubmitJob.html
        """
        submit_job_api = deepcopy(self.submit_job_kwargs)
        submit_job_api = merge_dicts(submit_job_api, exec_config)
        submit_job_api["containerOverrides"]["command"] = cmd
        if "environment" not in submit_job_api["containerOverrides"]:
            submit_job_api["containerOverrides"]["environment"] = []
        submit_job_api["containerOverrides"]["environment"].append(
            {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}
        )
        return submit_job_api

    def end(self, heartbeat_interval=10):
        """Wait for all currently running tasks to end and prevent any new jobs from running."""
        try:
            while True:
                self.sync()
                if not self.active_workers:
                    break
                time.sleep(heartbeat_interval)
        except Exception:
            # This should never happen because sync() should never raise an exception.
            self.log.exception("Failed to end %s", self.__class__.__name__)

    def terminate(self):
        """Kill all Batch Jobs by calling Boto3's TerminateJob API."""
        try:
            for job_id in self.active_workers.get_all_jobs():
                self.batch.terminate_job(jobId=job_id, reason="Airflow Executor received a SIGTERM")
            self.end()
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process.
            self.log.exception("Failed to terminate %s", self.__class__.__name__)

    @staticmethod
    def _load_submit_kwargs() -> dict:
        from airflow.providers.amazon.aws.executors.batch.batch_executor_config import build_submit_kwargs

        submit_kwargs = build_submit_kwargs()

        if "containerOverrides" not in submit_kwargs or "command" not in submit_kwargs["containerOverrides"]:
            raise KeyError(
                'SubmitJob API needs kwargs["containerOverrides"]["command"] field,'
                " and value should be NULL or empty."
            )
        return submit_kwargs

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Adopt task instances which have an external_executor_id (the Batch job ID).

        Anything that is not adopted will be cleared by the scheduler and becomes eligible for re-scheduling.
        """
        with Stats.timer("batch_executor.adopt_task_instances.duration"):
            adopted_tis: list[TaskInstance] = []

            if job_ids := [ti.external_executor_id for ti in tis if ti.external_executor_id]:
                batch_jobs = self._describe_jobs(job_ids)

                for batch_job in batch_jobs:
                    ti = next(ti for ti in tis if ti.external_executor_id == batch_job.job_id)
                    self.active_workers.add_job(
                        job_id=batch_job.job_id,
                        airflow_task_key=ti.key,
                        airflow_cmd=ti.command_as_list(),
                        queue=ti.queue,
                        exec_config=ti.executor_config,
                        attempt_number=ti.try_number,
                    )
                    adopted_tis.append(ti)

            if adopted_tis:
                tasks = [f"{task} in state {task.state}" for task in adopted_tis]
                task_instance_str = "\n\t".join(tasks)
                self.log.info(
                    "Adopted the following %d tasks from a dead executor:\n\t%s",
                    len(adopted_tis),
                    task_instance_str,
                )

            not_adopted_tis = [ti for ti in tis if ti not in adopted_tis]
            return not_adopted_tis
