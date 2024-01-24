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
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, List

import boto3
from marshmallow import ValidationError

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.amazon.aws.executors.batch.boto_schema import (
    BatchDescribeJobsResponseSchema,
    BatchSubmitJobResponseSchema,
)
from airflow.providers.amazon.aws.executors.batch.utils import (
    BatchExecutorException,
    BatchJob,
    BatchJobCollection,
)
from airflow.utils.state import State

CommandType = List[str]
ExecutorConfigType = Dict[str, Any]


class AwsBatchExecutor(BaseExecutor):
    """
    The Airflow Scheduler creates a shell command, and passes it to the executor.

    This Batch Executor simply runs said airflow command a resource provisioned and managed
    by AWS Batch. It then periodically checks in with the launched jobs (via job-ids) to
    determine the status.
    The `submit_job_kwargs` configuration points to a dictionary that returns a dictionary. The
    keys of the resulting dictionary should match the kwargs for the SubmitJob definition per AWS'
    documentation (see below).
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

    # AWS only allows a maximum number of JOBs in the describe_jobs function
    DESCRIBE_JOBS_BATCH_SIZE = 99

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        region = conf.get("batch", "region", fallback="us-west-2")
        self.active_workers = BatchJobCollection()
        self.batch = boto3.client("batch", region_name=region)
        self.submit_job_kwargs = self._load_submit_kwargs()

    def sync(self):
        """Checks and update state on all running tasks."""
        all_job_ids = self.active_workers.get_all_jobs()
        if not all_job_ids:
            self.log.debug("No active tasks, skipping sync")
            return
        try:
            describe_job_response = self._describe_tasks(all_job_ids)
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process
            self.log.exception("Failed to sync %s", self.__class__.__name__)

        self.log.debug("Active Workers: %s", describe_job_response)

        for job in describe_job_response:
            if job.get_job_state() == State.FAILED:
                task_key = self.active_workers.pop_by_id(job.job_id)
                self.fail(task_key)
            elif job.get_job_state() == State.SUCCESS:
                task_key = self.active_workers.pop_by_id(job.job_id)
                self.success(task_key)

    def _describe_tasks(self, job_ids) -> list[BatchJob]:
        all_jobs = []
        for i in range(0, len(job_ids), self.__class__.DESCRIBE_JOBS_BATCH_SIZE):
            batched_job_ids = job_ids[i : i + self.__class__.DESCRIBE_JOBS_BATCH_SIZE]
            if not batched_job_ids:
                continue
            boto_describe_tasks = self.batch.describe_jobs(jobs=batched_job_ids)
            try:
                describe_tasks_response = BatchDescribeJobsResponseSchema().load(boto_describe_tasks)
            except ValidationError as err:
                self.log.error("Batch DescribeJobs API Response: %s", boto_describe_tasks)
                raise BatchExecutorException(
                    f"DescribeJobs API call does not match expected JSON shape. Are you sure that the correct version of Boto3 is installed? {err}"
                )
            all_jobs.extend(describe_tasks_response["jobs"])
        return all_jobs

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue=None, executor_config=None):
        """Save the task to be executed in the next sync using Boto3's RunTask API."""
        if executor_config and "command" in executor_config:
            raise ValueError('Executor Config should never override "command"')
        try:
            job_id = self._submit_job(key, command, queue, executor_config or {})
        except Exception:
            # We catch any and all exceptions because otherwise they would bubble
            # up and kill the scheduler process.
            self.log.exception("Failed to submit Batch job %s", self.__class__.__name__)

        self.active_workers.add_job(job_id, key)

    def _submit_job(
        self, key: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ) -> str:
        """
        Overrides the submit_job_kwargs, and calls the boto3 API submit_job endpoint.

        The command and executor config will be placed in the container-override section of the JSON request,
        before calling Boto3's "submit_job" function.
        """
        submit_job_api = self._submit_job_kwargs(key, cmd, queue, exec_config)
        self.log.info("submitting job with these args %s", submit_job_api)

        boto_run_task = self.batch.submit_job(**submit_job_api)
        try:
            submit_job_response = BatchSubmitJobResponseSchema().load(boto_run_task)
        except ValidationError as err:
            self.log.error("Batch SubmitJob Response: %s", err)
            raise BatchExecutorException(
                f"RunTask API call does not match expected JSON shape. Are you sure that the correct version of Boto3 is installed? {err}"
            )
        return submit_job_response["job_id"]

    def _submit_job_kwargs(
        self, key: TaskInstanceKey, cmd: CommandType, queue: str, exec_config: ExecutorConfigType
    ) -> dict:
        """
        Overrides the Airflow command to update the container overrides so kwargs are specific to this task.

        One last chance to modify Boto3's "submit_job" kwarg params before it gets passed into the Boto3
        client. For the latest kwarg parameters:
        .. seealso:: https://docs.aws.amazon.com/batch/latest/APIReference/API_SubmitJob.html
        """
        submit_job_api = deepcopy(self.submit_job_kwargs)
        submit_job_api["containerOverrides"].update(exec_config)
        submit_job_api["containerOverrides"]["command"] = cmd
        return submit_job_api

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
        # Some checks with some helpful errors
        assert isinstance(submit_kwargs, dict)

        if "containerOverrides" not in submit_kwargs or "command" not in submit_kwargs["containerOverrides"]:
            raise KeyError(
                'SubmitJob API needs kwargs["containerOverrides"]["command"] field,'
                " and value should be NULL or empty."
            )
        return submit_kwargs
