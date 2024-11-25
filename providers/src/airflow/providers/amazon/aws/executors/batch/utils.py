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

import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey

CommandType = list[str]
ExecutorConfigType = dict[str, Any]

CONFIG_GROUP_NAME = "aws_batch_executor"

CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "max_submit_job_attempts": "3",
    "check_health_on_startup": "True",
}


@dataclass
class BatchQueuedJob:
    """Represents a Batch job that is queued. The job will be run in the next heartbeat."""

    key: TaskInstanceKey
    command: CommandType
    queue: str
    executor_config: ExecutorConfigType
    attempt_number: int
    next_attempt_time: datetime.datetime


@dataclass
class BatchJobInfo:
    """Contains information about a currently running Batch job."""

    cmd: CommandType
    queue: str
    config: ExecutorConfigType


class BatchJob:
    """Data Transfer Object for an AWS Batch Job."""

    STATE_MAPPINGS = {
        "SUBMITTED": State.QUEUED,
        "PENDING": State.QUEUED,
        "RUNNABLE": State.QUEUED,
        "STARTING": State.QUEUED,
        "RUNNING": State.RUNNING,
        "SUCCEEDED": State.SUCCESS,
        "FAILED": State.FAILED,
    }

    def __init__(self, job_id: str, status: str, status_reason: str | None = None):
        self.job_id = job_id
        self.status = status
        self.status_reason = status_reason

    def get_job_state(self) -> str:
        """Return the state of the job."""
        return self.STATE_MAPPINGS.get(self.status, State.QUEUED)

    def __repr__(self):
        """Return a visual representation of the Job status."""
        return f"({self.job_id} -> {self.status}, {self.get_job_state()})"


class BatchJobCollection:
    """A collection to manage running Batch Jobs."""

    def __init__(self):
        self.key_to_id: dict[TaskInstanceKey, str] = {}
        self.id_to_key: dict[str, TaskInstanceKey] = {}
        self.id_to_failure_counts: dict[str, int] = defaultdict(int)
        self.id_to_job_info: dict[str, BatchJobInfo] = {}

    def add_job(
        self,
        job_id: str,
        airflow_task_key: TaskInstanceKey,
        airflow_cmd: CommandType,
        queue: str,
        exec_config: ExecutorConfigType,
        attempt_number: int,
    ):
        """Add a job to the collection."""
        self.key_to_id[airflow_task_key] = job_id
        self.id_to_key[job_id] = airflow_task_key
        self.id_to_failure_counts[job_id] = attempt_number
        self.id_to_job_info[job_id] = BatchJobInfo(cmd=airflow_cmd, queue=queue, config=exec_config)

    def pop_by_id(self, job_id: str) -> TaskInstanceKey:
        """Delete job from collection based off of Batch Job ID."""
        task_key = self.id_to_key[job_id]
        del self.key_to_id[task_key]
        del self.id_to_key[job_id]
        del self.id_to_failure_counts[job_id]
        return task_key

    def failure_count_by_id(self, job_id: str) -> int:
        """Get the number of times a job has failed given a Batch Job Id."""
        return self.id_to_failure_counts[job_id]

    def increment_failure_count(self, job_id: str):
        """Increment the failure counter given a Batch Job Id."""
        self.id_to_failure_counts[job_id] += 1

    def get_all_jobs(self) -> list[str]:
        """Get all AWS ARNs in collection."""
        return list(self.id_to_key.keys())

    def __len__(self):
        """Return the number of jobs in collection."""
        return len(self.key_to_id)


class BatchSubmitJobKwargsConfigKeys(BaseConfigKeys):
    """Keys loaded into the config which are valid Batch submit_job kwargs."""

    JOB_NAME = "job_name"
    JOB_QUEUE = "job_queue"
    JOB_DEFINITION = "job_definition"
    EKS_PROPERTIES_OVERRIDE = "eks_properties_override"
    NODE_OVERRIDE = "node_override"


class AllBatchConfigKeys(BatchSubmitJobKwargsConfigKeys):
    """All keys loaded into the config which are related to the Batch Executor."""

    MAX_SUBMIT_JOB_ATTEMPTS = "max_submit_job_attempts"
    AWS_CONN_ID = "conn_id"
    SUBMIT_JOB_KWARGS = "submit_job_kwargs"
    REGION_NAME = "region_name"
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"


class BatchExecutorException(Exception):
    """Thrown when something unexpected has occurred within the AWS Batch ecosystem."""
