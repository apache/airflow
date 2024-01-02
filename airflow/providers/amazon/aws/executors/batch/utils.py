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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys
from airflow.utils.state import State

CONFIG_GROUP_NAME = "aws_batch_executor"

CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "max_submit_job_attempts": "3",
    "check_health_on_startup": "True",
}


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
        """This is the primary logic that handles state in an AWS Batch Job."""
        return self.STATE_MAPPINGS.get(self.status, State.QUEUED)

    def __repr__(self):
        return f"({self.job_id} -> {self.status}, {self.get_job_state()})"


class BatchJobCollection:
    """A Two-way dictionary between Airflow task ids and Batch Job IDs."""

    def __init__(self):
        self.key_to_id: dict[TaskInstanceKey, str] = {}
        self.id_to_key: dict[str, TaskInstanceKey] = {}

    def add_job(self, job_id: str, airflow_task_key: TaskInstanceKey):
        """Adds a task to the collection."""
        self.key_to_id[airflow_task_key] = job_id
        self.id_to_key[job_id] = airflow_task_key

    def pop_by_id(self, job_id: str) -> TaskInstanceKey:
        """Deletes task from collection based off of Batch Job ID."""
        task_key = self.id_to_key[job_id]
        del self.key_to_id[task_key]
        del self.id_to_key[job_id]
        return task_key

    def get_all_jobs(self) -> list[str]:
        """Get all AWS ARNs in collection."""
        return list(self.id_to_key.keys())

    def __len__(self):
        """Determines the number of jobs in collection."""
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
