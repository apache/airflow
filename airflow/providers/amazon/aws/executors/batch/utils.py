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

from airflow.utils.state import State
from typing import Optional, Dict, List
from airflow.models.taskinstance import TaskInstanceKey



class BatchJob:
    """
    Data Transfer Object for an AWS Batch Job
    """
    STATE_MAPPINGS = {
        'SUBMITTED': State.QUEUED,
        'PENDING': State.QUEUED,
        'RUNNABLE': State.QUEUED,
        'STARTING': State.QUEUED,
        'RUNNING': State.RUNNING,
        'SUCCEEDED': State.SUCCESS,
        'FAILED': State.FAILED
    }

    def __init__(self, job_id: str, status: str, status_reason: Optional[str] = None):
        self.job_id = job_id
        self.status = status
        self.status_reason = status_reason

    def get_job_state(self) -> str:
        """
        This is the primary logic that handles state in an AWS Batch Job
        """
        return self.STATE_MAPPINGS.get(self.status, State.QUEUED)

    def __repr__(self):
        return '({} -> {}, {})'.format(self.job_id, self.status, self.get_job_state())

class BatchJobCollection:
    """
    A Two-way dictionary between Airflow task ids and Batch Job IDs
    """
    def __init__(self):
        self.key_to_id: Dict[TaskInstanceKey, str] = {}
        self.id_to_key: Dict[str, TaskInstanceKey] = {}

    def add_job(self, job_id: str, airflow_task_key: TaskInstanceKey):
        """Adds a task to the collection"""
        self.key_to_id[airflow_task_key] = job_id
        self.id_to_key[job_id] = airflow_task_key

    def pop_by_id(self, job_id: str) -> TaskInstanceKey:
        """Deletes task from collection based off of Batch Job ID"""
        task_key = self.id_to_key[job_id]
        del self.key_to_id[task_key]
        del self.id_to_key[job_id]
        return task_key

    def get_all_jobs(self) -> List[str]:
        """Get all AWS ARNs in collection"""
        return list(self.id_to_key.keys())

    def __len__(self):
        """Determines the number of jobs in collection"""
        return len(self.key_to_id)


class BatchExecutorException(Exception):
    """Thrown when something unexpected has occurred within the AWS Batch ecosystem"""
