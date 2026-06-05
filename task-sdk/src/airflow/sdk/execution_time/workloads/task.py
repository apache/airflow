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
"""Task workload schemas for Task SDK execution-time communication."""

from __future__ import annotations

import uuid

from pydantic import BaseModel, Field


class BaseTaskInstanceDTO(BaseModel):
    """
    Base schema for TaskInstance with the minimal fields shared by Executors and the Task SDK.

    This class is duplicated in :mod:`airflow.executors.workloads.task` and the
    two definitions are kept in sync by the ``check-task-instance-dto-sync``
    prek hook. Update both files together.
    """

    id: uuid.UUID
    dag_version_id: uuid.UUID
    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    map_index: int = -1

    pool_slots: int
    queue: str
    priority_weight: int
    executor_config: dict | None = Field(default=None, exclude=True)

    parent_context_carrier: dict | None = None
    context_carrier: dict | None = None


class TaskInstanceDTO(BaseTaskInstanceDTO):
    """Task SDK TaskInstanceDTO."""
