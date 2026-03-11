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
Client-side Pydantic models for the Triggerer execution API.

These mirror the server-side models in:
  airflow-core/src/airflow/api_fastapi/execution_api/datamodels/triggerer.py
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class TriggerTaskInstanceDTO(BaseModel):
    """
    Client-side mirror of airflow.executors.workloads.task.TaskInstanceDTO.

    Must match TaskInstanceDTO exactly so that the supervisor can do:
        TaskInstanceDTO.model_validate(ti_dto.model_dump())
    without any missing-field errors.
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
    executor_config: dict[str, Any] | None = None
    parent_context_carrier: dict[str, Any] | None = None
    context_carrier: dict[str, Any] | None = None


class TriggerDetailResponse(BaseModel):
    """Client-side model for a single trigger detail."""

    id: int
    classpath: str
    encrypted_kwargs: str
    task_instance: TriggerTaskInstanceDTO | None = None
    log_path: str | None = None
    timeout_after: datetime | None = None


class NextTriggersResponse(BaseModel):
    """
    Client-side model for POST /execution/triggerer/{triggerer_id}/next-triggers response.
    """

    triggers: list[TriggerDetailResponse]
    trigger_ids_with_non_task_associations: list[int]
