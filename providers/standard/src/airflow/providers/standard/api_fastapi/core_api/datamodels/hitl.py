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

from collections.abc import MutableMapping
from datetime import datetime

from pydantic import field_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.operators.hitl import HITLOperator


class AddHITLResponsePayload(BaseModel):
    """Schema for adding an HITLResponse for a specific Task Instance."""

    content: str


class HITLResponse(BaseModel):
    """Schema for added HITLResponse."""

    ti_id: str
    content: str
    created_at: datetime


class HITLInputRequest(BaseModel):
    """Schema for details from Human-in-the-loop task."""

    ti_id: str
    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    map_index: int = -1

    options: list[str]
    subject: str
    body: str | None = None
    default: str | None = None
    params: MutableMapping | None = None

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: MutableMapping | None) -> dict | None:
        """Convert params attribute to dict representation."""
        if params is None:
            return None
        return {k: v.dump() for k, v in params.items()}

    @classmethod
    def from_task_instance(cls, task_instance: TI, task: HITLOperator) -> HITLInputRequest:
        return HITLInputRequest(
            ti_id=task_instance.id,
            task_id=task_instance.task_id,
            dag_id=task_instance.dag_id,
            run_id=task_instance.run_id,
            try_number=task_instance.try_number,
            map_index=task_instance.map_index,
            options=task.options,
            subject=task.subject,
            body=task.body,
            default=task.default,
            params=task.params,
        )


class HITLInputRequestCollection(BaseModel):
    """Schema for details from Human-in-the-loop tasks."""

    input_requests: list[HITLInputRequest]
    total_entries: int
