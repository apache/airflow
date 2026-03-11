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

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from airflow.executors.workloads.task import TaskInstanceDTO


class NextTriggersBody(BaseModel):
    """Request body sent by TriggerRunnerSupervisor each loop iteration."""

    capacity: int
    queues: set[str] | None = None


class TriggerDetail(BaseModel):
    """
    Serialized details of a single trigger.

    Uses TaskInstanceDTO (which has pool_slots/queue/priority_weight)
    so the supervisor can pass it directly into RunTrigger.ti.
    """

    id: int
    classpath: str
    encrypted_kwargs: str
    task_instance: TaskInstanceDTO | None = None
    log_path: str | None = None
    timeout_after: datetime | None = None


class NextTriggersResponse(BaseModel):
    """Response from POST /triggerer/{triggerer_id}/next-triggers."""

    triggers: list[TriggerDetail]
    trigger_ids_with_non_task_associations: list[int]


class TriggerEventBody(BaseModel):
    """
    Request body for POST /triggers/{trigger_id}/event.

    ``event`` is a pre-serialized TriggerEvent dict produced by the
    supervisor via ``event.model_dump(mode="json")``.
    We accept a plain dict here to avoid Pydantic discriminated-union
    issues with TriggerEvent (which has no ``type`` literal field).
    The route handler reconstructs the typed object using TypeAdapter.
    """

    event: dict[str, Any]


class TriggerFailureBody(BaseModel):
    """Request body for POST /triggers/{trigger_id}/failure."""

    error: list[str] | None = None
